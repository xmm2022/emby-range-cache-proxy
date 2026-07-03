from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from aiohttp import ClientError, ClientSession, ClientTimeout, web

from .auth import AuthUnavailable, AuthorizationError, EmbyAuthClient
from .cache import CacheReadError, HeadTailCache, adaptive_head_tail, cache_key
from .config import Config
from .middle_cache import MiddleRangeCache
from .models import ByteRange, MediaSource, RequestContext, SourceMetadata
from .origin import OriginClient, OriginError
from .prefetch import PrefetchWorker, enqueue_prefetch_for_session
from .prewarm import PrewarmWorker
from .ranges import content_range_header, plan_playback_range
from .requests import parse_original_request
from .session import SessionRecorder
from .session_observer import EmbySessionObserver
from .sources import resolve_media_source
from .state import SessionStateStore

LOGGER = logging.getLogger(__name__)

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def create_app(config: Config) -> web.Application:
    app = web.Application()
    app["config"] = config
    app["cache"] = HeadTailCache(config.cache_dir, max_bytes=config.cache.max_bytes)
    app["cache_build_locks"] = {}
    app["cache_build_locks_guard"] = asyncio.Lock()
    if config.session.enabled or config.middle_cache.enabled or config.prefetch.enabled:
        state_path = (
            Path(config.session.state_db)
            if config.session.state_db
            else Path(config.cache_dir) / "state" / "phase2.sqlite3"
        )
        phase2_store = SessionStateStore(state_path)
        app["phase2_store"] = phase2_store
        app["middle_cache"] = MiddleRangeCache(
            config.cache_dir,
            phase2_store,
            max_bytes=config.middle_cache.max_bytes,
            ttl_seconds=config.middle_cache.ttl_seconds,
        )
        if config.session.enabled:
            app["session_recorder"] = SessionRecorder(phase2_store)
            app.cleanup_ctx.append(session_recorder_lifecycle)
            app.cleanup_ctx.append(session_planner_lifecycle)
        if config.prefetch.enabled and config.middle_cache.enabled:
            app.cleanup_ctx.append(prefetch_worker_lifecycle)
    if config.prewarm.enabled and config.prewarm_api_key:
        app.cleanup_ctx.append(prewarm_lifecycle)
    app.router.add_get("/healthz", healthz)
    app.router.add_route("*", "/{tail:.*}", proxy_handler)
    return app


async def session_recorder_lifecycle(app: web.Application) -> AsyncIterator[None]:
    recorder: SessionRecorder = app["session_recorder"]
    recorder.start()
    try:
        yield
    finally:
        await recorder.stop()


async def session_planner_lifecycle(app: web.Application) -> AsyncIterator[None]:
    config: Config = app["config"]
    store: SessionStateStore = app["phase2_store"]
    observer = EmbySessionObserver(config, store)
    task = asyncio.create_task(_session_planner_loop(config, store, observer))
    app["session_planner_task"] = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _session_planner_loop(
    config: Config,
    store: SessionStateStore,
    observer: EmbySessionObserver,
) -> None:
    while True:
        try:
            now = time.time()
            if config.session.observer_enabled:
                await observer.run_once(now=now)

            await asyncio.to_thread(
                store.mark_idle_sessions,
                now=now,
                idle_seconds=config.session.idle_seconds,
            )
            await asyncio.to_thread(
                store.expire_old_sessions,
                now=now,
                expire_seconds=config.session.expire_seconds,
            )

            if config.prefetch.enabled and config.middle_cache.enabled:
                candidates = await asyncio.to_thread(store.prefetch_candidate_sessions)
                candidates_by_hash = {
                    session.session_hash: session for session in candidates
                }
                for session in candidates_by_hash.values():
                    priority = 20 if session.status == "stopped" else 10
                    await asyncio.to_thread(
                        enqueue_prefetch_for_session,
                        store,
                        session,
                        prefetch=config.prefetch,
                        middle_cache=config.middle_cache,
                        now=now,
                        priority=priority,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            LOGGER.warning("session planner scan failed: %s", type(error).__name__)

        await asyncio.sleep(config.session.observer_interval_seconds)


async def prefetch_worker_lifecycle(app: web.Application) -> AsyncIterator[None]:
    config: Config = app["config"]
    worker = PrefetchWorker(config, app["phase2_store"], app["middle_cache"])
    task = asyncio.create_task(_prefetch_worker_loop(config, worker))
    app["prefetch_task"] = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _prefetch_worker_loop(config: Config, worker: PrefetchWorker) -> None:
    while True:
        try:
            await worker.run_once(now=time.time())
        except Exception as error:
            LOGGER.warning("prefetch worker failed: error_type=%s", type(error).__name__)
        queue_depth = await asyncio.to_thread(worker.store.queue_depth)
        await asyncio.sleep(config.prefetch.error_backoff_seconds if queue_depth == 0 else 1)


async def prewarm_lifecycle(app: web.Application) -> AsyncIterator[None]:
    config: Config = app["config"]
    task = asyncio.create_task(_prewarm_loop(config))
    app["prewarm_task"] = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _prewarm_loop(config: Config) -> None:
    worker = PrewarmWorker(config)
    while True:
        try:
            await worker.run_once()
        except Exception as error:
            LOGGER.warning("prewarm scan failed: %s", type(error).__name__)
        await asyncio.sleep(config.prewarm.interval_seconds)


async def healthz(request: web.Request) -> web.Response:
    return web.Response(text="ok\n")


async def proxy_handler(request: web.Request) -> web.StreamResponse:
    config: Config = request.app["config"]
    cache: HeadTailCache = request.app["cache"]
    if config.prewarm_api_key and _request_contains_internal_key(request, config.prewarm_api_key):
        _log_decision("deny", "internal_key_used_for_playback", request)
        raise web.HTTPForbidden(text="forbidden\n") from None

    ctx = parse_original_request(request.method, request.raw_path, request.headers)
    if ctx is None:
        _log_decision("fallback", "not_eligible", request)
        return await stream_fallback(request, config)
    if not _pre_authorization_rollout_scope(config, item_id=ctx.item_id, media_source_id=ctx.media_source_id):
        _log_decision("fallback", "not_eligible", request, ctx=ctx)
        return await stream_fallback(request, config)

    try:
        async with EmbyAuthClient(config.emby_base_url) as auth:
            source = await auth.authorize(ctx)
    except AuthorizationError:
        _log_decision("deny", "authorization_failed", request, ctx=ctx)
        raise web.HTTPForbidden(text="forbidden\n") from None
    except (AuthUnavailable, ClientError, TimeoutError, OSError) as error:
        _log_decision("fallback", "auth_unavailable", request, ctx=ctx, error=error, level=logging.WARNING)
        return await stream_fallback(request, config)

    source = resolve_media_source(
        source,
        config.path_mappings,
        url_prefix_allowlist=config.rollout.path_prefix_allowlist,
    )
    if not _is_http_source(source):
        _log_decision("fallback", "non_http_source", request, ctx=ctx)
        return await stream_fallback(request, config)
    if not config.rollout.in_scope(item_id=ctx.item_id, media_source_id=ctx.media_source_id, path=source.path):
        _log_decision("fallback", "path_rollout_miss", request, ctx=ctx)
        return await stream_fallback(request, config)

    try:
        return await serve_authorized_range(request, config, cache, source, ctx)
    except (OriginError, ValueError, ClientError, TimeoutError, OSError) as error:
        _log_decision("fallback", "proxy_error", request, ctx=ctx, error=error, level=logging.WARNING)
        return await stream_fallback(request, config)


async def serve_authorized_range(
    request: web.Request,
    config: Config,
    cache: HeadTailCache,
    source: MediaSource,
    ctx: RequestContext,
) -> web.StreamResponse:
    started_at = time.monotonic()
    async with OriginClient(chunk_bytes=config.cache.chunk_bytes) as origin:
        metadata = await origin.head(source.path)
        head_size, tail_size = adaptive_head_tail(metadata.size)
        byte_range = plan_playback_range(
            request.headers.get("Range"),
            size=metadata.size,
            head_bytes=head_size,
            tail_bytes=tail_size,
            default_open_range_bytes=config.cache.default_open_range_bytes,
            open_head_response_bytes=config.cache.open_head_response_bytes,
        )
        key = cache_key(source, metadata)
        cache_block = _cache_block_for_request(byte_range, metadata, head_size=head_size, tail_size=tail_size)
        status = 206 if request.headers.get("Range") else 200
        headers = _range_response_headers(byte_range, metadata, include_content_range=status == 206)

        if request.method == "HEAD":
            _log_proxy_result("head", request, ctx=ctx, byte_range=byte_range, metadata=metadata, started_at=started_at)
            return web.Response(status=status, headers=headers)

        def record_session() -> None:
            _record_session_progress(request, ctx, key, metadata, byte_range)

        session_recorded = False

        def record_completed_session() -> None:
            nonlocal session_recorded
            if session_recorded:
                return
            session_recorded = True
            record_session()

        middle_cache: MiddleRangeCache | None = request.app.get("middle_cache")
        if config.middle_cache.enabled and middle_cache is not None:
            try:
                cached_chunks = middle_cache.iter_block(
                    key,
                    byte_range,
                    chunk_bytes=config.cache.chunk_bytes,
                    now=time.time(),
                )
            except Exception as error:
                LOGGER.warning("middle cache read failed: %s", type(error).__name__)
                cached_chunks = None
            if cached_chunks is not None:
                return await _serve_cached_response(
                    request,
                    status=status,
                    headers=headers,
                    cached_chunks=cached_chunks,
                    ctx=ctx,
                    byte_range=byte_range,
                    metadata=metadata,
                    started_at=started_at,
                    block_name="middle",
                    block_range=byte_range,
                    record_session=record_session,
                )

        if cache_block is not None:
            block_name, block_range = cache_block
            cached_chunks = cache.iter_block(key, block_name, byte_range, chunk_bytes=config.cache.chunk_bytes)
            if cached_chunks is not None:
                return await _serve_cached_response(
                    request,
                    status=status,
                    headers=headers,
                    cached_chunks=cached_chunks,
                    ctx=ctx,
                    byte_range=byte_range,
                    metadata=metadata,
                    started_at=started_at,
                    block_name=block_name,
                    block_range=block_range,
                    record_session=record_session,
                )
            build_lock = await _cache_build_lock(request.app, key, block_name)
            if build_lock.locked():
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(_wait_for_cache_build(build_lock), config.cache.build_wait_seconds)
                cached_chunks = cache.iter_block(key, block_name, byte_range, chunk_bytes=config.cache.chunk_bytes)
                if cached_chunks is not None:
                    return await _serve_cached_response(
                        request,
                        status=status,
                        headers=headers,
                        cached_chunks=cached_chunks,
                        ctx=ctx,
                        byte_range=byte_range,
                        metadata=metadata,
                        started_at=started_at,
                        block_name=block_name,
                        block_range=block_range,
                        record_session=record_session,
                    )
                if build_lock.locked():
                    block_name = None
                    block_range = byte_range
                    build_lock = None
                else:
                    await build_lock.acquire()
                    cached_chunks = cache.iter_block(key, block_name, byte_range, chunk_bytes=config.cache.chunk_bytes)
                    if cached_chunks is not None:
                        build_lock.release()
                        return await _serve_cached_response(
                            request,
                            status=status,
                            headers=headers,
                            cached_chunks=cached_chunks,
                            ctx=ctx,
                            byte_range=byte_range,
                            metadata=metadata,
                            started_at=started_at,
                            block_name=block_name,
                            block_range=block_range,
                            record_session=record_session,
                        )
            else:
                await build_lock.acquire()
                cached_chunks = cache.iter_block(key, block_name, byte_range, chunk_bytes=config.cache.chunk_bytes)
                if cached_chunks is not None:
                    build_lock.release()
                    return await _serve_cached_response(
                        request,
                        status=status,
                        headers=headers,
                        cached_chunks=cached_chunks,
                        ctx=ctx,
                        byte_range=byte_range,
                        metadata=metadata,
                        started_at=started_at,
                        block_name=block_name,
                        block_range=block_range,
                        record_session=record_session,
                    )
        else:
            block_name = None
            block_range = byte_range
            build_lock = None

        response = web.StreamResponse(status=status, headers=headers)
        try:
            async with origin.open_range(source.path, block_range, size=metadata.size) as upstream:
                await response.prepare(request)
                prepare_ms = _elapsed_ms_since(started_at)
                writer = cache.stage_block(key, block_name, block_range) if block_name is not None else None
                upstream_offset = block_range.start
                response_bytes_written = 0
                response_done = False
                origin_read_bytes = 0
                first_body_ms: int | None = None
                try:
                    async for chunk in upstream.content.iter_chunked(config.cache.chunk_bytes):
                        if chunk:
                            chunk_start = upstream_offset
                            chunk_end = upstream_offset + len(chunk) - 1
                            upstream_offset += len(chunk)
                            origin_read_bytes += len(chunk)
                            if writer is not None:
                                writer.write(chunk)
                            if not response_done:
                                overlap_start = max(chunk_start, byte_range.start)
                                overlap_end = min(chunk_end, byte_range.end)
                                if overlap_start <= overlap_end:
                                    data = chunk[overlap_start - chunk_start : overlap_end - chunk_start + 1]
                                    await response.write(data)
                                    if first_body_ms is None:
                                        first_body_ms = _elapsed_ms_since(started_at)
                                    response_bytes_written += len(data)
                                    if response_bytes_written == byte_range.length:
                                        await _write_eof_safely(response)
                                        response_done = True
                                        record_completed_session()
                except (OriginError, ClientError, TimeoutError, OSError):
                    if writer is not None:
                        writer.abort()
                    if not response_done:
                        response.force_close()
                        await _write_eof_safely(response)
                    return response

                if writer is not None:
                    try:
                        writer.commit()
                        cache.evict_if_needed()
                        _log_proxy_result(
                            "cache_build",
                            request,
                            ctx=ctx,
                            byte_range=byte_range,
                            metadata=metadata,
                            started_at=started_at,
                            block_name=block_name,
                            block_range=block_range,
                            served_bytes=response_bytes_written,
                            cache_read_bytes=0,
                            origin_read_bytes=origin_read_bytes,
                            prepare_ms=prepare_ms,
                            first_body_ms=first_body_ms,
                        )
                    except (ValueError, OSError):
                        writer.abort()
                        if not response_done:
                            response.force_close()
                else:
                    _log_proxy_result(
                        "origin_stream",
                        request,
                        ctx=ctx,
                        byte_range=byte_range,
                        metadata=metadata,
                        started_at=started_at,
                        served_bytes=response_bytes_written,
                        cache_read_bytes=0,
                        origin_read_bytes=origin_read_bytes,
                        prepare_ms=prepare_ms,
                        first_body_ms=first_body_ms,
                    )
        finally:
            if build_lock is not None and build_lock.locked():
                build_lock.release()

        if not response_done:
            await _write_eof_safely(response)
        return response


async def _serve_cached_response(
    request: web.Request,
    *,
    status: int,
    headers: dict[str, str],
    cached_chunks,
    ctx: RequestContext,
    byte_range: ByteRange,
    metadata: SourceMetadata,
    started_at: float,
    block_name: str,
    block_range: ByteRange,
    record_session: Callable[[], None] | None = None,
) -> web.StreamResponse:
    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)
    prepare_ms = _elapsed_ms_since(started_at)
    served_bytes, cache_read_bytes, cache_error, first_body_ms = await _write_cached_chunks(
        response, cached_chunks, started_at=started_at
    )
    if cache_error:
        response.force_close()
    await _write_eof_safely(response)
    if not cache_error and served_bytes == byte_range.length and record_session is not None:
        record_session()
    _log_proxy_result(
        "cache_error" if cache_error else "cache_hit",
        request,
        ctx=ctx,
        byte_range=byte_range,
        metadata=metadata,
        started_at=started_at,
        block_name=block_name,
        block_range=block_range,
        served_bytes=served_bytes,
        cache_read_bytes=cache_read_bytes,
        origin_read_bytes=0,
        prepare_ms=prepare_ms,
        first_body_ms=first_body_ms,
    )
    return response


def _record_session_progress(
    request: web.Request,
    ctx: RequestContext,
    key: str,
    metadata: SourceMetadata,
    byte_range: ByteRange,
) -> None:
    if request.method != "GET":
        return
    recorder: SessionRecorder | None = request.app.get("session_recorder")
    if recorder is None:
        return
    recorder.record_nowait(ctx, key, metadata, byte_range, observed_at=time.time())


async def _write_cached_chunks(
    response: web.StreamResponse, cached_chunks, *, started_at: float | None = None
) -> tuple[int, int, bool, int | None]:
    sent = 0
    read = 0
    first_body_ms: int | None = None
    try:
        for chunk in cached_chunks:
            read += len(chunk)
            await response.write(chunk)
            if first_body_ms is None and started_at is not None:
                first_body_ms = _elapsed_ms_since(started_at)
            sent += len(chunk)
    except CacheReadError:
        return sent, read, True, first_body_ms
    except (ConnectionError, RuntimeError, OSError):
        close = getattr(cached_chunks, "close", None)
        if close is not None:
            close()
        return sent, read, False, first_body_ms
    return sent, read, False, first_body_ms


async def stream_fallback(request: web.Request, config: Config) -> web.StreamResponse:
    url = f"{config.fallback_base_url.rstrip('/')}{request.raw_path}"
    headers = _forward_request_headers(request)
    body = None if request.method in {"GET", "HEAD"} else _request_body(request)
    timeout = ClientTimeout(total=None, sock_connect=30.0, sock_read=None)
    async with ClientSession(timeout=timeout) as session:
        async with session.request(
            request.method,
            url,
            headers=headers,
            data=body,
            allow_redirects=False,
        ) as upstream:
            response = web.StreamResponse(
                status=upstream.status,
                reason=upstream.reason,
                headers=_forward_response_headers(upstream.headers),
            )
            await response.prepare(request)
            if request.method != "HEAD":
                async for chunk in upstream.content.iter_chunked(config.cache.chunk_bytes):
                    if chunk:
                        await response.write(chunk)
            await response.write_eof()
            return response


async def _write_eof_safely(response: web.StreamResponse) -> None:
    with suppress(ConnectionError, RuntimeError, OSError):
        await response.write_eof()


def _is_http_source(source: MediaSource) -> bool:
    scheme = urlsplit(source.path).scheme.lower()
    return scheme in {"http", "https"}


async def _cache_build_lock(app: web.Application, key: str, block_name: str) -> asyncio.Lock:
    locks: dict[tuple[str, str], asyncio.Lock] = app["cache_build_locks"]
    guard: asyncio.Lock = app["cache_build_locks_guard"]
    async with guard:
        return locks.setdefault((key, block_name), asyncio.Lock())


async def _wait_for_cache_build(lock: asyncio.Lock) -> None:
    async with lock:
        pass


def _request_contains_internal_key(request: web.Request, internal_key: str) -> bool:
    parsed = urlsplit(request.raw_path)
    for name, value in parse_qsl(parsed.query, keep_blank_values=True):
        if name.lower() in {"api_key", "token", "x-emby-token"} and value == internal_key:
            return True
    return any(
        name.lower() == "x-emby-token" and value == internal_key
        for name, value in request.headers.items()
    )


def _log_decision(
    action: str,
    reason: str,
    request: web.Request,
    *,
    ctx: RequestContext | None = None,
    error: BaseException | None = None,
    level: int = logging.INFO,
) -> None:
    if ctx is None:
        if error is None:
            LOGGER.log(level, "%s reason=%s path=%s", action, reason, request.path)
        else:
            LOGGER.log(
                level,
                "%s reason=%s path=%s error_type=%s",
                action,
                reason,
                request.path,
                type(error).__name__,
            )
        return

    if error is None:
        LOGGER.log(
            level,
            "%s reason=%s item_id=%s media_source_id=%s path=%s",
            action,
            reason,
            ctx.item_id,
            ctx.media_source_id,
            request.path,
        )
        return

    LOGGER.log(
        level,
        "%s reason=%s item_id=%s media_source_id=%s path=%s error_type=%s",
        action,
        reason,
        ctx.item_id,
        ctx.media_source_id,
        request.path,
        type(error).__name__,
    )


def _log_proxy_result(
    result: str,
    request: web.Request,
    *,
    ctx: RequestContext,
    byte_range: ByteRange,
    metadata: SourceMetadata,
    started_at: float,
    block_name: str | None = None,
    block_range: ByteRange | None = None,
    served_bytes: int = 0,
    cache_read_bytes: int = 0,
    origin_read_bytes: int = 0,
    prepare_ms: int | None = None,
    first_body_ms: int | None = None,
) -> None:
    request_range = request.headers.get("Range", "none")
    block = block_name or "none"
    block_value = "none" if block_range is None else f"{block_range.start}-{block_range.end}"
    elapsed_ms = _elapsed_ms_since(started_at)
    LOGGER.info(
        "proxy result=%s item_id=%s media_source_id=%s method=%s path=%s "
        "request_range=%s planned_range=%s-%s total_size=%s block=%s block_range=%s "
        "served_bytes=%s cache_read_bytes=%s origin_read_bytes=%s "
        "prepare_ms=%s first_body_ms=%s elapsed_ms=%s",
        result,
        ctx.item_id,
        ctx.media_source_id,
        request.method,
        request.path,
        request_range,
        byte_range.start,
        byte_range.end,
        metadata.size,
        block,
        block_value,
        served_bytes,
        cache_read_bytes,
        origin_read_bytes,
        _format_optional_ms(prepare_ms),
        _format_optional_ms(first_body_ms),
        elapsed_ms,
    )


def _elapsed_ms_since(started_at: float) -> int:
    return int((time.monotonic() - started_at) * 1000)


def _format_optional_ms(value: int | None) -> str:
    return "none" if value is None else str(value)


def _pre_authorization_rollout_scope(config: Config, *, item_id: str, media_source_id: str) -> bool:
    return (
        config.rollout.enabled
        and config.rollout.item_allowed(item_id)
        and config.rollout.media_source_allowed(media_source_id)
    )


def _cache_block_for_request(
    byte_range: ByteRange,
    metadata: SourceMetadata,
    *,
    head_size: int | None = None,
    tail_size: int | None = None,
) -> tuple[str, ByteRange] | None:
    if head_size is None or tail_size is None:
        head_size, tail_size = adaptive_head_tail(metadata.size)
    head_range = ByteRange(0, min(head_size, metadata.size) - 1)
    tail_range = ByteRange(max(0, metadata.size - tail_size), metadata.size - 1)
    if _range_contains(head_range, byte_range):
        return "head", head_range
    if _range_contains(tail_range, byte_range):
        return "tail", tail_range
    return None


def _range_contains(container: ByteRange, requested: ByteRange) -> bool:
    return requested.start >= container.start and requested.end <= container.end


def _range_response_headers(
    byte_range: ByteRange,
    metadata: SourceMetadata,
    *,
    include_content_range: bool,
) -> dict[str, str]:
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(byte_range.length),
    }
    if include_content_range:
        headers["Content-Range"] = content_range_header(byte_range, size=metadata.size)
    if metadata.content_type:
        headers["Content-Type"] = metadata.content_type
    if metadata.etag:
        headers["ETag"] = metadata.etag
    if metadata.last_modified:
        headers["Last-Modified"] = metadata.last_modified
    return headers


def _forward_request_headers(request: web.Request) -> dict[str, str]:
    return {
        name: value
        for name, value in request.headers.items()
        if name.lower() not in HOP_BY_HOP_HEADERS and name.lower() != "host"
    }


def _forward_response_headers(headers) -> dict[str, str]:
    return {name: value for name, value in headers.items() if name.lower() not in HOP_BY_HOP_HEADERS}


async def _request_body(request: web.Request) -> AsyncIterator[bytes]:
    async for chunk in request.content.iter_chunked(1024 * 1024):
        if chunk:
            yield chunk
