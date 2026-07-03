import asyncio
import logging
import time

from aiohttp import ClientTimeout
from aiohttp import web

import emby_range_cache_proxy.app as app_module
from emby_range_cache_proxy import prefetch as prefetch_module
from emby_range_cache_proxy.auth import AuthUnavailable, AuthorizationError
from emby_range_cache_proxy.app import create_app
from emby_range_cache_proxy.cache import CacheReadError
from emby_range_cache_proxy.config import (
    CacheConfig,
    Config,
    MiddleCacheConfig,
    PathMapping,
    PrefetchConfig,
    PrewarmConfig,
    RolloutConfig,
    SessionConfig,
)
from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.models import ByteRange, RequestContext, SourceMetadata
from emby_range_cache_proxy.state import (
    PlaybackSessionUpdate,
    SessionStateStore,
    hash_identifier,
)


FULL_HEAD_BODY = b"0123456789" + b"H" * 90


async def test_healthz(aiohttp_client, tmp_path):
    app = create_app(Config(emby_base_url="http://emby", fallback_base_url="http://emby", cache_dir=str(tmp_path)))
    client = await aiohttp_client(app)

    response = await client.get("/healthz")

    assert response.status == 200
    assert await response.text() == "ok\n"


async def test_prewarm_lifecycle_starts_and_cancels_background_task(aiohttp_client, monkeypatch, tmp_path):
    started = asyncio.Event()
    calls = []

    class FakePrewarmWorker:
        def __init__(self, config):
            calls.append(("init", config))

        async def run_once(self):
            calls.append(("run_once", None))
            started.set()
            await asyncio.sleep(3600)

    monkeypatch.setattr(app_module, "PrewarmWorker", FakePrewarmWorker)
    app = create_app(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path),
            prewarm_api_key="internal",
            prewarm=PrewarmConfig(enabled=True, interval_seconds=60),
        )
    )
    client = await aiohttp_client(app)

    await asyncio.wait_for(started.wait(), timeout=1)
    task = app["prewarm_task"]

    assert calls == [("init", app["config"]), ("run_once", None)]
    assert not task.done()

    await client.close()

    assert task.cancelled()


async def test_prewarm_lifecycle_does_not_start_without_internal_key(aiohttp_client, monkeypatch, tmp_path):
    class FakePrewarmWorker:
        def __init__(self, config):
            raise AssertionError("prewarm worker must not start without internal key")

    monkeypatch.setattr(app_module, "PrewarmWorker", FakePrewarmWorker)
    app = create_app(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path),
            prewarm_api_key=None,
            prewarm=PrewarmConfig(enabled=True, interval_seconds=60),
        )
    )
    client = await aiohttp_client(app)

    assert "prewarm_task" not in app

    await client.close()


async def test_session_planner_lifecycle_marks_idle_and_enqueues_prefetch(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(prefetch_module, "adaptive_head_tail", lambda size: (128, 128))
    app = create_app(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path),
            session=SessionConfig(
                enabled=True,
                idle_seconds=1,
                observer_interval_seconds=1,
            ),
            middle_cache=MiddleCacheConfig(enabled=True, segment_bytes=64),
            prefetch=PrefetchConfig(
                enabled=True,
                window_bytes=128,
                resume_overlap_bytes=0,
                max_session_bytes=256,
                max_queue_depth=10,
            ),
        )
    )
    store: SessionStateStore = app["phase2_store"]
    store.record_playback(
        PlaybackSessionUpdate(
            session_hash="s" * 64,
            device_hash=None,
            item_id="1",
            media_source_id="ms1",
            cache_key="a" * 64,
            origin_signature="o" * 64,
            media_size=1000,
            byte_range=ByteRange(300, 350),
            observed_at=time.time() - 2.0,
        )
    )

    client = await aiohttp_client(app)
    try:
        deadline = asyncio.get_running_loop().time() + 2.0
        while store.queue_depth() != 2:
            if asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(0.05)
        session = store.get_session("s" * 64)

        assert "session_planner_task" in app
        assert store.queue_depth() == 2
        assert session is not None
        assert session.status == "idle"
        assert session.queued_until == 447
    finally:
        await client.close()


async def test_out_of_scope_falls_back_to_emby(aiohttp_client, tmp_path):
    async def fallback(request):
        return web.Response(status=206, body=b"emby", headers={"Content-Range": "bytes 0-3/4"})

    fallback_app = web.Application()
    fallback_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    fallback_server = await aiohttp_client(fallback_app)

    app = create_app(
        Config(
            emby_base_url=str(fallback_server.make_url("")),
            fallback_base_url=str(fallback_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=False),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})

    assert response.status == 206
    assert await response.read() == b"emby"


async def test_authorized_head_range_is_served_and_cached(aiohttp_client, tmp_path):
    origin_get_calls = 0

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        if origin_get_calls > 1:
            return web.Response(status=500, body=b"origin should not be hit after cache fill")
        assert request.headers["Range"] == "bytes=0-99"
        return web.Response(status=206, body=FULL_HEAD_BODY, headers={"Content-Range": "bytes 0-99/100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
    assert origin_get_calls == 1


async def test_authorized_request_records_session_state(aiohttp_client, monkeypatch, tmp_path):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=b"0123456789abcdef", headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            session=SessionConfig(enabled=True),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t&PlaySessionId=play1&DeviceId=dev1",
        headers={"Range": "bytes=0-9"},
    )

    assert response.status == 206
    assert await response.read() == b"0123456789"

    await app["session_recorder"].stop()
    store: SessionStateStore = app["phase2_store"]
    session = store.get_session(hash_identifier("play1"))
    assert session is not None
    assert session.item_id == "1"
    assert session.max_observed_offset == 9


async def test_authorized_head_request_does_not_record_session_state(
    aiohttp_client, tmp_path
):
    origin_get_calls = 0

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        return web.Response(status=500, body=b"origin GET must not be called for HEAD")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            session=SessionConfig(enabled=True),
        )
    )
    client = await aiohttp_client(app)

    response = await client.head(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t&PlaySessionId=head-play",
        headers={"Range": "bytes=0-9"},
    )

    assert response.status == 206
    assert await response.read() == b""
    assert origin_get_calls == 0

    await app["session_recorder"].stop()
    store: SessionStateStore = app["phase2_store"]
    assert store.get_session(hash_identifier("head-play")) is None


async def test_origin_fallback_does_not_record_session_state(aiohttp_client, tmp_path):
    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-99"
        return web.Response(status=200, body=b"origin ignored range")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    async def fallback(request):
        return web.Response(status=206, body=b"emby", headers={"Content-Range": "bytes 0-3/4"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            session=SessionConfig(enabled=True),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t&PlaySessionId=fallback-play",
        headers={"Range": "bytes=0-3"},
    )

    assert response.status == 206
    assert await response.read() == b"emby"

    await app["session_recorder"].stop()
    store: SessionStateStore = app["phase2_store"]
    assert store.get_session(hash_identifier("fallback-play")) is None


async def test_cached_response_does_not_record_session_when_cache_read_fails(monkeypatch):
    class FakeRequest:
        method = "GET"
        path = "/emby/videos/1/original.mkv"
        headers = {"Range": "bytes=0-3"}

    class FakeResponse:
        def __init__(self, *, status, headers):
            self.status = status
            self.headers = headers
            self.chunks = []
            self.force_closed = False
            self.eof_written = False

        async def prepare(self, request):
            self.request = request

        async def write(self, chunk):
            self.chunks.append(chunk)

        def force_close(self):
            self.force_closed = True

        async def write_eof(self):
            self.eof_written = True

    def stream_response_factory(*, status, headers):
        return FakeResponse(status=status, headers=headers)

    def chunks():
        yield b"01"
        raise CacheReadError("cache ended early")

    recorded = []
    monkeypatch.setattr(app_module.web, "StreamResponse", stream_response_factory)

    response = await app_module._serve_cached_response(
        FakeRequest(),
        status=206,
        headers={"Content-Length": "4", "Content-Range": "bytes 0-3/10"},
        cached_chunks=chunks(),
        ctx=RequestContext(
            method="GET",
            raw_path="/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
            item_id="1",
            media_source_id="ms1",
            token="t",
            extension="mkv",
        ),
        byte_range=ByteRange(0, 3),
        metadata=SourceMetadata(url="http://origin/movie.mkv", size=10),
        started_at=0.0,
        block_name="head",
        block_range=ByteRange(0, 9),
        record_session=lambda: recorded.append("recorded"),
    )

    assert response.force_closed
    assert recorded == []


async def test_authorized_middle_cache_hit_does_not_touch_origin_get(aiohttp_client, monkeypatch, tmp_path):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    monkeypatch.setattr(app_module.time, "time", lambda: 1.0)
    origin_gets = 0

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_gets
        origin_gets += 1
        return web.Response(status=500, body=b"origin GET must not be called")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)
    origin_url = str(origin_server.make_url("/movie.mkv"))

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path / "cache"),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            middle_cache=MiddleCacheConfig(enabled=True, ttl_seconds=60),
        )
    )
    store: SessionStateStore = app["phase2_store"]
    middle: MiddleRangeCache = app["middle_cache"]
    source = app_module.MediaSource("1", "ms1", origin_url, "Http", 100)
    metadata = SourceMetadata(url=origin_url, size=100)
    key = app_module.cache_key(source, metadata)
    middle.store_block(key, ByteRange(32, 47), b"middle-cache-hit", now=1.0)
    assert store.find_middle_block(key, ByteRange(32, 47)) is not None

    client = await aiohttp_client(app)
    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
        headers={"Range": "bytes=32-47"},
    )

    assert response.status == 206
    assert await response.read() == b"middle-cache-hit"
    assert origin_gets == 0


async def test_middle_cache_error_falls_back_to_head_tail_without_leaking_build_lock(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_gets = 0
    body = b"0123456789abcdef" + b"T" * 84

    class FailingMiddleCache:
        def __init__(self):
            self.calls = 0

        def iter_block(self, key, requested, *, chunk_bytes, now):
            self.calls += 1
            raise OSError("middle cache unavailable")

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_gets
        origin_gets += 1
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": str(len(body))})

    async def fallback(request):
        return web.Response(status=500, body=b"fallback must not be used")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)
    origin_url = str(origin_server.make_url("/movie.mkv"))

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path / "cache"),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            middle_cache=MiddleCacheConfig(enabled=True, ttl_seconds=60),
        )
    )
    fake_middle = FailingMiddleCache()
    app["middle_cache"] = fake_middle
    client = await aiohttp_client(app)

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
        headers={"Range": "bytes=0-3"},
    )

    assert response.status == 206
    assert await response.read() == b"0123"

    source = app_module.MediaSource("1", "ms1", origin_url, "Http", 100)
    metadata = SourceMetadata(url=origin_url, size=100)
    key = app_module.cache_key(source, metadata)
    assert not app["cache_build_locks"][(key, "head")].locked()

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
        headers={"Range": "bytes=8-11"},
    )

    assert response.status == 206
    assert await response.read() == b"89ab"
    assert origin_gets == 1
    assert fake_middle.calls == 2


async def test_middle_cache_hit_for_head_range_does_not_request_head_tail_build_lock(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    monkeypatch.setattr(app_module.time, "time", lambda: 1.0)
    origin_gets = 0

    async def forbidden_cache_build_lock(app, key, block_name):
        raise AssertionError("middle cache hit must not request head/tail build lock")

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_gets
        origin_gets += 1
        return web.Response(status=500, body=b"origin GET must not be called")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    async def fallback(request):
        return web.Response(status=500, body=b"fallback must not be used")

    monkeypatch.setattr(app_module, "_cache_build_lock", forbidden_cache_build_lock)

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)
    origin_url = str(origin_server.make_url("/movie.mkv"))

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path / "cache"),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            middle_cache=MiddleCacheConfig(enabled=True, ttl_seconds=60),
        )
    )
    middle: MiddleRangeCache = app["middle_cache"]
    source = app_module.MediaSource("1", "ms1", origin_url, "Http", 100)
    metadata = SourceMetadata(url=origin_url, size=100)
    key = app_module.cache_key(source, metadata)
    middle.store_block(key, ByteRange(0, 15), b"0123456789abcdef", now=1.0)

    client = await aiohttp_client(app)
    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
        headers={"Range": "bytes=0-3"},
    )

    assert response.status == 206
    assert await response.read() == b"0123"
    assert origin_gets == 0


async def test_middle_cache_miss_proxies_origin_without_writing_middle_block(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        assert request.headers["Range"] == "bytes=32-47"
        return web.Response(status=206, body=b"origin-middle!!!", headers={"Content-Range": "bytes 32-47/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path / "cache"),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            middle_cache=MiddleCacheConfig(enabled=True, ttl_seconds=60),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
        headers={"Range": "bytes=32-47"},
    )

    assert response.status == 206
    assert await response.read() == b"origin-middle!!!"
    assert list((tmp_path / "cache").glob("*/mid/*.bin")) == []


async def test_head_request_builds_full_adaptive_head_block_for_later_subrange(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_get_calls = 0
    body = b"0123456789abcdef" + b"T" * 84

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": str(len(body)), "Content-Type": "video/x-matroska"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})

    assert response.status == 206
    assert response.headers["Content-Type"] == "video/x-matroska"
    assert await response.read() == b"0123"

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=8-11"})

    assert response.status == 206
    assert await response.read() == b"89ab"
    assert origin_get_calls == 1


async def test_open_ended_head_range_is_limited_and_served_from_head_cache(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_get_calls = 0
    body = b"0123456789abcdef" + b"T" * 84

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": str(len(body)), "Content-Type": "video/x-matroska"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 0-15/100"
    assert response.headers["Content-Length"] == "16"
    assert await response.read() == b"0123456789abcdef"

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=8-"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 8-15/100"
    assert response.headers["Content-Length"] == "8"
    assert await response.read() == b"89abcdef"
    assert origin_get_calls == 1


async def test_open_ended_head_range_can_be_smaller_than_cached_head_block(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_get_calls = 0
    body = b"0123456789abcdef" + b"T" * 84

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": str(len(body)), "Content-Type": "video/x-matroska"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            cache=CacheConfig(open_head_response_bytes=8),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 0-7/100"
    assert response.headers["Content-Length"] == "8"
    assert await response.read() == b"01234567"

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=8-"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 8-15/100"
    assert response.headers["Content-Length"] == "8"
    assert await response.read() == b"89abcdef"
    assert origin_get_calls == 1


async def test_cache_build_and_hit_are_logged_without_token(
    aiohttp_client, caplog, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_get_calls = 0
    body = b"0123456789abcdef" + b"T" * 84

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": str(len(body)), "Content-Type": "video/x-matroska"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)
    caplog.set_level(logging.INFO, logger="emby_range_cache_proxy.app")

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=secret-token",
        headers={"Range": "bytes=0-3"},
    )
    assert response.status == 206
    assert await response.read() == b"0123"

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=secret-token",
        headers={"Range": "bytes=8-11"},
    )
    assert response.status == 206
    assert await response.read() == b"89ab"

    messages = [record.getMessage() for record in caplog.records]
    assert any("proxy result=cache_build" in message and "block=head" in message for message in messages)
    assert any("proxy result=cache_hit" in message and "block=head" in message for message in messages)
    assert any("request_range=bytes=0-3" in message and "planned_range=0-3" in message for message in messages)
    assert any(
        "proxy result=cache_build" in message
        and "served_bytes=4" in message
        and "cache_read_bytes=0" in message
        and "origin_read_bytes=16" in message
        and "prepare_ms=" in message
        and "first_body_ms=" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    assert any(
        "proxy result=cache_hit" in message
        and "served_bytes=4" in message
        and "cache_read_bytes=4" in message
        and "origin_read_bytes=0" in message
        and "prepare_ms=" in message
        and "first_body_ms=" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    assert not any("secret-token" in message for message in messages)
    assert origin_get_calls == 1


async def test_cached_body_is_written_in_configured_chunks():
    class FakeResponse:
        def __init__(self):
            self.chunks = []

        async def write(self, chunk):
            self.chunks.append(chunk)

    response = FakeResponse()

    written, read, cache_error, first_body_ms = await app_module._write_cached_chunks(
        response, [b"0123", b"4567", b"89"]
    )

    assert written == 10
    assert read == 10
    assert not cache_error
    assert first_body_ms is None
    assert response.chunks == [b"0123", b"4567", b"89"]


async def test_cached_body_reports_bytes_written_before_disconnect():
    class FakeResponse:
        def __init__(self):
            self.chunks = []

        async def write(self, chunk):
            if len(self.chunks) == 2:
                raise ConnectionError("client disconnected")
            self.chunks.append(chunk)

    response = FakeResponse()

    written, read, cache_error, first_body_ms = await app_module._write_cached_chunks(
        response, [b"0123", b"4567", b"89"]
    )

    assert written == 8
    assert read == 10
    assert not cache_error
    assert first_body_ms is None
    assert response.chunks == [b"0123", b"4567"]


async def test_cached_response_forces_close_when_cache_read_fails(monkeypatch, caplog):
    class FakeRequest:
        method = "GET"
        path = "/emby/videos/1/original.mkv"
        headers = {"Range": "bytes=0-3"}

    class FakeResponse:
        def __init__(self, *, status, headers):
            self.status = status
            self.headers = headers
            self.chunks = []
            self.force_closed = False
            self.eof_written = False

        async def prepare(self, request):
            self.request = request

        async def write(self, chunk):
            self.chunks.append(chunk)

        def force_close(self):
            self.force_closed = True

        async def write_eof(self):
            self.eof_written = True

    created = {}

    def stream_response_factory(*, status, headers):
        response = FakeResponse(status=status, headers=headers)
        created["response"] = response
        return response

    def chunks():
        yield b"01"
        raise CacheReadError("cache ended early")

    monkeypatch.setattr(app_module.web, "StreamResponse", stream_response_factory)

    with caplog.at_level(logging.INFO, logger="emby_range_cache_proxy.app"):
        response = await app_module._serve_cached_response(
            FakeRequest(),
            status=206,
            headers={"Content-Length": "4", "Content-Range": "bytes 0-3/10"},
            cached_chunks=chunks(),
            ctx=RequestContext(
                method="GET",
                raw_path="/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=redacted",
                item_id="1",
                media_source_id="ms1",
                token="redacted",
                extension="mkv",
            ),
            byte_range=ByteRange(0, 3),
            metadata=SourceMetadata(url="http://origin/movie.mkv", size=10),
            started_at=0.0,
            block_name="head",
            block_range=ByteRange(0, 9),
        )

    assert response is created["response"]
    assert response.chunks == [b"01"]
    assert response.force_closed
    assert response.eof_written
    assert "proxy result=cache_error" in caplog.text
    assert "proxy result=cache_hit" not in caplog.text


async def test_cache_hit_streams_file_without_reading_entire_block(
    aiohttp_client, monkeypatch, tmp_path
):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_get_calls = 0
    body = b"0123456789abcdef" + b"T" * 84

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": str(len(body)), "Content-Type": "video/x-matroska"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
        headers={"Range": "bytes=0-3"},
    )
    assert response.status == 206
    assert await response.read() == b"0123"

    def forbidden_read_block(*args, **kwargs):
        raise AssertionError("cache hits must stream from file instead of read_block")

    monkeypatch.setattr(app["cache"], "read_block", forbidden_read_block)
    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t",
        headers={"Range": "bytes=8-11"},
    )

    assert response.status == 206
    assert await response.read() == b"89ab"
    assert origin_get_calls == 1


async def test_strm_media_source_is_resolved_and_cached(aiohttp_client, monkeypatch, tmp_path):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_get_calls = 0
    strm_root = tmp_path / "strm"
    strm_root.mkdir()
    body = b"0123456789abcdef" + b"T" * 84

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": "/strm/movie.strm",
                        "Protocol": "File",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        assert request.headers["Range"] == "bytes=0-15"
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(
            status=206,
            headers={"Content-Length": "16", "Content-Range": "bytes 0-15/100"},
        )

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)
    (strm_root / "movie.strm").write_text(f"{origin_server.make_url('/movie.mkv')}\n")

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path / "cache"),
            rollout=RolloutConfig(
                enabled=True,
                item_allowlist={"1"},
                path_prefix_allowlist=(str(origin_server.make_url("")),),
            ),
            path_mappings=(PathMapping("/strm/", str(strm_root)),),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})

    assert response.status == 206
    assert await response.read() == b"0123"

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=8-11"})

    assert response.status == 206
    assert await response.read() == b"89ab"
    assert origin_get_calls == 1


async def test_strm_media_source_without_path_prefix_falls_back(aiohttp_client, monkeypatch, tmp_path):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    strm_root = tmp_path / "strm"
    strm_root.mkdir()

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": "/strm/movie.strm",
                        "Protocol": "File",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        return web.Response(status=500, body=b"origin must not be read")

    async def fallback(request):
        return web.Response(status=206, body=b"fallback", headers={"Content-Range": "bytes 0-7/8"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)
    (strm_root / "movie.strm").write_text(f"{origin_server.make_url('/movie.mkv')}\n")

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path / "cache"),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            path_mappings=(PathMapping("/strm/", str(strm_root)),),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-7"})

    assert response.status == 206
    assert await response.read() == b"fallback"


async def test_concurrent_head_misses_share_single_full_block_build(aiohttp_client, monkeypatch, tmp_path):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_get_calls = 0
    release_origin = asyncio.Event()
    body = b"0123456789abcdef" + b"T" * 84

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": len(body),
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        assert request.headers["Range"] == "bytes=0-15"
        await release_origin.wait()
        return web.Response(status=206, body=body[0:16], headers={"Content-Range": "bytes 0-15/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": str(len(body))})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    first = asyncio.create_task(
        client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})
    )
    await asyncio.sleep(0)
    second = asyncio.create_task(
        client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=8-11"})
    )

    while origin_get_calls == 0:
        await asyncio.sleep(0)
    release_origin.set()

    first_response, second_response = await asyncio.gather(first, second)

    assert first_response.status == 206
    assert second_response.status == 206
    assert await first_response.read() == b"0123"
    assert await second_response.read() == b"89ab"
    assert origin_get_calls == 1


async def test_authorized_head_range_returns_headers_without_origin_get(aiohttp_client, tmp_path):
    origin_get_calls = 0
    origin_head_calls = 0

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal origin_get_calls
        origin_get_calls += 1
        return web.Response(status=500, body=b"origin GET must not be called for HEAD")

    async def origin_head(request):
        nonlocal origin_head_calls
        origin_head_calls += 1
        return web.Response(headers={"Content-Length": "100", "ETag": "etag"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.head("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 0-9/100"
    assert response.headers["Content-Length"] == "10"
    assert response.headers["ETag"] == "etag"
    assert await response.read() == b""
    assert origin_head_calls == 1
    assert origin_get_calls == 0


async def test_origin_ignoring_range_falls_back_before_proxy_response(aiohttp_client, tmp_path):
    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def fallback(request):
        return web.Response(status=206, body=b"emby", headers={"Content-Range": "bytes 0-3/4"})

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-99"
        return web.Response(status=200, body=b"0123456789")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 0-3/4"
    assert await response.read() == b"emby"


async def test_cache_evict_error_after_origin_stream_does_not_fallback(aiohttp_client, tmp_path):
    fallback_calls = 0

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def fallback(request):
        nonlocal fallback_calls
        fallback_calls += 1
        return web.Response(status=206, body=b"fallback", headers={"Content-Range": "bytes 0-7/8"})

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-99"
        return web.Response(status=206, body=FULL_HEAD_BODY, headers={"Content-Range": "bytes 0-99/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )

    def fail_evict():
        raise OSError("simulated evict failure")

    app["cache"].evict_if_needed = fail_evict
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
    assert fallback_calls == 0


async def test_cache_evict_error_after_successful_get_still_records_session(
    aiohttp_client, tmp_path
):
    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def fallback(request):
        return web.Response(status=206, body=b"fallback", headers={"Content-Range": "bytes 0-7/8"})

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-99"
        return web.Response(status=206, body=FULL_HEAD_BODY, headers={"Content-Range": "bytes 0-99/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
            session=SessionConfig(enabled=True),
        )
    )

    def fail_evict():
        raise OSError("simulated evict failure")

    app["cache"].evict_if_needed = fail_evict
    client = await aiohttp_client(app)

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t&PlaySessionId=evict-play",
        headers={"Range": "bytes=0-9"},
    )

    assert response.status == 206
    assert await response.read() == b"0123456789"

    await app["session_recorder"].stop()
    store: SessionStateStore = app["phase2_store"]
    session = store.get_session(hash_identifier("evict-play"))
    assert session is not None
    assert session.max_observed_offset == 9


async def test_matching_path_prefix_is_evaluated_after_authorization(aiohttp_client, tmp_path):
    playback_info_calls = 0

    async def playback_info(request):
        nonlocal playback_info_calls
        playback_info_calls += 1
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-99"
        return web.Response(status=206, body=FULL_HEAD_BODY, headers={"Content-Range": "bytes 0-99/100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(
                enabled=True,
                item_allowlist={"1"},
                path_prefix_allowlist=(str(origin_server.make_url("")),),
            ),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
    assert playback_info_calls == 1


async def test_non_matching_path_prefix_falls_back_after_authorization(aiohttp_client, tmp_path):
    playback_info_calls = 0

    async def playback_info(request):
        nonlocal playback_info_calls
        playback_info_calls += 1
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def fallback(request):
        return web.Response(status=206, body=b"fallback", headers={"Content-Range": "bytes 0-7/8"})

    async def origin(request):
        return web.Response(status=500, body=b"origin must not be read")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(
                enabled=True,
                item_allowlist={"1"},
                path_prefix_allowlist=("http://does-not-match/",),
            ),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-7"})

    assert response.status == 206
    assert await response.read() == b"fallback"
    assert playback_info_calls == 1


async def test_authorization_error_returns_403_without_origin_or_fallback(aiohttp_client, tmp_path):
    async def playback_info(request):
        return web.Response(status=403)

    async def fallback(request):
        raise AssertionError("fallback must not be read after explicit authorization failure")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 403
    assert await response.text() == "forbidden\n"


async def test_authorization_unavailable_falls_back_to_emby(aiohttp_client, monkeypatch, tmp_path):
    fallback_calls = 0

    class FakeAuthClient:
        def __init__(self, base_url):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def authorize(self, ctx):
            raise AuthUnavailable("Emby authorization unavailable: timeout")

    async def fallback(request):
        nonlocal fallback_calls
        fallback_calls += 1
        return web.Response(status=206, body=b"fallback", headers={"Content-Range": "bytes 0-7/8"})

    fallback_app = web.Application()
    fallback_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    fallback_server = await aiohttp_client(fallback_app)
    monkeypatch.setattr(app_module, "EmbyAuthClient", FakeAuthClient)

    app = create_app(
        Config(
            emby_base_url=str(fallback_server.make_url("")),
            fallback_base_url=str(fallback_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"fallback"
    assert fallback_calls == 1


async def test_stream_fallback_uses_long_stream_timeout(monkeypatch, tmp_path):
    captured = {}

    class FakeRequest:
        method = "GET"
        raw_path = "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t"
        headers = {}

    class FakeSession:
        def __init__(self, *, timeout):
            captured["timeout"] = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def request(self, *args, **kwargs):
            raise AssertionError("timeout should be asserted before network request")

    monkeypatch.setattr(app_module, "ClientSession", FakeSession)

    try:
        await app_module.stream_fallback(
            FakeRequest(),
            Config(emby_base_url="http://emby", fallback_base_url="http://emby", cache_dir=str(tmp_path)),
        )
    except AssertionError:
        pass

    timeout = captured["timeout"]
    assert isinstance(timeout, ClientTimeout)
    assert timeout.total is None
    assert timeout.sock_connect == 30.0
    assert timeout.sock_read is None
