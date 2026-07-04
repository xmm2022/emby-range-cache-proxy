from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

from aiohttp import ClientError

from .cache import adaptive_head_tail, cache_key
from .config import Config, MiddleCacheConfig, PrefetchConfig
from .middle_cache import MiddleRangeCache
from .models import ByteRange, MediaSource
from .origin import OriginClient, OriginError
from .state import PlaybackSessionRecord, PrefetchTaskRecord, SessionStateStore

LOGGER = logging.getLogger(__name__)


@dataclass
class PrefetchRunResult:
    completed: int = 0
    failed: int = 0
    skipped: int = 0


class PrefetchSourceMismatch(Exception):
    pass


def short_hash(value: str | None) -> str:
    return "none" if value is None else value[:12]


class BandwidthLimiter:
    def __init__(
        self,
        *,
        bytes_per_second: int,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if bytes_per_second <= 0:
            raise ValueError("bytes_per_second must be positive")
        self.bytes_per_second = bytes_per_second
        self.sleep = sleep

    async def consume(self, byte_count: int) -> None:
        if byte_count <= 0:
            return
        await self.sleep(byte_count / self.bytes_per_second)


class PrefetchWorker:
    def __init__(
        self,
        config: Config,
        store: SessionStateStore,
        middle_cache: MiddleRangeCache,
        *,
        source_lookup: dict[tuple[str, str], str] | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.middle_cache = middle_cache
        self.source_lookup = dict(source_lookup or {})
        self.limiter = BandwidthLimiter(
            bytes_per_second=config.prefetch.bandwidth_bytes_per_second
        )

    async def run_once(self, *, now: float) -> PrefetchRunResult:
        if not self.config.prefetch.enabled or not self.config.middle_cache.enabled:
            return PrefetchRunResult()

        result = PrefetchRunResult()
        for _ in range(self.config.prefetch.concurrency):
            tasks = await asyncio.to_thread(
                self.store.claim_prefetch_tasks,
                limit=1,
                now=now,
                running_stale_seconds=self.config.prefetch.error_backoff_seconds,
            )
            if not tasks:
                break
            task = tasks[0]
            task_result = await self._run_task(task, now=now)
            result.completed += task_result.completed
            result.failed += task_result.failed
            result.skipped += task_result.skipped
        return result

    async def _run_task(
        self, task: PrefetchTaskRecord, *, now: float
    ) -> PrefetchRunResult:
        _log_prefetch_event("prefetch_started", task)
        byte_range = ByteRange(task.start, task.end)
        if byte_range.length > self.middle_cache.max_bytes:
            await asyncio.to_thread(
                self.store.skip_prefetch_task,
                task.id,
                error_class="RangeTooLarge",
                now=now,
                expected_attempts=task.attempts,
            )
            _log_prefetch_event("prefetch_skipped", task, reason="RangeTooLarge")
            return PrefetchRunResult(skipped=1)

        url = self.source_lookup.get((task.item_id, task.media_source_id))
        if url is None:
            source_metadata = await asyncio.to_thread(
                self.store.get_source_metadata,
                task.item_id,
                task.media_source_id,
                task.cache_key,
            )
            if source_metadata is not None:
                url = source_metadata.origin_url
        if url is None:
            await asyncio.to_thread(
                self.store.skip_prefetch_task,
                task.id,
                error_class="SourceUnavailable",
                now=now,
                retry_after_seconds=self.config.prefetch.error_backoff_seconds,
                expected_attempts=task.attempts,
            )
            _log_prefetch_event(
                "prefetch_skipped", task, reason="SourceUnavailable"
            )
            return PrefetchRunResult(skipped=1)

        try:
            data = bytearray()
            async with OriginClient(
                chunk_bytes=self.config.cache.chunk_bytes
            ) as origin:
                metadata = await origin.head(url)
                expected_key = cache_key(
                    MediaSource(
                        item_id=task.item_id,
                        media_source_id=task.media_source_id,
                        path=url,
                        protocol="Http",
                        size=metadata.size,
                    ),
                    metadata,
                )
                if expected_key != task.cache_key:
                    raise PrefetchSourceMismatch("prefetch source metadata mismatch")
                async with origin.open_range(
                    url,
                    byte_range,
                    size=metadata.size,
                ) as response:
                    async for chunk in response.content.iter_chunked(
                        self.config.cache.chunk_bytes
                    ):
                        if not chunk:
                            continue
                        await self.limiter.consume(len(chunk))
                        data.extend(chunk)

            stored = await asyncio.to_thread(
                self._store_completed_task,
                task,
                byte_range,
                bytes(data),
                now,
            )
            if not stored:
                _log_prefetch_event("prefetch_skipped", task, reason="PublishRace")
                return PrefetchRunResult(skipped=1)
            _log_prefetch_event("prefetch_complete", task)
            return PrefetchRunResult(completed=1)
        except asyncio.CancelledError:
            await asyncio.to_thread(
                self.store.requeue_prefetch_task,
                task.id,
                now=now,
                error_class="CancelledError",
                expected_attempts=task.attempts,
            )
            raise
        except (
            PrefetchSourceMismatch,
            OriginError,
            ClientError,
            asyncio.TimeoutError,
            TimeoutError,
            OSError,
            ValueError,
        ) as error:
            retry_after_seconds = (
                None
                if isinstance(error, PrefetchSourceMismatch)
                else self.config.prefetch.error_backoff_seconds
            )
            await asyncio.to_thread(
                self.store.fail_prefetch_task,
                task.id,
                error_class=error.__class__.__name__,
                now=now,
                retry_after_seconds=retry_after_seconds,
                expected_attempts=task.attempts,
            )
            _log_prefetch_event(
                "prefetch_failed", task, error_class=error.__class__.__name__
            )
            return PrefetchRunResult(failed=1)

    def _store_completed_task(
        self,
        task: PrefetchTaskRecord,
        byte_range: ByteRange,
        data: bytes,
        now: float,
    ) -> bool:
        stored = self.middle_cache.store_prefetch_block(
            task.id,
            expected_attempts=task.attempts,
            key=task.cache_key,
            byte_range=byte_range,
            data=data,
            now=now,
        )
        if not stored:
            return False
        self.middle_cache.evict_expired(now=now)
        self.middle_cache.evict_lru_if_needed()
        return True


def _log_prefetch_event(
    event: str,
    task: PrefetchTaskRecord,
    *,
    reason: str | None = None,
    error_class: str | None = None,
) -> None:
    LOGGER.info(
        "%s item_id=%s media_source_id=%s cache_key=%s range=%s-%s "
        "reason=%s error_class=%s",
        event,
        task.item_id,
        task.media_source_id,
        short_hash(task.cache_key),
        task.start,
        task.end,
        reason or "none",
        error_class or "none",
    )


def align_down(value: int, alignment: int) -> int:
    return value - (value % alignment)


def align_up(value: int, alignment: int) -> int:
    remainder = value % alignment
    if remainder == 0:
        return value
    return value + alignment - remainder


def middle_prefetch_window_bytes(head_size: int, prefetch: PrefetchConfig) -> int:
    if head_size <= 0:
        return 0
    return min(head_size, prefetch.window_bytes, prefetch.max_session_bytes)


def middle_prefetch_overlap_bytes(
    window_bytes: int, prefetch: PrefetchConfig
) -> int:
    if window_bytes <= 1:
        return 0
    return min(prefetch.resume_overlap_bytes, window_bytes // 2)


def plan_middle_ranges(
    *,
    media_size: int,
    head_size: int,
    tail_size: int,
    anchor_offset: int,
    queued_until: int | None,
    prefetch: PrefetchConfig,
    middle_cache: MiddleCacheConfig,
) -> list[ByteRange]:
    segment = middle_cache.segment_bytes
    window_bytes = middle_prefetch_window_bytes(head_size, prefetch)
    if window_bytes <= 0:
        return []
    head_end = min(head_size, media_size) - 1
    tail_start = max(0, media_size - tail_size)
    middle_start = head_end + 1
    middle_end = tail_start - 1
    if middle_start > middle_end:
        return []

    if queued_until is None:
        if anchor_offset <= head_end:
            return []
        overlap_bytes = middle_prefetch_overlap_bytes(window_bytes, prefetch)
        start = max(middle_start, anchor_offset - overlap_bytes)
        if window_bytes > segment:
            start = max(middle_start, align_down(start, segment))
    else:
        start = max(middle_start, queued_until + 1)
    session_end = min(start + window_bytes - 1, middle_end)

    ranges: list[ByteRange] = []
    current = start
    while current <= session_end:
        segment_end = current + segment - 1
        if current % segment:
            segment_end = align_up(current, segment) - 1
        end = min(segment_end, session_end, middle_end)
        if end >= middle_start and current <= middle_end:
            ranges.append(ByteRange(max(current, middle_start), end))
        current = end + 1
    return ranges


def enqueue_prefetch_for_session(
    store: SessionStateStore,
    session: PlaybackSessionRecord,
    *,
    prefetch: PrefetchConfig,
    middle_cache: MiddleCacheConfig,
    now: float,
    priority: int,
) -> int:
    head_size, tail_size = adaptive_head_tail(session.media_size)
    target_ranges = plan_middle_ranges(
        media_size=session.media_size,
        head_size=head_size,
        tail_size=tail_size,
        anchor_offset=session.last_range_end,
        queued_until=None,
        prefetch=prefetch,
        middle_cache=middle_cache,
    )
    if not target_ranges:
        return 0
    target_start = target_ranges[0].start
    target_end = target_ranges[-1].end
    queued_until = (
        session.queued_until
        if session.queued_until is not None
        and target_start <= session.queued_until < target_end
        else None
    )

    ranges = plan_middle_ranges(
        media_size=session.media_size,
        head_size=head_size,
        tail_size=tail_size,
        anchor_offset=session.last_range_end,
        queued_until=queued_until,
        prefetch=prefetch,
        middle_cache=middle_cache,
    )

    inserted = 0
    highest_end: int | None = None
    stop_queueing = False
    for byte_range in ranges:
        if stop_queueing:
            break
        if byte_range.start > target_end:
            break
        if byte_range.end > target_end:
            byte_range = ByteRange(byte_range.start, target_end)
        missing_ranges = _subtract_ranges(
            byte_range,
            store.reusable_prefetch_ranges(session.cache_key, byte_range),
        )
        if not missing_ranges:
            highest_end = byte_range.end
            continue

        for missing_range in missing_ranges:
            task = store.enqueue_prefetch_task(
                session.item_id,
                session.media_source_id,
                session.cache_key,
                missing_range.start,
                missing_range.end,
                priority=priority,
                now=now,
                max_queue_depth=prefetch.max_queue_depth,
            )
            if task is None:
                if store.prefetch_task_exists(
                    session.cache_key,
                    missing_range.start,
                    missing_range.end,
                ):
                    highest_end = missing_range.end
                    continue
                stop_queueing = True
                break
            inserted += 1
            LOGGER.info(
                "prefetch_queued item_id=%s media_source_id=%s session=%s "
                "cache_key=%s range=%s-%s priority=%s",
                session.item_id,
                session.media_source_id,
                short_hash(session.session_hash),
                short_hash(session.cache_key),
                missing_range.start,
                missing_range.end,
                priority,
            )
            highest_end = missing_range.end
        if stop_queueing:
            break

    if highest_end is not None:
        store.update_session_queued_until(
            session.session_hash,
            highest_end,
            now=now,
        )
    return inserted


def _subtract_ranges(
    byte_range: ByteRange, existing_ranges: list[ByteRange]
) -> list[ByteRange]:
    missing: list[ByteRange] = []
    current = byte_range.start
    for existing in existing_ranges:
        if existing.end < current:
            continue
        if existing.start > byte_range.end:
            break
        if existing.start > current:
            missing.append(ByteRange(current, min(existing.start - 1, byte_range.end)))
        current = max(current, existing.end + 1)
        if current > byte_range.end:
            break
    if current <= byte_range.end:
        missing.append(ByteRange(current, byte_range.end))
    return missing
