from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from aiohttp import ClientError

from .cache import cache_key
from .config import Config, MiddleCacheConfig, PrefetchConfig
from .middle_cache import MiddleRangeCache
from .models import ByteRange, MediaSource
from .origin import OriginClient, OriginError
from .state import PrefetchTaskRecord, SessionStateStore


@dataclass
class PrefetchRunResult:
    completed: int = 0
    failed: int = 0
    skipped: int = 0


class PrefetchSourceMismatch(Exception):
    pass


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
        byte_range = ByteRange(task.start, task.end)
        if byte_range.length > self.middle_cache.max_bytes:
            await asyncio.to_thread(
                self.store.skip_prefetch_task,
                task.id,
                error_class="RangeTooLarge",
                now=now,
                expected_attempts=task.attempts,
            )
            return PrefetchRunResult(skipped=1)

        url = self.source_lookup.get((task.item_id, task.media_source_id))
        if url is None:
            await asyncio.to_thread(
                self.store.skip_prefetch_task,
                task.id,
                error_class="SourceUnavailable",
                now=now,
                retry_after_seconds=self.config.prefetch.error_backoff_seconds,
                expected_attempts=task.attempts,
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
                return PrefetchRunResult(skipped=1)
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
            return PrefetchRunResult(failed=1)

    def _store_completed_task(
        self,
        task: PrefetchTaskRecord,
        byte_range: ByteRange,
        data: bytes,
        now: float,
    ) -> bool:
        stored = self.middle_cache.store_block_if_current(
            task.cache_key,
            byte_range,
            data,
            now=now,
            precommit=lambda: self.store.refresh_prefetch_task_attempt(
                task.id,
                now=now,
                expected_attempts=task.attempts,
            ),
        )
        if not stored:
            return False
        self.middle_cache.evict_expired(now=now)
        self.middle_cache.evict_lru_if_needed()
        return self.store.complete_prefetch_task(
            task.id,
            now=now,
            expected_attempts=task.attempts,
        )


def align_down(value: int, alignment: int) -> int:
    return value - (value % alignment)


def align_up(value: int, alignment: int) -> int:
    remainder = value % alignment
    if remainder == 0:
        return value
    return value + alignment - remainder


def plan_middle_ranges(
    *,
    media_size: int,
    head_size: int,
    tail_size: int,
    max_observed_offset: int,
    queued_until: int | None,
    prefetch: PrefetchConfig,
    middle_cache: MiddleCacheConfig,
) -> list[ByteRange]:
    segment = middle_cache.segment_bytes
    head_end = min(head_size, media_size) - 1
    tail_start = max(0, media_size - tail_size)
    middle_start = head_end + 1
    middle_end = tail_start - 1
    if middle_start > middle_end:
        return []
    if middle_end - middle_start + 1 < segment:
        return []

    start = max(middle_start, max_observed_offset - prefetch.resume_overlap_bytes)
    start = max(middle_start, align_down(start, segment))
    if queued_until is not None:
        start = max(start, queued_until + 1)
        start = max(middle_start, align_up(start, segment))
    window_end = min(start + prefetch.window_bytes - 1, middle_end)
    session_end = min(start + prefetch.max_session_bytes - 1, window_end)

    ranges: list[ByteRange] = []
    current = start
    while current <= session_end:
        end = min(current + segment - 1, session_end, middle_end)
        if end >= middle_start and current <= middle_end:
            ranges.append(ByteRange(max(current, middle_start), end))
        current = end + 1
    return ranges
