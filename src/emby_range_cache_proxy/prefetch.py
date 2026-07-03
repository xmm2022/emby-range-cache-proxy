from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from aiohttp import ClientError

from .config import Config, MiddleCacheConfig, PrefetchConfig
from .middle_cache import MiddleRangeCache
from .models import ByteRange
from .origin import OriginClient, OriginError
from .state import PrefetchTaskRecord, SessionStateStore


@dataclass
class PrefetchRunResult:
    completed: int = 0
    failed: int = 0
    skipped: int = 0


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

        tasks = self.store.claim_prefetch_tasks(
            limit=self.config.prefetch.concurrency,
            now=now,
        )
        result = PrefetchRunResult()
        for task in tasks:
            task_result = await self._run_task(task, now=now)
            result.completed += task_result.completed
            result.failed += task_result.failed
            result.skipped += task_result.skipped
        return result

    async def _run_task(
        self, task: PrefetchTaskRecord, *, now: float
    ) -> PrefetchRunResult:
        url = self.source_lookup.get((task.item_id, task.media_source_id))
        if url is None:
            self.store.fail_prefetch_task(
                task.id,
                error_class="SourceUnavailable",
                now=now,
            )
            return PrefetchRunResult(skipped=1)

        try:
            data = bytearray()
            byte_range = ByteRange(task.start, task.end)
            async with OriginClient(
                chunk_bytes=self.config.cache.chunk_bytes
            ) as origin:
                async with origin.open_range(
                    url,
                    byte_range,
                    size=max(task.end + 1, 1),
                ) as response:
                    async for chunk in response.content.iter_chunked(
                        self.config.cache.chunk_bytes
                    ):
                        if not chunk:
                            continue
                        await self.limiter.consume(len(chunk))
                        data.extend(chunk)

            self.middle_cache.store_block(
                task.cache_key,
                byte_range,
                bytes(data),
                now=now,
            )
            self.middle_cache.evict_expired(now=now)
            self.middle_cache.evict_lru_if_needed()
            self.store.complete_prefetch_task(task.id, now=now)
            return PrefetchRunResult(completed=1)
        except (
            OriginError,
            ClientError,
            asyncio.TimeoutError,
            TimeoutError,
            OSError,
            ValueError,
        ) as error:
            self.store.fail_prefetch_task(
                task.id,
                error_class=error.__class__.__name__,
                now=now,
            )
            return PrefetchRunResult(failed=1)


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
