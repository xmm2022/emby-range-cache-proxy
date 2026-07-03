from __future__ import annotations

from .config import MiddleCacheConfig, PrefetchConfig
from .models import ByteRange


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
