from __future__ import annotations

import re

from .models import ByteRange

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)$")


def parse_range_header(value: str | None, *, size: int) -> ByteRange:
    if size <= 0:
        raise ValueError("size must be positive")
    if not value:
        return ByteRange(0, size - 1)
    if "," in value:
        raise ValueError("multiple ranges are not supported")
    match = _RANGE_RE.fullmatch(value.strip())
    if not match:
        raise ValueError("invalid range header")
    left, right = match.groups()
    if left == "" and right == "":
        raise ValueError("empty range")
    if left == "":
        length = int(right)
        if length <= 0:
            raise ValueError("invalid suffix range")
        return ByteRange(max(0, size - length), size - 1)
    start = int(left)
    if start >= size:
        raise ValueError("range start beyond size")
    end = int(right) if right else size - 1
    if end < start:
        raise ValueError("range end before start")
    return ByteRange(start, min(end, size - 1))


def plan_playback_range(
    value: str | None,
    *,
    size: int,
    head_bytes: int,
    tail_bytes: int,
    default_open_range_bytes: int,
) -> ByteRange:
    byte_range = parse_range_header(value, size=size)
    if not value:
        return byte_range

    match = _RANGE_RE.fullmatch(value.strip())
    if match is None:
        return byte_range
    left, right = match.groups()
    if left == "" or right != "":
        return byte_range

    start = byte_range.start
    tail_start = max(0, size - tail_bytes)
    if start < min(head_bytes, size):
        return ByteRange(start, min(head_bytes, size) - 1)
    if start >= tail_start:
        return ByteRange(start, size - 1)
    return ByteRange(start, min(start + default_open_range_bytes - 1, size - 1))


def intersect_ranges(left: ByteRange, right: ByteRange) -> ByteRange | None:
    start = max(left.start, right.start)
    end = min(left.end, right.end)
    if end < start:
        return None
    return ByteRange(start, end)


def content_range_header(byte_range: ByteRange, *, size: int) -> str:
    return f"bytes {byte_range.start}-{byte_range.end}/{size}"
