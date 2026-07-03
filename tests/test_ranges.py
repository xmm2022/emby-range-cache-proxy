import pytest

from emby_range_cache_proxy.ranges import ByteRange, intersect_ranges, parse_range_header, plan_playback_range


def test_parse_closed_range():
    assert parse_range_header("bytes=10-19", size=100) == ByteRange(10, 19)


def test_parse_open_ended_range_clamps_to_size():
    assert parse_range_header("bytes=90-", size=100) == ByteRange(90, 99)


def test_parse_suffix_range():
    assert parse_range_header("bytes=-10", size=100) == ByteRange(90, 99)


def test_reject_multiple_ranges():
    with pytest.raises(ValueError, match="multiple ranges"):
        parse_range_header("bytes=0-1,4-5", size=100)


def test_intersection():
    assert intersect_ranges(ByteRange(0, 99), ByteRange(50, 149)) == ByteRange(50, 99)
    assert intersect_ranges(ByteRange(0, 10), ByteRange(11, 20)) is None


def test_plan_open_ended_range_inside_head_to_head_window():
    assert plan_playback_range(
        "bytes=8-",
        size=100,
        head_bytes=16,
        tail_bytes=4,
        default_open_range_bytes=8,
    ) == ByteRange(8, 15)


def test_plan_open_ended_range_inside_tail_to_eof():
    assert plan_playback_range(
        "bytes=97-",
        size=100,
        head_bytes=16,
        tail_bytes=4,
        default_open_range_bytes=8,
    ) == ByteRange(97, 99)


def test_plan_open_ended_range_inside_middle_to_default_window():
    assert plan_playback_range(
        "bytes=20-",
        size=100,
        head_bytes=16,
        tail_bytes=4,
        default_open_range_bytes=8,
    ) == ByteRange(20, 27)


def test_plan_closed_range_unchanged():
    assert plan_playback_range(
        "bytes=20-30",
        size=100,
        head_bytes=16,
        tail_bytes=4,
        default_open_range_bytes=8,
    ) == ByteRange(20, 30)
