import pytest

from emby_range_cache_proxy.ranges import ByteRange, intersect_ranges, parse_range_header


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
