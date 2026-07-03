from emby_range_cache_proxy.config import MiddleCacheConfig, PrefetchConfig
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.prefetch import plan_middle_ranges


def test_plan_middle_ranges_aligns_skips_head_tail_and_caps_window():
    ranges = plan_middle_ranges(
        media_size=1000,
        head_size=100,
        tail_size=100,
        max_observed_offset=350,
        queued_until=None,
        prefetch=PrefetchConfig(window_bytes=256, resume_overlap_bytes=50, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == [
        ByteRange(256, 319),
        ByteRange(320, 383),
        ByteRange(384, 447),
        ByteRange(448, 511),
    ]


def test_plan_middle_ranges_deduplicates_using_queued_until():
    ranges = plan_middle_ranges(
        media_size=1000,
        head_size=100,
        tail_size=100,
        max_observed_offset=350,
        queued_until=511,
        prefetch=PrefetchConfig(window_bytes=256, resume_overlap_bytes=50, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == [
        ByteRange(512, 575),
        ByteRange(576, 639),
        ByteRange(640, 703),
        ByteRange(704, 767),
    ]


def test_plan_middle_ranges_queued_until_non_boundary_does_not_repeat_bytes():
    ranges = plan_middle_ranges(
        media_size=1000,
        head_size=100,
        tail_size=100,
        max_observed_offset=350,
        queued_until=550,
        prefetch=PrefetchConfig(window_bytes=512, resume_overlap_bytes=50, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges
    assert all(byte_range.start > 550 for byte_range in ranges)
    assert ranges[0] == ByteRange(576, 639)


def test_plan_middle_ranges_caps_by_max_session_bytes():
    ranges = plan_middle_ranges(
        media_size=1000,
        head_size=100,
        tail_size=100,
        max_observed_offset=350,
        queued_until=None,
        prefetch=PrefetchConfig(window_bytes=512, resume_overlap_bytes=50, max_session_bytes=100),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == [
        ByteRange(256, 319),
        ByteRange(320, 355),
    ]


def test_plan_middle_ranges_returns_partial_final_segment():
    ranges = plan_middle_ranges(
        media_size=1000,
        head_size=0,
        tail_size=0,
        max_observed_offset=128,
        queued_until=None,
        prefetch=PrefetchConfig(window_bytes=100, resume_overlap_bytes=0, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == [
        ByteRange(128, 191),
        ByteRange(192, 227),
    ]


def test_plan_middle_ranges_returns_empty_when_middle_space_smaller_than_segment():
    ranges = plan_middle_ranges(
        media_size=200,
        head_size=128,
        tail_size=64,
        max_observed_offset=100,
        queued_until=None,
        prefetch=PrefetchConfig(window_bytes=256, resume_overlap_bytes=0, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == []


def test_plan_middle_ranges_returns_empty_when_no_middle_space():
    ranges = plan_middle_ranges(
        media_size=192,
        head_size=128,
        tail_size=64,
        max_observed_offset=100,
        queued_until=None,
        prefetch=PrefetchConfig(window_bytes=256, resume_overlap_bytes=0, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == []
