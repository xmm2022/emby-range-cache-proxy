import asyncio

from aiohttp import web

from emby_range_cache_proxy.config import (
    Config,
    MiddleCacheConfig,
    PrefetchConfig,
    RolloutConfig,
)
from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.prefetch import (
    BandwidthLimiter,
    PrefetchWorker,
    plan_middle_ranges,
)
from emby_range_cache_proxy.state import SessionStateStore


async def test_bandwidth_limiter_waits_when_chunk_exceeds_rate():
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    limiter = BandwidthLimiter(bytes_per_second=10, sleep=fake_sleep)

    await limiter.consume(25)

    assert sleeps == [2.5]


async def test_prefetch_worker_fetches_claimed_task_into_middle_cache(
    aiohttp_client, tmp_path
):
    async def handler(request):
        assert request.headers["Range"] == "bytes=10-19"
        return web.Response(
            status=206,
            body=b"0123456789",
            headers={"Content-Range": "bytes 10-19/20"},
        )

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", handler)
    origin = await aiohttp_client(origin_app)
    origin_url = str(origin.make_url("/movie.mkv"))
    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    store.enqueue_prefetch_task(
        "1",
        "ms1",
        "a" * 64,
        10,
        19,
        priority=1,
        now=1.0,
        max_queue_depth=10,
    )
    config = Config(
        emby_base_url="http://emby",
        fallback_base_url="http://fallback",
        cache_dir=str(tmp_path),
        rollout=RolloutConfig(enabled=True),
        middle_cache=MiddleCacheConfig(enabled=True),
        prefetch=PrefetchConfig(
            enabled=True, bandwidth_bytes_per_second=1024, concurrency=1
        ),
    )
    worker = PrefetchWorker(
        config, store, middle, source_lookup={("1", "ms1"): origin_url}
    )

    result = await worker.run_once(now=2.0)

    assert result.completed == 1
    assert result.failed == 0
    assert result.skipped == 0
    chunks = middle.iter_block("a" * 64, ByteRange(10, 19), chunk_bytes=4, now=3.0)
    assert chunks is not None
    assert b"".join(chunks) == b"0123456789"


async def test_prefetch_worker_skips_when_disabled(tmp_path):
    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    store.enqueue_prefetch_task(
        "1",
        "ms1",
        "a" * 64,
        10,
        19,
        priority=1,
        now=1.0,
        max_queue_depth=10,
    )
    worker = PrefetchWorker(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://fallback",
            cache_dir=str(tmp_path),
        ),
        store,
        middle,
    )

    result = await worker.run_once(now=2.0)

    assert result.completed == 0
    assert result.failed == 0
    assert result.skipped == 0
    assert store.queue_depth() == 1


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
