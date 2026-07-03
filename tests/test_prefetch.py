import asyncio
import sqlite3

import pytest
from aiohttp import web

from emby_range_cache_proxy import middle_cache as middle_cache_module
from emby_range_cache_proxy.cache import cache_key
from emby_range_cache_proxy.config import (
    Config,
    MiddleCacheConfig,
    PrefetchConfig,
    RolloutConfig,
)
from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.models import ByteRange, MediaSource, SourceMetadata
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
    request_methods = []

    async def handler(request):
        request_methods.append(request.method)
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100", "ETag": '"v1"'})
        assert request.headers["Range"] == "bytes=10-19"
        return web.Response(
            status=206,
            body=b"0123456789",
            headers={"Content-Range": "bytes 10-19/100", "ETag": '"v1"'},
        )

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", handler)
    origin = await aiohttp_client(origin_app)
    origin_url = str(origin.make_url("/movie.mkv"))
    key = cache_key(
        MediaSource(
            item_id="1",
            media_source_id="ms1",
            path=origin_url,
            protocol="Http",
            size=100,
        ),
        SourceMetadata(url=origin_url, size=100, etag='"v1"'),
    )
    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    store.enqueue_prefetch_task(
        "1",
        "ms1",
        key,
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
    assert request_methods == ["HEAD", "GET"]
    chunks = middle.iter_block(key, ByteRange(10, 19), chunk_bytes=4, now=3.0)
    assert chunks is not None
    assert b"".join(chunks) == b"0123456789"


async def test_prefetch_worker_fails_cache_key_mismatch_without_writing_middle_cache(
    aiohttp_client, tmp_path
):
    async def handler(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "20"})
        assert request.headers["Range"] == "bytes=10-19"
        return web.Response(
            status=206,
            body=b"0123456789",
            headers={"Content-Range": "bytes 10-19/20"},
        )

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", handler)
    origin = await aiohttp_client(origin_app)
    origin_url = str(origin.make_url("/movie.mkv"))
    wrong_key = "a" * 64
    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    store.enqueue_prefetch_task(
        "1",
        "ms1",
        wrong_key,
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
        middle_cache=MiddleCacheConfig(enabled=True),
        prefetch=PrefetchConfig(enabled=True, bandwidth_bytes_per_second=1024),
    )
    worker = PrefetchWorker(
        config, store, middle, source_lookup={("1", "ms1"): origin_url}
    )

    result = await worker.run_once(now=2.0)

    assert result.completed == 0
    assert result.failed == 1
    assert result.skipped == 0
    assert (
        middle.iter_block(wrong_key, ByteRange(10, 19), chunk_bytes=4, now=3.0)
        is None
    )


async def test_prefetch_worker_requeues_running_task_when_cancelled(
    aiohttp_client, tmp_path
):
    async def handler(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "20"})
        return web.Response(
            status=206,
            body=b"0123456789",
            headers={"Content-Range": "bytes 10-19/20"},
        )

    class CancellingLimiter:
        async def consume(self, byte_count):
            raise asyncio.CancelledError

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", handler)
    origin = await aiohttp_client(origin_app)
    origin_url = str(origin.make_url("/movie.mkv"))
    key = cache_key(
        MediaSource(
            item_id="1",
            media_source_id="ms1",
            path=origin_url,
            protocol="Http",
            size=20,
        ),
        SourceMetadata(url=origin_url, size=20),
    )
    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    task = store.enqueue_prefetch_task(
        "1",
        "ms1",
        key,
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
        middle_cache=MiddleCacheConfig(enabled=True),
        prefetch=PrefetchConfig(enabled=True, bandwidth_bytes_per_second=1024),
    )
    worker = PrefetchWorker(
        config, store, middle, source_lookup={("1", "ms1"): origin_url}
    )
    worker.limiter = CancellingLimiter()

    with pytest.raises(asyncio.CancelledError):
        await worker.run_once(now=2.0)

    assert task is not None
    requeued = store.claim_prefetch_tasks(limit=1, now=3.0)
    assert len(requeued) == 1
    assert requeued[0].id == task.id
    assert requeued[0].attempts == 2
    assert requeued[0].last_error_class == "CancelledError"


async def test_prefetch_worker_skips_range_larger_than_middle_cache(
    aiohttp_client, tmp_path
):
    request_methods = []

    async def handler(request):
        request_methods.append(request.method)
        return web.Response(
            status=206,
            body=b"0123456789",
            headers={"Content-Range": "bytes 10-19/20"},
        )

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", handler)
    origin = await aiohttp_client(origin_app)
    origin_url = str(origin.make_url("/movie.mkv"))

    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(tmp_path / "mid", store, max_bytes=5, ttl_seconds=60)
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
        middle_cache=MiddleCacheConfig(enabled=True),
        prefetch=PrefetchConfig(enabled=True, bandwidth_bytes_per_second=1024),
    )
    worker = PrefetchWorker(
        config,
        store,
        middle,
        source_lookup={("1", "ms1"): origin_url},
    )

    result = await worker.run_once(now=2.0)

    assert result.completed == 0
    assert result.failed == 0
    assert result.skipped == 1
    assert request_methods == []


async def test_prefetch_worker_old_attempt_does_not_publish_middle_cache(tmp_path):
    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    task = store.enqueue_prefetch_task(
        "1",
        "ms1",
        "a" * 64,
        0,
        2,
        priority=1,
        now=1.0,
        max_queue_depth=10,
    )
    first = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    second = store.claim_prefetch_tasks(
        limit=1,
        now=12.0,
        running_stale_seconds=10,
    )[0]
    worker = PrefetchWorker(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://fallback",
            cache_dir=str(tmp_path),
        ),
        store,
        middle,
    )

    worker._store_completed_task(second, ByteRange(0, 2), b"new", 13.0)
    before = store.find_middle_block("a" * 64, ByteRange(0, 2))
    stale_result = worker._store_completed_task(
        first, ByteRange(0, 2), b"old", 14.0
    )
    chunks = middle.iter_block("a" * 64, ByteRange(0, 2), chunk_bytes=3, now=15.0)
    after = store.find_middle_block("a" * 64, ByteRange(0, 2))

    assert task is not None
    assert stale_result is False
    assert chunks is not None
    assert b"".join(chunks) == b"new"
    assert before.created_at == 13.0
    assert after.created_at == 13.0


async def test_prefetch_worker_publish_cannot_be_reclaimed_before_complete(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "state.db"
    store = SessionStateStore(db_path)
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    task = store.enqueue_prefetch_task(
        "1",
        "ms1",
        "a" * 64,
        0,
        2,
        priority=1,
        now=1.0,
        max_queue_depth=10,
    )
    first = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    racing_store = SessionStateStore(db_path)

    def connect_without_waiting():
        conn = sqlite3.connect(db_path, timeout=0)
        conn.row_factory = sqlite3.Row
        return conn

    racing_store._connect = connect_without_waiting
    race = {"attempted": False, "claimed": False, "locked": False}
    original_replace = middle_cache_module.os.replace

    def replace_and_try_reclaim(source, target):
        if not race["attempted"]:
            race["attempted"] = True
            try:
                claimed = racing_store.claim_prefetch_tasks(
                    limit=1,
                    now=24.0,
                    running_stale_seconds=10,
                )
                race["claimed"] = bool(claimed)
            except sqlite3.OperationalError as error:
                race["locked"] = "locked" in str(error)
        original_replace(source, target)

    monkeypatch.setattr(middle_cache_module.os, "replace", replace_and_try_reclaim)
    worker = PrefetchWorker(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://fallback",
            cache_dir=str(tmp_path),
        ),
        store,
        middle,
    )

    result = worker._store_completed_task(first, ByteRange(0, 2), b"old", 13.0)
    chunks = middle.iter_block("a" * 64, ByteRange(0, 2), chunk_bytes=3, now=14.0)

    assert task is not None
    assert race["attempted"] is True
    assert race["claimed"] is False
    assert result is True
    assert chunks is not None
    assert b"".join(chunks) == b"old"
    with sqlite3.connect(db_path) as conn:
        status, attempts = conn.execute(
            "SELECT status, attempts FROM prefetch_tasks WHERE id = ?",
            (task.id,),
        ).fetchone()
    assert status == "done"
    assert attempts == 1


async def test_prefetch_worker_source_missing_skips_with_retry(tmp_path):
    store = SessionStateStore(tmp_path / "state.db")
    middle = MiddleRangeCache(
        tmp_path / "mid", store, max_bytes=1024, ttl_seconds=60
    )
    task = store.enqueue_prefetch_task(
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
        middle_cache=MiddleCacheConfig(enabled=True),
        prefetch=PrefetchConfig(
            enabled=True,
            bandwidth_bytes_per_second=1024,
            error_backoff_seconds=10,
        ),
    )
    worker = PrefetchWorker(config, store, middle)

    result = await worker.run_once(now=2.0)
    early = store.claim_prefetch_tasks(limit=1, now=11.0)
    retried = store.claim_prefetch_tasks(limit=1, now=12.0)

    assert task is not None
    assert result.completed == 0
    assert result.failed == 0
    assert result.skipped == 1
    assert early == []
    assert len(retried) == 1
    assert retried[0].id == task.id
    assert retried[0].status == "running"


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
