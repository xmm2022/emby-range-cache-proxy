import pytest

from emby_range_cache_proxy import middle_cache as middle_cache_module
from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.state import MiddleBlockRecord, SessionStateStore


def _key(char="a"):
    return char * 64


def test_middle_cache_store_and_iter_block(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    cache.store_block(_key(), ByteRange(1024, 1033), b"0123456789", now=10.0)
    chunks = cache.iter_block(_key(), ByteRange(1026, 1030), chunk_bytes=2, now=20.0)

    assert chunks is not None
    assert list(chunks) == [b"23", b"45", b"6"]
    block = store.find_middle_block(_key(), ByteRange(1026, 1030))
    assert block.last_access_at == 20.0
    assert block.expires_at == 80.0


def test_middle_cache_guarded_store_does_not_publish_when_precommit_fails(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(
        tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60
    )
    cache.store_block(_key(), ByteRange(0, 2), b"new", now=10.0)

    published = cache.store_block_if_current(
        _key(),
        ByteRange(0, 2),
        b"old",
        now=9.0,
        precommit=lambda: False,
    )
    chunks = cache.iter_block(_key(), ByteRange(0, 2), chunk_bytes=3, now=11.0)
    record = store.find_middle_block(_key(), ByteRange(0, 2))

    assert published is False
    assert chunks is not None
    assert b"".join(chunks) == b"new"
    assert record.created_at == 10.0


def test_middle_cache_prefetch_store_does_not_publish_when_attempt_mismatch(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(
        tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60
    )
    cache.store_block(_key(), ByteRange(0, 2), b"new", now=10.0)
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key=_key(),
        start=0,
        end=2,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)[0]

    published = cache.store_prefetch_block(
        claimed.id,
        expected_attempts=2,
        key=_key(),
        byte_range=ByteRange(0, 2),
        data=b"old",
        now=9.0,
    )
    chunks = cache.iter_block(_key(), ByteRange(0, 2), chunk_bytes=3, now=11.0)
    record = store.find_middle_block(_key(), ByteRange(0, 2))

    assert task is not None
    assert published is False
    assert chunks is not None
    assert b"".join(chunks) == b"new"
    assert record.created_at == 10.0


def test_middle_cache_prefetch_store_rolls_back_when_sidecar_publish_fails(
    tmp_path, monkeypatch
):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(
        tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60
    )
    cache.store_block(_key(), ByteRange(0, 2), b"old", now=10.0)
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key=_key(),
        start=0,
        end=2,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    original_replace = middle_cache_module.os.replace
    calls = []

    def fail_second_replace(source, target):
        calls.append(target)
        if len(calls) == 2:
            raise OSError("sidecar replace failed")
        original_replace(source, target)

    monkeypatch.setattr(middle_cache_module.os, "replace", fail_second_replace)

    with pytest.raises(OSError, match="sidecar replace failed"):
        cache.store_prefetch_block(
            claimed.id,
            expected_attempts=claimed.attempts,
            key=_key(),
            byte_range=ByteRange(0, 2),
            data=b"new",
            now=20.0,
        )
    chunks = cache.iter_block(_key(), ByteRange(0, 2), chunk_bytes=3, now=21.0)
    record = store.find_middle_block(_key(), ByteRange(0, 2))

    assert task is not None
    assert chunks is not None
    assert b"".join(chunks) == b"old"
    assert record.created_at == 10.0
    with store._connect() as conn:
        status = conn.execute(
            "SELECT status FROM prefetch_tasks WHERE id = ?",
            (task.id,),
        ).fetchone()["status"]
    assert status == "running"


def test_middle_cache_prefetch_store_rolls_back_when_state_finalizer_fails(
    tmp_path, monkeypatch
):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(
        tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60
    )
    cache.store_block(_key(), ByteRange(0, 2), b"old", now=10.0)
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key=_key(),
        start=0,
        end=2,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)[0]

    def fail_after_publish(*args, publish, **kwargs):
        publish()
        raise RuntimeError("database finalizer failed")

    monkeypatch.setattr(
        store,
        "publish_middle_block_and_complete_prefetch_task",
        fail_after_publish,
    )

    with pytest.raises(RuntimeError, match="database finalizer failed"):
        cache.store_prefetch_block(
            claimed.id,
            expected_attempts=claimed.attempts,
            key=_key(),
            byte_range=ByteRange(0, 2),
            data=b"new",
            now=20.0,
        )
    chunks = cache.iter_block(_key(), ByteRange(0, 2), chunk_bytes=3, now=21.0)
    record = store.find_middle_block(_key(), ByteRange(0, 2))

    assert task is not None
    assert chunks is not None
    assert b"".join(chunks) == b"old"
    assert record.created_at == 10.0
    with store._connect() as conn:
        status = conn.execute(
            "SELECT status FROM prefetch_tasks WHERE id = ?",
            (task.id,),
        ).fetchone()["status"]
    assert status == "running"


def test_middle_cache_prefetch_store_ignores_backup_cleanup_failure_after_commit(
    tmp_path, monkeypatch
):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(
        tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60
    )
    cache.store_block(_key(), ByteRange(0, 2), b"old", now=10.0)
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key=_key(),
        start=0,
        end=2,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    cleanup_calls = 0

    def fail_first_backup_cleanup(backup):
        nonlocal cleanup_calls
        cleanup_calls += 1
        if cleanup_calls == 1:
            raise OSError("backup cleanup failed")
        if backup is not None:
            backup.unlink(missing_ok=True)

    monkeypatch.setattr(cache, "_cleanup_backup", fail_first_backup_cleanup)

    published = cache.store_prefetch_block(
        claimed.id,
        expected_attempts=claimed.attempts,
        key=_key(),
        byte_range=ByteRange(0, 2),
        data=b"new",
        now=20.0,
    )
    chunks = cache.iter_block(_key(), ByteRange(0, 2), chunk_bytes=3, now=21.0)
    record = store.find_middle_block(_key(), ByteRange(0, 2))

    assert task is not None
    assert published is True
    assert cleanup_calls >= 1
    assert chunks is not None
    assert b"".join(chunks) == b"new"
    assert record.created_at == 20.0
    with store._connect() as conn:
        status = conn.execute(
            "SELECT status FROM prefetch_tasks WHERE id = ?",
            (task.id,),
        ).fetchone()["status"]
    assert status == "done"


def test_middle_cache_miss_for_partial_coverage(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    cache.store_block(_key(), ByteRange(1024, 1033), b"0123456789", now=10.0)

    assert cache.iter_block(_key(), ByteRange(1030, 1035), chunk_bytes=2, now=20.0) is None


def test_middle_cache_rejects_invalid_key(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    with pytest.raises(ValueError, match="cache key"):
        cache.store_block("../bad", ByteRange(0, 1), b"ab", now=1.0)


def test_middle_cache_removes_truncated_file(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    cache.store_block(_key(), ByteRange(1024, 1033), b"0123456789", now=10.0)
    block = store.find_middle_block(_key(), ByteRange(1024, 1033))
    (tmp_path / "cache" / block.path).write_bytes(b"short")

    assert cache.iter_block(_key(), ByteRange(1024, 1033), chunk_bytes=4, now=20.0) is None
    assert store.find_middle_block(_key(), ByteRange(1024, 1033)) is None


def test_middle_cache_missing_sidecar_removes_metadata(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    cache.store_block(_key(), ByteRange(1024, 1033), b"0123456789", now=10.0)
    block = store.find_middle_block(_key(), ByteRange(1024, 1033))
    (tmp_path / "cache" / block.path).with_suffix(".range").unlink()

    assert cache.iter_block(_key(), ByteRange(1024, 1033), chunk_bytes=4, now=20.0) is None
    assert store.find_middle_block(_key(), ByteRange(1024, 1033)) is None


def test_middle_cache_evicts_expired_and_lru_blocks(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=15, ttl_seconds=10)

    cache.store_block(_key("a"), ByteRange(0, 9), b"aaaaaaaaaa", now=1.0)
    cache.store_block(_key("b"), ByteRange(0, 9), b"bbbbbbbbbb", now=2.0)
    expired = cache.evict_expired(now=12.0)
    cache.store_block(_key("c"), ByteRange(0, 9), b"cccccccccc", now=13.0)
    cache.store_block(_key("d"), ByteRange(0, 9), b"dddddddddd", now=14.0)
    lru = cache.evict_lru_if_needed()

    assert expired == 2
    assert lru == 1
    assert store.find_middle_block(_key("a"), ByteRange(0, 9)) is None
    assert store.find_middle_block(_key("b"), ByteRange(0, 9)) is None
    assert store.find_middle_block(_key("c"), ByteRange(0, 9)) is None
    assert store.find_middle_block(_key("d"), ByteRange(0, 9)) is not None


def test_middle_cache_does_not_delete_forged_metadata_path(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)
    victim = tmp_path / "victim.txt"
    victim.write_text("keep")
    store.upsert_middle_block(
        MiddleBlockRecord(
            cache_key=_key(),
            start=0,
            end=3,
            path="../victim.txt",
            size=4,
            created_at=1.0,
            last_access_at=1.0,
            expires_at=61.0,
        )
    )

    assert cache.iter_block(_key(), ByteRange(0, 3), chunk_bytes=2, now=2.0) is None
    assert victim.read_text() == "keep"
    assert store.find_middle_block(_key(), ByteRange(0, 3)) is None


def test_middle_cache_evicts_exact_ttl_boundary(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=10)

    cache.store_block(_key(), ByteRange(0, 3), b"data", now=0.0)
    expired = cache.evict_expired(now=10.0)

    assert expired == 1
    assert store.find_middle_block(_key(), ByteRange(0, 3)) is None


def test_middle_cache_iter_treats_expired_record_as_miss(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=10)

    cache.store_block(_key(), ByteRange(0, 3), b"data", now=0.0)
    result = cache.iter_block(_key(), ByteRange(0, 3), chunk_bytes=2, now=10.0)

    assert result is None
    assert store.find_middle_block(_key(), ByteRange(0, 3)) is None
    assert not (tmp_path / "cache" / _key() / "mid" / "0-3.bin").exists()
    assert not (tmp_path / "cache" / _key() / "mid" / "0-3.range").exists()


def test_middle_cache_removes_files_when_metadata_upsert_fails(tmp_path, monkeypatch):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    def fail_upsert(block):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(store, "upsert_middle_block", fail_upsert)

    with pytest.raises(RuntimeError, match="database unavailable"):
        cache.store_block(_key(), ByteRange(0, 3), b"data", now=1.0)

    block_dir = tmp_path / "cache" / _key() / "mid"
    assert not list(block_dir.glob("0-3.*"))


def test_middle_cache_rejects_invalid_ranges(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    with pytest.raises(ValueError, match="byte range"):
        cache.store_block(_key(), ByteRange(-1, 1), b"abc", now=1.0)
    with pytest.raises(ValueError, match="byte range"):
        cache.iter_block(_key(), ByteRange(5, 4), chunk_bytes=2, now=1.0)
