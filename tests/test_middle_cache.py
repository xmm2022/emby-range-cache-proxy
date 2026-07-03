import pytest

from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.state import SessionStateStore


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


def test_middle_cache_evicts_expired_and_lru_blocks(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=15, ttl_seconds=10)

    cache.store_block(_key("a"), ByteRange(0, 9), b"aaaaaaaaaa", now=1.0)
    cache.store_block(_key("b"), ByteRange(0, 9), b"bbbbbbbbbb", now=2.0)
    expired = cache.evict_expired(now=12.0)
    cache.store_block(_key("c"), ByteRange(0, 9), b"cccccccccc", now=13.0)
    cache.store_block(_key("d"), ByteRange(0, 9), b"dddddddddd", now=14.0)
    lru = cache.evict_lru_if_needed()

    assert expired == 1
    assert lru == 1
    assert store.find_middle_block(_key("a"), ByteRange(0, 9)) is None
    assert store.find_middle_block(_key("c"), ByteRange(0, 9)) is None
    assert store.find_middle_block(_key("d"), ByteRange(0, 9)) is not None
