from emby_range_cache_proxy.cache import HeadTailCache, adaptive_head_tail, cache_key
from emby_range_cache_proxy.models import ByteRange, MediaSource, SourceMetadata


def _source(size: int = 100) -> MediaSource:
    return MediaSource(
        item_id="151357",
        media_source_id="mediasource_151357",
        path="http://origin/movie.mkv",
        protocol="Http",
        size=size,
        container="mkv",
    )


def _metadata(size: int = 100) -> SourceMetadata:
    return SourceMetadata(url="http://origin/movie.mkv", size=size, etag="etag", last_modified="date")


def test_adaptive_sizes():
    assert adaptive_head_tail(1024**3) == (16 * 1024**2, 4 * 1024**2)
    assert adaptive_head_tail(4 * 1024**3) == (32 * 1024**2, 8 * 1024**2)
    assert adaptive_head_tail(12 * 1024**3) == (64 * 1024**2, 8 * 1024**2)
    assert adaptive_head_tail(80 * 1024**3) == (128 * 1024**2, 16 * 1024**2)


def test_cache_key_changes_when_size_changes():
    left = cache_key(_source(100), _metadata(100))
    right = cache_key(_source(101), _metadata(101))

    assert left != right
    assert "http://origin" not in left


def test_store_and_read_head_block(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = cache_key(_source(100), _metadata(100))

    cache.store_block(key, "head", ByteRange(0, 9), b"0123456789")

    assert cache.read_block(key, "head", ByteRange(2, 5)) == b"2345"
    assert cache.read_block(key, "head", ByteRange(10, 11)) is None


def test_evict_lru(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=12)

    cache.store_block("a", "head", ByteRange(0, 9), b"0123456789")
    cache.store_block("b", "head", ByteRange(0, 9), b"abcdefghij")
    cache.evict_if_needed()

    remaining = sorted(path.name for path in tmp_path.glob("*/*.bin"))
    assert len(remaining) == 1
