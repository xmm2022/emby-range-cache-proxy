import pytest

import emby_range_cache_proxy.cache as cache_module
from emby_range_cache_proxy.cache import CacheReadError, HeadTailCache, adaptive_head_tail, cache_key
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


def _key(char: str) -> str:
    return char * 64


def test_adaptive_sizes():
    assert adaptive_head_tail(1024**3) == (16 * 1024**2, 8 * 1024**2)
    assert adaptive_head_tail(4 * 1024**3) == (32 * 1024**2, 8 * 1024**2)
    assert adaptive_head_tail(12 * 1024**3) == (64 * 1024**2, 8 * 1024**2)
    assert adaptive_head_tail(80 * 1024**3) == (128 * 1024**2, 16 * 1024**2)


def test_adaptive_sizes_at_exact_boundaries():
    gib = 1024**3
    mib = 1024**2

    assert adaptive_head_tail(2 * gib) == (32 * mib, 8 * mib)
    assert adaptive_head_tail(8 * gib) == (64 * mib, 8 * mib)
    assert adaptive_head_tail(30 * gib) == (64 * mib, 8 * mib)
    assert adaptive_head_tail(30 * gib + 1) == (128 * mib, 16 * mib)


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


def test_iter_block_yields_requested_range_in_chunks(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = cache_key(_source(100), _metadata(100))
    cache.store_block(key, "head", ByteRange(0, 9), b"0123456789")

    chunks = cache.iter_block(key, "head", ByteRange(2, 8), chunk_bytes=3)

    assert chunks is not None
    assert list(chunks) == [b"234", b"567", b"8"]


def test_iter_block_rejects_truncated_cache_entry(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = cache_key(_source(100), _metadata(100))
    cache.store_block(key, "head", ByteRange(0, 9), b"0123456789")
    cache.block_path(key, "head").write_bytes(b"01234")

    assert cache.iter_block(key, "head", ByteRange(0, 9), chunk_bytes=3) is None
    assert not cache.block_path(key, "head").exists()
    assert not cache.meta_path(key, "head").exists()


def test_iter_block_raises_cache_error_when_file_is_truncated_during_read(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = cache_key(_source(100), _metadata(100))
    cache.store_block(key, "head", ByteRange(0, 9), b"0123456789")
    chunks = cache.iter_block(key, "head", ByteRange(0, 9), chunk_bytes=3)

    assert chunks is not None
    assert next(chunks) == b"012"
    cache.block_path(key, "head").write_bytes(b"012")
    with pytest.raises(CacheReadError):
        list(chunks)
    assert not cache.block_path(key, "head").exists()
    assert not cache.meta_path(key, "head").exists()


def test_invalid_key_rejected_and_cannot_escape_root(tmp_path):
    cache_root = tmp_path / "cache"
    outside = tmp_path / "escape"
    cache = HeadTailCache(cache_root, max_bytes=1024 * 1024)

    try:
        cache.store_block("../escape", "head", ByteRange(0, 2), b"abc")
    except ValueError:
        pass
    else:
        raise AssertionError("expected invalid traversal key to be rejected")

    assert not outside.exists()


def test_invalid_block_name_rejected_by_block_and_meta_paths(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = _key("a")

    for path_for in (cache.block_path, cache.meta_path):
        try:
            path_for(key, "middle")
        except ValueError:
            pass
        else:
            raise AssertionError("expected invalid block name to be rejected")


def test_malformed_range_returns_none_and_removes_cache_entry(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = _key("a")
    directory = tmp_path / key
    directory.mkdir()
    block = directory / "head.bin"
    meta = directory / "head.range"
    block.write_bytes(b"0123456789")
    meta.write_text("not-a-range\n")

    assert cache.read_block(key, "head", ByteRange(0, 1)) is None
    assert not block.exists()
    assert not meta.exists()


def test_store_block_rejects_short_data_for_declared_range(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = _key("a")

    try:
        cache.store_block(key, "head", ByteRange(0, 9), b"short")
    except ValueError:
        pass
    else:
        raise AssertionError("expected short data to be rejected")

    assert not (tmp_path / key).exists()


def test_stage_block_commits_complete_data(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = _key("a")

    writer = cache.stage_block(key, "head", ByteRange(0, 9))
    writer.write(b"01234")
    writer.write(b"56789")
    writer.commit()

    assert cache.read_block(key, "head", ByteRange(2, 5)) == b"2345"


def test_stage_block_rejects_short_data_and_removes_temp(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = _key("a")

    writer = cache.stage_block(key, "head", ByteRange(0, 9))
    writer.write(b"short")
    try:
        writer.commit()
    except ValueError:
        pass
    else:
        raise AssertionError("expected short staged data to be rejected")

    assert cache.read_block(key, "head", ByteRange(0, 4)) is None
    assert list(tmp_path.glob("*/*.tmp")) == []


def test_stage_block_commit_failure_during_bin_replace_leaves_no_entry(monkeypatch, tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = _key("a")
    original_replace = cache_module.os.replace

    def fail_bin_replace(src, dst):
        if str(dst).endswith("head.bin"):
            raise OSError("simulated bin replace failure")
        original_replace(src, dst)

    monkeypatch.setattr(cache_module.os, "replace", fail_bin_replace)
    writer = cache.stage_block(key, "head", ByteRange(0, 9))
    writer.write(b"0123456789")

    try:
        writer.commit()
    except OSError:
        pass
    else:
        raise AssertionError("expected commit failure")

    assert cache.read_block(key, "head", ByteRange(0, 9)) is None
    assert not cache.block_path(key, "head").exists()
    assert not cache.meta_path(key, "head").exists()


def test_stage_block_commit_failure_during_meta_replace_removes_bin_and_range(monkeypatch, tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = _key("a")
    original_replace = cache_module.os.replace

    def fail_meta_replace(src, dst):
        if str(dst).endswith("head.range"):
            raise OSError("simulated meta replace failure")
        original_replace(src, dst)

    monkeypatch.setattr(cache_module.os, "replace", fail_meta_replace)
    writer = cache.stage_block(key, "head", ByteRange(0, 9))
    writer.write(b"0123456789")

    try:
        writer.commit()
    except OSError:
        pass
    else:
        raise AssertionError("expected commit failure")

    assert cache.read_block(key, "head", ByteRange(0, 9)) is None
    assert not cache.block_path(key, "head").exists()
    assert not cache.meta_path(key, "head").exists()


def test_evict_lru(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=12)
    oldest = _key("a")
    newest = _key("b")

    cache.store_block(oldest, "head", ByteRange(0, 9), b"0123456789")
    cache.store_block(newest, "head", ByteRange(0, 9), b"abcdefghij")
    cache.evict_if_needed()

    remaining = sorted(path.parent.name for path in tmp_path.glob("*/*.bin"))
    assert remaining == [newest]
    assert not (tmp_path / oldest / "head.range").exists()
