from emby_range_cache_proxy.config import PathMapping
from emby_range_cache_proxy.models import MediaSource
from emby_range_cache_proxy.sources import resolve_media_source


def test_http_media_source_is_left_unchanged():
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="http://127.0.0.1:18096/movie.mkv",
        protocol="Http",
        size=100,
    )

    assert resolve_media_source(source, ()) == source


def test_strm_media_source_uses_configured_path_mapping(tmp_path):
    host_root = tmp_path / "strm"
    strm_file = host_root / "movie.strm"
    strm_file.parent.mkdir()
    strm_file.write_text("\n# comment\nhttp://127.0.0.1:18096/movie.mkv\n")
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="/strm/movie.strm",
        protocol="File",
        size=100,
    )

    resolved = resolve_media_source(
        source,
        (PathMapping("/strm/", str(host_root)),),
        url_prefix_allowlist=("http://127.0.0.1:18096/",),
    )

    assert resolved.path == "http://127.0.0.1:18096/movie.mkv"
    assert resolved.item_id == source.item_id
    assert resolved.media_source_id == source.media_source_id


def test_strm_media_source_without_readable_mapping_is_left_unchanged(tmp_path):
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="/strm/missing.strm",
        protocol="File",
        size=100,
    )

    assert resolve_media_source(source, (PathMapping("/strm/", str(tmp_path / "missing")),)) == source


def test_strm_media_source_without_allowed_url_prefix_is_left_unchanged(tmp_path):
    host_root = tmp_path / "strm"
    host_root.mkdir()
    (host_root / "movie.strm").write_text("http://127.0.0.1:18096/movie.mkv\n")
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="/strm/movie.strm",
        protocol="File",
        size=100,
    )

    assert resolve_media_source(source, (PathMapping("/strm/", str(host_root)),)) == source


def test_strm_media_source_rejects_unallowed_url_prefix(tmp_path):
    host_root = tmp_path / "strm"
    host_root.mkdir()
    (host_root / "movie.strm").write_text("http://169.254.169.254/latest/meta-data\n")
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="/strm/movie.strm",
        protocol="File",
        size=100,
    )

    resolved = resolve_media_source(
        source,
        (PathMapping("/strm/", str(host_root)),),
        url_prefix_allowlist=("http://127.0.0.1:18096/",),
    )

    assert resolved == source


def test_path_mapping_without_trailing_slash_does_not_match_sibling(tmp_path):
    host_root = tmp_path / "strm"
    sibling = host_root / "_evil"
    sibling.mkdir(parents=True)
    (sibling / "movie.strm").write_text("http://127.0.0.1:18096/movie.mkv\n")
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="/strm_evil/movie.strm",
        protocol="File",
        size=100,
    )

    resolved = resolve_media_source(
        source,
        (PathMapping("/strm", str(host_root)),),
        url_prefix_allowlist=("http://127.0.0.1:18096/",),
    )

    assert resolved == source


def test_path_mapping_rejects_parent_traversal(tmp_path):
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="/strm/../secret.strm",
        protocol="File",
        size=100,
    )

    assert resolve_media_source(source, (PathMapping("/strm/", str(tmp_path)),)) == source


def test_strm_reader_ignores_url_after_read_limit(tmp_path):
    host_root = tmp_path / "strm"
    host_root.mkdir()
    (host_root / "movie.strm").write_text("#" + ("x" * 20000) + "\nhttp://127.0.0.1:18096/movie.mkv\n")
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="/strm/movie.strm",
        protocol="File",
        size=100,
    )

    resolved = resolve_media_source(
        source,
        (PathMapping("/strm/", str(host_root)),),
        url_prefix_allowlist=("http://127.0.0.1:18096/",),
    )

    assert resolved == source
