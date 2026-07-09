from aiohttp import web

import emby_range_cache_proxy.prewarm as prewarm_module
from emby_range_cache_proxy.cache import HeadTailCache, cache_key
from emby_range_cache_proxy.config import Config, PathMapping, PrewarmConfig, RolloutConfig
from emby_range_cache_proxy.models import ByteRange, MediaSource, SourceMetadata
from emby_range_cache_proxy.prewarm import PrewarmWorker


async def test_prewarm_uses_configured_playback_info_timeout(monkeypatch, tmp_path):
    captured = {}

    class FakeSession:
        def __init__(self, *, timeout):
            captured["timeout"] = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def get(self, *args, **kwargs):
            raise AssertionError("timeout should be asserted before network request")

    monkeypatch.setattr(prewarm_module, "ClientSession", FakeSession)
    worker = PrewarmWorker(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path),
            prewarm_api_key="internal",
            prewarm=PrewarmConfig(enabled=True, playback_info_timeout_seconds=19),
        )
    )

    try:
        await worker.run_once()
    except AssertionError:
        pass

    assert captured["timeout"].total == 19


async def test_prewarm_uses_internal_key_and_builds_head_tail(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))

    async def items(request):
        assert request.query["api_key"] == "internal"
        return web.json_response({"Items": [{"Id": "1"}]})

    async def playback_info(request):
        assert request.query["api_key"] == "internal"
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        range_header = request.headers["Range"]
        if range_header == "bytes=0-15":
            return web.Response(
                status=206,
                body=b"0123456789abcdef",
                headers={"Content-Range": "bytes 0-15/100"},
            )
        if range_header == "bytes=96-99":
            return web.Response(
                status=206,
                body=b"wxyz",
                headers={"Content-Range": "bytes 96-99/100"},
            )
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}, media_source_allowlist={"ms1"}),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=1),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    assert result.scanned == 1
    assert result.prewarmed == 1


async def test_prewarm_resolves_strm_with_path_mapping_and_url_prefix(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))
    strm_root = tmp_path / "strm"
    strm_root.mkdir()

    async def items(request):
        return web.json_response({"Items": [{"Id": "1"}]})

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": "/strm/movie.strm",
                        "Protocol": "File",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        if request.headers["Range"] == "bytes=0-15":
            return web.Response(status=206, body=b"0123456789abcdef", headers={"Content-Range": "bytes 0-15/100"})
        if request.headers["Range"] == "bytes=96-99":
            return web.Response(status=206, body=b"wxyz", headers={"Content-Range": "bytes 96-99/100"})
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)
    (strm_root / "movie.strm").write_text(f"{origin_server.make_url('/movie.mkv')}\n")

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        path_mappings=(PathMapping("/strm/", str(strm_root)),),
        rollout=RolloutConfig(
            enabled=True,
            item_allowlist={"1"},
            media_source_allowlist={"ms1"},
            path_prefix_allowlist=(str(origin_server.make_url("")),),
        ),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=1),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    assert result.scanned == 1
    assert result.prewarmed == 1


async def test_prewarm_skips_non_dict_items_and_continues(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))

    async def items(request):
        assert request.query["api_key"] == "internal"
        return web.json_response({"Items": ["bad", {"Id": "1"}]})

    async def playback_info(request):
        assert request.query["api_key"] == "internal"
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        range_header = request.headers["Range"]
        if range_header == "bytes=0-15":
            return web.Response(
                status=206,
                body=b"0123456789abcdef",
                headers={"Content-Range": "bytes 0-15/100"},
            )
        if range_header == "bytes=96-99":
            return web.Response(
                status=206,
                body=b"wxyz",
                headers={"Content-Range": "bytes 96-99/100"},
            )
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}, media_source_allowlist={"ms1"}),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=2),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    assert result.scanned == 2
    assert result.prewarmed == 1
    assert result.skipped == 1


async def test_prewarm_uses_origin_head_metadata_for_runtime_cache_key(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))
    head_seen = False

    async def items(request):
        return web.json_response({"Items": [{"Id": "1"}]})

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        nonlocal head_seen
        headers = {
            "Content-Length": "100",
            "ETag": '"etag-from-origin"',
            "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
        }
        if request.method == "HEAD":
            head_seen = True
            return web.Response(headers=headers)
        if request.headers["Range"] == "bytes=0-15":
            return web.Response(
                status=206,
                body=b"0123456789abcdef",
                headers={"Content-Range": "bytes 0-15/100"},
            )
        if request.headers["Range"] == "bytes=96-99":
            return web.Response(
                status=206,
                body=b"wxyz",
                headers={"Content-Range": "bytes 96-99/100"},
            )
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}, media_source_allowlist={"ms1"}),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=1),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path=str(origin_server.make_url("/movie.mkv")),
        protocol="Http",
        size=100,
        container="mkv",
    )
    metadata = SourceMetadata(
        url=str(origin_server.make_url("/movie.mkv")),
        size=100,
        etag='"etag-from-origin"',
        last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
    )
    cache = HeadTailCache(tmp_path, max_bytes=config.cache.max_bytes)
    key = cache_key(source, metadata)

    assert result.prewarmed == 1
    assert head_seen is True
    assert cache.read_block(key, "head", ByteRange(0, 15)) == b"0123456789abcdef"
    assert cache.read_block(key, "tail", ByteRange(96, 99)) == b"wxyz"


async def test_prewarm_item_queries_selected_media_source_and_builds_head_tail(
    aiohttp_client, tmp_path, monkeypatch
):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))
    range_requests = []

    async def playback_info(request):
        assert request.query["api_key"] == "internal"
        assert request.query["MediaSourceId"] == "ms1"
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "other",
                        "Path": "http://example.invalid/other.mkv",
                        "Protocol": "Http",
                        "Size": 100,
                    },
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                    },
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        range_requests.append(request.headers["Range"])
        if request.headers["Range"] == "bytes=0-15":
            return web.Response(status=206, body=b"0123456789abcdef", headers={"Content-Range": "bytes 0-15/100"})
        if request.headers["Range"] == "bytes=96-99":
            return web.Response(status=206, body=b"wxyz", headers={"Content-Range": "bytes 96-99/100"})
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}, media_source_allowlist={"ms1"}),
    )
    worker = PrewarmWorker(config)

    result = await worker.prewarm_item("1", "ms1")

    assert result.scanned == 1
    assert result.prewarmed == 1
    assert result.skipped == 0
    assert range_requests == ["bytes=0-15", "bytes=96-99"]


async def test_prewarm_item_skips_complete_existing_head_tail(
    aiohttp_client, tmp_path, monkeypatch
):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))
    range_requests = []

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                    }
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": "100",
                    "ETag": '"stable"',
                    "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
                }
            )
        range_requests.append(request.headers["Range"])
        return web.Response(status=500)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}, media_source_allowlist={"ms1"}),
    )
    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path=str(origin_server.make_url("/movie.mkv")),
        protocol="Http",
        size=100,
    )
    metadata = SourceMetadata(
        url=str(origin_server.make_url("/movie.mkv")),
        size=100,
        etag='"stable"',
        last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
    )
    key = cache_key(source, metadata)
    cache = HeadTailCache(tmp_path, max_bytes=config.cache.max_bytes)
    cache.store_block(key, "head", ByteRange(0, 15), b"0123456789abcdef")
    cache.store_block(key, "tail", ByteRange(96, 99), b"wxyz")
    worker = PrewarmWorker(config)

    result = await worker.prewarm_item("1", "ms1")

    assert result.scanned == 1
    assert result.prewarmed == 0
    assert result.skipped == 1
    assert range_requests == []


async def test_prewarm_item_resolves_strm_with_path_mapping_and_url_prefix(
    aiohttp_client, tmp_path, monkeypatch
):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))
    strm_root = tmp_path / "strm"
    strm_root.mkdir()

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": "/strm/movie.strm",
                        "Protocol": "File",
                        "Size": 100,
                    }
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        if request.headers["Range"] == "bytes=0-15":
            return web.Response(status=206, body=b"0123456789abcdef", headers={"Content-Range": "bytes 0-15/100"})
        if request.headers["Range"] == "bytes=96-99":
            return web.Response(status=206, body=b"wxyz", headers={"Content-Range": "bytes 96-99/100"})
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)
    (strm_root / "movie.strm").write_text(f"{origin_server.make_url('/movie.mkv')}\n")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        path_mappings=(PathMapping("/strm/", str(strm_root)),),
        rollout=RolloutConfig(
            enabled=True,
            item_allowlist={"1"},
            media_source_allowlist={"ms1"},
            path_prefix_allowlist=(str(origin_server.make_url("")),),
        ),
    )
    worker = PrewarmWorker(config)

    result = await worker.prewarm_item("1", "ms1")

    assert result.scanned == 1
    assert result.prewarmed == 1
    assert result.skipped == 0


async def test_prewarm_item_downloads_through_bandwidth_limiter(
    aiohttp_client, tmp_path, monkeypatch
):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))
    consumed = []

    class RecordingLimiter:
        async def consume(self, byte_count):
            consumed.append(byte_count)

    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                    }
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        if request.headers["Range"] == "bytes=0-15":
            return web.Response(status=206, body=b"0123456789abcdef", headers={"Content-Range": "bytes 0-15/100"})
        if request.headers["Range"] == "bytes=96-99":
            return web.Response(status=206, body=b"wxyz", headers={"Content-Range": "bytes 96-99/100"})
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}, media_source_allowlist={"ms1"}),
    )
    worker = PrewarmWorker(config)
    worker.limiter = RecordingLimiter()

    result = await worker.prewarm_item("1", "ms1")

    assert result.prewarmed == 1
    assert consumed == [16, 4]


async def test_prewarm_skips_non_json_playback_info_and_continues(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(prewarm_module, "adaptive_head_tail", lambda size: (16, 4))

    async def items(request):
        return web.json_response({"Items": [{"Id": "bad"}, {"Id": "1"}]})

    async def playback_info(request):
        if request.match_info["item_id"] == "bad":
            return web.Response(text="api_key=internal")
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                    }
                ]
            }
        )

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        if request.headers["Range"] == "bytes=0-15":
            return web.Response(status=206, body=b"0123456789abcdef", headers={"Content-Range": "bytes 0-15/100"})
        if request.headers["Range"] == "bytes=96-99":
            return web.Response(status=206, body=b"wxyz", headers={"Content-Range": "bytes 96-99/100"})
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, media_source_allowlist={"ms1"}),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=2),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    assert result.scanned == 2
    assert result.prewarmed == 1
    assert result.skipped == 1


async def test_prewarm_recent_items_bad_status_does_not_expose_internal_key(aiohttp_client, tmp_path):
    async def items(request):
        return web.Response(status=500, text="api_key=internal")

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=1),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    assert result.scanned == 0
    assert result.prewarmed == 0
    assert "internal" not in str(result)
    assert "api_key" not in str(result)


async def test_prewarm_top_level_malformed_payload_is_empty(aiohttp_client, tmp_path):
    async def items(request):
        return web.json_response(["bad"])

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=1),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    assert result.scanned == 0
    assert result.prewarmed == 0
    assert result.skipped == 0
