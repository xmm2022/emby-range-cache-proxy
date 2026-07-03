from aiohttp import web

import emby_range_cache_proxy.prewarm as prewarm_module
from emby_range_cache_proxy.config import Config, PrewarmConfig, RolloutConfig
from emby_range_cache_proxy.prewarm import PrewarmWorker


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
