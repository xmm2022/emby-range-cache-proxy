from aiohttp import web

from emby_range_cache_proxy.app import create_app
from emby_range_cache_proxy.config import Config, RolloutConfig


async def test_healthz(aiohttp_client, tmp_path):
    app = create_app(Config(emby_base_url="http://emby", fallback_base_url="http://emby", cache_dir=str(tmp_path)))
    client = await aiohttp_client(app)

    response = await client.get("/healthz")

    assert response.status == 200
    assert await response.text() == "ok\n"


async def test_out_of_scope_falls_back_to_emby(aiohttp_client, tmp_path):
    async def fallback(request):
        return web.Response(status=206, body=b"emby", headers={"Content-Range": "bytes 0-3/4"})

    fallback_app = web.Application()
    fallback_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    fallback_server = await aiohttp_client(fallback_app)

    app = create_app(
        Config(
            emby_base_url=str(fallback_server.make_url("")),
            fallback_base_url=str(fallback_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=False),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})

    assert response.status == 206
    assert await response.read() == b"emby"


async def test_authorized_head_range_is_served_and_cached(aiohttp_client, tmp_path):
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
        assert request.headers["Range"] == "bytes=0-9"
        return web.Response(status=206, body=b"0123456789", headers={"Content-Range": "bytes 0-9/100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", lambda request: web.Response(body=b"fallback"))
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", lambda request: web.Response(headers={"Content-Length": "100"}))
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
