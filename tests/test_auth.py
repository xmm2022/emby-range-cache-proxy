from aiohttp import web
import pytest

from emby_range_cache_proxy.auth import AuthorizationError, EmbyAuthClient
from emby_range_cache_proxy.models import RequestContext


def _ctx(token: str = "user-token") -> RequestContext:
    return RequestContext(
        method="GET",
        raw_path="/emby/videos/151357/original.mkv?MediaSourceId=mediasource_151357&api_key=user-token",
        item_id="151357",
        media_source_id="mediasource_151357",
        token=token,
        extension="mkv",
    )


async def test_authorize_selects_exact_media_source(aiohttp_client):
    async def playback_info(request):
        assert request.query["api_key"] == "user-token"
        assert request.match_info["item_id"] == "151357"
        return web.json_response(
            {
                "MediaSources": [
                    {"Id": "other", "Path": "http://origin/other.mkv", "Protocol": "Http", "Size": 1},
                    {
                        "Id": "mediasource_151357",
                        "Path": "http://origin/movie.mkv",
                        "Protocol": "Http",
                        "Size": 88513978283,
                        "Container": "mkv",
                        "Bitrate": 78740027,
                    },
                ]
            }
        )

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        source = await client.authorize(_ctx())

    assert source.media_source_id == "mediasource_151357"
    assert source.path == "http://origin/movie.mkv"
    assert source.size == 88513978283


async def test_authorize_rejects_missing_media_source(aiohttp_client):
    async def playback_info(request):
        return web.json_response({"MediaSources": []})

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="media source not allowed"):
            await client.authorize(_ctx())


async def test_authorize_rejects_emby_403(aiohttp_client):
    async def playback_info(request):
        return web.Response(status=403)

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="Emby authorization failed"):
            await client.authorize(_ctx())
