import asyncio

from aiohttp import web
import pytest

from emby_range_cache_proxy.auth import AuthUnavailable, AuthorizationError, EmbyAuthClient
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
        assert request.query["MediaSourceId"] == "mediasource_151357"
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


async def test_authorize_rejects_non_json_without_leaking_token(aiohttp_client):
    async def playback_info(request):
        return web.Response(text="not json", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="invalid PlaybackInfo response") as exc_info:
            await client.authorize(_ctx(token="secret-token"))

    message = str(exc_info.value)
    assert "secret-token" not in message
    assert "api_key" not in message


async def test_authorize_wraps_timeout_as_auth_unavailable_without_leaking_token(aiohttp_client):
    async def playback_info(request):
        await asyncio.sleep(0.05)
        return web.json_response({"MediaSources": []})

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url("")), timeout_seconds=0.001) as client:
        with pytest.raises(AuthUnavailable, match="Emby authorization unavailable: timeout") as exc_info:
            await client.authorize(_ctx())

    message = str(exc_info.value)
    assert "api_key" not in message
    assert "user-token" not in message
    assert str(server.make_url("")) not in message


async def test_authorize_wraps_client_error_as_auth_unavailable_without_leaking_token(unused_tcp_port):
    base_url = f"http://127.0.0.1:{unused_tcp_port}"

    async with EmbyAuthClient(base_url, timeout_seconds=0.1) as client:
        with pytest.raises(AuthUnavailable, match="Emby authorization unavailable: client error") as exc_info:
            await client.authorize(_ctx(token="secret-token"))

    message = str(exc_info.value)
    assert "api_key" not in message
    assert "secret-token" not in message
    assert base_url not in message


@pytest.mark.parametrize(
    "payload",
    [
        {"MediaSources": None},
        {"MediaSources": ["not a media source"]},
    ],
)
async def test_authorize_rejects_malformed_media_sources(aiohttp_client, payload):
    async def playback_info(request):
        return web.json_response(payload)

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="invalid PlaybackInfo response"):
            await client.authorize(_ctx())


@pytest.mark.parametrize(
    "field",
    ["Size", "Bitrate"],
)
async def test_authorize_rejects_bad_numeric_fields(aiohttp_client, field):
    source = {
        "Id": "mediasource_151357",
        "Path": "http://origin/movie.mkv",
        "Protocol": "Http",
        "Size": 88513978283,
        "Bitrate": 78740027,
    }
    source[field] = "not-an-int"

    async def playback_info(request):
        return web.json_response({"MediaSources": [source]})

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match=f"invalid media source {field}"):
            await client.authorize(_ctx())


async def test_authorize_rejects_missing_media_source_path(aiohttp_client):
    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {"Id": "mediasource_151357", "Protocol": "Http", "Size": 88513978283}
                ]
            }
        )

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="media source path is empty"):
            await client.authorize(_ctx())


async def test_authorize_rejects_non_string_media_source_path(aiohttp_client):
    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "mediasource_151357",
                        "Path": 123,
                        "Protocol": "Http",
                        "Size": 88513978283,
                    }
                ]
            }
        )

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="media source path is invalid"):
            await client.authorize(_ctx())


async def test_authorize_after_exit_requires_context_manager(aiohttp_client):
    async def playback_info(request):
        return web.json_response({"MediaSources": []})

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    client = EmbyAuthClient(str(server.make_url("")))
    async with client:
        pass

    with pytest.raises(RuntimeError, match="async context manager"):
        await client.authorize(_ctx())


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
