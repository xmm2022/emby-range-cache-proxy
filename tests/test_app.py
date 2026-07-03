from aiohttp import web

import emby_range_cache_proxy.app as app_module
from emby_range_cache_proxy.auth import AuthorizationError
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
    origin_get_calls = 0

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
        nonlocal origin_get_calls
        origin_get_calls += 1
        if origin_get_calls > 1:
            return web.Response(status=500, body=b"origin should not be hit after cache fill")
        assert request.headers["Range"] == "bytes=0-9"
        return web.Response(status=206, body=b"0123456789", headers={"Content-Range": "bytes 0-9/100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
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

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
    assert origin_get_calls == 1


async def test_authorized_head_range_returns_headers_without_origin_get(aiohttp_client, tmp_path):
    origin_get_calls = 0
    origin_head_calls = 0

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
        nonlocal origin_get_calls
        origin_get_calls += 1
        return web.Response(status=500, body=b"origin GET must not be called for HEAD")

    async def origin_head(request):
        nonlocal origin_head_calls
        origin_head_calls += 1
        return web.Response(headers={"Content-Length": "100", "ETag": "etag"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
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

    response = await client.head("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 0-9/100"
    assert response.headers["Content-Length"] == "10"
    assert response.headers["ETag"] == "etag"
    assert await response.read() == b""
    assert origin_head_calls == 1
    assert origin_get_calls == 0


async def test_origin_ignoring_range_falls_back_before_proxy_response(aiohttp_client, tmp_path):
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

    async def fallback(request):
        return web.Response(status=206, body=b"emby", headers={"Content-Range": "bytes 0-3/4"})

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-3"
        return web.Response(status=200, body=b"0123456789")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
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

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})

    assert response.status == 206
    assert response.headers["Content-Range"] == "bytes 0-3/4"
    assert await response.read() == b"emby"


async def test_cache_evict_error_after_origin_stream_does_not_fallback(aiohttp_client, tmp_path):
    fallback_calls = 0

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

    async def fallback(request):
        nonlocal fallback_calls
        fallback_calls += 1
        return web.Response(status=206, body=b"fallback", headers={"Content-Range": "bytes 0-7/8"})

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-9"
        return web.Response(status=206, body=b"0123456789", headers={"Content-Range": "bytes 0-9/100"})

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )

    def fail_evict():
        raise OSError("simulated evict failure")

    app["cache"].evict_if_needed = fail_evict
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
    assert fallback_calls == 0


async def test_matching_path_prefix_is_evaluated_after_authorization(aiohttp_client, tmp_path):
    playback_info_calls = 0

    async def playback_info(request):
        nonlocal playback_info_calls
        playback_info_calls += 1
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
        return web.Response(status=206, body=b"0123456789", headers={"Content-Range": "bytes 0-9/100"})

    async def fallback(request):
        return web.Response(body=b"fallback")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(
                enabled=True,
                item_allowlist={"1"},
                path_prefix_allowlist=(str(origin_server.make_url("")),),
            ),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
    assert playback_info_calls == 1


async def test_non_matching_path_prefix_falls_back_after_authorization(aiohttp_client, tmp_path):
    playback_info_calls = 0

    async def playback_info(request):
        nonlocal playback_info_calls
        playback_info_calls += 1
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

    async def fallback(request):
        return web.Response(status=206, body=b"fallback", headers={"Content-Range": "bytes 0-7/8"})

    async def origin(request):
        return web.Response(status=500, body=b"origin must not be read")

    async def origin_head(request):
        return web.Response(headers={"Content-Length": "100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", origin_head)
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(
                enabled=True,
                item_allowlist={"1"},
                path_prefix_allowlist=("http://does-not-match/",),
            ),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-7"})

    assert response.status == 206
    assert await response.read() == b"fallback"
    assert playback_info_calls == 1


async def test_authorization_error_returns_403_without_origin_or_fallback(aiohttp_client, tmp_path):
    async def playback_info(request):
        return web.Response(status=403)

    async def fallback(request):
        raise AssertionError("fallback must not be read after explicit authorization failure")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)

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

    assert response.status == 403
    assert await response.text() == "forbidden\n"


async def test_authorization_timeout_error_returns_403_without_fallback(aiohttp_client, monkeypatch, tmp_path):
    class FakeAuthClient:
        def __init__(self, base_url):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def authorize(self, ctx):
            raise AuthorizationError("Emby authorization failed: timeout")

    async def fallback(request):
        raise AssertionError("fallback must not be read after AuthorizationError")

    fallback_app = web.Application()
    fallback_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    fallback_server = await aiohttp_client(fallback_app)
    monkeypatch.setattr(app_module, "EmbyAuthClient", FakeAuthClient)

    app = create_app(
        Config(
            emby_base_url=str(fallback_server.make_url("")),
            fallback_base_url=str(fallback_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 403
    assert await response.text() == "forbidden\n"
