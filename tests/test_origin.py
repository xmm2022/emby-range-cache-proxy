import asyncio

import pytest
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError

from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.origin import OriginClient, OriginError


async def test_origin_head_reads_size_and_validators(aiohttp_client):
    async def handler(request):
        return web.Response(
            status=200,
            headers={"Content-Length": "100", "ETag": "abc", "Last-Modified": "Fri, 03 Jul 2026 00:00:00 GMT"},
        )

    app = web.Application()
    app.router.add_head("/movie.mkv", handler)
    server = await aiohttp_client(app)

    async with OriginClient() as client:
        metadata = await client.head(str(server.make_url("/movie.mkv")))

    assert metadata.size == 100
    assert metadata.etag == "abc"
    assert metadata.last_modified == "Fri, 03 Jul 2026 00:00:00 GMT"


async def test_stream_range_requests_exact_bytes(aiohttp_client):
    body = b"0123456789"

    async def handler(request):
        assert request.headers["Range"] == "bytes=2-5"
        return web.Response(status=206, body=body[2:6], headers={"Content-Range": "bytes 2-5/10"})

    app = web.Application()
    app.router.add_get("/movie.mkv", handler)
    server = await aiohttp_client(app)

    chunks = []
    async with OriginClient(chunk_bytes=2) as client:
        async for chunk in client.stream_range(str(server.make_url("/movie.mkv")), ByteRange(2, 5)):
            chunks.append(chunk)

    assert b"".join(chunks) == b"2345"


class FakeOriginResponse:
    status = 200
    url = "https://origin.example/movie.mkv"

    def __init__(self, headers):
        self.headers = headers

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class FakeHeadSession:
    def __init__(self, headers):
        self.headers = headers

    def head(self, url, *, allow_redirects):
        return FakeOriginResponse(self.headers)

    async def close(self):
        return None


async def test_head_rejects_non_integer_content_length(monkeypatch):
    client = OriginClient()
    monkeypatch.setattr(client, "_session", FakeHeadSession({"Content-Length": "abc"}))

    with pytest.raises(OriginError, match="origin provided invalid Content-Length"):
        await client.head("https://origin.example/movie.mkv")


async def test_head_rejects_negative_content_length(monkeypatch):
    client = OriginClient()
    monkeypatch.setattr(client, "_session", FakeHeadSession({"Content-Length": "-1"}))

    with pytest.raises(OriginError, match="origin provided invalid Content-Length"):
        await client.head("https://origin.example/movie.mkv")


async def test_head_wraps_client_error_without_leaking_url(monkeypatch):
    secret_url = "https://origin.example/movie.mkv?api_key=secret-token"

    class FakeSession:
        def head(self, url, *, allow_redirects):
            raise ClientConnectionError(f"cannot connect to {url}")

        async def close(self):
            return None

    client = OriginClient()
    monkeypatch.setattr(client, "_session", FakeSession())

    with pytest.raises(OriginError) as error:
        await client.head(secret_url)

    assert str(error.value) == "origin HEAD failed: client error"
    assert "secret-token" not in str(error.value)
    assert secret_url not in str(error.value)


async def test_stream_range_wraps_timeout_without_leaking_url(monkeypatch):
    secret_url = "https://origin.example/movie.mkv?api_key=secret-token"

    class FakeSession:
        def get(self, url, *, headers, allow_redirects):
            raise asyncio.TimeoutError(f"timed out reading {url}")

        async def close(self):
            return None

    client = OriginClient()
    monkeypatch.setattr(client, "_session", FakeSession())

    with pytest.raises(OriginError) as error:
        async for _ in client.stream_range(secret_url, ByteRange(0, 1)):
            pass

    assert str(error.value) == "origin range GET failed: timeout"
    assert "secret-token" not in str(error.value)
    assert secret_url not in str(error.value)


async def test_head_bad_status_raises_origin_error(aiohttp_client):
    async def handler(request):
        return web.Response(status=404)

    app = web.Application()
    app.router.add_head("/movie.mkv", handler)
    server = await aiohttp_client(app)

    async with OriginClient() as client:
        with pytest.raises(OriginError, match="origin HEAD failed: status=404"):
            await client.head(str(server.make_url("/movie.mkv")))


async def test_client_cannot_be_reused_after_context_exit():
    client = OriginClient()

    async with client:
        pass

    with pytest.raises(RuntimeError, match="OriginClient must be used as an async context manager"):
        await client.head("https://origin.example/movie.mkv")


async def test_client_timeout_does_not_set_total_stream_timeout():
    async with OriginClient(timeout_seconds=7.5) as client:
        assert client._session is not None
        timeout = client._session.timeout

    assert timeout.total is None
    assert timeout.sock_connect == 7.5
    assert timeout.sock_read == 7.5
