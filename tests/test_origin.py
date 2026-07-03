from aiohttp import web

from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.origin import OriginClient


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
