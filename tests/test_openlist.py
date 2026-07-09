from aiohttp import web

from emby_range_cache_proxy.config import OpenListConfig
from emby_range_cache_proxy.models import MediaSource
from emby_range_cache_proxy.openlist import (
    openlist_path_from_source,
    resolve_openlist_media_source,
)


def test_openlist_path_from_pseudo_url():
    assert openlist_path_from_source("openlist:///Movies/a.mkv", "https://openlist.example") == "/Movies/a.mkv"
    assert openlist_path_from_source("openlist://Movies/a.mkv", "https://openlist.example") == "/Movies/a.mkv"


def test_openlist_path_from_download_url_with_base_path():
    value = "https://example.test/list/d/Movies/%E7%89%87.mkv?sign=old"

    assert openlist_path_from_source(value, "https://example.test/list") == "/Movies/片.mkv"


def test_openlist_path_rejects_traversal():
    assert openlist_path_from_source("openlist:///Movies/../secret.mkv", "https://openlist.example") is None
    assert openlist_path_from_source("https://evil.example/d/movie.mkv", "https://openlist.example") is None


async def test_resolve_openlist_media_source_uses_fs_get_sign(aiohttp_client):
    seen = {}

    async def fs_get(request):
        seen["authorization"] = request.headers.get("Authorization")
        seen["payload"] = await request.json()
        return web.json_response(
            {
                "code": 200,
                "message": "success",
                "data": {
                    "name": "a.mkv",
                    "size": 123,
                    "is_dir": False,
                    "sign": "abc=:0",
                    "raw_url": "https://temporary.example/a.mkv",
                },
            }
        )

    app = web.Application()
    app.router.add_post("/api/fs/get", fs_get)
    server = await aiohttp_client(app)
    base_url = str(server.make_url("/")).rstrip("/")

    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path="openlist:///Movies/a.mkv",
        protocol="OpenList",
        size=None,
    )

    resolved = await resolve_openlist_media_source(
        source,
        OpenListConfig(enabled=True, base_url=base_url, token="openlist-token"),
    )

    assert seen["authorization"] == "openlist-token"
    assert seen["payload"] == {"path": "/Movies/a.mkv", "password": ""}
    assert resolved.path == f"{base_url}/d/Movies/a.mkv?sign=abc%3D%3A0"
    assert resolved.protocol == "Http"


async def test_resolve_openlist_media_source_falls_back_to_raw_url(aiohttp_client):
    async def fs_get(request):
        return web.json_response(
            {
                "code": 200,
                "data": {
                    "name": "a.mkv",
                    "size": 123,
                    "is_dir": False,
                    "raw_url": "/p/Movies/a.mkv?sign=proxy",
                },
            }
        )

    app = web.Application()
    app.router.add_post("/api/fs/get", fs_get)
    server = await aiohttp_client(app)
    base_url = str(server.make_url("/")).rstrip("/")

    source = MediaSource(
        item_id="1",
        media_source_id="ms1",
        path=f"{base_url}/d/Movies/a.mkv?sign=old",
        protocol="Http",
        size=None,
    )

    resolved = await resolve_openlist_media_source(
        source,
        OpenListConfig(enabled=True, base_url=base_url),
    )

    assert resolved.path == f"{base_url}/p/Movies/a.mkv?sign=proxy"
