import logging

from aiohttp import web

import emby_range_cache_proxy.app as app_module
from emby_range_cache_proxy.app import create_app
from emby_range_cache_proxy.config import Config, RolloutConfig


async def test_auth_403_does_not_touch_origin_cache_or_fallback(aiohttp_client, monkeypatch, tmp_path):
    fallback_hits = 0
    playback_info_hits = 0

    class ForbiddenOriginClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("origin must not be touched after authorization failure")

    class ForbiddenCache:
        def read_block(self, *args, **kwargs):
            raise AssertionError("cache read must not run after authorization failure")

        def stage_block(self, *args, **kwargs):
            raise AssertionError("cache write must not run after authorization failure")

        def evict_if_needed(self):
            raise AssertionError("cache eviction must not run after authorization failure")

    async def playback_info(request):
        nonlocal playback_info_hits
        playback_info_hits += 1
        return web.Response(status=403)

    async def fallback(request):
        nonlocal fallback_hits
        fallback_hits += 1
        return web.Response(status=500, body=b"fallback must not be touched")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)
    monkeypatch.setattr(app_module, "OriginClient", ForbiddenOriginClient)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    app["cache"] = ForbiddenCache()
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=bad", headers={"Range": "bytes=0-3"})

    assert response.status == 403
    assert await response.text() == "forbidden\n"
    assert playback_info_hits == 1
    assert fallback_hits == 0


async def test_decision_logs_redact_sensitive_query_and_header_values(aiohttp_client, caplog, tmp_path):
    async def fallback(request):
        return web.Response(body=b"fallback")

    emby_app = web.Application()
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    emby_server = await aiohttp_client(emby_app)
    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=False),
        )
    )
    client = await aiohttp_client(app)
    query = "MediaSourceId=ms1&api_key=api-secret&PlaySessionId=play-secret&DeviceId=device-secret"

    with caplog.at_level(logging.INFO, logger="emby_range_cache_proxy.app"):
        response = await client.get(
            f"/emby/videos/1/original.mkv?{query}",
            headers={"Range": "bytes=0-3", "X-Emby-Token": "header-secret"},
        )

    assert response.status == 200
    messages = "\n".join(record.getMessage() for record in caplog.records if record.name == "emby_range_cache_proxy.app")
    assert "fallback reason=not_eligible" in messages
    assert "path=/emby/videos/1/original.mkv" in messages
    assert query not in messages
    assert "api-secret" not in messages
    assert "play-secret" not in messages
    assert "device-secret" not in messages
    assert "header-secret" not in messages
    assert "/emby/videos/1/original.mkv?MediaSourceId" not in messages


def test_readme_mentions_no_active_arbitrary_middle_cache():
    text = open("README.md", encoding="utf-8").read()

    assert "does not actively cache arbitrary middle playback ranges" in text
