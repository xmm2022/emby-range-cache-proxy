from aiohttp import web

from emby_range_cache_proxy.config import Config, SessionConfig
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.session_observer import (
    EmbySessionObserver,
    extract_observed_session_hashes,
)
from emby_range_cache_proxy.state import (
    PlaybackSessionUpdate,
    SessionStateStore,
    hash_identifier,
)


def test_extract_observed_session_hashes_ignores_missing_ids():
    payload = [
        {"PlaySessionId": "play1", "DeviceId": "dev1"},
        {"NowPlayingItem": {"Id": "1"}},
        "bad",
    ]

    observed = extract_observed_session_hashes(payload)

    assert observed == {hash_identifier("play1")}


async def test_observer_records_seen_sessions_and_marks_missing_stopped(
    aiohttp_client, tmp_path
):
    calls = 0

    async def sessions(request):
        nonlocal calls
        assert request.query["api_key"] == "internal"
        calls += 1
        if calls == 1:
            return web.json_response([{"PlaySessionId": "play1"}])
        return web.json_response([])

    emby_app = web.Application()
    emby_app.router.add_get("/Sessions", sessions)
    emby = await aiohttp_client(emby_app)
    store = SessionStateStore(tmp_path / "state.sqlite3")
    session_hash = hash_identifier("play1")
    store.record_playback(
        PlaybackSessionUpdate(
            session_hash=session_hash,
            device_hash=None,
            item_id="1",
            media_source_id="ms1",
            cache_key="a" * 64,
            origin_signature="origin-sig",
            media_size=1000,
            byte_range=ByteRange(0, 99),
            observed_at=1.0,
        )
    )
    store.record_observed_sessions({session_hash}, observed_at=1.0)

    observer = EmbySessionObserver(
        Config(
            emby_base_url=str(emby.make_url("")),
            fallback_base_url=str(emby.make_url("")),
            cache_dir=str(tmp_path / "cache"),
            prewarm_api_key="internal",
            session=SessionConfig(
                enabled=True, observer_enabled=True, stop_grace_seconds=60
            ),
        ),
        store,
    )

    first = await observer.run_once(now=10.0)
    stopped = await observer.run_once(now=100.0)

    assert first.observed == 1
    assert stopped.stopped == 1


async def test_observer_without_internal_key_is_noop(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    observer = EmbySessionObserver(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path / "cache"),
        ),
        store,
    )

    result = await observer.run_once(now=1.0)

    assert result.observed == 0
    assert result.stopped == 0
