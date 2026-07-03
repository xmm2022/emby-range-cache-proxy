import asyncio

from emby_range_cache_proxy.config import SessionConfig
from emby_range_cache_proxy.models import ByteRange, RequestContext, SourceMetadata
from emby_range_cache_proxy.session import SessionRecorder, build_session_update, origin_signature
from emby_range_cache_proxy.state import SessionStateStore, hash_identifier


def _ctx(play_session_id="play1", device_id="device1"):
    return RequestContext(
        method="GET",
        raw_path="/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=user",
        item_id="1",
        media_source_id="ms1",
        token="user",
        extension="mkv",
        play_session_id=play_session_id,
        device_id=device_id,
    )


def test_build_session_update_hashes_identifiers():
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000, etag='"e"', last_modified="date")

    update = build_session_update(
        ctx=_ctx(),
        cache_key="a" * 64,
        metadata=metadata,
        byte_range=ByteRange(100, 199),
        observed_at=10.0,
    )

    assert update.session_hash == hash_identifier("play1")
    assert update.device_hash == hash_identifier("device1")
    assert update.origin_signature == origin_signature(metadata)
    assert update.media_size == 1000
    assert update.byte_range == ByteRange(100, 199)


def test_build_session_update_uses_synthetic_session_without_play_session_id():
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)

    update = build_session_update(
        ctx=_ctx(play_session_id=None, device_id="device1"),
        cache_key="a" * 64,
        metadata=metadata,
        byte_range=ByteRange(100, 199),
        observed_at=600.0,
    )

    assert update.session_hash == hash_identifier("synthetic:1:ms1:" + hash_identifier("device1") + ":0")


async def test_session_recorder_queue_does_not_block_when_full(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    recorder = SessionRecorder(store, queue_size=1)
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)

    recorder.record_nowait(_ctx("play1"), "a" * 64, metadata, ByteRange(0, 9), observed_at=1.0)
    recorder.record_nowait(_ctx("play2"), "b" * 64, metadata, ByteRange(10, 19), observed_at=2.0)
    await recorder.drain_once()

    assert store.get_session(hash_identifier("play1")) is not None
    assert store.get_session(hash_identifier("play2")) is None


def test_mark_idle_and_expire_sessions(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    recorder = SessionRecorder(store, queue_size=10)
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)
    recorder.record_nowait(_ctx("play1"), "a" * 64, metadata, ByteRange(0, 9), observed_at=1.0)
    asyncio.run(recorder.drain_once())

    idle = recorder.mark_idle_and_expired(
        SessionConfig(enabled=True, idle_seconds=180, expire_seconds=600),
        now=200.0,
    )

    assert [session.session_hash for session in idle] == [hash_identifier("play1")]
