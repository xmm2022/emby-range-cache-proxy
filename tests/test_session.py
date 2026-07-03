import asyncio
import logging

from emby_range_cache_proxy.config import SessionConfig
from emby_range_cache_proxy.models import ByteRange, RequestContext, SourceMetadata
from emby_range_cache_proxy.session import (
    SessionRecorder,
    SourceMetadataRecorder,
    build_session_update,
    origin_signature,
)
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


def test_build_session_update_uses_token_hash_when_play_session_and_device_missing():
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)

    update = build_session_update(
        ctx=_ctx(play_session_id=None, device_id=None),
        cache_key="a" * 64,
        metadata=metadata,
        byte_range=ByteRange(100, 199),
        observed_at=600.0,
    )

    assert update.session_hash == hash_identifier(
        "synthetic:1:ms1:" + hash_identifier("user") + ":0"
    )


async def test_session_recorder_queue_does_not_block_when_full(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    recorder = SessionRecorder(store, queue_size=1)
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)

    assert (
        recorder.record_nowait(_ctx("play1"), "a" * 64, metadata, ByteRange(0, 9), observed_at=1.0)
        is True
    )
    assert (
        recorder.record_nowait(_ctx("play2"), "b" * 64, metadata, ByteRange(10, 19), observed_at=2.0)
        is False
    )
    await recorder.drain_once()

    assert store.get_session(hash_identifier("play1")) is not None
    assert store.get_session(hash_identifier("play2")) is None


async def test_session_recorder_start_processes_updates_and_stop_drains_queue(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    recorder = SessionRecorder(store, queue_size=10)
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)

    recorder.start()
    assert (
        recorder.record_nowait(_ctx("play1"), "a" * 64, metadata, ByteRange(0, 9), observed_at=1.0)
        is True
    )
    await recorder.stop()

    assert store.get_session(hash_identifier("play1")) is not None
    assert recorder._task is not None
    assert recorder._task.done()


async def test_session_recorder_start_is_idempotent(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    recorder = SessionRecorder(store, queue_size=10)

    recorder.start()
    task = recorder._task
    recorder.start()

    assert recorder._task is task
    await recorder.stop()


async def test_session_recorder_stop_drains_accepted_updates(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    recorder = SessionRecorder(store, queue_size=10)
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)

    assert (
        recorder.record_nowait(_ctx("play1"), "a" * 64, metadata, ByteRange(0, 9), observed_at=1.0)
        is True
    )
    assert (
        recorder.record_nowait(_ctx("play2"), "b" * 64, metadata, ByteRange(10, 19), observed_at=2.0)
        is True
    )
    recorder.start()
    await recorder.stop()

    assert store.get_session(hash_identifier("play1")) is not None
    assert store.get_session(hash_identifier("play2")) is not None


async def test_session_recorder_worker_logs_failed_write_and_continues(caplog):
    class FakeStore:
        def __init__(self):
            self.calls = 0
            self.recorded = []

        def record_playback(self, update):
            self.calls += 1
            if self.calls == 1:
                raise OSError("temporary sqlite failure")
            self.recorded.append(update)

    store = FakeStore()
    recorder = SessionRecorder(store, queue_size=10)
    metadata = SourceMetadata(url="http://origin/movie.mkv", size=1000)
    caplog.set_level(logging.WARNING, logger="emby_range_cache_proxy.session")

    recorder.start()
    assert (
        recorder.record_nowait(_ctx("play1"), "a" * 64, metadata, ByteRange(0, 9), observed_at=1.0)
        is True
    )
    assert (
        recorder.record_nowait(_ctx("play2"), "b" * 64, metadata, ByteRange(10, 19), observed_at=2.0)
        is True
    )
    await recorder.stop()

    assert recorder._task is not None
    assert recorder._task.done()
    assert store.calls == 2
    assert [update.session_hash for update in store.recorded] == [hash_identifier("play2")]
    assert "session recorder write failed" in caplog.text


async def test_source_metadata_recorder_prunes_old_records(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    store.upsert_source_metadata(
        item_id="old",
        media_source_id="ms-old",
        cache_key="a" * 64,
        origin_url="http://origin/old.mkv?api_key=signed",
        origin_signature="old",
        media_size=100,
        updated_at=1.0,
    )
    recorder = SourceMetadataRecorder(store, retention_seconds=5)

    assert recorder.record_nowait(
        item_id="new",
        media_source_id="ms-new",
        cache_key="b" * 64,
        origin_url="http://origin/new.mkv?api_key=signed",
        origin_signature="new",
        media_size=200,
        updated_at=10.0,
    )

    await recorder.drain_once()

    assert store.get_source_metadata("old", "ms-old", "a" * 64) is None
    assert store.get_source_metadata("new", "ms-new", "b" * 64) is not None


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
