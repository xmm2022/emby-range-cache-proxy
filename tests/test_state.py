from pathlib import Path

from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.state import (
    MiddleBlockRecord,
    PlaybackSessionUpdate,
    PrefetchTaskRecord,
    SessionStateStore,
    hash_identifier,
)


def test_hash_identifier_is_stable_and_does_not_expose_value():
    value = "play-session-secret"

    hashed = hash_identifier(value)

    assert hashed == hash_identifier(value)
    assert len(hashed) == 64
    assert value not in hashed
    assert hash_identifier(None) is None


def test_record_playback_update_creates_and_advances_session(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    update = PlaybackSessionUpdate(
        session_hash="s" * 64,
        device_hash="d" * 64,
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        origin_signature="origin-sig",
        media_size=1000,
        byte_range=ByteRange(100, 199),
        observed_at=10.0,
    )

    store.record_playback(update)
    store.record_playback(update.with_range(ByteRange(300, 349), observed_at=20.0))
    session = store.get_session("s" * 64)

    assert session is not None
    assert session.item_id == "1"
    assert session.media_source_id == "ms1"
    assert session.cache_key == "a" * 64
    assert session.last_range_start == 300
    assert session.last_range_end == 349
    assert session.max_observed_offset == 349
    assert session.status == "active"
    assert session.first_seen_at == 10.0
    assert session.last_seen_at == 20.0


def test_mark_idle_sessions_and_expire_old_sessions(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    update = PlaybackSessionUpdate(
        session_hash="s" * 64,
        device_hash=None,
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        origin_signature="origin-sig",
        media_size=1000,
        byte_range=ByteRange(0, 99),
        observed_at=10.0,
    )
    store.record_playback(update)

    idle = store.mark_idle_sessions(now=200.0, idle_seconds=180)
    expired = store.expire_old_sessions(now=1000.0, expire_seconds=600)

    assert [session.session_hash for session in idle] == ["s" * 64]
    assert store.get_session("s" * 64).status == "expired"
    assert expired == 1


def test_observer_absence_marks_session_stopped_after_grace(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    store.record_playback(
        PlaybackSessionUpdate(
            session_hash="s" * 64,
            device_hash=None,
            item_id="1",
            media_source_id="ms1",
            cache_key="a" * 64,
            origin_signature="origin-sig",
            media_size=1000,
            byte_range=ByteRange(0, 99),
            observed_at=10.0,
        )
    )

    store.record_observed_sessions({"s" * 64}, observed_at=20.0)
    stopped = store.mark_missing_observed_sessions_stopped(now=100.0, stop_grace_seconds=60)

    assert [session.session_hash for session in stopped] == ["s" * 64]
    assert store.get_session("s" * 64).status == "stopped"


def test_prefetch_tasks_are_deduplicated_and_claimed(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    first = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    second = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=20,
        now=2.0,
        max_queue_depth=10,
    )

    claimed = store.claim_prefetch_tasks(limit=1, now=3.0)

    assert first is not None
    assert second is None
    assert len(claimed) == 1
    assert claimed[0].start == 1024
    assert claimed[0].end == 2047
    assert store.queue_depth() == 0


def test_middle_block_metadata_lifecycle(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    block = MiddleBlockRecord(
        cache_key="a" * 64,
        start=1024,
        end=2047,
        path=str(Path("a" * 64) / "mid" / "1024-2047.bin"),
        size=1024,
        created_at=1.0,
        last_access_at=1.0,
        expires_at=11.0,
    )

    store.upsert_middle_block(block)
    store.touch_middle_block("a" * 64, 1024, 2047, now=5.0, ttl_seconds=10)
    expired = store.expired_middle_blocks(now=14.0)

    assert store.find_middle_block("a" * 64, ByteRange(1200, 1300)).last_access_at == 5.0
    assert expired == []
    assert store.expired_middle_blocks(now=16.0)[0].start == 1024
