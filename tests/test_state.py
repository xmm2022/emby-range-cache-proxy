import sqlite3
from pathlib import Path

from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.state import (
    MiddleBlockRecord,
    PlaybackSessionUpdate,
    PrefetchTaskRecord,
    SESSION_STATUSES,
    SessionStateStore,
    TASK_STATUSES,
    hash_identifier,
)


class _InterleavingCursor:
    def __init__(self, cursor, on_fetchall):
        self._cursor = cursor
        self._on_fetchall = on_fetchall
        self._triggered = False

    def fetchall(self):
        rows = self._cursor.fetchall()
        self._trigger(rows)
        return rows

    def fetchone(self):
        row = self._cursor.fetchone()
        self._trigger([row] if row is not None else [])
        return row

    def _trigger(self, rows):
        if rows and not self._triggered:
            self._triggered = True
            self._on_fetchall(rows)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _InterleavingConnection:
    def __init__(self, path, sql_fragment, on_fetchall):
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._sql_fragment = sql_fragment
        self._on_fetchall = on_fetchall

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        self._conn.close()

    def execute(self, sql, parameters=()):
        cursor = self._conn.execute(sql, parameters)
        if self._sql_fragment in " ".join(sql.split()):
            return _InterleavingCursor(cursor, self._on_fetchall)
        return cursor

    def executemany(self, sql, parameters):
        return self._conn.executemany(sql, parameters)

    def executescript(self, sql):
        return self._conn.executescript(sql)


def test_hash_identifier_is_stable_and_does_not_expose_value():
    value = "play-session-secret"

    hashed = hash_identifier(value)

    assert hashed == hash_identifier(value)
    assert len(hashed) == 64
    assert value not in hashed
    assert hash_identifier(None) is None


def test_state_status_constants_match_public_contract():
    assert SESSION_STATUSES == {"active", "idle", "stopped", "expired"}
    assert TASK_STATUSES == {"queued", "running", "done", "failed", "skipped"}


def test_state_store_accepts_public_path_keyword(tmp_path):
    path = tmp_path / "state.sqlite3"

    store = SessionStateStore(path=path)

    assert store.path == path


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


def test_mark_idle_sessions_does_not_override_newer_active_session(
    tmp_path, monkeypatch
):
    path = tmp_path / "state.sqlite3"
    store = SessionStateStore(path)
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

    def refresh_session(_rows):
        with sqlite3.connect(path) as conn:
            conn.execute(
                """
                UPDATE playback_sessions
                SET status = 'active', last_seen_at = ?
                WHERE session_hash = ?
                """,
                (100.0, "s" * 64),
            )

    monkeypatch.setattr(
        store,
        "_connect",
        lambda: _InterleavingConnection(
            path,
            "WHERE status = 'active' AND last_seen_at <= ?",
            refresh_session,
        ),
    )

    idle = store.mark_idle_sessions(now=200.0, idle_seconds=180)
    session = store.get_session("s" * 64)

    assert idle == []
    assert session.status == "active"
    assert session.last_seen_at == 100.0


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


def test_prefetch_candidate_sessions_returns_idle_and_stopped_only(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    assert hasattr(store, "prefetch_candidate_sessions")
    for session_hash, observed_at in (
        ("a" * 64, 50.0),
        ("b" * 64, 60.0),
        ("c" * 64, 95.0),
        ("d" * 64, 10.0),
    ):
        store.record_playback(
            PlaybackSessionUpdate(
                session_hash=session_hash,
                device_hash=None,
                item_id="1",
                media_source_id="ms1",
                cache_key=session_hash,
                origin_signature="origin-sig",
                media_size=1000,
                byte_range=ByteRange(0, 99),
                observed_at=observed_at,
            )
        )
    store.mark_idle_sessions(now=100.0, idle_seconds=40)
    store.record_observed_sessions({"b" * 64}, observed_at=20.0)
    store.mark_missing_observed_sessions_stopped(now=100.0, stop_grace_seconds=60)
    store.expire_old_sessions(now=100.0, expire_seconds=80)

    candidates = store.prefetch_candidate_sessions()

    assert [(session.session_hash, session.status) for session in candidates] == [
        ("a" * 64, "idle"),
        ("b" * 64, "stopped"),
    ]


def test_mark_missing_observed_sessions_stopped_does_not_override_recent_observation(
    tmp_path, monkeypatch
):
    path = tmp_path / "state.sqlite3"
    store = SessionStateStore(path)
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

    def refresh_observation(_rows):
        with sqlite3.connect(path) as conn:
            conn.execute(
                """
                UPDATE playback_sessions
                SET status = 'active', last_emby_observed_at = ?
                WHERE session_hash = ?
                """,
                (90.0, "s" * 64),
            )

    monkeypatch.setattr(
        store,
        "_connect",
        lambda: _InterleavingConnection(
            path,
            "AND status IN ('active', 'idle') AND last_emby_observed_at <= ?",
            refresh_observation,
        ),
    )

    stopped = store.mark_missing_observed_sessions_stopped(
        now=100.0, stop_grace_seconds=60
    )
    session = store.get_session("s" * 64)

    assert stopped == []
    assert session.status == "active"
    assert session.last_emby_observed_at == 90.0


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


def test_claim_prefetch_tasks_does_not_return_task_if_status_changed_before_update(
    tmp_path, monkeypatch
):
    path = tmp_path / "state.sqlite3"
    store = SessionStateStore(path)
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )

    def mark_running(rows):
        with sqlite3.connect(path) as conn:
            conn.execute(
                """
                UPDATE prefetch_tasks
                SET status = 'running', attempts = 1, updated_at = ?
                WHERE id = ?
                """,
                (2.5, rows[0]["id"]),
            )

    monkeypatch.setattr(
        store,
        "_connect",
        lambda: _InterleavingConnection(
            path,
            "WHERE status = 'queued' OR",
            mark_running,
        ),
    )

    claimed = store.claim_prefetch_tasks(limit=1, now=3.0)

    assert claimed == []
    with sqlite3.connect(path) as conn:
        status, attempts = conn.execute(
            "SELECT status, attempts FROM prefetch_tasks WHERE id = ?",
            (task.id,),
        ).fetchone()
    assert status == "running"
    assert attempts == 1


def test_enqueue_prefetch_task_respects_max_queue_depth(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")

    first = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=1,
    )
    second = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=2048,
        end=3071,
        priority=10,
        now=2.0,
        max_queue_depth=1,
    )

    assert first is not None
    assert second is None
    assert store.queue_depth() == 1


def test_prefetch_task_exists_counts_only_reusable_tasks(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    assert hasattr(store, "prefetch_task_exists")
    queued = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    running = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=2048,
        end=3071,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    retryable = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=3072,
        end=4095,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    permanent = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=4096,
        end=5119,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    assert queued is not None
    assert running is not None
    assert retryable is not None
    assert permanent is not None
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)
    assert claimed[0].id == queued.id
    store.claim_prefetch_tasks(limit=1, now=2.0)
    store.fail_prefetch_task(
        retryable.id,
        error_class="OriginError",
        now=3.0,
        retry_after_seconds=10,
    )
    store.fail_prefetch_task(
        permanent.id,
        error_class="PrefetchSourceMismatch",
        now=3.0,
        retry_after_seconds=None,
    )

    assert store.prefetch_task_exists("a" * 64, 1024, 2047)
    assert store.prefetch_task_exists("a" * 64, 2048, 3071)
    assert store.prefetch_task_exists("a" * 64, 3072, 4095)
    assert not store.prefetch_task_exists("a" * 64, 4096, 5119)
    assert not store.prefetch_task_exists("a" * 64, 5120, 6143)


def test_update_session_queued_until_is_monotonic_and_keeps_last_seen_at(tmp_path):
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

    store.update_session_queued_until("s" * 64, 447, now=20.0)
    store.update_session_queued_until("s" * 64, 383, now=30.0)
    session = store.get_session("s" * 64)

    assert session.last_seen_at == 10.0
    assert session.queued_until == 447


def test_complete_prefetch_task_marks_task_done(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    store = SessionStateStore(db_path)
    store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)

    store.complete_prefetch_task(claimed[0].id, now=3.0)

    with sqlite3.connect(db_path) as conn:
        status = conn.execute(
            "SELECT status FROM prefetch_tasks WHERE id = ?",
            (claimed[0].id,),
        ).fetchone()[0]
    assert status == "done"


def test_failed_prefetch_task_after_backoff_is_reclaimed(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)

    store.fail_prefetch_task(
        claimed[0].id,
        error_class="OriginError",
        now=3.0,
        retry_after_seconds=10,
    )
    early = store.claim_prefetch_tasks(limit=1, now=12.0)
    retried = store.claim_prefetch_tasks(limit=1, now=13.0)

    assert task is not None
    assert early == []
    assert len(retried) == 1
    assert retried[0].id == task.id
    assert retried[0].status == "running"
    assert retried[0].attempts == 2
    assert retried[0].next_attempt_at is None


def test_skipped_prefetch_task_after_backoff_is_reclaimed(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)

    store.skip_prefetch_task(
        claimed[0].id,
        error_class="SourceUnavailable",
        now=3.0,
        retry_after_seconds=10,
    )
    early = store.claim_prefetch_tasks(limit=1, now=12.0)
    retried = store.claim_prefetch_tasks(limit=1, now=13.0)

    assert task is not None
    assert early == []
    assert len(retried) == 1
    assert retried[0].id == task.id
    assert retried[0].status == "running"
    assert retried[0].attempts == 2
    assert retried[0].next_attempt_at is None


def test_requeue_prefetch_task_restores_running_task_to_queue(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)

    store.requeue_prefetch_task(
        claimed[0].id,
        now=3.0,
        error_class="CancelledError",
    )
    requeued = store.claim_prefetch_tasks(limit=1, now=4.0)

    assert task is not None
    assert len(requeued) == 1
    assert requeued[0].id == task.id
    assert requeued[0].status == "running"
    assert requeued[0].attempts == 2
    assert requeued[0].last_error_class == "CancelledError"


def test_claim_prefetch_tasks_reclaims_stale_running_task(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    store.claim_prefetch_tasks(limit=1, now=2.0)

    fresh = store.claim_prefetch_tasks(
        limit=1,
        now=11.0,
        running_stale_seconds=10,
    )
    stale = store.claim_prefetch_tasks(
        limit=1,
        now=12.0,
        running_stale_seconds=10,
    )

    assert task is not None
    assert fresh == []
    assert len(stale) == 1
    assert stale[0].id == task.id
    assert stale[0].status == "running"
    assert stale[0].attempts == 2


def test_claimable_prefetch_task_count_matches_retry_and_stale_claim_conditions(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")

    failed_due = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=0,
        end=63,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    store.fail_prefetch_task(
        claimed.id,
        error_class="OriginError",
        now=3.0,
        retry_after_seconds=10,
    )

    skipped_due = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="b" * 64,
        start=64,
        end=127,
        priority=10,
        now=4.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=5.0)[0]
    store.skip_prefetch_task(
        claimed.id,
        error_class="SourceUnavailable",
        now=6.0,
        retry_after_seconds=10,
    )

    failed_pending = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="c" * 64,
        start=128,
        end=191,
        priority=10,
        now=7.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=8.0)[0]
    store.fail_prefetch_task(
        claimed.id,
        error_class="OriginError",
        now=9.0,
        retry_after_seconds=100,
    )

    skipped_pending = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="d" * 64,
        start=192,
        end=255,
        priority=10,
        now=10.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=11.0)[0]
    store.skip_prefetch_task(
        claimed.id,
        error_class="SourceUnavailable",
        now=12.0,
        retry_after_seconds=100,
    )

    running_stale = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="e" * 64,
        start=256,
        end=319,
        priority=10,
        now=13.0,
        max_queue_depth=10,
    )
    store.claim_prefetch_tasks(limit=1, now=14.0)

    running_fresh = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="f" * 64,
        start=320,
        end=383,
        priority=100,
        now=41.0,
        max_queue_depth=10,
    )
    store.claim_prefetch_tasks(limit=1, now=42.0)

    assert failed_due is not None
    assert skipped_due is not None
    assert failed_pending is not None
    assert skipped_pending is not None
    assert running_stale is not None
    assert running_fresh is not None
    assert (
        store.claimable_prefetch_task_count(
            now=50.0,
            running_stale_seconds=10,
        )
        == 3
    )
    assert (
        store.claimable_prefetch_task_count(
            now=50.0,
            running_stale_seconds=None,
        )
        == 2
    )


def test_existing_prefetch_task_table_gets_next_attempt_at_column(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE prefetch_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT NOT NULL,
                media_source_id TEXT NOT NULL,
                cache_key TEXT NOT NULL,
                start INTEGER NOT NULL,
                end INTEGER NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_error_class TEXT,
                UNIQUE(cache_key, start, end)
            );
            """
        )

    SessionStateStore(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(prefetch_tasks)").fetchall()
        }
    assert "next_attempt_at" in columns


def test_existing_prefetch_task_table_backfills_retryable_failed_and_skipped(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE prefetch_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT NOT NULL,
                media_source_id TEXT NOT NULL,
                cache_key TEXT NOT NULL,
                start INTEGER NOT NULL,
                end INTEGER NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_error_class TEXT,
                UNIQUE(cache_key, start, end)
            );
            """
        )
        conn.execute(
            """
            INSERT INTO prefetch_tasks (
                item_id, media_source_id, cache_key, start, end, priority,
                status, attempts, created_at, updated_at, last_error_class
            )
            VALUES ('1', 'ms1', ?, 1024, 2047, 10, 'failed', 1, 1.0, 5.0, 'OriginError')
            """,
            ("a" * 64,),
        )
        conn.execute(
            """
            INSERT INTO prefetch_tasks (
                item_id, media_source_id, cache_key, start, end, priority,
                status, attempts, created_at, updated_at, last_error_class
            )
            VALUES ('1', 'ms1', ?, 2048, 3071, 9, 'skipped', 1, 1.0, 6.0, 'SourceUnavailable')
            """,
            ("b" * 64,),
        )

    store = SessionStateStore(db_path)
    duplicate = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=7.0,
        max_queue_depth=10,
    )

    claimed = store.claim_prefetch_tasks(limit=2, now=6.0)

    assert duplicate is None
    assert [task.cache_key for task in claimed] == ["a" * 64, "b" * 64]
    assert [task.status for task in claimed] == ["running", "running"]
    assert [task.attempts for task in claimed] == [2, 2]


def test_existing_prefetch_task_table_does_not_backfill_existing_null_retry_policy(
    tmp_path,
):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)
    store.fail_prefetch_task(claimed[0].id, error_class="PermanentError", now=3.0)

    migrated = SessionStateStore(tmp_path / "state.sqlite3")
    retried = migrated.claim_prefetch_tasks(limit=1, now=4.0)

    assert task is not None
    assert retried == []


def test_existing_next_attempt_column_backfills_only_retryable_null_errors(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE prefetch_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT NOT NULL,
                media_source_id TEXT NOT NULL,
                cache_key TEXT NOT NULL,
                start INTEGER NOT NULL,
                end INTEGER NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_error_class TEXT,
                next_attempt_at REAL,
                UNIQUE(cache_key, start, end)
            );
            """
        )
        rows = [
            ("a" * 64, 0, 2, 50, "failed", 5.0, "OriginError"),
            ("b" * 64, 3, 5, 40, "skipped", 6.0, "SourceUnavailable"),
            ("c" * 64, 6, 8, 30, "failed", 7.0, "PermanentError"),
            ("d" * 64, 9, 11, 20, "failed", 8.0, "PrefetchSourceMismatch"),
            ("e" * 64, 12, 14, 10, "skipped", 9.0, "RangeTooLarge"),
        ]
        conn.executemany(
            """
            INSERT INTO prefetch_tasks (
                item_id, media_source_id, cache_key, start, end, priority,
                status, attempts, created_at, updated_at, last_error_class,
                next_attempt_at
            )
            VALUES ('1', 'ms1', ?, ?, ?, ?, ?, 1, 1.0, ?, ?, NULL)
            """,
            rows,
        )

    store = SessionStateStore(db_path)
    duplicate = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=0,
        end=2,
        priority=50,
        now=10.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=10, now=10.0)

    assert duplicate is None
    assert [task.cache_key for task in claimed] == ["a" * 64, "b" * 64]
    with sqlite3.connect(db_path) as conn:
        permanent_rows = conn.execute(
            """
            SELECT cache_key, next_attempt_at
            FROM prefetch_tasks
            WHERE cache_key IN (?, ?, ?)
            ORDER BY cache_key ASC
            """,
            ("c" * 64, "d" * 64, "e" * 64),
        ).fetchall()
    assert permanent_rows == [
        ("c" * 64, None),
        ("d" * 64, None),
        ("e" * 64, None),
    ]


def test_old_attempt_finalizers_do_not_override_new_completed_attempt(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    first = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    second = store.claim_prefetch_tasks(
        limit=1,
        now=12.0,
        running_stale_seconds=10,
    )[0]

    store.complete_prefetch_task(second.id, now=13.0, expected_attempts=2)
    store.fail_prefetch_task(
        first.id,
        error_class="LateFailure",
        now=14.0,
        expected_attempts=1,
    )
    store.requeue_prefetch_task(
        first.id,
        now=15.0,
        error_class="LateCancel",
        expected_attempts=1,
    )
    store.complete_prefetch_task(first.id, now=16.0, expected_attempts=1)

    assert task is not None
    with sqlite3.connect(tmp_path / "state.sqlite3") as conn:
        status, attempts, updated_at, last_error_class = conn.execute(
            """
            SELECT status, attempts, updated_at, last_error_class
            FROM prefetch_tasks
            WHERE id = ?
            """,
            (task.id,),
        ).fetchone()
    assert status == "done"
    assert attempts == 2
    assert updated_at == 13.0
    assert last_error_class is None


def test_old_cancellation_requeue_does_not_requeue_new_running_attempt(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=1024,
        end=2047,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    first = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    second = store.claim_prefetch_tasks(
        limit=1,
        now=12.0,
        running_stale_seconds=10,
    )[0]

    store.requeue_prefetch_task(
        first.id,
        now=13.0,
        error_class="LateCancel",
        expected_attempts=1,
    )
    requeued = store.claim_prefetch_tasks(limit=1, now=14.0)

    assert task is not None
    assert second.attempts == 2
    assert requeued == []
    with sqlite3.connect(tmp_path / "state.sqlite3") as conn:
        status, attempts, updated_at, last_error_class = conn.execute(
            """
            SELECT status, attempts, updated_at, last_error_class
            FROM prefetch_tasks
            WHERE id = ?
            """,
            (task.id,),
        ).fetchone()
    assert status == "running"
    assert attempts == 2
    assert updated_at == 12.0
    assert last_error_class is None


def test_publish_middle_block_and_complete_checks_attempt_before_publish(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=0,
        end=2,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    claimed = store.claim_prefetch_tasks(limit=1, now=2.0)[0]
    publish_called = False

    def publish():
        nonlocal publish_called
        publish_called = True

    result = store.publish_middle_block_and_complete_prefetch_task(
        claimed.id,
        expected_attempts=2,
        block=MiddleBlockRecord(
            cache_key="a" * 64,
            start=0,
            end=2,
            path=("a" * 64) + "/mid/0-2.bin",
            size=3,
            created_at=3.0,
            last_access_at=3.0,
            expires_at=63.0,
        ),
        now=3.0,
        publish=publish,
    )

    assert task is not None
    assert result is False
    assert publish_called is False
    assert store.find_middle_block("a" * 64, ByteRange(0, 2)) is None


def test_record_playback_keeps_max_observed_offset_when_later_update_has_smaller_range(
    tmp_path,
):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    update = PlaybackSessionUpdate(
        session_hash="s" * 64,
        device_hash=None,
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        origin_signature="origin-sig",
        media_size=1000,
        byte_range=ByteRange(400, 500),
        observed_at=10.0,
    )

    store.record_playback(update)
    store.record_playback(update.with_range(ByteRange(100, 199), observed_at=20.0))

    assert store.get_session("s" * 64).max_observed_offset == 500


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
