from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable

from .models import ByteRange


SESSION_STATUSES = {"active", "idle", "stopped", "expired"}
TASK_STATUSES = {"queued", "running", "done", "failed", "skipped"}
PERMANENT_PREFETCH_ERROR_CLASSES = frozenset(
    {"PermanentError", "PrefetchSourceMismatch", "RangeTooLarge"}
)


def hash_identifier(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PlaybackSessionUpdate:
    session_hash: str
    device_hash: str | None
    item_id: str
    media_source_id: str
    cache_key: str
    origin_signature: str
    media_size: int
    byte_range: ByteRange
    observed_at: float

    def with_range(
        self, byte_range: ByteRange, *, observed_at: float
    ) -> PlaybackSessionUpdate:
        return replace(self, byte_range=byte_range, observed_at=observed_at)


@dataclass(frozen=True)
class PlaybackSessionRecord:
    session_hash: str
    device_hash: str | None
    item_id: str
    media_source_id: str
    cache_key: str
    origin_signature: str
    media_size: int
    last_range_start: int
    last_range_end: int
    max_observed_offset: int
    first_seen_at: float
    last_seen_at: float
    last_emby_observed_at: float | None
    status: str
    queued_until: int | None


@dataclass(frozen=True)
class PrefetchTaskRecord:
    id: int
    item_id: str
    media_source_id: str
    cache_key: str
    start: int
    end: int
    priority: int
    status: str
    attempts: int
    created_at: float
    updated_at: float
    last_error_class: str | None
    next_attempt_at: float | None


@dataclass(frozen=True)
class MiddleBlockRecord:
    cache_key: str
    start: int
    end: int
    path: str
    size: int
    created_at: float
    last_access_at: float
    expires_at: float


class SessionStateStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def record_playback(self, update: PlaybackSessionUpdate) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO playback_sessions (
                    session_hash, device_hash, item_id, media_source_id,
                    cache_key, origin_signature, media_size, last_range_start,
                    last_range_end, max_observed_offset, first_seen_at,
                    last_seen_at, last_emby_observed_at, status, queued_until
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'active', NULL)
                ON CONFLICT(session_hash) DO UPDATE SET
                    device_hash = excluded.device_hash,
                    item_id = excluded.item_id,
                    media_source_id = excluded.media_source_id,
                    cache_key = excluded.cache_key,
                    origin_signature = excluded.origin_signature,
                    media_size = excluded.media_size,
                    last_range_start = excluded.last_range_start,
                    last_range_end = excluded.last_range_end,
                    max_observed_offset = MAX(
                        playback_sessions.max_observed_offset,
                        excluded.max_observed_offset
                    ),
                    last_seen_at = excluded.last_seen_at,
                    status = 'active'
                """,
                (
                    update.session_hash,
                    update.device_hash,
                    update.item_id,
                    update.media_source_id,
                    update.cache_key,
                    update.origin_signature,
                    update.media_size,
                    update.byte_range.start,
                    update.byte_range.end,
                    update.byte_range.end,
                    update.observed_at,
                    update.observed_at,
                ),
            )

    def get_session(self, session_hash: str) -> PlaybackSessionRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM playback_sessions WHERE session_hash = ?",
                (session_hash,),
            ).fetchone()
        return _playback_session_from_row(row) if row is not None else None

    def recent_active_sessions(
        self, *, now: float, active_seconds: int
    ) -> list[PlaybackSessionRecord]:
        cutoff = now - active_seconds
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM playback_sessions
                WHERE status = 'active' AND last_seen_at >= ?
                ORDER BY last_seen_at DESC, session_hash ASC
                """,
                (cutoff,),
            ).fetchall()
        return [_playback_session_from_row(row) for row in rows]

    def mark_idle_sessions(
        self, *, now: float, idle_seconds: int
    ) -> list[PlaybackSessionRecord]:
        cutoff = now - idle_seconds
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM playback_sessions
                WHERE status = 'active' AND last_seen_at <= ?
                ORDER BY last_seen_at ASC, session_hash ASC
                """,
                (cutoff,),
            ).fetchall()
            session_hashes = [row["session_hash"] for row in rows]
            transitioned_hashes = []
            for session_hash in session_hashes:
                cursor = conn.execute(
                    """
                    UPDATE playback_sessions
                    SET status = 'idle'
                    WHERE session_hash = ?
                      AND status = 'active'
                      AND last_seen_at <= ?
                    """,
                    (session_hash, cutoff),
                )
                if cursor.rowcount:
                    transitioned_hashes.append(session_hash)
            rows = self._select_sessions(conn, transitioned_hashes)
        return [_playback_session_from_row(row) for row in rows]

    def expire_old_sessions(self, *, now: float, expire_seconds: int) -> int:
        cutoff = now - expire_seconds
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE playback_sessions
                SET status = 'expired'
                WHERE status != 'expired' AND last_seen_at <= ?
                """,
                (cutoff,),
            )
            return cursor.rowcount

    def record_observed_sessions(
        self, session_hashes: set[str], *, observed_at: float
    ) -> None:
        if not session_hashes:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                UPDATE playback_sessions
                SET last_emby_observed_at = ?
                WHERE session_hash = ?
                """,
                [(observed_at, session_hash) for session_hash in session_hashes],
            )

    def mark_missing_observed_sessions_stopped(
        self, *, now: float, stop_grace_seconds: int
    ) -> list[PlaybackSessionRecord]:
        cutoff = now - stop_grace_seconds
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM playback_sessions
                WHERE last_emby_observed_at IS NOT NULL
                  AND status IN ('active', 'idle')
                  AND last_emby_observed_at <= ?
                ORDER BY last_emby_observed_at ASC, session_hash ASC
                """,
                (cutoff,),
            ).fetchall()
            session_hashes = [row["session_hash"] for row in rows]
            transitioned_hashes = []
            for session_hash in session_hashes:
                cursor = conn.execute(
                    """
                    UPDATE playback_sessions
                    SET status = 'stopped'
                    WHERE session_hash = ?
                      AND last_emby_observed_at IS NOT NULL
                      AND status IN ('active', 'idle')
                      AND last_emby_observed_at <= ?
                    """,
                    (session_hash, cutoff),
                )
                if cursor.rowcount:
                    transitioned_hashes.append(session_hash)
            rows = self._select_sessions(conn, transitioned_hashes)
        return [_playback_session_from_row(row) for row in rows]

    def prefetch_candidate_sessions(self) -> list[PlaybackSessionRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM playback_sessions
                WHERE status IN ('idle', 'stopped')
                ORDER BY last_seen_at ASC, session_hash ASC
                """
            ).fetchall()
        return [_playback_session_from_row(row) for row in rows]

    def update_session_queued_until(
        self, session_hash: str, queued_until: int, *, now: float
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE playback_sessions
                SET queued_until = CASE
                    WHEN queued_until IS NULL OR queued_until < ? THEN ?
                    ELSE queued_until
                END
                WHERE session_hash = ? AND status != 'expired'
                """,
                (queued_until, queued_until, session_hash),
            )

    def enqueue_prefetch_task(
        self,
        item_id: str,
        media_source_id: str,
        cache_key: str,
        start: int,
        end: int,
        *,
        priority: int,
        now: float,
        max_queue_depth: int,
    ) -> PrefetchTaskRecord | None:
        with self._connect() as conn:
            _begin_immediate(conn)
            queued = conn.execute(
                "SELECT COUNT(*) AS count FROM prefetch_tasks WHERE status = 'queued'"
            ).fetchone()["count"]
            if queued >= max_queue_depth:
                return None
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO prefetch_tasks (
                        item_id, media_source_id, cache_key, start, end,
                        priority, status, attempts, created_at, updated_at,
                        last_error_class
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, NULL)
                    """,
                    (
                        item_id,
                        media_source_id,
                        cache_key,
                        start,
                        end,
                        priority,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                return None
            row = conn.execute(
                "SELECT * FROM prefetch_tasks WHERE id = ?",
                (cursor.lastrowid,),
            ).fetchone()
        return _prefetch_task_from_row(row)

    def prefetch_task_exists(self, cache_key: str, start: int, end: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM prefetch_tasks
                WHERE cache_key = ?
                  AND start = ?
                  AND end = ?
                  AND (
                        status IN ('queued', 'running')
                     OR (
                            status IN ('failed', 'skipped')
                            AND next_attempt_at IS NOT NULL
                        )
                  )
                LIMIT 1
                """,
                (cache_key, start, end),
            ).fetchone()
        return row is not None

    def claim_prefetch_tasks(
        self,
        *,
        limit: int,
        now: float,
        running_stale_seconds: int | None = None,
    ) -> list[PrefetchTaskRecord]:
        if limit <= 0:
            return []
        stale_cutoff = (
            None if running_stale_seconds is None else now - running_stale_seconds
        )
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM prefetch_tasks
                WHERE status = 'queued'
                   OR (
                        status IN ('failed', 'skipped')
                        AND next_attempt_at IS NOT NULL
                        AND next_attempt_at <= ?
                   )
                   OR (
                        ? IS NOT NULL
                        AND status = 'running'
                        AND updated_at <= ?
                   )
                ORDER BY priority DESC, created_at ASC, id ASC
                LIMIT ?
                """,
                (now, running_stale_seconds, stale_cutoff, limit),
            ).fetchall()
            task_ids = [row["id"] for row in rows]
            transitioned_ids = []
            for task_id in task_ids:
                cursor = conn.execute(
                    """
                    UPDATE prefetch_tasks
                    SET status = 'running',
                        attempts = attempts + 1,
                        updated_at = ?,
                        next_attempt_at = NULL
                    WHERE id = ?
                      AND (
                            status = 'queued'
                         OR (
                                status IN ('failed', 'skipped')
                                AND next_attempt_at IS NOT NULL
                                AND next_attempt_at <= ?
                            )
                         OR (
                                ? IS NOT NULL
                                AND status = 'running'
                                AND updated_at <= ?
                            )
                      )
                    """,
                    (now, task_id, now, running_stale_seconds, stale_cutoff),
                )
                if cursor.rowcount:
                    transitioned_ids.append(task_id)
            rows = self._select_prefetch_tasks(conn, transitioned_ids)
        return [_prefetch_task_from_row(row) for row in rows]

    def complete_prefetch_task(
        self, task_id: int, *, now: float, expected_attempts: int | None = None
    ) -> bool:
        expected_sql, expected_params = _expected_attempts_clause(expected_attempts)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE prefetch_tasks
                SET status = 'done',
                    updated_at = ?,
                    last_error_class = NULL,
                    next_attempt_at = NULL
                WHERE id = ?
                {expected_sql}
                """,
                (now, task_id, *expected_params),
            )
            return cursor.rowcount == 1

    def fail_prefetch_task(
        self,
        task_id: int,
        *,
        error_class: str,
        now: float,
        retry_after_seconds: int | None = None,
        expected_attempts: int | None = None,
    ) -> bool:
        next_attempt_at = (
            None if retry_after_seconds is None else now + retry_after_seconds
        )
        expected_sql, expected_params = _expected_attempts_clause(expected_attempts)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE prefetch_tasks
                SET status = 'failed',
                    updated_at = ?,
                    last_error_class = ?,
                    next_attempt_at = ?
                WHERE id = ?
                {expected_sql}
                """,
                (now, error_class, next_attempt_at, task_id, *expected_params),
            )
            return cursor.rowcount == 1

    def skip_prefetch_task(
        self,
        task_id: int,
        *,
        error_class: str,
        now: float,
        retry_after_seconds: int | None = None,
        expected_attempts: int | None = None,
    ) -> bool:
        next_attempt_at = (
            None if retry_after_seconds is None else now + retry_after_seconds
        )
        expected_sql, expected_params = _expected_attempts_clause(expected_attempts)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE prefetch_tasks
                SET status = 'skipped',
                    updated_at = ?,
                    last_error_class = ?,
                    next_attempt_at = ?
                WHERE id = ?
                {expected_sql}
                """,
                (now, error_class, next_attempt_at, task_id, *expected_params),
            )
            return cursor.rowcount == 1

    def requeue_prefetch_task(
        self,
        task_id: int,
        *,
        now: float,
        error_class: str | None = None,
        expected_attempts: int | None = None,
    ) -> bool:
        expected_sql, expected_params = _expected_attempts_clause(expected_attempts)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE prefetch_tasks
                SET status = 'queued',
                    updated_at = ?,
                    last_error_class = ?,
                    next_attempt_at = NULL
                WHERE id = ? AND status = 'running'
                {expected_sql}
                """,
                (now, error_class, task_id, *expected_params),
            )
            return cursor.rowcount == 1

    def queue_depth(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM prefetch_tasks WHERE status = 'queued'"
            ).fetchone()
        return row["count"]

    def claimable_prefetch_task_count(
        self,
        *,
        now: float,
        running_stale_seconds: int | None = None,
    ) -> int:
        stale_cutoff = (
            None if running_stale_seconds is None else now - running_stale_seconds
        )
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM prefetch_tasks
                WHERE status = 'queued'
                   OR (
                        status IN ('failed', 'skipped')
                        AND next_attempt_at IS NOT NULL
                        AND next_attempt_at <= ?
                   )
                   OR (
                        ? IS NOT NULL
                        AND status = 'running'
                        AND updated_at <= ?
                   )
                """,
                (now, running_stale_seconds, stale_cutoff),
            ).fetchone()
        return row["count"]

    def upsert_middle_block(self, block: MiddleBlockRecord) -> None:
        with self._connect() as conn:
            _upsert_middle_block(conn, block)

    def publish_middle_block_and_complete_prefetch_task(
        self,
        task_id: int,
        *,
        expected_attempts: int,
        block: MiddleBlockRecord,
        now: float,
        publish: Callable[[], None],
    ) -> bool:
        with self._connect() as conn:
            _begin_immediate(conn)
            current = conn.execute(
                """
                SELECT 1
                FROM prefetch_tasks
                WHERE id = ?
                  AND status = 'running'
                  AND attempts = ?
                """,
                (task_id, expected_attempts),
            ).fetchone()
            if current is None:
                return False

            publish()
            _upsert_middle_block(conn, block)
            cursor = conn.execute(
                """
                UPDATE prefetch_tasks
                SET status = 'done',
                    updated_at = ?,
                    last_error_class = NULL,
                    next_attempt_at = NULL
                WHERE id = ?
                  AND status = 'running'
                  AND attempts = ?
                """,
                (now, task_id, expected_attempts),
            )
            return cursor.rowcount == 1

    def find_middle_block(
        self, cache_key: str, byte_range: ByteRange
    ) -> MiddleBlockRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM middle_blocks
                WHERE cache_key = ? AND start <= ? AND end >= ?
                ORDER BY start DESC, end ASC
                LIMIT 1
                """,
                (cache_key, byte_range.start, byte_range.end),
            ).fetchone()
        return _middle_block_from_row(row) if row is not None else None

    def touch_middle_block(
        self, cache_key: str, start: int, end: int, *, now: float, ttl_seconds: int
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE middle_blocks
                SET last_access_at = ?, expires_at = ?
                WHERE cache_key = ? AND start = ? AND end = ?
                """,
                (now, now + ttl_seconds, cache_key, start, end),
            )

    def expired_middle_blocks(self, *, now: float) -> list[MiddleBlockRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM middle_blocks
                WHERE expires_at <= ?
                ORDER BY expires_at ASC, cache_key ASC, start ASC, end ASC
                """,
                (now,),
            ).fetchall()
        return [_middle_block_from_row(row) for row in rows]

    def least_recent_middle_blocks(self) -> list[MiddleBlockRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM middle_blocks
                ORDER BY last_access_at ASC, cache_key ASC, start ASC, end ASC
                """
            ).fetchall()
        return [_middle_block_from_row(row) for row in rows]

    def delete_middle_block_record(self, cache_key: str, start: int, end: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM middle_blocks
                WHERE cache_key = ? AND start = ? AND end = ?
                """,
                (cache_key, start, end),
            )

    def middle_cache_bytes(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(size), 0) AS total FROM middle_blocks"
            ).fetchone()
        return row["total"]

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS playback_sessions (
                    session_hash TEXT PRIMARY KEY,
                    device_hash TEXT,
                    item_id TEXT NOT NULL,
                    media_source_id TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    origin_signature TEXT NOT NULL,
                    media_size INTEGER NOT NULL,
                    last_range_start INTEGER NOT NULL,
                    last_range_end INTEGER NOT NULL,
                    max_observed_offset INTEGER NOT NULL,
                    first_seen_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL,
                    last_emby_observed_at REAL,
                    status TEXT NOT NULL,
                    queued_until INTEGER
                );
                CREATE TABLE IF NOT EXISTS prefetch_tasks (
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
                CREATE TABLE IF NOT EXISTS middle_blocks (
                    cache_key TEXT NOT NULL,
                    start INTEGER NOT NULL,
                    end INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    last_access_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY(cache_key, start, end)
                );
                """
            )
            _ensure_column(conn, "prefetch_tasks", "next_attempt_at", "REAL")
            _backfill_retryable_prefetch_tasks(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _select_sessions(
        self, conn: sqlite3.Connection, session_hashes: list[str]
    ) -> list[sqlite3.Row]:
        if not session_hashes:
            return []
        placeholders = ", ".join("?" for _ in session_hashes)
        rows = conn.execute(
            f"""
            SELECT * FROM playback_sessions
            WHERE session_hash IN ({placeholders})
            """,
            session_hashes,
        ).fetchall()
        by_hash = {row["session_hash"]: row for row in rows}
        return [by_hash[session_hash] for session_hash in session_hashes]

    def _select_prefetch_tasks(
        self, conn: sqlite3.Connection, task_ids: list[int]
    ) -> list[sqlite3.Row]:
        if not task_ids:
            return []
        placeholders = ", ".join("?" for _ in task_ids)
        rows = conn.execute(
            f"""
            SELECT * FROM prefetch_tasks
            WHERE id IN ({placeholders})
            """,
            task_ids,
        ).fetchall()
        by_id = {row["id"]: row for row in rows}
        return [by_id[task_id] for task_id in task_ids]


def _begin_immediate(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")


def _upsert_middle_block(conn: sqlite3.Connection, block: MiddleBlockRecord) -> None:
    conn.execute(
        """
        INSERT INTO middle_blocks (
            cache_key, start, end, path, size, created_at,
            last_access_at, expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cache_key, start, end) DO UPDATE SET
            path = excluded.path,
            size = excluded.size,
            created_at = excluded.created_at,
            last_access_at = excluded.last_access_at,
            expires_at = excluded.expires_at
        """,
        (
            block.cache_key,
            block.start,
            block.end,
            block.path,
            block.size,
            block.created_at,
            block.last_access_at,
            block.expires_at,
        ),
    )


def _ensure_column(
    conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str
) -> bool:
    columns = {
        row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})")
    }
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        return True
    return False


def _expected_attempts_clause(
    expected_attempts: int | None,
) -> tuple[str, tuple[int, ...]]:
    if expected_attempts is None:
        return "", ()
    return "AND status = 'running' AND attempts = ?", (expected_attempts,)


def _backfill_retryable_prefetch_tasks(conn: sqlite3.Connection) -> None:
    permanent_errors = tuple(sorted(PERMANENT_PREFETCH_ERROR_CLASSES))
    placeholders = ", ".join("?" for _ in permanent_errors)
    conn.execute(
        f"""
        UPDATE prefetch_tasks
        SET next_attempt_at = updated_at
        WHERE status IN ('failed', 'skipped')
          AND next_attempt_at IS NULL
          AND (
                last_error_class IS NULL
             OR last_error_class NOT IN ({placeholders})
          )
        """,
        permanent_errors,
    )


def _playback_session_from_row(row: sqlite3.Row) -> PlaybackSessionRecord:
    return PlaybackSessionRecord(
        session_hash=row["session_hash"],
        device_hash=row["device_hash"],
        item_id=row["item_id"],
        media_source_id=row["media_source_id"],
        cache_key=row["cache_key"],
        origin_signature=row["origin_signature"],
        media_size=row["media_size"],
        last_range_start=row["last_range_start"],
        last_range_end=row["last_range_end"],
        max_observed_offset=row["max_observed_offset"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        last_emby_observed_at=row["last_emby_observed_at"],
        status=row["status"],
        queued_until=row["queued_until"],
    )


def _prefetch_task_from_row(row: sqlite3.Row) -> PrefetchTaskRecord:
    return PrefetchTaskRecord(
        id=row["id"],
        item_id=row["item_id"],
        media_source_id=row["media_source_id"],
        cache_key=row["cache_key"],
        start=row["start"],
        end=row["end"],
        priority=row["priority"],
        status=row["status"],
        attempts=row["attempts"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_error_class=row["last_error_class"],
        next_attempt_at=row["next_attempt_at"],
    )


def _middle_block_from_row(row: sqlite3.Row) -> MiddleBlockRecord:
    return MiddleBlockRecord(
        cache_key=row["cache_key"],
        start=row["start"],
        end=row["end"],
        path=row["path"],
        size=row["size"],
        created_at=row["created_at"],
        last_access_at=row["last_access_at"],
        expires_at=row["expires_at"],
    )
