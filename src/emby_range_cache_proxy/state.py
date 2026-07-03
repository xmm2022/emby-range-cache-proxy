from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path

from .models import ByteRange


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
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def record_playback(self, update: PlaybackSessionUpdate) -> None:
        with self._connect() as conn:
            current = conn.execute(
                """
                SELECT max_observed_offset, first_seen_at
                FROM playback_sessions
                WHERE session_hash = ?
                """,
                (update.session_hash,),
            ).fetchone()
            if current is None:
                conn.execute(
                    """
                    INSERT INTO playback_sessions (
                        session_hash, device_hash, item_id, media_source_id,
                        cache_key, origin_signature, media_size, last_range_start,
                        last_range_end, max_observed_offset, first_seen_at,
                        last_seen_at, last_emby_observed_at, status, queued_until
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'active', NULL)
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
                return

            conn.execute(
                """
                UPDATE playback_sessions
                SET device_hash = ?,
                    item_id = ?,
                    media_source_id = ?,
                    cache_key = ?,
                    origin_signature = ?,
                    media_size = ?,
                    last_range_start = ?,
                    last_range_end = ?,
                    max_observed_offset = ?,
                    last_seen_at = ?,
                    status = 'active'
                WHERE session_hash = ?
                """,
                (
                    update.device_hash,
                    update.item_id,
                    update.media_source_id,
                    update.cache_key,
                    update.origin_signature,
                    update.media_size,
                    update.byte_range.start,
                    update.byte_range.end,
                    max(current["max_observed_offset"], update.byte_range.end),
                    update.observed_at,
                    update.session_hash,
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
            if session_hashes:
                conn.executemany(
                    "UPDATE playback_sessions SET status = 'idle' WHERE session_hash = ?",
                    [(session_hash,) for session_hash in session_hashes],
                )
                rows = self._select_sessions(conn, session_hashes)
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
            if session_hashes:
                conn.executemany(
                    """
                    UPDATE playback_sessions
                    SET status = 'stopped'
                    WHERE session_hash = ?
                    """,
                    [(session_hash,) for session_hash in session_hashes],
                )
                rows = self._select_sessions(conn, session_hashes)
        return [_playback_session_from_row(row) for row in rows]

    def update_session_queued_until(
        self, session_hash: str, queued_until: int, *, now: float
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE playback_sessions
                SET queued_until = ?, last_seen_at = ?
                WHERE session_hash = ?
                """,
                (queued_until, now, session_hash),
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

    def claim_prefetch_tasks(
        self, *, limit: int, now: float
    ) -> list[PrefetchTaskRecord]:
        if limit <= 0:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM prefetch_tasks
                WHERE status = 'queued'
                ORDER BY priority DESC, created_at ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            task_ids = [row["id"] for row in rows]
            if task_ids:
                conn.executemany(
                    """
                    UPDATE prefetch_tasks
                    SET status = 'running',
                        attempts = attempts + 1,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    [(now, task_id) for task_id in task_ids],
                )
                rows = self._select_prefetch_tasks(conn, task_ids)
        return [_prefetch_task_from_row(row) for row in rows]

    def complete_prefetch_task(self, task_id: int, *, now: float) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE prefetch_tasks
                SET status = 'completed',
                    updated_at = ?,
                    last_error_class = NULL
                WHERE id = ?
                """,
                (now, task_id),
            )

    def fail_prefetch_task(
        self, task_id: int, *, error_class: str, now: float
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE prefetch_tasks
                SET status = 'failed',
                    updated_at = ?,
                    last_error_class = ?
                WHERE id = ?
                """,
                (now, error_class, task_id),
            )

    def queue_depth(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM prefetch_tasks WHERE status = 'queued'"
            ).fetchone()
        return row["count"]

    def upsert_middle_block(self, block: MiddleBlockRecord) -> None:
        with self._connect() as conn:
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

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _select_sessions(
        self, conn: sqlite3.Connection, session_hashes: list[str]
    ) -> list[sqlite3.Row]:
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
