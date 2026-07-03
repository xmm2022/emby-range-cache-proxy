# Emby Range Cache Proxy Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add playback session tracking and idle/stop-driven middle-range prefetch while keeping normal playback requests fast and backwards-compatible.

**Architecture:** Phase 2 adds disabled-by-default config sections, a small SQLite state store, a middle-range cache that is separate from head/tail blocks, a session observer for Emby `/Sessions`, a prefetch planner, and a low-priority worker. Foreground requests still authorize with the user token first; middle cache is read only after authorization and only on complete hits.

**Tech Stack:** Python 3.11, `aiohttp`, stdlib `sqlite3`, stdlib `asyncio`, `pytest`, `pytest-asyncio`, `pytest-aiohttp`.

---

## Scope Check

This plan implements the approved Phase 2 spec as one feature because every new runtime behavior is disabled by default and each component has a narrow test boundary. The test-server rollout remains staged by config: session recording, then Emby session observation, then middle-cache reads, then background prefetch.

## File Structure

- `src/emby_range_cache_proxy/config.py`: add `SessionConfig`, `MiddleCacheConfig`, `PrefetchConfig`, parsing, and validation.
- `config.example.json`: document disabled Phase 2 defaults.
- `src/emby_range_cache_proxy/state.py`: SQLite schema, hashed session identity, playback session progress, prefetch task queue, and middle block metadata.
- `src/emby_range_cache_proxy/middle_cache.py`: completed middle block file storage, safe reads, safe writes, TTL/LRU eviction, and metadata updates.
- `src/emby_range_cache_proxy/session.py`: non-blocking foreground session recorder and idle/stopped decision helpers.
- `src/emby_range_cache_proxy/session_observer.py`: read-only Emby `/Sessions` polling with sanitized hashes.
- `src/emby_range_cache_proxy/prefetch.py`: planner, bandwidth limiter, and background prefetch worker.
- `src/emby_range_cache_proxy/app.py`: lifecycle wiring, session recording, middle cache read path, and worker startup.
- `tests/test_config.py`: Phase 2 config parsing and validation.
- `tests/test_state.py`: SQLite state behavior.
- `tests/test_middle_cache.py`: middle block file behavior and eviction.
- `tests/test_session.py`: recorder queue and idle decisions.
- `tests/test_session_observer.py`: Emby session polling behavior.
- `tests/test_prefetch.py`: planner, limiter, and worker behavior.
- `tests/test_app.py`: foreground integration behavior.
- `tests/test_deploy_examples.py`: deployment config example checks.
- `README.md`: document Phase 2 disabled defaults and rollout order.

## Task 1: Phase 2 Config

**Files:**
- Modify: `src/emby_range_cache_proxy/config.py`
- Modify: `config.example.json`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write failing config tests**

Append these tests to `tests/test_config.py`:

```python
from emby_range_cache_proxy.config import MiddleCacheConfig, PrefetchConfig, SessionConfig


def test_phase2_config_defaults_are_disabled(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
            }
        )
    )

    config = load_config(path)

    assert config.session.enabled is False
    assert config.session.state_db is None
    assert config.session.observer_enabled is False
    assert config.session.observer_interval_seconds == 30
    assert config.session.idle_seconds == 180
    assert config.session.stop_grace_seconds == 60
    assert config.session.expire_seconds == 86400
    assert config.middle_cache.enabled is False
    assert config.middle_cache.max_bytes == 128 * 1024**3
    assert config.middle_cache.ttl_seconds == 7 * 24 * 60 * 60
    assert config.middle_cache.segment_bytes == 64 * 1024**2
    assert config.middle_cache.min_free_bytes == 50 * 1024**3
    assert config.prefetch.enabled is False
    assert config.prefetch.window_bytes == 2 * 1024**3
    assert config.prefetch.resume_overlap_bytes == 128 * 1024**2
    assert config.prefetch.max_session_bytes == 4 * 1024**3
    assert config.prefetch.max_queue_depth == 200
    assert config.prefetch.concurrency == 1
    assert config.prefetch.per_origin_concurrency == 1
    assert config.prefetch.bandwidth_bytes_per_second == 30 * 1024**2
    assert config.prefetch.pause_when_rollout_session_active is True
    assert config.prefetch.error_backoff_seconds == 300


def test_phase2_config_reads_explicit_values(tmp_path):
    path = tmp_path / "config.json"
    db_path = tmp_path / "phase2.sqlite3"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "session": {
                    "enabled": True,
                    "state_db": str(db_path),
                    "observer_enabled": True,
                    "observer_interval_seconds": 45,
                    "idle_seconds": 240,
                    "stop_grace_seconds": 90,
                    "expire_seconds": 7200,
                },
                "middle_cache": {
                    "enabled": True,
                    "max_bytes": 123,
                    "ttl_seconds": 456,
                    "segment_bytes": 789,
                    "min_free_bytes": 321,
                },
                "prefetch": {
                    "enabled": True,
                    "window_bytes": 111,
                    "resume_overlap_bytes": 222,
                    "max_session_bytes": 333,
                    "max_queue_depth": 44,
                    "concurrency": 2,
                    "per_origin_concurrency": 1,
                    "bandwidth_bytes_per_second": 555,
                    "pause_when_rollout_session_active": False,
                    "error_backoff_seconds": 66,
                },
            }
        )
    )

    config = load_config(path)

    assert config.session.enabled is True
    assert config.session.state_db == str(db_path)
    assert config.session.observer_enabled is True
    assert config.session.observer_interval_seconds == 45
    assert config.session.idle_seconds == 240
    assert config.session.stop_grace_seconds == 90
    assert config.session.expire_seconds == 7200
    assert config.middle_cache.enabled is True
    assert config.middle_cache.max_bytes == 123
    assert config.middle_cache.ttl_seconds == 456
    assert config.middle_cache.segment_bytes == 789
    assert config.middle_cache.min_free_bytes == 321
    assert config.prefetch.enabled is True
    assert config.prefetch.window_bytes == 111
    assert config.prefetch.resume_overlap_bytes == 222
    assert config.prefetch.max_session_bytes == 333
    assert config.prefetch.max_queue_depth == 44
    assert config.prefetch.concurrency == 2
    assert config.prefetch.per_origin_concurrency == 1
    assert config.prefetch.bandwidth_bytes_per_second == 555
    assert config.prefetch.pause_when_rollout_session_active is False
    assert config.prefetch.error_backoff_seconds == 66


@pytest.mark.parametrize(
    ("factory", "kwargs", "match"),
    [
        (SessionConfig, {"observer_interval_seconds": 0}, "observer_interval_seconds"),
        (SessionConfig, {"idle_seconds": 0}, "idle_seconds"),
        (SessionConfig, {"stop_grace_seconds": 0}, "stop_grace_seconds"),
        (SessionConfig, {"expire_seconds": 0}, "expire_seconds"),
        (MiddleCacheConfig, {"max_bytes": 0}, "middle_cache.max_bytes"),
        (MiddleCacheConfig, {"ttl_seconds": 0}, "middle_cache.ttl_seconds"),
        (MiddleCacheConfig, {"segment_bytes": 0}, "middle_cache.segment_bytes"),
        (MiddleCacheConfig, {"min_free_bytes": -1}, "middle_cache.min_free_bytes"),
        (PrefetchConfig, {"window_bytes": 0}, "prefetch.window_bytes"),
        (PrefetchConfig, {"resume_overlap_bytes": -1}, "prefetch.resume_overlap_bytes"),
        (PrefetchConfig, {"max_session_bytes": 0}, "prefetch.max_session_bytes"),
        (PrefetchConfig, {"max_queue_depth": 0}, "prefetch.max_queue_depth"),
        (PrefetchConfig, {"concurrency": 0}, "prefetch.concurrency"),
        (PrefetchConfig, {"per_origin_concurrency": 0}, "prefetch.per_origin_concurrency"),
        (PrefetchConfig, {"bandwidth_bytes_per_second": 0}, "prefetch.bandwidth_bytes_per_second"),
        (PrefetchConfig, {"error_backoff_seconds": 0}, "prefetch.error_backoff_seconds"),
    ],
)
def test_phase2_config_rejects_invalid_values(factory, kwargs, match):
    with pytest.raises(ValueError, match=match):
        factory(**kwargs)
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest tests/test_config.py -q
```

Expected: FAIL with `ImportError` or `AttributeError` for `SessionConfig`, `MiddleCacheConfig`, and `PrefetchConfig`.

- [ ] **Step 3: Implement config dataclasses and parsing**

In `src/emby_range_cache_proxy/config.py`, add these dataclasses after `PrewarmConfig`:

```python
@dataclass
class SessionConfig:
    enabled: bool = False
    state_db: str | None = None
    observer_enabled: bool = False
    observer_interval_seconds: int = 30
    idle_seconds: int = 180
    stop_grace_seconds: int = 60
    expire_seconds: int = 24 * 60 * 60

    def __post_init__(self) -> None:
        if self.observer_interval_seconds <= 0:
            raise ValueError("session.observer_interval_seconds must be positive")
        if self.idle_seconds <= 0:
            raise ValueError("session.idle_seconds must be positive")
        if self.stop_grace_seconds <= 0:
            raise ValueError("session.stop_grace_seconds must be positive")
        if self.expire_seconds <= 0:
            raise ValueError("session.expire_seconds must be positive")


@dataclass
class MiddleCacheConfig:
    enabled: bool = False
    max_bytes: int = 128 * 1024**3
    ttl_seconds: int = 7 * 24 * 60 * 60
    segment_bytes: int = 64 * 1024**2
    min_free_bytes: int = 50 * 1024**3

    def __post_init__(self) -> None:
        if self.max_bytes <= 0:
            raise ValueError("middle_cache.max_bytes must be positive")
        if self.ttl_seconds <= 0:
            raise ValueError("middle_cache.ttl_seconds must be positive")
        if self.segment_bytes <= 0:
            raise ValueError("middle_cache.segment_bytes must be positive")
        if self.min_free_bytes < 0:
            raise ValueError("middle_cache.min_free_bytes must be non-negative")


@dataclass
class PrefetchConfig:
    enabled: bool = False
    window_bytes: int = 2 * 1024**3
    resume_overlap_bytes: int = 128 * 1024**2
    max_session_bytes: int = 4 * 1024**3
    max_queue_depth: int = 200
    concurrency: int = 1
    per_origin_concurrency: int = 1
    bandwidth_bytes_per_second: int = 30 * 1024**2
    pause_when_rollout_session_active: bool = True
    error_backoff_seconds: int = 300

    def __post_init__(self) -> None:
        if self.window_bytes <= 0:
            raise ValueError("prefetch.window_bytes must be positive")
        if self.resume_overlap_bytes < 0:
            raise ValueError("prefetch.resume_overlap_bytes must be non-negative")
        if self.max_session_bytes <= 0:
            raise ValueError("prefetch.max_session_bytes must be positive")
        if self.max_queue_depth <= 0:
            raise ValueError("prefetch.max_queue_depth must be positive")
        if self.concurrency <= 0:
            raise ValueError("prefetch.concurrency must be positive")
        if self.per_origin_concurrency <= 0:
            raise ValueError("prefetch.per_origin_concurrency must be positive")
        if self.bandwidth_bytes_per_second <= 0:
            raise ValueError("prefetch.bandwidth_bytes_per_second must be positive")
        if self.error_backoff_seconds <= 0:
            raise ValueError("prefetch.error_backoff_seconds must be positive")
```

Add fields to `Config`:

```python
    session: SessionConfig = field(default_factory=SessionConfig)
    middle_cache: MiddleCacheConfig = field(default_factory=MiddleCacheConfig)
    prefetch: PrefetchConfig = field(default_factory=PrefetchConfig)
```

Add parser helpers:

```python
def _session(data: dict[str, Any]) -> SessionConfig:
    return SessionConfig(
        enabled=bool(data.get("enabled", False)),
        state_db=None if data.get("state_db") is None else str(data["state_db"]),
        observer_enabled=bool(data.get("observer_enabled", False)),
        observer_interval_seconds=int(data.get("observer_interval_seconds", 30)),
        idle_seconds=int(data.get("idle_seconds", 180)),
        stop_grace_seconds=int(data.get("stop_grace_seconds", 60)),
        expire_seconds=int(data.get("expire_seconds", 24 * 60 * 60)),
    )


def _middle_cache(data: dict[str, Any]) -> MiddleCacheConfig:
    return MiddleCacheConfig(
        enabled=bool(data.get("enabled", False)),
        max_bytes=int(data.get("max_bytes", 128 * 1024**3)),
        ttl_seconds=int(data.get("ttl_seconds", 7 * 24 * 60 * 60)),
        segment_bytes=int(data.get("segment_bytes", 64 * 1024**2)),
        min_free_bytes=int(data.get("min_free_bytes", 50 * 1024**3)),
    )


def _prefetch(data: dict[str, Any]) -> PrefetchConfig:
    return PrefetchConfig(
        enabled=bool(data.get("enabled", False)),
        window_bytes=int(data.get("window_bytes", 2 * 1024**3)),
        resume_overlap_bytes=int(data.get("resume_overlap_bytes", 128 * 1024**2)),
        max_session_bytes=int(data.get("max_session_bytes", 4 * 1024**3)),
        max_queue_depth=int(data.get("max_queue_depth", 200)),
        concurrency=int(data.get("concurrency", 1)),
        per_origin_concurrency=int(data.get("per_origin_concurrency", 1)),
        bandwidth_bytes_per_second=int(data.get("bandwidth_bytes_per_second", 30 * 1024**2)),
        pause_when_rollout_session_active=bool(data.get("pause_when_rollout_session_active", True)),
        error_backoff_seconds=int(data.get("error_backoff_seconds", 300)),
    )
```

In `load_config`, pass:

```python
        session=_session(raw.get("session", {})),
        middle_cache=_middle_cache(raw.get("middle_cache", {})),
        prefetch=_prefetch(raw.get("prefetch", {})),
```

- [ ] **Step 4: Update `config.example.json`**

Add disabled Phase 2 sections after `prewarm`:

```json
  "session": {
    "enabled": false,
    "state_db": null,
    "observer_enabled": false,
    "observer_interval_seconds": 30,
    "idle_seconds": 180,
    "stop_grace_seconds": 60,
    "expire_seconds": 86400
  },
  "middle_cache": {
    "enabled": false,
    "max_bytes": 137438953472,
    "ttl_seconds": 604800,
    "segment_bytes": 67108864,
    "min_free_bytes": 53687091200
  },
  "prefetch": {
    "enabled": false,
    "window_bytes": 2147483648,
    "resume_overlap_bytes": 134217728,
    "max_session_bytes": 4294967296,
    "max_queue_depth": 200,
    "concurrency": 1,
    "per_origin_concurrency": 1,
    "bandwidth_bytes_per_second": 31457280,
    "pause_when_rollout_session_active": true,
    "error_backoff_seconds": 300
  }
```

Keep the file valid JSON by adding a comma after the existing `prewarm` object.

- [ ] **Step 5: Run config tests and commit**

Run:

```bash
python -m pytest tests/test_config.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/config.py config.example.json tests/test_config.py
git commit -m "Add phase 2 configuration"
```

## Task 2: SQLite State Store

**Files:**
- Create: `src/emby_range_cache_proxy/state.py`
- Create: `tests/test_state.py`

- [ ] **Step 1: Write failing state tests**

Create `tests/test_state.py`:

```python
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
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest tests/test_state.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'emby_range_cache_proxy.state'`.

- [ ] **Step 3: Implement `state.py` public API**

Create `src/emby_range_cache_proxy/state.py` with these public dataclasses and methods:

```python
from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable

from .models import ByteRange


SESSION_STATUSES = {"active", "idle", "stopped", "expired"}
TASK_STATUSES = {"queued", "running", "done", "failed", "skipped"}


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

    def with_range(self, byte_range: ByteRange, *, observed_at: float) -> "PlaybackSessionUpdate":
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
```

Implement `SessionStateStore` as a synchronous class using one SQLite connection per method call. The constructor is:

```python
class SessionStateStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
```

The class must expose these exact method signatures:

- `record_playback(self, update: PlaybackSessionUpdate) -> None`
- `get_session(self, session_hash: str) -> PlaybackSessionRecord | None`
- `recent_active_sessions(self, *, now: float, active_seconds: int) -> list[PlaybackSessionRecord]`
- `mark_idle_sessions(self, *, now: float, idle_seconds: int) -> list[PlaybackSessionRecord]`
- `expire_old_sessions(self, *, now: float, expire_seconds: int) -> int`
- `record_observed_sessions(self, session_hashes: set[str], *, observed_at: float) -> None`
- `mark_missing_observed_sessions_stopped(self, *, now: float, stop_grace_seconds: int) -> list[PlaybackSessionRecord]`
- `update_session_queued_until(self, session_hash: str, queued_until: int, *, now: float) -> None`
- `enqueue_prefetch_task(self, item_id: str, media_source_id: str, cache_key: str, start: int, end: int, *, priority: int, now: float, max_queue_depth: int) -> PrefetchTaskRecord | None`
- `claim_prefetch_tasks(self, *, limit: int, now: float) -> list[PrefetchTaskRecord]`
- `complete_prefetch_task(self, task_id: int, *, now: float) -> None`
- `fail_prefetch_task(self, task_id: int, *, error_class: str, now: float) -> None`
- `queue_depth(self) -> int`
- `upsert_middle_block(self, block: MiddleBlockRecord) -> None`
- `find_middle_block(self, cache_key: str, byte_range: ByteRange) -> MiddleBlockRecord | None`
- `touch_middle_block(self, cache_key: str, start: int, end: int, *, now: float, ttl_seconds: int) -> None`
- `expired_middle_blocks(self, *, now: float) -> list[MiddleBlockRecord]`
- `least_recent_middle_blocks(self) -> list[MiddleBlockRecord]`
- `delete_middle_block_record(self, cache_key: str, start: int, end: int) -> None`
- `middle_cache_bytes(self) -> int`

Use this schema:

```sql
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
```

Mapping rules:

- `record_playback` inserts new sessions with `status='active'`; existing `idle` or `stopped` sessions become `active` again on a new foreground range.
- `max_observed_offset` is the greater of the existing value and the new range end.
- `mark_idle_sessions` updates only rows with `status='active'` and `last_seen_at <= now - idle_seconds`.
- `record_observed_sessions` updates `last_emby_observed_at` for provided hashes.
- `mark_missing_observed_sessions_stopped` updates rows with non-null `last_emby_observed_at`, `status IN ('active', 'idle')`, and `last_emby_observed_at <= now - stop_grace_seconds`.
- `claim_prefetch_tasks` selects queued rows ordered by `priority DESC, created_at ASC`, then updates them to `running` and increments `attempts`.
- `queue_depth` counts rows with `status='queued'`.
- `find_middle_block` returns a row whose `start <= byte_range.start` and `end >= byte_range.end`.

- [ ] **Step 4: Run state tests and commit**

Run:

```bash
python -m pytest tests/test_state.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/state.py tests/test_state.py
git commit -m "Add phase 2 state store"
```

## Task 3: Middle Range Cache

**Files:**
- Create: `src/emby_range_cache_proxy/middle_cache.py`
- Create: `tests/test_middle_cache.py`

- [ ] **Step 1: Write failing middle-cache tests**

Create `tests/test_middle_cache.py`:

```python
import pytest

from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.state import SessionStateStore


def _key(char="a"):
    return char * 64


def test_middle_cache_store_and_iter_block(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    cache.store_block(_key(), ByteRange(1024, 1033), b"0123456789", now=10.0)
    chunks = cache.iter_block(_key(), ByteRange(1026, 1030), chunk_bytes=2, now=20.0)

    assert chunks is not None
    assert list(chunks) == [b"23", b"45", b"6"]
    block = store.find_middle_block(_key(), ByteRange(1026, 1030))
    assert block.last_access_at == 20.0
    assert block.expires_at == 80.0


def test_middle_cache_miss_for_partial_coverage(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    cache.store_block(_key(), ByteRange(1024, 1033), b"0123456789", now=10.0)

    assert cache.iter_block(_key(), ByteRange(1030, 1035), chunk_bytes=2, now=20.0) is None


def test_middle_cache_rejects_invalid_key(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    with pytest.raises(ValueError, match="cache key"):
        cache.store_block("../bad", ByteRange(0, 1), b"ab", now=1.0)


def test_middle_cache_removes_truncated_file(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)

    cache.store_block(_key(), ByteRange(1024, 1033), b"0123456789", now=10.0)
    block = store.find_middle_block(_key(), ByteRange(1024, 1033))
    (tmp_path / "cache" / block.path).write_bytes(b"short")

    assert cache.iter_block(_key(), ByteRange(1024, 1033), chunk_bytes=4, now=20.0) is None
    assert store.find_middle_block(_key(), ByteRange(1024, 1033)) is None


def test_middle_cache_evicts_expired_and_lru_blocks(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    cache = MiddleRangeCache(tmp_path / "cache", store, max_bytes=15, ttl_seconds=10)

    cache.store_block(_key("a"), ByteRange(0, 9), b"aaaaaaaaaa", now=1.0)
    cache.store_block(_key("b"), ByteRange(0, 9), b"bbbbbbbbbb", now=2.0)
    expired = cache.evict_expired(now=12.0)
    cache.store_block(_key("c"), ByteRange(0, 9), b"cccccccccc", now=13.0)
    cache.store_block(_key("d"), ByteRange(0, 9), b"dddddddddd", now=14.0)
    lru = cache.evict_lru_if_needed()

    assert expired == 1
    assert lru == 1
    assert store.find_middle_block(_key("a"), ByteRange(0, 9)) is None
    assert store.find_middle_block(_key("c"), ByteRange(0, 9)) is None
    assert store.find_middle_block(_key("d"), ByteRange(0, 9)) is not None
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest tests/test_middle_cache.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'emby_range_cache_proxy.middle_cache'`.

- [ ] **Step 3: Implement `MiddleRangeCache`**

Create `src/emby_range_cache_proxy/middle_cache.py` with this constructor and public method contract:

```python
from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Iterator

from .cache import CacheReadError, KEY_PATTERN
from .models import ByteRange
from .state import MiddleBlockRecord, SessionStateStore


RANGE_NAME_PATTERN = re.compile(r"^(?P<start>\\d+)-(?P<end>\\d+)$")


class MiddleRangeCache:
    def __init__(self, root: str | Path, store: SessionStateStore, *, max_bytes: int, ttl_seconds: int) -> None:
        self.root = Path(root)
        self.store = store
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self.root.mkdir(parents=True, exist_ok=True)
```

The class must expose:

- `store_block(self, key: str, byte_range: ByteRange, data: bytes, *, now: float) -> None`
- `iter_block(self, key: str, requested: ByteRange, *, chunk_bytes: int, now: float) -> Iterator[bytes] | None`
- `evict_expired(self, *, now: float) -> int`
- `evict_lru_if_needed(self) -> int`
- `remove_block(self, record: MiddleBlockRecord) -> None`

Implementation rules:

- Validate keys with `KEY_PATTERN.fullmatch`.
- Store files at `{root}/{key}/mid/{start}-{end}.bin`.
- Store range sidecars at `{root}/{key}/mid/{start}-{end}.range`.
- Write temp files named `{start}-{end}.bin.{uuid}.tmp`.
- Commit with `os.replace`.
- Store relative paths in SQLite, for example `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/mid/1024-2047.bin`.
- `store_block` raises `ValueError("data length must match byte_range length")` when `len(data) != byte_range.length`.
- `iter_block` returns `None` unless one complete metadata row contains the requested range and both `.bin` and `.range` files are valid.
- `iter_block` touches metadata with `store.touch_middle_block(key, record.start, record.end, now=now, ttl_seconds=self.ttl_seconds)` before returning chunks.
- The chunk iterator raises `CacheReadError` if a read returns fewer bytes than expected after initial validation.
- `remove_block` deletes `.bin`, `.range`, and the SQLite metadata row.
- `evict_expired` deletes `store.expired_middle_blocks(now=now)`.
- `evict_lru_if_needed` deletes `store.least_recent_middle_blocks()` until `store.middle_cache_bytes() <= self.max_bytes`.

- [ ] **Step 4: Run middle-cache tests and commit**

Run:

```bash
python -m pytest tests/test_middle_cache.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/middle_cache.py tests/test_middle_cache.py
git commit -m "Add middle range cache"
```

## Task 4: Session Recorder And Idle Decisions

**Files:**
- Create: `src/emby_range_cache_proxy/session.py`
- Create: `tests/test_session.py`

- [ ] **Step 1: Write failing session tests**

Create `tests/test_session.py`:

```python
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
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest tests/test_session.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'emby_range_cache_proxy.session'`.

- [ ] **Step 3: Implement `session.py`**

Create `src/emby_range_cache_proxy/session.py`:

```python
from __future__ import annotations

import asyncio
import hashlib
import time
from contextlib import suppress

from .config import SessionConfig
from .models import ByteRange, RequestContext, SourceMetadata
from .state import PlaybackSessionUpdate, SessionStateStore, hash_identifier


def origin_signature(metadata: SourceMetadata) -> str:
    material = "\n".join([metadata.url, str(metadata.size), metadata.etag or "", metadata.last_modified or ""])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def build_session_update(
    *,
    ctx: RequestContext,
    cache_key: str,
    metadata: SourceMetadata,
    byte_range: ByteRange,
    observed_at: float,
) -> PlaybackSessionUpdate:
    device_hash = hash_identifier(ctx.device_id)
    if ctx.play_session_id:
        session_hash = hash_identifier(ctx.play_session_id)
    else:
        bucket = int(observed_at // 900)
        session_hash = hash_identifier(f"synthetic:{ctx.item_id}:{ctx.media_source_id}:{device_hash}:{bucket}")
    assert session_hash is not None
    return PlaybackSessionUpdate(
        session_hash=session_hash,
        device_hash=device_hash,
        item_id=ctx.item_id,
        media_source_id=ctx.media_source_id,
        cache_key=cache_key,
        origin_signature=origin_signature(metadata),
        media_size=metadata.size,
        byte_range=byte_range,
        observed_at=observed_at,
    )


class SessionRecorder:
    def __init__(self, store: SessionStateStore, *, queue_size: int = 1000) -> None:
        self.store = store
        self.queue: asyncio.Queue[PlaybackSessionUpdate] = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    def record_nowait(
        self,
        ctx: RequestContext,
        cache_key: str,
        metadata: SourceMetadata,
        byte_range: ByteRange,
        *,
        observed_at: float | None = None,
    ) -> bool:
        update = build_session_update(
            ctx=ctx,
            cache_key=cache_key,
            metadata=metadata,
            byte_range=byte_range,
            observed_at=time.time() if observed_at is None else observed_at,
        )
        try:
            self.queue.put_nowait(update)
            return True
        except asyncio.QueueFull:
            return False

    async def drain_once(self) -> int:
        count = 0
        while True:
            try:
                update = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return count
            await asyncio.to_thread(self.store.record_playback, update)
            count += 1

    async def run(self) -> None:
        while not self._stopped.is_set():
            update = await self.queue.get()
            await asyncio.to_thread(self.store.record_playback, update)

    def start(self) -> None:
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    def mark_idle_and_expired(self, config: SessionConfig, *, now: float) -> list:
        idle = self.store.mark_idle_sessions(now=now, idle_seconds=config.idle_seconds)
        self.store.expire_old_sessions(now=now, expire_seconds=config.expire_seconds)
        return idle
```

- [ ] **Step 4: Run session tests and commit**

Run:

```bash
python -m pytest tests/test_session.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/session.py tests/test_session.py
git commit -m "Record playback session state"
```

## Task 5: Emby Session Observer

**Files:**
- Create: `src/emby_range_cache_proxy/session_observer.py`
- Create: `tests/test_session_observer.py`

- [ ] **Step 1: Write failing observer tests**

Create `tests/test_session_observer.py`:

```python
from aiohttp import web

from emby_range_cache_proxy.config import Config, SessionConfig
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.session_observer import EmbySessionObserver, extract_observed_session_hashes
from emby_range_cache_proxy.state import PlaybackSessionUpdate, SessionStateStore, hash_identifier


def test_extract_observed_session_hashes_ignores_missing_ids():
    payload = [
        {"PlaySessionId": "play1", "DeviceId": "dev1"},
        {"NowPlayingItem": {"Id": "1"}},
        "bad",
    ]

    observed = extract_observed_session_hashes(payload)

    assert observed == {hash_identifier("play1")}


async def test_observer_records_seen_sessions_and_marks_missing_stopped(aiohttp_client, tmp_path):
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
            session=SessionConfig(enabled=True, observer_enabled=True, stop_grace_seconds=60),
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
        Config(emby_base_url="http://emby", fallback_base_url="http://emby", cache_dir=str(tmp_path / "cache")),
        store,
    )

    result = await observer.run_once(now=1.0)

    assert result.observed == 0
    assert result.stopped == 0
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
python -m pytest tests/test_session_observer.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'emby_range_cache_proxy.session_observer'`.

- [ ] **Step 3: Implement observer**

Create `src/emby_range_cache_proxy/session_observer.py`:

```python
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from aiohttp import ClientError, ClientSession

from .config import Config
from .state import PlaybackSessionRecord, SessionStateStore, hash_identifier


@dataclass(frozen=True)
class ObserverResult:
    observed: int
    stopped: int
    stopped_sessions: Sequence[PlaybackSessionRecord] = ()


def extract_observed_session_hashes(payload: Any) -> set[str]:
    if not isinstance(payload, list):
        return set()
    observed: set[str] = set()
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        play_session_id = entry.get("PlaySessionId")
        if play_session_id:
            hashed = hash_identifier(str(play_session_id))
            if hashed is not None:
                observed.add(hashed)
    return observed


class EmbySessionObserver:
    def __init__(self, config: Config, store: SessionStateStore) -> None:
        self.config = config
        self.store = store

    async def run_once(self, *, now: float) -> ObserverResult:
        if not self.config.session.observer_enabled or not self.config.prewarm_api_key:
            return ObserverResult(observed=0, stopped=0)
        payload = await self._sessions_payload()
        if payload is None:
            return ObserverResult(observed=0, stopped=0)
        observed = extract_observed_session_hashes(payload)
        self.store.record_observed_sessions(observed, observed_at=now)
        stopped_sessions = self.store.mark_missing_observed_sessions_stopped(
            now=now,
            stop_grace_seconds=self.config.session.stop_grace_seconds,
        )
        return ObserverResult(
            observed=len(observed),
            stopped=len(stopped_sessions),
            stopped_sessions=tuple(stopped_sessions),
        )

    async def _sessions_payload(self) -> Any | None:
        url = f"{self.config.emby_base_url.rstrip('/')}/Sessions"
        try:
            async with ClientSession() as session:
                async with session.get(url, params={"api_key": self.config.prewarm_api_key}) as response:
                    if response.status >= 400:
                        return None
                    return await response.json()
        except (ClientError, TimeoutError, OSError, ValueError):
            return None
```

- [ ] **Step 4: Run observer tests and commit**

Run:

```bash
python -m pytest tests/test_session_observer.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/session_observer.py tests/test_session_observer.py
git commit -m "Observe Emby playback sessions"
```

## Task 6: Prefetch Planner

**Files:**
- Create: `src/emby_range_cache_proxy/prefetch.py`
- Create: `tests/test_prefetch.py`

- [ ] **Step 1: Write failing planner tests**

Create `tests/test_prefetch.py` with the planner tests first:

```python
from emby_range_cache_proxy.config import MiddleCacheConfig, PrefetchConfig
from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.prefetch import plan_middle_ranges


def test_plan_middle_ranges_aligns_skips_head_tail_and_caps_window():
    ranges = plan_middle_ranges(
        media_size=1000,
        head_size=100,
        tail_size=100,
        max_observed_offset=350,
        queued_until=None,
        prefetch=PrefetchConfig(window_bytes=256, resume_overlap_bytes=50, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == [
        ByteRange(256, 319),
        ByteRange(320, 383),
        ByteRange(384, 447),
        ByteRange(448, 511),
        ByteRange(512, 575),
    ]


def test_plan_middle_ranges_deduplicates_using_queued_until():
    ranges = plan_middle_ranges(
        media_size=1000,
        head_size=100,
        tail_size=100,
        max_observed_offset=350,
        queued_until=511,
        prefetch=PrefetchConfig(window_bytes=256, resume_overlap_bytes=50, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == [ByteRange(512, 575)]


def test_plan_middle_ranges_returns_empty_when_no_middle_space():
    ranges = plan_middle_ranges(
        media_size=200,
        head_size=128,
        tail_size=64,
        max_observed_offset=100,
        queued_until=None,
        prefetch=PrefetchConfig(window_bytes=256, resume_overlap_bytes=0, max_session_bytes=512),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
    )

    assert ranges == []
```

- [ ] **Step 2: Run planner tests and verify failure**

Run:

```bash
python -m pytest tests/test_prefetch.py -q
```

Expected: FAIL with `ModuleNotFoundError` or `ImportError` for `plan_middle_ranges`.

- [ ] **Step 3: Implement planner in `prefetch.py`**

Create `src/emby_range_cache_proxy/prefetch.py` with:

```python
from __future__ import annotations

from .config import MiddleCacheConfig, PrefetchConfig
from .models import ByteRange


def align_down(value: int, alignment: int) -> int:
    return value - (value % alignment)


def plan_middle_ranges(
    *,
    media_size: int,
    head_size: int,
    tail_size: int,
    max_observed_offset: int,
    queued_until: int | None,
    prefetch: PrefetchConfig,
    middle_cache: MiddleCacheConfig,
) -> list[ByteRange]:
    segment = middle_cache.segment_bytes
    head_end = min(head_size, media_size) - 1
    tail_start = max(0, media_size - tail_size)
    middle_start = head_end + 1
    middle_end = tail_start - 1
    if middle_start > middle_end:
        return []

    start = max(middle_start, max_observed_offset - prefetch.resume_overlap_bytes)
    start = max(middle_start, align_down(start, segment))
    if queued_until is not None:
        start = max(start, queued_until + 1)
        start = max(middle_start, align_down(start, segment))
    window_end = min(start + prefetch.window_bytes - 1, middle_end)
    session_end = min(start + prefetch.max_session_bytes - 1, window_end)

    ranges: list[ByteRange] = []
    current = start
    while current <= session_end:
        end = min(current + segment - 1, session_end, middle_end)
        if end >= middle_start and current <= middle_end:
            ranges.append(ByteRange(max(current, middle_start), end))
        current = end + 1
    return ranges
```

- [ ] **Step 4: Run planner tests and commit planner slice**

Run:

```bash
python -m pytest tests/test_prefetch.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/prefetch.py tests/test_prefetch.py
git commit -m "Plan middle range prefetch tasks"
```

## Task 7: Prefetch Worker And Bandwidth Limiter

**Files:**
- Modify: `src/emby_range_cache_proxy/prefetch.py`
- Modify: `tests/test_prefetch.py`

- [ ] **Step 1: Add failing worker tests**

Append to `tests/test_prefetch.py`:

```python
import asyncio

from aiohttp import web

from emby_range_cache_proxy.config import Config, MiddleCacheConfig, PrefetchConfig, RolloutConfig
from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.prefetch import BandwidthLimiter, PrefetchWorker
from emby_range_cache_proxy.state import SessionStateStore


async def test_bandwidth_limiter_waits_when_chunk_exceeds_rate():
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    limiter = BandwidthLimiter(bytes_per_second=10, sleep=fake_sleep)

    await limiter.consume(25)

    assert sleeps == [2.5]


async def test_prefetch_worker_fetches_claimed_task_into_middle_cache(aiohttp_client, tmp_path):
    async def origin(request):
        assert request.headers["Range"] == "bytes=10-19"
        return web.Response(status=206, body=b"0123456789", headers={"Content-Range": "bytes 10-19/20"})

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin)
    origin = await aiohttp_client(origin_app)
    store = SessionStateStore(tmp_path / "state.sqlite3")
    middle = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)
    task = store.enqueue_prefetch_task(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        start=10,
        end=19,
        priority=10,
        now=1.0,
        max_queue_depth=10,
    )
    worker = PrefetchWorker(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path / "cache"),
            rollout=RolloutConfig(enabled=True),
            middle_cache=MiddleCacheConfig(enabled=True, ttl_seconds=60),
            prefetch=PrefetchConfig(enabled=True, concurrency=1, bandwidth_bytes_per_second=1024 * 1024),
        ),
        store,
        middle,
        source_lookup={("1", "ms1"): str(origin.make_url("/movie.mkv"))},
    )

    result = await worker.run_once(now=2.0)

    assert result.completed == 1
    assert result.failed == 0
    chunks = middle.iter_block("a" * 64, ByteRange(10, 19), chunk_bytes=4, now=3.0)
    assert chunks is not None
    assert b"".join(chunks) == b"0123456789"


async def test_prefetch_worker_skips_when_disabled(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    middle = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)
    worker = PrefetchWorker(
        Config(emby_base_url="http://emby", fallback_base_url="http://emby", cache_dir=str(tmp_path / "cache")),
        store,
        middle,
        source_lookup={},
    )

    result = await worker.run_once(now=1.0)

    assert result.completed == 0
    assert result.failed == 0
    assert result.skipped == 0
```

- [ ] **Step 2: Run worker tests and verify failure**

Run:

```bash
python -m pytest tests/test_prefetch.py -q
```

Expected: FAIL with missing `BandwidthLimiter` and `PrefetchWorker`.

- [ ] **Step 3: Implement worker classes**

Append to `src/emby_range_cache_proxy/prefetch.py`:

```python
import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from aiohttp import ClientError

from .config import Config
from .middle_cache import MiddleRangeCache
from .origin import OriginClient, OriginError
from .state import PrefetchTaskRecord, SessionStateStore


SleepCallable = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class PrefetchRunResult:
    completed: int
    failed: int
    skipped: int


class BandwidthLimiter:
    def __init__(self, *, bytes_per_second: int, sleep: SleepCallable = asyncio.sleep) -> None:
        self.bytes_per_second = bytes_per_second
        self.sleep = sleep

    async def consume(self, byte_count: int) -> None:
        if byte_count <= 0:
            return
        await self.sleep(byte_count / self.bytes_per_second)


class PrefetchWorker:
    def __init__(
        self,
        config: Config,
        store: SessionStateStore,
        middle_cache: MiddleRangeCache,
        *,
        source_lookup: dict[tuple[str, str], str] | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.middle_cache = middle_cache
        self.source_lookup = source_lookup or {}
        self.limiter = BandwidthLimiter(bytes_per_second=config.prefetch.bandwidth_bytes_per_second)

    async def run_once(self, *, now: float) -> PrefetchRunResult:
        if not self.config.prefetch.enabled or not self.config.middle_cache.enabled:
            return PrefetchRunResult(completed=0, failed=0, skipped=0)
        tasks = self.store.claim_prefetch_tasks(limit=self.config.prefetch.concurrency, now=now)
        completed = failed = skipped = 0
        for task in tasks:
            outcome = await self._run_task(task, now=now)
            if outcome == "completed":
                completed += 1
            elif outcome == "failed":
                failed += 1
            else:
                skipped += 1
        return PrefetchRunResult(completed=completed, failed=failed, skipped=skipped)

    async def _run_task(self, task: PrefetchTaskRecord, *, now: float) -> str:
        url = self.source_lookup.get((task.item_id, task.media_source_id))
        if url is None:
            self.store.fail_prefetch_task(task.id, error_class="SourceUnavailable", now=now)
            return "skipped"
        writer_data = bytearray()
        byte_range = ByteRange(task.start, task.end)
        try:
            async with OriginClient(chunk_bytes=self.config.cache.chunk_bytes) as origin:
                source_size = max(task.end + 1, 1)
                async with origin.open_range(url, byte_range, size=source_size) as upstream:
                    async for chunk in upstream.content.iter_chunked(self.config.cache.chunk_bytes):
                        if chunk:
                            await self.limiter.consume(len(chunk))
                            writer_data.extend(chunk)
            self.middle_cache.store_block(task.cache_key, byte_range, bytes(writer_data), now=now)
            self.middle_cache.evict_expired(now=now)
            self.middle_cache.evict_lru_if_needed()
            self.store.complete_prefetch_task(task.id, now=now)
            return "completed"
        except (OriginError, ClientError, TimeoutError, OSError, ValueError) as error:
            self.store.fail_prefetch_task(task.id, error_class=type(error).__name__, now=now)
            return "failed"
```

The worker source lookup is intentionally injected in this task. The app integration task replaces test-only lookup with a resolver that uses authorized session metadata and Emby source resolution.

- [ ] **Step 4: Run worker tests and commit**

Run:

```bash
python -m pytest tests/test_prefetch.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/prefetch.py tests/test_prefetch.py
git commit -m "Run bounded prefetch tasks"
```

## Task 8: App Integration For Session Recording And Middle Cache Reads

**Files:**
- Modify: `src/emby_range_cache_proxy/app.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: Add failing integration tests**

Append to `tests/test_app.py`:

```python
from emby_range_cache_proxy.middle_cache import MiddleRangeCache
from emby_range_cache_proxy.state import SessionStateStore, hash_identifier


async def test_authorized_request_records_session_state(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))

    async def playback_info(request):
        return web.json_response({"MediaSources": [{"Id": "ms1", "Path": str(origin_server.make_url("/movie.mkv")), "Protocol": "Http", "Size": 100}]})

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        return web.Response(status=206, body=b"0123456789abcdef", headers={"Content-Range": "bytes 0-15/100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)
    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path / "cache"),
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        session=SessionConfig(enabled=True),
    )
    app = create_app(config)
    client = await aiohttp_client(app)

    response = await client.get(
        "/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t&PlaySessionId=play1&DeviceId=dev1",
        headers={"Range": "bytes=0-9"},
    )
    assert response.status == 206
    await response.read()
    await app["session_recorder"].drain_once()

    store = app["phase2_store"]
    session = store.get_session(hash_identifier("play1"))
    assert session is not None
    assert session.item_id == "1"
    assert session.max_observed_offset == 9


async def test_authorized_middle_cache_hit_does_not_touch_origin_get(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))
    origin_gets = 0

    async def playback_info(request):
        return web.json_response({"MediaSources": [{"Id": "ms1", "Path": str(origin_server.make_url("/movie.mkv")), "Protocol": "Http", "Size": 100}]})

    async def origin(request):
        nonlocal origin_gets
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        origin_gets += 1
        return web.Response(status=500, body=b"origin get should not run")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)
    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path / "cache"),
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        middle_cache=MiddleCacheConfig(enabled=True, ttl_seconds=60),
    )
    app = create_app(config)
    store = app["phase2_store"]
    middle = app["middle_cache"]
    source = app_module.MediaSource("1", "ms1", str(origin_server.make_url("/movie.mkv")), "Http", 100)
    metadata = SourceMetadata(url=str(origin_server.make_url("/movie.mkv")), size=100)
    key = app_module.cache_key(source, metadata)
    middle.store_block(key, ByteRange(32, 47), b"middle-cache-hit", now=1.0)
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=32-47"})

    assert response.status == 206
    assert await response.read() == b"middle-cache-hit"
    assert origin_gets == 0


async def test_middle_cache_miss_proxies_origin_without_writing_middle_block(aiohttp_client, tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "adaptive_head_tail", lambda size: (16, 4))

    async def playback_info(request):
        return web.json_response({"MediaSources": [{"Id": "ms1", "Path": str(origin_server.make_url("/movie.mkv")), "Protocol": "Http", "Size": 100}]})

    async def origin(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "100"})
        assert request.headers["Range"] == "bytes=32-47"
        return web.Response(status=206, body=b"origin-middle!!!", headers={"Content-Range": "bytes 32-47/100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)
    origin_app = web.Application()
    origin_app.router.add_route("*", "/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path / "cache"),
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        middle_cache=MiddleCacheConfig(enabled=True, ttl_seconds=60),
    )
    app = create_app(config)
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=32-47"})

    assert response.status == 206
    assert await response.read() == b"origin-middle!!!"
    assert list((tmp_path / "cache").glob("*/mid/*.bin")) == []
```

Add missing imports at the top of `tests/test_app.py`:

```python
from emby_range_cache_proxy.config import MiddleCacheConfig, SessionConfig
```

- [ ] **Step 2: Run app tests and verify failure**

Run:

```bash
python -m pytest tests/test_app.py -q
```

Expected: FAIL with missing `phase2_store`, `session_recorder`, or `middle_cache` app keys.

- [ ] **Step 3: Wire Phase 2 state in `create_app`**

Modify `create_app` in `src/emby_range_cache_proxy/app.py`:

```python
from pathlib import Path

from .middle_cache import MiddleRangeCache
from .session import SessionRecorder
from .state import SessionStateStore
```

After head/tail cache initialization, add:

```python
    if config.session.enabled or config.middle_cache.enabled or config.prefetch.enabled:
        state_path = Path(config.session.state_db) if config.session.state_db else Path(config.cache_dir) / "state" / "phase2.sqlite3"
        phase2_store = SessionStateStore(state_path)
        app["phase2_store"] = phase2_store
        app["middle_cache"] = MiddleRangeCache(
            config.cache_dir,
            phase2_store,
            max_bytes=config.middle_cache.max_bytes,
            ttl_seconds=config.middle_cache.ttl_seconds,
        )
        if config.session.enabled:
            recorder = SessionRecorder(phase2_store)
            app["session_recorder"] = recorder
```

Only start background recorder lifecycle when `config.session.enabled` is true:

```python
    if config.session.enabled:
        app.cleanup_ctx.append(session_recorder_lifecycle)
```

Add lifecycle:

```python
async def session_recorder_lifecycle(app: web.Application) -> AsyncIterator[None]:
    recorder: SessionRecorder = app["session_recorder"]
    recorder.start()
    try:
        yield
    finally:
        await recorder.stop()
```

- [ ] **Step 4: Record sessions after metadata and cache key are known**

In `serve_authorized_range`, after:

```python
        key = cache_key(source, metadata)
```

add:

```python
        recorder: SessionRecorder | None = request.app.get("session_recorder")
        if recorder is not None:
            recorder.record_nowait(ctx, key, metadata, byte_range, observed_at=time.time())
```

This write is intentionally best-effort and non-blocking.

- [ ] **Step 5: Add middle-cache hit path after head/tail miss**

In `serve_authorized_range`, after determining no head/tail cached response will be used and before opening the origin stream, add:

```python
        middle_cache: MiddleRangeCache | None = request.app.get("middle_cache")
        if config.middle_cache.enabled and middle_cache is not None:
            middle_chunks = middle_cache.iter_block(
                key,
                byte_range,
                chunk_bytes=config.cache.chunk_bytes,
                now=time.time(),
            )
            if middle_chunks is not None:
                return await _serve_cached_response(
                    request,
                    status=status,
                    headers=headers,
                    cached_chunks=middle_chunks,
                    ctx=ctx,
                    byte_range=byte_range,
                    metadata=metadata,
                    started_at=started_at,
                    block_name="middle",
                    block_range=byte_range,
                )
```

Do not call `stage_block` for middle misses in this handler.

- [ ] **Step 6: Run app tests and commit**

Run:

```bash
python -m pytest tests/test_app.py -q
```

Expected: PASS.

Run the full suite:

```bash
python -m pytest -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/app.py tests/test_app.py
git commit -m "Integrate session tracking and middle cache reads"
```

## Task 9: Queue Idle/Stop Prefetch Work

**Files:**
- Modify: `src/emby_range_cache_proxy/prefetch.py`
- Modify: `src/emby_range_cache_proxy/app.py`
- Modify: `tests/test_prefetch.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: Add failing queue planning tests**

Append to `tests/test_prefetch.py`:

```python
from emby_range_cache_proxy.cache import adaptive_head_tail
from emby_range_cache_proxy.prefetch import enqueue_prefetch_for_session
from emby_range_cache_proxy.state import PlaybackSessionRecord


def test_enqueue_prefetch_for_session_inserts_deduplicated_tasks(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")
    session = PlaybackSessionRecord(
        session_hash="s" * 64,
        device_hash=None,
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        origin_signature="origin",
        media_size=1000,
        last_range_start=300,
        last_range_end=350,
        max_observed_offset=350,
        first_seen_at=1.0,
        last_seen_at=10.0,
        last_emby_observed_at=None,
        status="idle",
        queued_until=None,
    )

    inserted = enqueue_prefetch_for_session(
        store,
        session,
        prefetch=PrefetchConfig(window_bytes=128, resume_overlap_bytes=0, max_session_bytes=256, max_queue_depth=10),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
        now=20.0,
        priority=10,
    )
    repeated = enqueue_prefetch_for_session(
        store,
        session,
        prefetch=PrefetchConfig(window_bytes=128, resume_overlap_bytes=0, max_session_bytes=256, max_queue_depth=10),
        middle_cache=MiddleCacheConfig(segment_bytes=64),
        now=21.0,
        priority=10,
    )

    assert inserted == 2
    assert repeated == 0
    assert store.queue_depth() == 2
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_prefetch.py -q
```

Expected: FAIL with missing `enqueue_prefetch_for_session`.

- [ ] **Step 3: Implement queue helper**

Append to `src/emby_range_cache_proxy/prefetch.py`:

```python
from .cache import adaptive_head_tail
from .state import PlaybackSessionRecord


def enqueue_prefetch_for_session(
    store: SessionStateStore,
    session: PlaybackSessionRecord,
    *,
    prefetch: PrefetchConfig,
    middle_cache: MiddleCacheConfig,
    now: float,
    priority: int,
) -> int:
    head_size, tail_size = adaptive_head_tail(session.media_size)
    planned = plan_middle_ranges(
        media_size=session.media_size,
        head_size=head_size,
        tail_size=tail_size,
        max_observed_offset=session.max_observed_offset,
        queued_until=session.queued_until,
        prefetch=prefetch,
        middle_cache=middle_cache,
    )
    inserted = 0
    highest_end = session.queued_until
    for byte_range in planned:
        existing = store.find_middle_block(session.cache_key, byte_range)
        if existing is not None:
            highest_end = max(highest_end or byte_range.end, byte_range.end)
            continue
        task = store.enqueue_prefetch_task(
            item_id=session.item_id,
            media_source_id=session.media_source_id,
            cache_key=session.cache_key,
            start=byte_range.start,
            end=byte_range.end,
            priority=priority,
            now=now,
            max_queue_depth=prefetch.max_queue_depth,
        )
        if task is not None:
            inserted += 1
            highest_end = max(highest_end or byte_range.end, byte_range.end)
    if highest_end is not None:
        store.update_session_queued_until(session.session_hash, highest_end, now=now)
    return inserted
```

- [ ] **Step 4: Add lifecycle loop for idle and observer checks**

In `app.py`, import:

```python
from .prefetch import PrefetchWorker, enqueue_prefetch_for_session
from .session_observer import EmbySessionObserver
```

Add cleanup context when `config.session.enabled` is true:

```python
    if config.session.enabled:
        app.cleanup_ctx.append(session_planner_lifecycle)
```

Add:

```python
async def session_planner_lifecycle(app: web.Application) -> AsyncIterator[None]:
    config: Config = app["config"]
    store: SessionStateStore = app["phase2_store"]
    observer = EmbySessionObserver(config, store)
    task = asyncio.create_task(_session_planner_loop(config, store, observer))
    app["session_planner_task"] = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _session_planner_loop(config: Config, store: SessionStateStore, observer: EmbySessionObserver) -> None:
    while True:
        now = time.time()
        stopped = []
        if config.session.observer_enabled:
            observer_result = await observer.run_once(now=now)
            stopped = list(observer_result.stopped_sessions)
        idle = store.mark_idle_sessions(now=now, idle_seconds=config.session.idle_seconds)
        store.expire_old_sessions(now=now, expire_seconds=config.session.expire_seconds)
        if config.prefetch.enabled and config.middle_cache.enabled:
            for session in idle:
                enqueue_prefetch_for_session(
                    store,
                    session,
                    prefetch=config.prefetch,
                    middle_cache=config.middle_cache,
                    now=now,
                    priority=10,
                )
            for session in stopped:
                enqueue_prefetch_for_session(
                    store,
                    session,
                    prefetch=config.prefetch,
                    middle_cache=config.middle_cache,
                    now=now,
                    priority=20,
                )
        await asyncio.sleep(config.session.observer_interval_seconds)
```

- [ ] **Step 5: Run targeted tests and commit**

Run:

```bash
python -m pytest tests/test_prefetch.py tests/test_app.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/prefetch.py src/emby_range_cache_proxy/app.py tests/test_prefetch.py tests/test_app.py
git commit -m "Queue prefetch after idle sessions"
```

## Task 10: Background Prefetch Lifecycle

**Files:**
- Modify: `src/emby_range_cache_proxy/app.py`
- Modify: `src/emby_range_cache_proxy/prefetch.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: Add failing lifecycle test**

Append to `tests/test_app.py`:

```python
async def test_prefetch_lifecycle_starts_only_when_enabled(aiohttp_client, monkeypatch, tmp_path):
    started = asyncio.Event()
    calls = []

    class FakePrefetchWorker:
        def __init__(self, config, store, middle_cache):
            calls.append(("init", config.prefetch.enabled))

        async def run_once(self, *, now):
            calls.append(("run_once", now > 0))
            started.set()
            await asyncio.sleep(3600)

    monkeypatch.setattr(app_module, "PrefetchWorker", FakePrefetchWorker)
    app = create_app(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path / "cache"),
            session=SessionConfig(enabled=True),
            middle_cache=MiddleCacheConfig(enabled=True),
            prefetch=PrefetchConfig(enabled=True),
        )
    )
    client = await aiohttp_client(app)

    await asyncio.wait_for(started.wait(), timeout=1)

    assert calls[0] == ("init", True)
    assert calls[1][0] == "run_once"

    await client.close()
```

- [ ] **Step 2: Run lifecycle test and verify failure**

Run:

```bash
python -m pytest tests/test_app.py::test_prefetch_lifecycle_starts_only_when_enabled -q
```

Expected: FAIL because no prefetch lifecycle exists.

- [ ] **Step 3: Add prefetch lifecycle**

In `create_app`, after Phase 2 state initialization:

```python
    if config.prefetch.enabled and config.middle_cache.enabled:
        app.cleanup_ctx.append(prefetch_worker_lifecycle)
```

Add lifecycle:

```python
async def prefetch_worker_lifecycle(app: web.Application) -> AsyncIterator[None]:
    config: Config = app["config"]
    worker = PrefetchWorker(config, app["phase2_store"], app["middle_cache"])
    task = asyncio.create_task(_prefetch_worker_loop(config, worker))
    app["prefetch_task"] = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _prefetch_worker_loop(config: Config, worker: PrefetchWorker) -> None:
    while True:
        try:
            await worker.run_once(now=time.time())
        except Exception as error:
            LOGGER.warning("prefetch worker failed: error_type=%s", type(error).__name__)
        await asyncio.sleep(config.prefetch.error_backoff_seconds if worker.store.queue_depth() == 0 else 1)
```

Keep `PrefetchWorker` source resolution conservative in this task: if the worker cannot resolve a source URL from a safe in-memory map or future resolver, it marks the task skipped. The next task adds durable source metadata.

- [ ] **Step 4: Run lifecycle tests and commit**

Run:

```bash
python -m pytest tests/test_app.py::test_prefetch_lifecycle_starts_only_when_enabled -q
```

Expected: PASS.

Run:

```bash
python -m pytest tests/test_app.py tests/test_prefetch.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/app.py tests/test_app.py
git commit -m "Start background prefetch worker"
```

## Task 11: Durable Source Metadata For Prefetch

**Files:**
- Modify: `src/emby_range_cache_proxy/state.py`
- Modify: `src/emby_range_cache_proxy/session.py`
- Modify: `src/emby_range_cache_proxy/prefetch.py`
- Modify: `tests/test_state.py`
- Modify: `tests/test_prefetch.py`

- [ ] **Step 1: Add failing source metadata tests**

Append to `tests/test_state.py`:

```python
def test_source_metadata_is_stored_without_raw_tokens(tmp_path):
    store = SessionStateStore(tmp_path / "state.sqlite3")

    store.upsert_source_metadata(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        origin_url="http://origin/movie.mkv",
        origin_signature="sig",
        media_size=1000,
        updated_at=1.0,
    )
    source = store.get_source_metadata("1", "ms1", "a" * 64)

    assert source.origin_url == "http://origin/movie.mkv"
    assert source.origin_signature == "sig"
    assert source.media_size == 1000
```

Append to `tests/test_prefetch.py`:

```python
async def test_prefetch_worker_uses_stored_source_metadata(aiohttp_client, tmp_path):
    async def origin(request):
        return web.Response(status=206, body=b"abcdefghij", headers={"Content-Range": "bytes 10-19/100"})

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin)
    origin = await aiohttp_client(origin_app)
    store = SessionStateStore(tmp_path / "state.sqlite3")
    store.upsert_source_metadata(
        item_id="1",
        media_source_id="ms1",
        cache_key="a" * 64,
        origin_url=str(origin.make_url("/movie.mkv")),
        origin_signature="sig",
        media_size=100,
        updated_at=1.0,
    )
    middle = MiddleRangeCache(tmp_path / "cache", store, max_bytes=1024 * 1024, ttl_seconds=60)
    store.enqueue_prefetch_task("1", "ms1", "a" * 64, 10, 19, priority=10, now=1.0, max_queue_depth=10)
    worker = PrefetchWorker(
        Config(
            emby_base_url="http://emby",
            fallback_base_url="http://emby",
            cache_dir=str(tmp_path / "cache"),
            middle_cache=MiddleCacheConfig(enabled=True),
            prefetch=PrefetchConfig(enabled=True),
        ),
        store,
        middle,
    )

    result = await worker.run_once(now=2.0)

    assert result.completed == 1
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_state.py tests/test_prefetch.py -q
```

Expected: FAIL with missing source metadata methods.

- [ ] **Step 3: Add source metadata schema and methods**

In `state.py`, add dataclass:

```python
@dataclass(frozen=True)
class SourceMetadataRecord:
    item_id: str
    media_source_id: str
    cache_key: str
    origin_url: str
    origin_signature: str
    media_size: int
    updated_at: float
```

Add schema:

```sql
CREATE TABLE IF NOT EXISTS source_metadata (
    item_id TEXT NOT NULL,
    media_source_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    origin_url TEXT NOT NULL,
    origin_signature TEXT NOT NULL,
    media_size INTEGER NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY(item_id, media_source_id, cache_key)
);
```

Add methods with these signatures:

- `upsert_source_metadata(self, *, item_id: str, media_source_id: str, cache_key: str, origin_url: str, origin_signature: str, media_size: int, updated_at: float) -> None`
- `get_source_metadata(self, item_id: str, media_source_id: str, cache_key: str) -> SourceMetadataRecord | None`

- [ ] **Step 4: Store source metadata from foreground requests**

In `app.py`, after computing `key = cache_key(source, metadata)`, add:

```python
        store: SessionStateStore | None = request.app.get("phase2_store")
        if store is not None:
            store.upsert_source_metadata(
                item_id=ctx.item_id,
                media_source_id=ctx.media_source_id,
                cache_key=key,
                origin_url=metadata.url,
                origin_signature=origin_signature(metadata),
                media_size=metadata.size,
                updated_at=time.time(),
            )
```

Import `origin_signature` from `.session`.

- [ ] **Step 5: Use stored source metadata in worker**

In `PrefetchWorker._run_task`, replace the lookup-only URL resolution with:

```python
        url = self.source_lookup.get((task.item_id, task.media_source_id))
        source_size = max(task.end + 1, 1)
        if url is None:
            source_metadata = self.store.get_source_metadata(task.item_id, task.media_source_id, task.cache_key)
            if source_metadata is not None:
                url = source_metadata.origin_url
                source_size = source_metadata.media_size
        if url is None:
            self.store.fail_prefetch_task(task.id, error_class="SourceUnavailable", now=now)
            return "skipped"
```

Use `source_size` in the later `origin.open_range(url, byte_range, size=source_size)` call.

- [ ] **Step 6: Run tests and commit**

Run:

```bash
python -m pytest tests/test_state.py tests/test_prefetch.py tests/test_app.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/state.py src/emby_range_cache_proxy/session.py src/emby_range_cache_proxy/prefetch.py src/emby_range_cache_proxy/app.py tests/test_state.py tests/test_prefetch.py
git commit -m "Persist source metadata for prefetch"
```

## Task 12: Observability And Documentation

**Files:**
- Modify: `src/emby_range_cache_proxy/app.py`
- Modify: `src/emby_range_cache_proxy/prefetch.py`
- Modify: `README.md`
- Modify: `tests/test_security_behavior.py`
- Modify: `tests/test_deploy_examples.py`

- [ ] **Step 1: Add failing log sanitization test**

Append to `tests/test_security_behavior.py`:

```python
def test_phase2_log_events_do_not_expose_sensitive_values(caplog):
    import logging

    logger = logging.getLogger("emby_range_cache_proxy.prefetch")

    with caplog.at_level(logging.INFO):
        logger.info(
            "prefetch_queued item_id=%s media_source_id=%s session=%s cache_key=%s range=%s-%s",
            "1",
            "ms1",
            "abcdef12",
            "a" * 12,
            10,
            20,
        )

    text = caplog.text
    assert "api_key" not in text
    assert "PlaySessionId" not in text
    assert "DeviceId" not in text
    assert "http://origin" not in text
```

- [ ] **Step 2: Add README assertions**

Append to `tests/test_deploy_examples.py`:

```python
def test_readme_documents_phase2_disabled_defaults():
    readme = Path("README.md").read_text()

    assert "Phase 2" in readme
    assert "session.enabled=false" in readme
    assert "middle_cache.enabled=false" in readme
    assert "prefetch.enabled=false" in readme
    assert "internal API key is not used for user playback authorization" in readme
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_security_behavior.py tests/test_deploy_examples.py -q
```

Expected: FAIL on README assertions until documentation is added.

- [ ] **Step 4: Add concise sanitized logging helpers**

In `prefetch.py`, define:

```python
import logging

LOGGER = logging.getLogger(__name__)


def short_hash(value: str | None) -> str:
    return "none" if value is None else value[:12]
```

Log task transitions using only item id, media source id, short cache key, numeric ranges, result, and error class:

```python
LOGGER.info(
    "prefetch_started item_id=%s media_source_id=%s cache_key=%s range=%s-%s",
    task.item_id,
    task.media_source_id,
    short_hash(task.cache_key),
    task.start,
    task.end,
)
```

Add corresponding `prefetch_complete`, `prefetch_failed`, and `prefetch_skipped` logs.

In `app.py`, add middle read logs next to hit/miss decisions:

```python
LOGGER.info(
    "middle_cache_hit item_id=%s media_source_id=%s cache_key=%s range=%s-%s",
    ctx.item_id,
    ctx.media_source_id,
    key[:12],
    byte_range.start,
    byte_range.end,
)
```

Use the same safe fields for `middle_cache_miss`.

- [ ] **Step 5: Update README**

Add a `## Phase 2` section to `README.md`:

```markdown
## Phase 2

Phase 2 adds disabled-by-default playback session recording and idle/stop-driven middle-range prefetch.

Safe defaults:

- `session.enabled=false`
- `middle_cache.enabled=false`
- `prefetch.enabled=false`

The internal API key is not used for user playback authorization. User playback requests continue to be authorized with the user's own Emby token through `PlaybackInfo`. The internal key is only for read-only session observation and background work when those features are explicitly enabled.

Recommended rollout order:

1. Deploy code with Phase 2 disabled.
2. Enable `session.enabled=true` for logging and state observation.
3. Enable `session.observer_enabled=true` after configuring the internal key.
4. Enable `middle_cache.enabled=true` with `prefetch.enabled=false`.
5. Enable `prefetch.enabled=true` for one or two allowlisted items.
```

- [ ] **Step 6: Run docs/security tests and commit**

Run:

```bash
python -m pytest tests/test_security_behavior.py tests/test_deploy_examples.py -q
```

Expected: PASS.

Commit:

```bash
git add src/emby_range_cache_proxy/app.py src/emby_range_cache_proxy/prefetch.py README.md tests/test_security_behavior.py tests/test_deploy_examples.py
git commit -m "Document phase 2 rollout"
```

## Task 13: Full Verification

**Files:**
- No planned source edits unless verification exposes a defect.

- [ ] **Step 1: Run full test suite**

Run:

```bash
python -m pytest -q
```

Expected: PASS.

- [ ] **Step 2: Run package import smoke test**

Run:

```bash
python - <<'PY'
from emby_range_cache_proxy.app import create_app
from emby_range_cache_proxy.config import Config

app = create_app(Config(emby_base_url="http://127.0.0.1:8096", fallback_base_url="http://127.0.0.1:8096", cache_dir="/tmp/emby-range-cache-proxy-smoke"))
print(sorted(app.router.routes(), key=lambda route: str(route.resource))[0])
PY
```

Expected: command exits `0` and prints one route object.

- [ ] **Step 3: Check git status**

Run:

```bash
git status --short --branch
```

Expected: clean working tree on the feature branch, ahead by the Phase 2 commits.

- [ ] **Step 4: Commit verification fixes if needed**

If Step 1 or Step 2 exposes a defect, fix the failing component with the smallest code change, rerun the failing command, rerun the full suite, and commit the fix:

Run `git status --short` and add exactly the files listed by the fix. Then commit:

```bash
git commit -m "Fix phase 2 verification issue"
```

Do not create a commit when `git status --short` prints no changed files.

## Task 14: Test-Server Deployment Verification

**Files:**
- Modify only if deployment exposes a repo defect: `README.md`, `config.example.json`, or `deploy/emby-range-cache-proxy.service`.

- [ ] **Step 1: Push the implementation branch**

Run:

```bash
git push origin main
```

Expected: push succeeds.

- [ ] **Step 2: Deploy with Phase 2 disabled on the test server**

Run on `82.47.35.45`:

```bash
ssh root@82.47.35.45 'cd /opt/emby-range-cache-proxy && git pull --ff-only && .venv/bin/python -m pip install -e ".[dev]" && sudo systemctl restart emby-range-cache-proxy && systemctl is-active emby-range-cache-proxy'
```

Expected: prints `active`.

- [ ] **Step 3: Verify health and no middle cache creation**

Run:

```bash
ssh root@82.47.35.45 'curl -fsS http://127.0.0.1:18180/healthz && find /home/nax/emby/cache/range-proxy -path "*/mid/*.bin" -type f | head'
```

Expected: health prints `ok`; the `find` command prints no middle block paths while Phase 2 is disabled.

- [ ] **Step 4: Enable session recording only**

Edit `/etc/emby-range-cache-proxy/config.json` so:

```json
"session": {
  "enabled": true,
  "state_db": null,
  "observer_enabled": false,
  "observer_interval_seconds": 30,
  "idle_seconds": 180,
  "stop_grace_seconds": 60,
  "expire_seconds": 86400
},
"middle_cache": {
  "enabled": false,
  "max_bytes": 137438953472,
  "ttl_seconds": 604800,
  "segment_bytes": 67108864,
  "min_free_bytes": 53687091200
},
"prefetch": {
  "enabled": false,
  "window_bytes": 2147483648,
  "resume_overlap_bytes": 134217728,
  "max_session_bytes": 4294967296,
  "max_queue_depth": 200,
  "concurrency": 1,
  "per_origin_concurrency": 1,
  "bandwidth_bytes_per_second": 31457280,
  "pause_when_rollout_session_active": true,
  "error_backoff_seconds": 300
}
```

Restart:

```bash
ssh root@82.47.35.45 'systemctl restart emby-range-cache-proxy && systemctl is-active emby-range-cache-proxy'
```

Expected: prints `active`.

- [ ] **Step 5: Verify Phase 1 playback behavior remains intact**

Use the already validated gray-list Items. Confirm:

- startup still hits warmed head/tail for the 800MiB, 5GiB, 20GiB, 29.55GiB, and large Remux cases;
- no foreground request logs raw tokens, raw query strings, raw origin URLs, raw `PlaySessionId`, or raw `DeviceId`;
- no `*/mid/*.bin` files exist while `prefetch.enabled=false`.

- [ ] **Step 6: Stop after session-only validation**

Do not enable test-server prefetch until session logs and state DB entries look sane for at least one manual playback session. The next rollout change should be a separate reviewed change to `/etc/emby-range-cache-proxy/config.json`.
