from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any


@dataclass
class RolloutConfig:
    enabled: bool = False
    item_allowlist: set[str] = field(default_factory=set)
    media_source_allowlist: set[str] = field(default_factory=set)
    path_prefix_allowlist: tuple[str, ...] = ()

    def item_allowed(self, item_id: str) -> bool:
        return not self.item_allowlist or item_id in self.item_allowlist

    def media_source_allowed(self, media_source_id: str) -> bool:
        return not self.media_source_allowlist or media_source_id in self.media_source_allowlist

    def path_allowed(self, path: str | None) -> bool:
        if not self.path_prefix_allowlist:
            return True
        if not path:
            return False
        return any(path.startswith(prefix) for prefix in self.path_prefix_allowlist)

    def in_scope(self, *, item_id: str, media_source_id: str, path: str | None = None) -> bool:
        if not self.enabled:
            return False
        return (
            self.item_allowed(item_id)
            and self.media_source_allowed(media_source_id)
            and self.path_allowed(path)
        )


@dataclass
class CacheConfig:
    max_bytes: int = 512 * 1024**3
    build_wait_seconds: float = 0.25
    chunk_bytes: int = 1024 * 1024
    default_open_range_bytes: int = 16 * 1024**2
    open_head_response_bytes: int | None = None


@dataclass
class PrewarmConfig:
    enabled: bool = False
    interval_seconds: int = 900
    max_items_per_scan: int = 100
    concurrency: int = 1

    def __post_init__(self) -> None:
        if self.interval_seconds < 60:
            raise ValueError("prewarm.interval_seconds must be >= 60")
        if self.concurrency <= 0:
            raise ValueError("prewarm.concurrency must be positive")


@dataclass
class SessionConfig:
    enabled: bool = False
    state_db: str | None = None
    observer_enabled: bool = False
    observer_interval_seconds: int = 30
    idle_seconds: int = 180
    stop_grace_seconds: int = 60
    expire_seconds: int = 86400

    def __post_init__(self) -> None:
        for field_name in (
            "observer_interval_seconds",
            "idle_seconds",
            "stop_grace_seconds",
            "expire_seconds",
        ):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"session.{field_name} must be positive")


@dataclass
class MiddleCacheConfig:
    enabled: bool = False
    max_bytes: int = 128 * 1024**3
    ttl_seconds: int = 7 * 24 * 60 * 60
    segment_bytes: int = 64 * 1024**2
    min_free_bytes: int = 50 * 1024**3

    def __post_init__(self) -> None:
        for field_name in ("max_bytes", "ttl_seconds", "segment_bytes"):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"middle_cache.{field_name} must be positive")
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
    poll_interval_seconds: int = 5
    error_backoff_seconds: int = 300

    def __post_init__(self) -> None:
        positive_fields = (
            "window_bytes",
            "max_session_bytes",
            "max_queue_depth",
            "concurrency",
            "per_origin_concurrency",
            "bandwidth_bytes_per_second",
            "poll_interval_seconds",
            "error_backoff_seconds",
        )
        for field_name in positive_fields:
            if getattr(self, field_name) <= 0:
                raise ValueError(f"prefetch.{field_name} must be positive")
        if self.resume_overlap_bytes < 0:
            raise ValueError("prefetch.resume_overlap_bytes must be non-negative")


@dataclass(frozen=True)
class PathMapping:
    source_prefix: str
    target_prefix: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_prefix", _normalize_path_mapping_source_prefix(self.source_prefix))


@dataclass
class Config:
    emby_base_url: str
    fallback_base_url: str
    cache_dir: str
    listen_host: str = "127.0.0.1"
    listen_port: int = 18180
    prewarm_api_key: str | None = None
    path_mappings: tuple[PathMapping, ...] = ()
    rollout: RolloutConfig = field(default_factory=RolloutConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    prewarm: PrewarmConfig = field(default_factory=PrewarmConfig)
    session: SessionConfig = field(default_factory=SessionConfig)
    middle_cache: MiddleCacheConfig = field(default_factory=MiddleCacheConfig)
    prefetch: PrefetchConfig = field(default_factory=PrefetchConfig)


def _string_list(values: Any, field_name: str) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (str, bytes)) or not isinstance(values, list):
        raise ValueError(f"rollout.{field_name} must be a list")
    return [str(value) for value in values]


def _string_set(values: Any, field_name: str) -> set[str]:
    return set(_string_list(values, field_name))


def _rollout(data: dict[str, Any]) -> RolloutConfig:
    return RolloutConfig(
        enabled=bool(data.get("enabled", False)),
        item_allowlist=_string_set(data.get("item_allowlist"), "item_allowlist"),
        media_source_allowlist=_string_set(data.get("media_source_allowlist"), "media_source_allowlist"),
        path_prefix_allowlist=tuple(_string_list(data.get("path_prefix_allowlist"), "path_prefix_allowlist")),
    )


def _cache(data: dict[str, Any]) -> CacheConfig:
    open_head_response_bytes = data.get("open_head_response_bytes")
    return CacheConfig(
        max_bytes=int(data.get("max_bytes", 512 * 1024**3)),
        build_wait_seconds=float(data.get("build_wait_seconds", 0.25)),
        chunk_bytes=int(data.get("chunk_bytes", 1024 * 1024)),
        default_open_range_bytes=int(data.get("default_open_range_bytes", 16 * 1024**2)),
        open_head_response_bytes=None if open_head_response_bytes is None else int(open_head_response_bytes),
    )


def _prewarm(data: dict[str, Any]) -> PrewarmConfig:
    return PrewarmConfig(
        enabled=bool(data.get("enabled", False)),
        interval_seconds=int(data.get("interval_seconds", 900)),
        max_items_per_scan=int(data.get("max_items_per_scan", 100)),
        concurrency=int(data.get("concurrency", 1)),
    )


def _phase2_bool(data: dict[str, Any], key: str, default: bool, field_name: str) -> bool:
    if key not in data:
        return default
    value = data[key]
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _phase2_int(data: dict[str, Any], key: str, default: int, field_name: str) -> int:
    if key not in data:
        return default
    value = data[key]
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{field_name} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc


def _session(data: dict[str, Any]) -> SessionConfig:
    state_db = data.get("state_db")
    return SessionConfig(
        enabled=_phase2_bool(data, "enabled", False, "session.enabled"),
        state_db=None if state_db is None else str(state_db),
        observer_enabled=_phase2_bool(data, "observer_enabled", False, "session.observer_enabled"),
        observer_interval_seconds=_phase2_int(
            data,
            "observer_interval_seconds",
            30,
            "session.observer_interval_seconds",
        ),
        idle_seconds=_phase2_int(data, "idle_seconds", 180, "session.idle_seconds"),
        stop_grace_seconds=_phase2_int(data, "stop_grace_seconds", 60, "session.stop_grace_seconds"),
        expire_seconds=_phase2_int(data, "expire_seconds", 86400, "session.expire_seconds"),
    )


def _middle_cache(data: dict[str, Any]) -> MiddleCacheConfig:
    return MiddleCacheConfig(
        enabled=_phase2_bool(data, "enabled", False, "middle_cache.enabled"),
        max_bytes=_phase2_int(data, "max_bytes", 128 * 1024**3, "middle_cache.max_bytes"),
        ttl_seconds=_phase2_int(data, "ttl_seconds", 7 * 24 * 60 * 60, "middle_cache.ttl_seconds"),
        segment_bytes=_phase2_int(data, "segment_bytes", 64 * 1024**2, "middle_cache.segment_bytes"),
        min_free_bytes=_phase2_int(data, "min_free_bytes", 50 * 1024**3, "middle_cache.min_free_bytes"),
    )


def _prefetch(data: dict[str, Any]) -> PrefetchConfig:
    return PrefetchConfig(
        enabled=_phase2_bool(data, "enabled", False, "prefetch.enabled"),
        window_bytes=_phase2_int(data, "window_bytes", 2 * 1024**3, "prefetch.window_bytes"),
        resume_overlap_bytes=_phase2_int(
            data,
            "resume_overlap_bytes",
            128 * 1024**2,
            "prefetch.resume_overlap_bytes",
        ),
        max_session_bytes=_phase2_int(data, "max_session_bytes", 4 * 1024**3, "prefetch.max_session_bytes"),
        max_queue_depth=_phase2_int(data, "max_queue_depth", 200, "prefetch.max_queue_depth"),
        concurrency=_phase2_int(data, "concurrency", 1, "prefetch.concurrency"),
        per_origin_concurrency=_phase2_int(
            data,
            "per_origin_concurrency",
            1,
            "prefetch.per_origin_concurrency",
        ),
        bandwidth_bytes_per_second=_phase2_int(
            data,
            "bandwidth_bytes_per_second",
            30 * 1024**2,
            "prefetch.bandwidth_bytes_per_second",
        ),
        pause_when_rollout_session_active=_phase2_bool(
            data,
            "pause_when_rollout_session_active",
            True,
            "prefetch.pause_when_rollout_session_active",
        ),
        poll_interval_seconds=_phase2_int(
            data,
            "poll_interval_seconds",
            5,
            "prefetch.poll_interval_seconds",
        ),
        error_backoff_seconds=_phase2_int(
            data,
            "error_backoff_seconds",
            300,
            "prefetch.error_backoff_seconds",
        ),
    )


def _normalize_path_mapping_source_prefix(value: str) -> str:
    prefix = str(value).strip()
    if not prefix.startswith("/"):
        raise ValueError("path_mappings source prefix must be absolute")
    parts = PurePosixPath(prefix).parts
    if len(parts) <= 1 or any(part in {".", ".."} for part in parts):
        raise ValueError("path_mappings source prefix must be a non-root directory")
    normalized = "/" + "/".join(parts[1:])
    if not normalized.endswith("/"):
        normalized += "/"
    return normalized


def _path_mappings(values: Any) -> tuple[PathMapping, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes)) or not isinstance(values, list):
        raise ValueError("path_mappings must be a list")
    mappings: list[PathMapping] = []
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            raise ValueError(f"path_mappings[{index}] must be an object")
        source_prefix = value.get("from", value.get("source_prefix"))
        target_prefix = value.get("to", value.get("target_prefix"))
        if not source_prefix or not target_prefix:
            raise ValueError(f"path_mappings[{index}] must include from and to")
        mappings.append(PathMapping(str(source_prefix), str(target_prefix)))
    return tuple(mappings)


def load_config(path: str | Path) -> Config:
    raw = json.loads(Path(path).read_text())
    return Config(
        emby_base_url=str(raw["emby_base_url"]).rstrip("/"),
        fallback_base_url=str(raw.get("fallback_base_url", raw["emby_base_url"])).rstrip("/"),
        cache_dir=str(raw["cache_dir"]),
        listen_host=str(raw.get("listen_host", "127.0.0.1")),
        listen_port=int(raw.get("listen_port", 18180)),
        prewarm_api_key=raw.get("prewarm_api_key"),
        path_mappings=_path_mappings(raw.get("path_mappings")),
        rollout=_rollout(raw.get("rollout", {})),
        cache=_cache(raw.get("cache", {})),
        prewarm=_prewarm(raw.get("prewarm", {})),
        session=_session(raw.get("session", {})),
        middle_cache=_middle_cache(raw.get("middle_cache", {})),
        prefetch=_prefetch(raw.get("prefetch", {})),
    )
