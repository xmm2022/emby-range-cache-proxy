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


@dataclass
class PrewarmConfig:
    enabled: bool = False
    interval_seconds: int = 900
    max_items_per_scan: int = 100
    concurrency: int = 1

    def __post_init__(self) -> None:
        if self.interval_seconds < 60:
            raise ValueError("prewarm.interval_seconds must be >= 60")


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
    return CacheConfig(
        max_bytes=int(data.get("max_bytes", 512 * 1024**3)),
        build_wait_seconds=float(data.get("build_wait_seconds", 0.25)),
        chunk_bytes=int(data.get("chunk_bytes", 1024 * 1024)),
        default_open_range_bytes=int(data.get("default_open_range_bytes", 16 * 1024**2)),
    )


def _prewarm(data: dict[str, Any]) -> PrewarmConfig:
    return PrewarmConfig(
        enabled=bool(data.get("enabled", False)),
        interval_seconds=int(data.get("interval_seconds", 900)),
        max_items_per_scan=int(data.get("max_items_per_scan", 100)),
        concurrency=int(data.get("concurrency", 1)),
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
    )
