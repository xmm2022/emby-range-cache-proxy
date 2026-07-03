from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
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


@dataclass
class PrewarmConfig:
    enabled: bool = False
    interval_seconds: int = 900
    max_items_per_scan: int = 100
    concurrency: int = 1

    def __post_init__(self) -> None:
        if self.interval_seconds < 60:
            raise ValueError("prewarm.interval_seconds must be >= 60")


@dataclass
class Config:
    emby_base_url: str
    fallback_base_url: str
    cache_dir: str
    listen_host: str = "127.0.0.1"
    listen_port: int = 18180
    prewarm_api_key: str | None = None
    rollout: RolloutConfig = field(default_factory=RolloutConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    prewarm: PrewarmConfig = field(default_factory=PrewarmConfig)


def _string_set(values: Any) -> set[str]:
    if values is None:
        return set()
    return {str(value) for value in values}


def _rollout(data: dict[str, Any]) -> RolloutConfig:
    return RolloutConfig(
        enabled=bool(data.get("enabled", False)),
        item_allowlist=_string_set(data.get("item_allowlist")),
        media_source_allowlist=_string_set(data.get("media_source_allowlist")),
        path_prefix_allowlist=tuple(str(v) for v in data.get("path_prefix_allowlist", [])),
    )


def _cache(data: dict[str, Any]) -> CacheConfig:
    return CacheConfig(
        max_bytes=int(data.get("max_bytes", 512 * 1024**3)),
        build_wait_seconds=float(data.get("build_wait_seconds", 0.25)),
        chunk_bytes=int(data.get("chunk_bytes", 1024 * 1024)),
    )


def _prewarm(data: dict[str, Any]) -> PrewarmConfig:
    return PrewarmConfig(
        enabled=bool(data.get("enabled", False)),
        interval_seconds=int(data.get("interval_seconds", 900)),
        max_items_per_scan=int(data.get("max_items_per_scan", 100)),
        concurrency=int(data.get("concurrency", 1)),
    )


def load_config(path: str | Path) -> Config:
    raw = json.loads(Path(path).read_text())
    return Config(
        emby_base_url=str(raw["emby_base_url"]).rstrip("/"),
        fallback_base_url=str(raw.get("fallback_base_url", raw["emby_base_url"])).rstrip("/"),
        cache_dir=str(raw["cache_dir"]),
        listen_host=str(raw.get("listen_host", "127.0.0.1")),
        listen_port=int(raw.get("listen_port", 18180)),
        prewarm_api_key=raw.get("prewarm_api_key"),
        rollout=_rollout(raw.get("rollout", {})),
        cache=_cache(raw.get("cache", {})),
        prewarm=_prewarm(raw.get("prewarm", {})),
    )
