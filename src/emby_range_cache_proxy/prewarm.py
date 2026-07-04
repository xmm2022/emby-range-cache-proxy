from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientSession

from .cache import HeadTailCache, adaptive_head_tail, cache_key
from .config import Config
from .models import ByteRange, MediaSource, SourceMetadata
from .origin import OriginClient, OriginError
from .prefetch import BandwidthLimiter
from .sources import resolve_media_source


@dataclass(frozen=True)
class PrewarmResult:
    scanned: int
    prewarmed: int
    skipped: int


class PrewarmWorker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.cache = HeadTailCache(config.cache_dir, max_bytes=config.cache.max_bytes)
        self.limiter = BandwidthLimiter(
            bytes_per_second=config.prefetch.bandwidth_bytes_per_second
        )

    async def run_once(self) -> PrewarmResult:
        if not self.config.prewarm.enabled or not self.config.prewarm_api_key:
            return PrewarmResult(scanned=0, prewarmed=0, skipped=0)

        async with ClientSession() as session:
            items = await self._recent_items(session)
            prewarmed = 0
            skipped = 0
            seen: set[tuple[str, str]] = set()

            for item in items:
                if not isinstance(item, dict):
                    skipped += 1
                    continue
                item_id = str(item.get("Id") or "")
                if not item_id:
                    skipped += 1
                    continue

                raw_sources = await self._raw_media_sources(session, item_id)
                if raw_sources is None:
                    skipped += 1
                    continue
                for raw_source in raw_sources:
                    source = _media_source_from_payload(item_id, raw_source)
                    if source is None:
                        skipped += 1
                        continue

                    dedupe_key = (source.item_id, source.media_source_id)
                    if dedupe_key in seen:
                        skipped += 1
                        continue
                    seen.add(dedupe_key)

                    source = resolve_media_source(
                        source,
                        self.config.path_mappings,
                        url_prefix_allowlist=self.config.rollout.path_prefix_allowlist,
                    )
                    if not self.config.rollout.in_scope(
                        item_id=source.item_id,
                        media_source_id=source.media_source_id,
                        path=source.path,
                    ):
                        skipped += 1
                        continue
                    if not _is_http_source(source):
                        skipped += 1
                        continue

                    try:
                        prewarmed_source = await self._prewarm_source(source)
                    except (ClientError, OriginError, TimeoutError, OSError, ValueError):
                        skipped += 1
                        continue
                    if not prewarmed_source:
                        skipped += 1
                        continue
                    prewarmed += 1

        return PrewarmResult(scanned=len(items), prewarmed=prewarmed, skipped=skipped)

    async def prewarm_item(self, item_id: str, media_source_id: str) -> PrewarmResult:
        if not self.config.prewarm_api_key:
            return PrewarmResult(scanned=0, prewarmed=0, skipped=1)

        async with ClientSession() as session:
            raw_sources = await self._raw_media_sources(
                session,
                item_id,
                media_source_id=media_source_id,
            )
            if raw_sources is None:
                return PrewarmResult(scanned=1, prewarmed=0, skipped=1)

            for raw_source in raw_sources:
                source = _media_source_from_payload(item_id, raw_source)
                if source is None or source.media_source_id != media_source_id:
                    continue
                source = resolve_media_source(
                    source,
                    self.config.path_mappings,
                    url_prefix_allowlist=self.config.rollout.path_prefix_allowlist,
                )
                if not self.config.rollout.in_scope(
                    item_id=source.item_id,
                    media_source_id=source.media_source_id,
                    path=source.path,
                ):
                    return PrewarmResult(scanned=1, prewarmed=0, skipped=1)
                if not _is_http_source(source):
                    return PrewarmResult(scanned=1, prewarmed=0, skipped=1)
                try:
                    prewarmed = await self._prewarm_source(source)
                except (ClientError, OriginError, TimeoutError, OSError, ValueError):
                    return PrewarmResult(scanned=1, prewarmed=0, skipped=1)
                return PrewarmResult(
                    scanned=1,
                    prewarmed=1 if prewarmed else 0,
                    skipped=0 if prewarmed else 1,
                )

        return PrewarmResult(scanned=1, prewarmed=0, skipped=1)

    async def _recent_items(self, session: ClientSession) -> list[object]:
        assert self.config.prewarm_api_key is not None
        url = f"{self.config.emby_base_url.rstrip('/')}/Items"
        params = {
            "api_key": self.config.prewarm_api_key,
            "SortBy": "DateCreated",
            "SortOrder": "Descending",
            "IncludeItemTypes": "Movie,Episode",
            "Recursive": "true",
            "Limit": str(self.config.prewarm.max_items_per_scan),
        }
        payload = await _get_json(session, url, params=params)
        if not isinstance(payload, dict):
            return []
        items = payload.get("Items", [])
        return items if isinstance(items, list) else []

    async def _raw_media_sources(
        self,
        session: ClientSession,
        item_id: str,
        *,
        media_source_id: str | None = None,
    ) -> list[object] | None:
        assert self.config.prewarm_api_key is not None
        url = f"{self.config.emby_base_url.rstrip('/')}/Items/{item_id}/PlaybackInfo"
        params = {"api_key": self.config.prewarm_api_key}
        if media_source_id:
            params["MediaSourceId"] = media_source_id
        payload = await _get_json(session, url, params=params)
        if payload is None:
            return None
        if not isinstance(payload, dict):
            return None
        sources = payload.get("MediaSources", [])
        return sources if isinstance(sources, list) else []

    async def _prewarm_source(self, source: MediaSource) -> bool:
        async with OriginClient(chunk_bytes=self.config.cache.chunk_bytes) as origin:
            metadata = await origin.head(source.path)
            key = cache_key(source, metadata)
            missing_ranges = [
                (block_name, byte_range)
                for block_name, byte_range in _prewarm_ranges(metadata.size)
                if not self._has_cached_block(key, block_name, byte_range)
            ]
            if not missing_ranges:
                return False
            for block_name, byte_range in missing_ranges:
                writer = self.cache.stage_block(key, block_name, byte_range)
                try:
                    async with origin.open_range(metadata.url, byte_range, size=metadata.size) as upstream:
                        async for chunk in upstream.content.iter_chunked(self.config.cache.chunk_bytes):
                            if chunk:
                                await self.limiter.consume(len(chunk))
                                writer.write(chunk)
                    writer.commit()
                except Exception:
                    writer.abort()
                    raise
            self.cache.evict_if_needed()
            return True

    def _has_cached_block(self, key: str, block_name: str, byte_range: ByteRange) -> bool:
        cached_chunks = self.cache.iter_block(
            key,
            block_name,
            byte_range,
            chunk_bytes=self.config.cache.chunk_bytes,
        )
        if cached_chunks is None:
            return False
        close = getattr(cached_chunks, "close", None)
        if close is not None:
            close()
        return True


def _media_source_from_payload(item_id: str, raw: object) -> MediaSource | None:
    if not isinstance(raw, dict):
        return None
    media_source_id = raw.get("Id")
    path = raw.get("Path")
    if not media_source_id or not path:
        return None
    try:
        size = int(raw["Size"]) if raw.get("Size") is not None else None
        bitrate = int(raw["Bitrate"]) if raw.get("Bitrate") is not None else None
    except (TypeError, ValueError):
        return None
    if size is not None and size <= 0:
        return None
    return MediaSource(
        item_id=item_id,
        media_source_id=str(media_source_id),
        path=str(path),
        protocol=str(raw.get("Protocol", "")),
        size=size,
        container=str(raw["Container"]) if raw.get("Container") is not None else None,
        bitrate=bitrate,
    )


def _prewarm_ranges(size: int) -> tuple[tuple[str, ByteRange], ...]:
    head_size, tail_size = adaptive_head_tail(size)
    head_range = ByteRange(0, min(head_size, size) - 1)
    tail_range = ByteRange(max(0, size - tail_size), size - 1)
    return (("head", head_range), ("tail", tail_range))


def _is_http_source(source: MediaSource) -> bool:
    return urlsplit(source.path).scheme.lower() in {"http", "https"}


async def _get_json(session: ClientSession, url: str, *, params: dict[str, str]) -> Any | None:
    try:
        async with session.get(url, params=params) as response:
            if response.status >= 400:
                return None
            return await response.json()
    except (ClientError, TimeoutError, OSError, ValueError):
        return None
