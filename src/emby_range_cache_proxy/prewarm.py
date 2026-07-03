from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientSession

from .cache import HeadTailCache, adaptive_head_tail, cache_key
from .config import Config
from .models import ByteRange, MediaSource, SourceMetadata
from .origin import OriginClient, OriginError


@dataclass(frozen=True)
class PrewarmResult:
    scanned: int
    prewarmed: int
    skipped: int


class PrewarmWorker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.cache = HeadTailCache(config.cache_dir, max_bytes=config.cache.max_bytes)

    async def run_once(self) -> PrewarmResult:
        if not self.config.prewarm.enabled or not self.config.prewarm_api_key:
            return PrewarmResult(scanned=0, prewarmed=0, skipped=0)

        async with ClientSession() as session:
            items = await self._recent_items(session)
            prewarmed = 0
            skipped = 0
            seen: set[tuple[str, str]] = set()

            for item in items:
                item_id = str(item.get("Id") or "")
                if not item_id:
                    skipped += 1
                    continue

                raw_sources = await self._raw_media_sources(session, item_id)
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
                        await self._prewarm_source(source)
                    except (ClientError, OriginError, TimeoutError, OSError, ValueError):
                        skipped += 1
                        continue
                    prewarmed += 1

        return PrewarmResult(scanned=len(items), prewarmed=prewarmed, skipped=skipped)

    async def _recent_items(self, session: ClientSession) -> list[dict]:
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
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            payload = await response.json()
        items = payload.get("Items", [])
        return items if isinstance(items, list) else []

    async def _raw_media_sources(self, session: ClientSession, item_id: str) -> list[dict]:
        assert self.config.prewarm_api_key is not None
        url = f"{self.config.emby_base_url.rstrip('/')}/Items/{item_id}/PlaybackInfo"
        async with session.get(url, params={"api_key": self.config.prewarm_api_key}) as response:
            response.raise_for_status()
            payload = await response.json()
        sources = payload.get("MediaSources", [])
        return sources if isinstance(sources, list) else []

    async def _prewarm_source(self, source: MediaSource) -> None:
        async with OriginClient(chunk_bytes=self.config.cache.chunk_bytes) as origin:
            metadata = SourceMetadata(url=source.path, size=source.size) if source.size else await origin.head(source.path)
            key = cache_key(source, metadata)
            for block_name, byte_range in _prewarm_ranges(metadata.size):
                writer = self.cache.stage_block(key, block_name, byte_range)
                try:
                    async with origin.open_range(metadata.url, byte_range, size=metadata.size) as upstream:
                        async for chunk in upstream.content.iter_chunked(self.config.cache.chunk_bytes):
                            if chunk:
                                writer.write(chunk)
                    writer.commit()
                except Exception:
                    writer.abort()
                    raise
            self.cache.evict_if_needed()


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
