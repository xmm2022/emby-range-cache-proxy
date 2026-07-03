from __future__ import annotations

import asyncio
import hashlib
import logging
import time

from .config import SessionConfig
from .models import ByteRange, RequestContext, SourceMetadata
from .state import (
    PlaybackSessionRecord,
    PlaybackSessionUpdate,
    SessionStateStore,
    hash_identifier,
)

LOGGER = logging.getLogger(__name__)


def origin_signature(metadata: SourceMetadata) -> str:
    material = "\n".join(
        [metadata.url, str(metadata.size), metadata.etag or "", metadata.last_modified or ""]
    )
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
        play_session_hash = hash_identifier(ctx.play_session_id)
        if play_session_hash is None:
            raise ValueError("play_session_id did not produce a session hash")
        session_hash = play_session_hash
    else:
        bucket = int(observed_at // 900)
        token_hash = hash_identifier(ctx.token)
        synthetic_identifier_hash = device_hash or token_hash or "anonymous"
        session_hash = hash_identifier(
            f"synthetic:{ctx.item_id}:{ctx.media_source_id}:{synthetic_identifier_hash}:{bucket}"
        )
        if session_hash is None:
            raise ValueError("synthetic session material did not produce a session hash")
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
        self.queue: asyncio.Queue[PlaybackSessionUpdate | None] = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def record_nowait(
        self,
        ctx: RequestContext,
        cache_key: str,
        metadata: SourceMetadata,
        byte_range: ByteRange,
        *,
        observed_at: float | None = None,
    ) -> bool:
        if self._stopping:
            return False
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
            if update is None:
                continue
            if await self._record_update(update):
                count += 1

    async def run(self) -> None:
        while True:
            update = await self.queue.get()
            if update is None:
                await self.drain_once()
                return
            await self._record_update(update)

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stopping = True
        if self._task is None:
            await self.drain_once()
            return
        if self._task.done():
            await self._task
            return
        await self.queue.put(None)
        await self._task

    async def _record_update(self, update: PlaybackSessionUpdate) -> bool:
        try:
            await asyncio.to_thread(self.store.record_playback, update)
        except Exception as error:
            LOGGER.warning("session recorder write failed: %s", type(error).__name__)
            return False
        return True

    def mark_idle_and_expired(
        self, config: SessionConfig, *, now: float
    ) -> list[PlaybackSessionRecord]:
        idle = self.store.mark_idle_sessions(now=now, idle_seconds=config.idle_seconds)
        self.store.expire_old_sessions(now=now, expire_seconds=config.expire_seconds)
        return idle
