from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout

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
    def __init__(
        self,
        config: Config,
        store: SessionStateStore,
        *,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.config = config
        self.store = store
        self.timeout_seconds = timeout_seconds

    async def run_once(self, *, now: float) -> ObserverResult:
        if not self.config.session.observer_enabled or not self.config.prewarm_api_key:
            return ObserverResult(observed=0, stopped=0)
        payload = await self._sessions_payload()
        if payload is None:
            return ObserverResult(observed=0, stopped=0)
        if not isinstance(payload, list):
            return ObserverResult(observed=0, stopped=0)
        observed = extract_observed_session_hashes(payload)
        await asyncio.to_thread(
            self.store.record_observed_sessions,
            observed,
            observed_at=now,
        )
        stopped_sessions = await asyncio.to_thread(
            self.store.mark_missing_observed_sessions_stopped,
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
            timeout = ClientTimeout(total=self.timeout_seconds)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(
                    url,
                    params={"api_key": self.config.prewarm_api_key},
                ) as response:
                    if response.status >= 400:
                        return None
                    return await response.json()
        except (ClientError, TimeoutError, OSError, ValueError):
            return None
