from __future__ import annotations

from dataclasses import dataclass

from aiohttp import ClientSession, ClientTimeout

from .models import MediaSource, RequestContext


class AuthorizationError(Exception):
    pass


@dataclass
class EmbyAuthClient:
    base_url: str
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self._session: ClientSession | None = None

    async def __aenter__(self) -> "EmbyAuthClient":
        self._session = ClientSession(timeout=ClientTimeout(total=self.timeout_seconds))
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()

    async def authorize(self, ctx: RequestContext) -> MediaSource:
        if self._session is None:
            raise RuntimeError("EmbyAuthClient must be used as an async context manager")
        url = f"{self.base_url}/Items/{ctx.item_id}/PlaybackInfo"
        async with self._session.get(
            url,
            params={"MediaSourceId": ctx.media_source_id, "api_key": ctx.token},
        ) as response:
            if response.status != 200:
                raise AuthorizationError(f"Emby authorization failed: status={response.status}")
            payload = await response.json()
        for source in payload.get("MediaSources", []):
            if source.get("Id") == ctx.media_source_id:
                path = source.get("Path")
                if not path:
                    raise AuthorizationError("media source path is empty")
                return MediaSource(
                    item_id=ctx.item_id,
                    media_source_id=ctx.media_source_id,
                    path=path,
                    protocol=str(source.get("Protocol", "")),
                    size=int(source["Size"]) if source.get("Size") is not None else None,
                    container=source.get("Container"),
                    bitrate=int(source["Bitrate"]) if source.get("Bitrate") is not None else None,
                )
        raise AuthorizationError("media source not allowed")
