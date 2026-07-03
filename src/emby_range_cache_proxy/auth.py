from __future__ import annotations

import asyncio
from dataclasses import dataclass

from aiohttp import ClientError, ClientSession, ClientTimeout, ContentTypeError

from .models import MediaSource, RequestContext


class AuthorizationError(Exception):
    pass


class AuthUnavailable(Exception):
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
            try:
                await self._session.close()
            finally:
                self._session = None

    async def authorize(self, ctx: RequestContext) -> MediaSource:
        if self._session is None:
            raise RuntimeError("EmbyAuthClient must be used as an async context manager")
        url = f"{self.base_url}/Items/{ctx.item_id}/PlaybackInfo"
        try:
            async with self._session.get(
                url,
                params={"MediaSourceId": ctx.media_source_id, "api_key": ctx.token},
            ) as response:
                if response.status in {401, 403, 404}:
                    raise AuthorizationError(f"Emby authorization failed: status={response.status}")
                if response.status != 200:
                    raise AuthUnavailable(f"Emby authorization unavailable: status={response.status}")
                try:
                    payload = await response.json()
                except (ContentTypeError, ValueError):
                    raise AuthorizationError("invalid PlaybackInfo response") from None
        except AuthorizationError:
            raise
        except (asyncio.TimeoutError, TimeoutError):
            raise AuthUnavailable("Emby authorization unavailable: timeout") from None
        except ClientError:
            raise AuthUnavailable("Emby authorization unavailable: client error") from None
        except OSError:
            raise AuthUnavailable("Emby authorization unavailable: os error") from None

        if not isinstance(payload, dict):
            raise AuthorizationError("invalid PlaybackInfo response")
        media_sources = payload.get("MediaSources", [])
        if not isinstance(media_sources, list):
            raise AuthorizationError("invalid PlaybackInfo response")

        for source in media_sources:
            if not isinstance(source, dict):
                raise AuthorizationError("invalid PlaybackInfo response")
            if source.get("Id") == ctx.media_source_id:
                path = source.get("Path")
                if not path:
                    raise AuthorizationError("media source path is empty")
                if not isinstance(path, str):
                    raise AuthorizationError("media source path is invalid")
                return MediaSource(
                    item_id=ctx.item_id,
                    media_source_id=ctx.media_source_id,
                    path=path,
                    protocol=str(source.get("Protocol", "")),
                    size=_optional_int(source.get("Size"), "Size"),
                    container=source.get("Container"),
                    bitrate=_optional_int(source.get("Bitrate"), "Bitrate"),
                )
        raise AuthorizationError("media source not allowed")


def _optional_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise AuthorizationError(f"invalid media source {field_name}") from None
