from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from aiohttp import ClientError, ClientSession, ClientTimeout

from .models import ByteRange, SourceMetadata


class OriginError(Exception):
    pass


class OriginClient:
    def __init__(self, *, chunk_bytes: int = 1024 * 1024, timeout_seconds: float = 30.0) -> None:
        self.chunk_bytes = chunk_bytes
        self.timeout_seconds = timeout_seconds
        self._session: ClientSession | None = None

    async def __aenter__(self) -> "OriginClient":
        self._session = ClientSession(
            timeout=ClientTimeout(total=None, sock_connect=self.timeout_seconds, sock_read=self.timeout_seconds)
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            finally:
                self._session = None

    async def head(self, url: str) -> SourceMetadata:
        if self._session is None:
            raise RuntimeError("OriginClient must be used as an async context manager")
        try:
            async with self._session.head(url, allow_redirects=True) as response:
                if response.status >= 400:
                    raise OriginError(f"origin HEAD failed: status={response.status}")
                length = _parse_content_length(response.headers.get("Content-Length"))
                return SourceMetadata(
                    url=str(response.url),
                    size=length,
                    etag=response.headers.get("ETag"),
                    last_modified=response.headers.get("Last-Modified"),
                )
        except OriginError:
            raise
        except (asyncio.TimeoutError, TimeoutError):
            raise OriginError("origin HEAD failed: timeout") from None
        except ClientError:
            raise OriginError("origin HEAD failed: client error") from None

    async def stream_range(self, url: str, byte_range: ByteRange) -> AsyncIterator[bytes]:
        if self._session is None:
            raise RuntimeError("OriginClient must be used as an async context manager")
        headers = {"Range": f"bytes={byte_range.start}-{byte_range.end}"}
        try:
            async with self._session.get(url, headers=headers, allow_redirects=True) as response:
                if response.status not in {200, 206}:
                    raise OriginError(f"origin range GET failed: status={response.status}")
                async for chunk in response.content.iter_chunked(self.chunk_bytes):
                    if chunk:
                        yield chunk
        except OriginError:
            raise
        except (asyncio.TimeoutError, TimeoutError):
            raise OriginError("origin range GET failed: timeout") from None
        except ClientError:
            raise OriginError("origin range GET failed: client error") from None


def _parse_content_length(value: str | None) -> int:
    if value is None:
        raise OriginError("origin did not provide Content-Length")
    try:
        length = int(value)
    except ValueError:
        raise OriginError("origin provided invalid Content-Length") from None
    if length < 0:
        raise OriginError("origin provided invalid Content-Length")
    return length
