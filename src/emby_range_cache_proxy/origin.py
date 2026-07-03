from __future__ import annotations

from collections.abc import AsyncIterator

from aiohttp import ClientSession, ClientTimeout

from .models import ByteRange, SourceMetadata


class OriginError(Exception):
    pass


class OriginClient:
    def __init__(self, *, chunk_bytes: int = 1024 * 1024, timeout_seconds: float = 30.0) -> None:
        self.chunk_bytes = chunk_bytes
        self.timeout_seconds = timeout_seconds
        self._session: ClientSession | None = None

    async def __aenter__(self) -> "OriginClient":
        self._session = ClientSession(timeout=ClientTimeout(total=self.timeout_seconds))
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()

    async def head(self, url: str) -> SourceMetadata:
        if self._session is None:
            raise RuntimeError("OriginClient must be used as an async context manager")
        async with self._session.head(url, allow_redirects=True) as response:
            if response.status >= 400:
                raise OriginError(f"origin HEAD failed: status={response.status}")
            length = response.headers.get("Content-Length")
            if not length:
                raise OriginError("origin did not provide Content-Length")
            return SourceMetadata(
                url=str(response.url),
                size=int(length),
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
            )

    async def stream_range(self, url: str, byte_range: ByteRange) -> AsyncIterator[bytes]:
        if self._session is None:
            raise RuntimeError("OriginClient must be used as an async context manager")
        headers = {"Range": f"bytes={byte_range.start}-{byte_range.end}"}
        async with self._session.get(url, headers=headers, allow_redirects=True) as response:
            if response.status not in {200, 206}:
                raise OriginError(f"origin range GET failed: status={response.status}")
            async for chunk in response.content.iter_chunked(self.chunk_bytes):
                if chunk:
                    yield chunk
