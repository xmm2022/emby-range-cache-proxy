from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aiohttp import ClientError, ClientResponse, ClientSession, ClientTimeout

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

    @asynccontextmanager
    async def open_range(self, url: str, byte_range: ByteRange, *, size: int) -> AsyncIterator[ClientResponse]:
        if self._session is None:
            raise RuntimeError("OriginClient must be used as an async context manager")
        headers = {"Range": f"bytes={byte_range.start}-{byte_range.end}"}
        response: ClientResponse | None = None
        try:
            response = await self._session.get(url, headers=headers, allow_redirects=True)
            if response.status != 206:
                raise OriginError(f"origin range GET failed: status={response.status}")
            if not _content_range_matches(response.headers.get("Content-Range"), byte_range, size=size):
                raise OriginError("origin range GET failed: invalid Content-Range")
            yield response
        except OriginError:
            raise
        except (asyncio.TimeoutError, TimeoutError):
            raise OriginError("origin range GET failed: timeout") from None
        except ClientError:
            raise OriginError("origin range GET failed: client error") from None
        finally:
            if response is not None:
                response.release()


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


_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")


def _content_range_matches(value: str | None, byte_range: ByteRange, *, size: int) -> bool:
    if value is None:
        return False
    match = _CONTENT_RANGE_RE.fullmatch(value)
    if match is None:
        return False
    start, end, total = (int(group) for group in match.groups())
    return start == byte_range.start and end == byte_range.end and total == size
