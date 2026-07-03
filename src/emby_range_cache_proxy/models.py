from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class ByteRange:
    start: int
    end: int

    @property
    def length(self) -> int:
        return self.end - self.start + 1


@dataclass(frozen=True)
class RequestContext:
    method: str
    raw_path: str
    item_id: str
    media_source_id: str
    token: str
    extension: str
    play_session_id: str | None = None
    device_id: str | None = None


@dataclass(frozen=True)
class MediaSource:
    item_id: str
    media_source_id: str
    path: str
    protocol: str
    size: int | None
    container: str | None = None
    bitrate: int | None = None


@dataclass(frozen=True)
class SourceMetadata:
    url: str
    size: int
    etag: str | None = None
    last_modified: str | None = None
