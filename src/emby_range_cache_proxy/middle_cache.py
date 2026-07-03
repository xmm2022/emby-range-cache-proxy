from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Iterator

from .cache import CacheReadError, KEY_PATTERN
from .models import ByteRange
from .state import MiddleBlockRecord, SessionStateStore


RANGE_NAME_PATTERN = re.compile(r"^(?P<start>\d+)-(?P<end>\d+)$")


class MiddleRangeCache:
    def __init__(
        self,
        root: str | Path,
        store: SessionStateStore,
        *,
        max_bytes: int,
        ttl_seconds: int,
    ) -> None:
        self.root = Path(root)
        self.store = store
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self.root.mkdir(parents=True, exist_ok=True)

    def store_block(
        self, key: str, byte_range: ByteRange, data: bytes, *, now: float
    ) -> None:
        key = self._validate_key(key)
        if len(data) != byte_range.length:
            raise ValueError("data length must match byte_range length")
        self.evict_expired(now=now)

        path, sidecar = self._paths(key, byte_range.start, byte_range.end)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        sidecar_tmp = sidecar.with_name(f"{sidecar.name}.{uuid.uuid4().hex}.tmp")
        try:
            tmp.write_bytes(data)
            sidecar_tmp.write_text(f"{byte_range.start}-{byte_range.end}\n")
            os.replace(tmp, path)
            os.replace(sidecar_tmp, sidecar)
        except OSError:
            tmp.unlink(missing_ok=True)
            sidecar_tmp.unlink(missing_ok=True)
            path.unlink(missing_ok=True)
            sidecar.unlink(missing_ok=True)
            raise

        self.store.upsert_middle_block(
            MiddleBlockRecord(
                cache_key=key,
                start=byte_range.start,
                end=byte_range.end,
                path=self._relative_path(key, byte_range.start, byte_range.end),
                size=len(data),
                created_at=now,
                last_access_at=now,
                expires_at=now + self.ttl_seconds,
            )
        )

    def iter_block(
        self,
        key: str,
        requested: ByteRange,
        *,
        chunk_bytes: int,
        now: float,
    ) -> Iterator[bytes] | None:
        key = self._validate_key(key)
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")

        record = self.store.find_middle_block(key, requested)
        if record is None:
            return None
        if not self._record_covers(record, requested) or not self._valid_record(record):
            self.remove_block(record)
            return None

        path = self.root / record.path
        sidecar = path.with_suffix(".range")
        if not self._valid_files(record, path, sidecar):
            self.remove_block(record)
            return None

        try:
            handle = path.open("rb", buffering=0)
            handle.seek(requested.start - record.start)
        except OSError:
            self.remove_block(record)
            return None

        self.store.touch_middle_block(
            key,
            record.start,
            record.end,
            now=now,
            ttl_seconds=self.ttl_seconds,
        )

        def chunks() -> Iterator[bytes]:
            remaining = requested.length
            try:
                while remaining > 0:
                    expected = min(chunk_bytes, remaining)
                    try:
                        data = handle.read(expected)
                    except OSError as error:
                        self.remove_block(record)
                        raise CacheReadError("failed to read cache block") from error
                    if len(data) != expected:
                        self.remove_block(record)
                        raise CacheReadError("cache block ended early")
                    remaining -= len(data)
                    yield data
            finally:
                handle.close()

        return chunks()

    def evict_expired(self, *, now: float) -> int:
        count = 0
        for record in self.store.expired_middle_blocks(now=now):
            if record.expires_at >= now:
                continue
            self.remove_block(record)
            count += 1
        return count

    def evict_lru_if_needed(self) -> int:
        count = 0
        for record in self.store.least_recent_middle_blocks():
            if self.store.middle_cache_bytes() <= self.max_bytes:
                break
            self.remove_block(record)
            count += 1
        return count

    def remove_block(self, record: MiddleBlockRecord) -> None:
        path = self.root / record.path
        path.unlink(missing_ok=True)
        path.with_suffix(".range").unlink(missing_ok=True)
        self.store.delete_middle_block_record(record.cache_key, record.start, record.end)

    def _validate_key(self, key: str) -> str:
        if not KEY_PATTERN.fullmatch(key):
            raise ValueError("cache key must be 64 lowercase hex characters")
        return key

    def _paths(self, key: str, start: int, end: int) -> tuple[Path, Path]:
        path = self.root / self._relative_path(key, start, end)
        return path, path.with_suffix(".range")

    def _relative_path(self, key: str, start: int, end: int) -> str:
        return (Path(key) / "mid" / f"{start}-{end}.bin").as_posix()

    def _record_covers(self, record: MiddleBlockRecord, requested: ByteRange) -> bool:
        return record.start <= requested.start and record.end >= requested.end

    def _valid_record(self, record: MiddleBlockRecord) -> bool:
        if record.size != record.end - record.start + 1:
            return False
        expected = self._relative_path(record.cache_key, record.start, record.end)
        return record.path == expected

    def _valid_files(
        self, record: MiddleBlockRecord, path: Path, sidecar: Path
    ) -> bool:
        try:
            sidecar_range = self._read_range(sidecar)
            size = path.stat().st_size
        except (OSError, ValueError):
            return False
        return (
            sidecar_range.start == record.start
            and sidecar_range.end == record.end
            and size == record.size
        )

    def _read_range(self, path: Path) -> ByteRange:
        text = path.read_text().strip()
        match = RANGE_NAME_PATTERN.fullmatch(text)
        if match is None:
            raise ValueError("invalid range sidecar")
        return ByteRange(int(match.group("start")), int(match.group("end")))
