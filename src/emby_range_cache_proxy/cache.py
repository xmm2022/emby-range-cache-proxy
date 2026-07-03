from __future__ import annotations

import hashlib
import os
import re
import time
import uuid
from pathlib import Path

from .models import ByteRange, MediaSource, SourceMetadata

GIB = 1024**3
MIB = 1024**2
KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def adaptive_head_tail(size: int) -> tuple[int, int]:
    if size < 2 * GIB:
        return 16 * MIB, 4 * MIB
    if size < 8 * GIB:
        return 32 * MIB, 8 * MIB
    if size <= 30 * GIB:
        return 64 * MIB, 8 * MIB
    return 128 * MIB, 16 * MIB


def cache_key(source: MediaSource, metadata: SourceMetadata) -> str:
    material = "\n".join(
        [
            source.media_source_id,
            metadata.url,
            str(metadata.size),
            metadata.etag or "",
            metadata.last_modified or "",
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class HeadTailCache:
    def __init__(self, root: str | Path, *, max_bytes: int) -> None:
        self.root = Path(root)
        self.max_bytes = max_bytes
        self.root.mkdir(parents=True, exist_ok=True)

    def block_path(self, key: str, block_name: str) -> Path:
        return self._cache_dir(key) / f"{self._validate_block_name(block_name)}.bin"

    def meta_path(self, key: str, block_name: str) -> Path:
        return self._cache_dir(key) / f"{self._validate_block_name(block_name)}.range"

    def store_block(self, key: str, block_name: str, byte_range: ByteRange, data: bytes) -> None:
        if len(data) != byte_range.length:
            raise ValueError("data length must match byte_range length")
        directory = self._cache_dir(key)
        directory.mkdir(parents=True, exist_ok=True)
        path = self.block_path(key, block_name)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)
        meta = self.meta_path(key, block_name)
        meta_tmp = meta.with_name(f"{meta.name}.tmp")
        meta_tmp.write_text(f"{byte_range.start}-{byte_range.end}\n")
        os.replace(meta_tmp, meta)
        self._touch(path)

    def stage_block(self, key: str, block_name: str, byte_range: ByteRange) -> "CacheBlockWriter":
        return CacheBlockWriter(self, key, block_name, byte_range)

    def read_block(self, key: str, block_name: str, requested: ByteRange) -> bytes | None:
        path = self.block_path(key, block_name)
        meta = self.meta_path(key, block_name)
        if not path.exists() or not meta.exists():
            return None
        try:
            stored = self._read_range(meta)
        except (OSError, ValueError):
            self._remove_entry(path, meta)
            return None
        if requested.start < stored.start or requested.end > stored.end:
            return None
        with path.open("rb") as handle:
            handle.seek(requested.start - stored.start)
            data = handle.read(requested.length)
        if len(data) != requested.length:
            self._remove_entry(path, meta)
            return None
        self._touch(path)
        return data

    def evict_if_needed(self) -> None:
        files = [path for path in self.root.glob("*/*.bin") if path.is_file()]
        total = sum(path.stat().st_size for path in files)
        if total <= self.max_bytes:
            return
        files.sort(key=lambda path: path.stat().st_mtime_ns)
        for path in files:
            if total <= self.max_bytes:
                break
            size = path.stat().st_size
            path.unlink(missing_ok=True)
            path.with_suffix(".range").unlink(missing_ok=True)
            total -= size

    def _read_range(self, path: Path) -> ByteRange:
        start, end = path.read_text().strip().split("-", 1)
        return ByteRange(int(start), int(end))

    def _touch(self, path: Path) -> None:
        now = time.time_ns()
        os.utime(path, ns=(now, now))

    def _cache_dir(self, key: str) -> Path:
        if not KEY_PATTERN.fullmatch(key):
            raise ValueError("cache key must be 64 lowercase hex characters")
        return self.root / key

    def _validate_block_name(self, block_name: str) -> str:
        if block_name not in {"head", "tail"}:
            raise ValueError("block_name must be head or tail")
        return block_name

    def _remove_entry(self, path: Path, meta: Path) -> None:
        path.unlink(missing_ok=True)
        meta.unlink(missing_ok=True)


class CacheBlockWriter:
    def __init__(self, cache: HeadTailCache, key: str, block_name: str, byte_range: ByteRange) -> None:
        self.cache = cache
        self.key = key
        self.block_name = cache._validate_block_name(block_name)
        self.byte_range = byte_range
        self.directory = cache._cache_dir(key)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = cache.block_path(key, block_name)
        self.meta = cache.meta_path(key, block_name)
        self.tmp = self.path.with_name(f"{self.path.name}.{uuid.uuid4().hex}.tmp")
        self._handle = self.tmp.open("wb")
        self.bytes_written = 0
        self._closed = False

    def write(self, data: bytes) -> None:
        if self._closed:
            raise ValueError("cache block writer is closed")
        self._handle.write(data)
        self.bytes_written += len(data)

    def commit(self) -> None:
        if self._closed:
            raise ValueError("cache block writer is closed")
        self._handle.close()
        self._closed = True
        if self.bytes_written != self.byte_range.length:
            self.tmp.unlink(missing_ok=True)
            raise ValueError("staged data length must match byte_range length")
        meta_tmp = self.meta.with_name(f"{self.meta.name}.{uuid.uuid4().hex}.tmp")
        meta_tmp.write_text(f"{self.byte_range.start}-{self.byte_range.end}\n")
        os.replace(self.tmp, self.path)
        os.replace(meta_tmp, self.meta)
        self.cache._touch(self.path)

    def abort(self) -> None:
        if not self._closed:
            self._handle.close()
            self._closed = True
        self.tmp.unlink(missing_ok=True)

    def __enter__(self) -> "CacheBlockWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None or not self._closed:
            self.abort()
