from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

from .models import ByteRange, MediaSource, SourceMetadata

GIB = 1024**3
MIB = 1024**2


def adaptive_head_tail(size: int) -> tuple[int, int]:
    if size < 2 * GIB:
        return 16 * MIB, 4 * MIB
    if size < 8 * GIB:
        return 32 * MIB, 8 * MIB
    if size < 30 * GIB:
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
        if block_name not in {"head", "tail"}:
            raise ValueError("block_name must be head or tail")
        return self.root / key / f"{block_name}.bin"

    def meta_path(self, key: str, block_name: str) -> Path:
        return self.root / key / f"{block_name}.range"

    def store_block(self, key: str, block_name: str, byte_range: ByteRange, data: bytes) -> None:
        directory = self.root / key
        directory.mkdir(parents=True, exist_ok=True)
        path = self.block_path(key, block_name)
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)
        self.meta_path(key, block_name).write_text(f"{byte_range.start}-{byte_range.end}\n")
        self._touch(path)

    def read_block(self, key: str, block_name: str, requested: ByteRange) -> bytes | None:
        path = self.block_path(key, block_name)
        meta = self.meta_path(key, block_name)
        if not path.exists() or not meta.exists():
            return None
        stored = self._read_range(meta)
        if requested.start < stored.start or requested.end > stored.end:
            return None
        with path.open("rb") as handle:
            handle.seek(requested.start - stored.start)
            data = handle.read(requested.length)
        if len(data) != requested.length:
            path.unlink(missing_ok=True)
            meta.unlink(missing_ok=True)
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
