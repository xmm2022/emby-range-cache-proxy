from __future__ import annotations

from dataclasses import replace
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from .config import PathMapping
from .models import MediaSource

STRM_READ_LIMIT_BYTES = 16 * 1024


def resolve_media_source(
    source: MediaSource,
    path_mappings: tuple[PathMapping, ...],
    *,
    url_prefix_allowlist: tuple[str, ...] = (),
) -> MediaSource:
    if _is_http(source.path):
        return source
    if not source.path.lower().endswith(".strm"):
        return source

    mapped_path = _map_source_path(source.path, path_mappings)
    if mapped_path is None or not mapped_path.is_file():
        return source

    try:
        url = _read_strm_url(mapped_path)
    except OSError:
        return source
    if not _is_http(url) or not _url_prefix_allowed(url, url_prefix_allowlist):
        return source
    return replace(source, path=url, protocol="Http")


def _map_source_path(source_path: str, path_mappings: tuple[PathMapping, ...]) -> Path | None:
    for mapping in path_mappings:
        prefix = mapping.source_prefix
        if not source_path.startswith(prefix):
            continue
        relative = source_path[len(prefix) :].lstrip("/")
        parts = PurePosixPath(relative).parts
        if not parts or any(part in {"", ".", ".."} for part in parts):
            return None

        root = Path(mapping.target_prefix).expanduser().resolve(strict=False)
        candidate = root.joinpath(*parts).resolve(strict=False)
        try:
            candidate.relative_to(root)
        except ValueError:
            return None
        return candidate
    return None


def _read_strm_url(path: Path) -> str:
    with path.open("rb") as handle:
        data = handle.read(STRM_READ_LIMIT_BYTES)
    for line in data.decode("utf-8", errors="ignore").splitlines():
        value = line.strip()
        if value and not value.startswith("#"):
            return value
    return ""


def _is_http(value: str) -> bool:
    return urlsplit(value).scheme.lower() in {"http", "https"}


def _url_prefix_allowed(url: str, prefixes: tuple[str, ...]) -> bool:
    return bool(prefixes) and any(url.startswith(prefix) for prefix in prefixes)
