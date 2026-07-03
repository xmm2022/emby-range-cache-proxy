from __future__ import annotations

import re
from collections.abc import Mapping
from urllib.parse import parse_qs, urlsplit

from .models import RequestContext

_ORIGINAL_RE = re.compile(r"^/emby/videos/(?P<item_id>\d+)/original\.(?P<ext>[A-Za-z0-9]+)$")
_SAFETY_QUERY_KEYS = {"MediaSourceId", "api_key", "PlaySessionId", "DeviceId"}


def parse_original_request(method: str, raw_path: str, headers: Mapping[str, str]) -> RequestContext | None:
    if method.upper() not in {"GET", "HEAD"}:
        return None
    parsed = urlsplit(raw_path)
    match = _ORIGINAL_RE.fullmatch(parsed.path)
    if not match:
        return None
    query = parse_qs(parsed.query, keep_blank_values=True)
    if _has_duplicate_safety_param(query):
        return None
    media_source_id = _first(query, "MediaSourceId")
    token = _first(query, "api_key") or _header_value(headers, "X-Emby-Token")
    if not media_source_id or not token:
        return None
    return RequestContext(
        method=method.upper(),
        raw_path=raw_path,
        item_id=match.group("item_id"),
        media_source_id=media_source_id,
        token=token,
        extension=match.group("ext").lower(),
        play_session_id=_first(query, "PlaySessionId"),
        device_id=_first(query, "DeviceId"),
    )


def _first(query: dict[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    if not values:
        return None
    return values[0]


def _has_duplicate_safety_param(query: dict[str, list[str]]) -> bool:
    return any(len(query.get(name, [])) > 1 for name in _SAFETY_QUERY_KEYS)


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None
