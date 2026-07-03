from __future__ import annotations

import re
from urllib.parse import parse_qs, urlsplit

from .models import RequestContext

_ORIGINAL_RE = re.compile(r"^/emby/videos/(?P<item_id>\d+)/original\.(?P<ext>[A-Za-z0-9]+)$")


def parse_original_request(method: str, raw_path: str, headers: dict[str, str]) -> RequestContext | None:
    if method.upper() not in {"GET", "HEAD"}:
        return None
    parsed = urlsplit(raw_path)
    match = _ORIGINAL_RE.fullmatch(parsed.path)
    if not match:
        return None
    query = parse_qs(parsed.query, keep_blank_values=False)
    media_source_id = _first(query, "MediaSourceId")
    token = _first(query, "api_key") or headers.get("X-Emby-Token") or headers.get("x-emby-token")
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
