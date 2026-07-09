from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SENSITIVE_QUERY_KEYS = {
    "api_key",
    "playsessionid",
    "deviceid",
    "x-emby-token",
    "token",
    "sign",
    "openlist_ts",
}


def stable_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def redact_url(url: str) -> str:
    parsed = urlsplit(url)
    redacted_pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in SENSITIVE_QUERY_KEYS:
            redacted_pairs.append((key, "[REDACTED]"))
        else:
            redacted_pairs.append((key, value))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(redacted_pairs), parsed.fragment))
