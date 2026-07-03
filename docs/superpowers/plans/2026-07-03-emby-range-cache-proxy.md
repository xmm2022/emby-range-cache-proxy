# Emby Range Cache Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-ready v1 Emby Range Cache Proxy that validates user playback tokens, serves HTTP Range origins, caches adaptive head/tail blocks, prewarms newly added media, and falls back safely to Emby.

**Architecture:** Implement a Python 3.11 `aiohttp` service with focused modules for config, request parsing, authorization, origin fetching, cache management, proxy routing, and prewarm. User requests are authorized only through Emby `PlaybackInfo`; the internal API key is used only by the prewarm worker.

**Tech Stack:** Python 3.11, `aiohttp`, `pytest`, `pytest-asyncio`, stdlib JSON config, systemd, Caddy.

---

## File Structure

- `pyproject.toml`: package metadata, runtime dependency on `aiohttp`, dev dependencies for tests.
- `config.example.json`: documented example config for test and production-style deployments.
- `src/emby_range_cache_proxy/__init__.py`: package version.
- `src/emby_range_cache_proxy/config.py`: dataclass config model, JSON loading, defaults, allowlist checks.
- `src/emby_range_cache_proxy/models.py`: shared dataclasses for request context, media source, authorization result, source metadata, and byte ranges.
- `src/emby_range_cache_proxy/ranges.py`: HTTP Range parser, response header helpers, range intersection.
- `src/emby_range_cache_proxy/sanitize.py`: token hashing and URL/header redaction.
- `src/emby_range_cache_proxy/requests.py`: Emby original-media request parser.
- `src/emby_range_cache_proxy/auth.py`: Emby `PlaybackInfo` client and media-source validation.
- `src/emby_range_cache_proxy/origin.py`: HTTP/HTTPS origin HEAD and Range streaming client.
- `src/emby_range_cache_proxy/cache.py`: adaptive head/tail sizing, cache-key generation, block storage, build locks, LRU eviction.
- `src/emby_range_cache_proxy/app.py`: `aiohttp` app, proxy handler, health endpoint, fallback handler.
- `src/emby_range_cache_proxy/prewarm.py`: recently-added media scan and head/tail prewarm worker.
- `src/emby_range_cache_proxy/cli.py`: command-line entry point.
- `deploy/emby-range-cache-proxy.service`: systemd unit example.
- `deploy/Caddyfile.range-cache.example`: Caddy grey-release and fallback example.
- `tests/`: focused unit and integration tests.

## Task 1: Project Scaffold And Config

**Files:**
- Create: `pyproject.toml`
- Create: `config.example.json`
- Create: `src/emby_range_cache_proxy/__init__.py`
- Create: `src/emby_range_cache_proxy/config.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write failing config tests**

Create `tests/test_config.py`:

```python
import json

from emby_range_cache_proxy.config import Config, load_config


def test_load_config_with_defaults(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "listen_host": "127.0.0.1",
                "listen_port": 18180,
                "cache_dir": str(tmp_path / "cache"),
                "fallback_base_url": "http://127.0.0.1:8096",
                "prewarm_api_key": "secret-prewarm-key",
                "rollout": {"enabled": True, "item_allowlist": ["151357"]},
            }
        )
    )

    config = load_config(path)

    assert config.emby_base_url == "http://127.0.0.1:8096"
    assert config.listen_host == "127.0.0.1"
    assert config.listen_port == 18180
    assert config.cache.max_bytes == 512 * 1024**3
    assert config.prewarm.enabled is False
    assert config.rollout.enabled is True
    assert config.rollout.item_allowed("151357") is True
    assert config.rollout.item_allowed("999999") is False


def test_empty_allowlists_mean_allowed_when_rollout_enabled():
    config = Config(
        emby_base_url="http://127.0.0.1:8096",
        fallback_base_url="http://127.0.0.1:8096",
        cache_dir="/tmp/cache",
    )

    assert config.rollout.enabled is False
    assert config.rollout.in_scope(item_id="1", media_source_id="ms1") is False

    config.rollout.enabled = True

    assert config.rollout.in_scope(item_id="1", media_source_id="ms1") is True
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_config.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'emby_range_cache_proxy'`.

- [ ] **Step 3: Add package scaffold**

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "emby-range-cache-proxy"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "aiohttp>=3.9,<4",
]

[project.optional-dependencies]
dev = [
    "pytest>=8,<9",
    "pytest-asyncio>=0.23,<1",
]

[project.scripts]
emby-range-cache-proxy = "emby_range_cache_proxy.cli:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

Create `src/emby_range_cache_proxy/__init__.py`:

```python
__version__ = "0.1.0"
```

Create `src/emby_range_cache_proxy/config.py`:

```python
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class RolloutConfig:
    enabled: bool = False
    item_allowlist: set[str] = field(default_factory=set)
    media_source_allowlist: set[str] = field(default_factory=set)
    path_prefix_allowlist: tuple[str, ...] = ()

    def item_allowed(self, item_id: str) -> bool:
        return not self.item_allowlist or item_id in self.item_allowlist

    def media_source_allowed(self, media_source_id: str) -> bool:
        return not self.media_source_allowlist or media_source_id in self.media_source_allowlist

    def path_allowed(self, path: str | None) -> bool:
        if not self.path_prefix_allowlist:
            return True
        if not path:
            return False
        return any(path.startswith(prefix) for prefix in self.path_prefix_allowlist)

    def in_scope(self, *, item_id: str, media_source_id: str, path: str | None = None) -> bool:
        if not self.enabled:
            return False
        return (
            self.item_allowed(item_id)
            and self.media_source_allowed(media_source_id)
            and self.path_allowed(path)
        )


@dataclass
class CacheConfig:
    max_bytes: int = 512 * 1024**3
    build_wait_seconds: float = 0.25
    chunk_bytes: int = 1024 * 1024


@dataclass
class PrewarmConfig:
    enabled: bool = False
    interval_seconds: int = 900
    max_items_per_scan: int = 100
    concurrency: int = 1


@dataclass
class Config:
    emby_base_url: str
    fallback_base_url: str
    cache_dir: str
    listen_host: str = "127.0.0.1"
    listen_port: int = 18180
    prewarm_api_key: str | None = None
    rollout: RolloutConfig = field(default_factory=RolloutConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    prewarm: PrewarmConfig = field(default_factory=PrewarmConfig)


def _string_set(values: Any) -> set[str]:
    if values is None:
        return set()
    return {str(value) for value in values}


def _rollout(data: dict[str, Any]) -> RolloutConfig:
    return RolloutConfig(
        enabled=bool(data.get("enabled", False)),
        item_allowlist=_string_set(data.get("item_allowlist")),
        media_source_allowlist=_string_set(data.get("media_source_allowlist")),
        path_prefix_allowlist=tuple(str(v) for v in data.get("path_prefix_allowlist", [])),
    )


def _cache(data: dict[str, Any]) -> CacheConfig:
    return CacheConfig(
        max_bytes=int(data.get("max_bytes", 512 * 1024**3)),
        build_wait_seconds=float(data.get("build_wait_seconds", 0.25)),
        chunk_bytes=int(data.get("chunk_bytes", 1024 * 1024)),
    )


def _prewarm(data: dict[str, Any]) -> PrewarmConfig:
    return PrewarmConfig(
        enabled=bool(data.get("enabled", False)),
        interval_seconds=int(data.get("interval_seconds", 900)),
        max_items_per_scan=int(data.get("max_items_per_scan", 100)),
        concurrency=int(data.get("concurrency", 1)),
    )


def load_config(path: str | Path) -> Config:
    raw = json.loads(Path(path).read_text())
    return Config(
        emby_base_url=str(raw["emby_base_url"]).rstrip("/"),
        fallback_base_url=str(raw.get("fallback_base_url", raw["emby_base_url"])).rstrip("/"),
        cache_dir=str(raw["cache_dir"]),
        listen_host=str(raw.get("listen_host", "127.0.0.1")),
        listen_port=int(raw.get("listen_port", 18180)),
        prewarm_api_key=raw.get("prewarm_api_key"),
        rollout=_rollout(raw.get("rollout", {})),
        cache=_cache(raw.get("cache", {})),
        prewarm=_prewarm(raw.get("prewarm", {})),
    )
```

Create `config.example.json`:

```json
{
  "emby_base_url": "http://127.0.0.1:8096",
  "fallback_base_url": "http://127.0.0.1:8096",
  "listen_host": "127.0.0.1",
  "listen_port": 18180,
  "cache_dir": "/home/nax/emby/cache/range-proxy",
  "prewarm_api_key": null,
  "rollout": {
    "enabled": false,
    "item_allowlist": [],
    "media_source_allowlist": [],
    "path_prefix_allowlist": []
  },
  "cache": {
    "max_bytes": 549755813888,
    "build_wait_seconds": 0.25,
    "chunk_bytes": 1048576
  },
  "prewarm": {
    "enabled": false,
    "interval_seconds": 900,
    "max_items_per_scan": 100,
    "concurrency": 1
  }
}
```

- [ ] **Step 4: Install package in editable mode**

Run:

```bash
python -m pip install -e ".[dev]"
```

Expected: command exits 0 and installs `aiohttp`, `pytest`, and `pytest-asyncio`.

- [ ] **Step 5: Run config tests**

Run:

```bash
python -m pytest tests/test_config.py -q
```

Expected: `2 passed`.

- [ ] **Step 6: Commit scaffold**

Run:

```bash
git add pyproject.toml config.example.json src/emby_range_cache_proxy/__init__.py src/emby_range_cache_proxy/config.py tests/test_config.py
git commit -m "feat: add proxy project scaffold"
```

Expected: commit succeeds.

## Task 2: Request Parsing, Range Parsing, And Redaction

**Files:**
- Create: `src/emby_range_cache_proxy/models.py`
- Create: `src/emby_range_cache_proxy/ranges.py`
- Create: `src/emby_range_cache_proxy/sanitize.py`
- Create: `src/emby_range_cache_proxy/requests.py`
- Create: `tests/test_ranges.py`
- Create: `tests/test_requests.py`
- Create: `tests/test_sanitize.py`

- [ ] **Step 1: Write failing parser and redaction tests**

Create `tests/test_ranges.py`:

```python
import pytest

from emby_range_cache_proxy.ranges import ByteRange, intersect_ranges, parse_range_header


def test_parse_closed_range():
    assert parse_range_header("bytes=10-19", size=100) == ByteRange(10, 19)


def test_parse_open_ended_range_clamps_to_size():
    assert parse_range_header("bytes=90-", size=100) == ByteRange(90, 99)


def test_parse_suffix_range():
    assert parse_range_header("bytes=-10", size=100) == ByteRange(90, 99)


def test_reject_multiple_ranges():
    with pytest.raises(ValueError, match="multiple ranges"):
        parse_range_header("bytes=0-1,4-5", size=100)


def test_intersection():
    assert intersect_ranges(ByteRange(0, 99), ByteRange(50, 149)) == ByteRange(50, 99)
    assert intersect_ranges(ByteRange(0, 10), ByteRange(11, 20)) is None
```

Create `tests/test_requests.py`:

```python
from emby_range_cache_proxy.requests import parse_original_request


def test_parse_original_request_with_query_token():
    ctx = parse_original_request(
        method="GET",
        raw_path="/emby/videos/151357/original.mkv?MediaSourceId=mediasource_151357&api_key=abc123",
        headers={},
    )

    assert ctx is not None
    assert ctx.item_id == "151357"
    assert ctx.media_source_id == "mediasource_151357"
    assert ctx.token == "abc123"
    assert ctx.extension == "mkv"


def test_parse_original_request_with_header_token():
    ctx = parse_original_request(
        method="HEAD",
        raw_path="/emby/videos/151357/original.mkv?MediaSourceId=mediasource_151357",
        headers={"X-Emby-Token": "header-token"},
    )

    assert ctx is not None
    assert ctx.token == "header-token"


def test_reject_non_original_path():
    assert parse_original_request("GET", "/web/index.html", {}) is None


def test_reject_missing_media_source_or_token():
    assert parse_original_request("GET", "/emby/videos/1/original.mkv?api_key=t", {}) is None
    assert parse_original_request("GET", "/emby/videos/1/original.mkv?MediaSourceId=m", {}) is None
```

Create `tests/test_sanitize.py`:

```python
from emby_range_cache_proxy.sanitize import redact_url, stable_token_hash


def test_redact_url_query_secrets():
    url = "https://a.inemby.pp.ua/emby/videos/1/original.mkv?api_key=secret&PlaySessionId=play&DeviceId=dev&MediaSourceId=ms1"

    redacted = redact_url(url)

    assert "secret" not in redacted
    assert "play" not in redacted
    assert "dev" not in redacted
    assert "MediaSourceId=ms1" in redacted


def test_stable_token_hash_is_not_plaintext():
    digest = stable_token_hash("secret-token")

    assert digest == stable_token_hash("secret-token")
    assert digest != "secret-token"
    assert len(digest) == 64
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_ranges.py tests/test_requests.py tests/test_sanitize.py -q
```

Expected: FAIL with missing modules or missing functions.

- [ ] **Step 3: Add shared models**

Create `src/emby_range_cache_proxy/models.py`:

```python
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
```

- [ ] **Step 4: Implement range parser**

Create `src/emby_range_cache_proxy/ranges.py`:

```python
from __future__ import annotations

import re

from .models import ByteRange

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)$")


def parse_range_header(value: str | None, *, size: int) -> ByteRange:
    if size <= 0:
        raise ValueError("size must be positive")
    if not value:
        return ByteRange(0, size - 1)
    if "," in value:
        raise ValueError("multiple ranges are not supported")
    match = _RANGE_RE.fullmatch(value.strip())
    if not match:
        raise ValueError("invalid range header")
    left, right = match.groups()
    if left == "" and right == "":
        raise ValueError("empty range")
    if left == "":
        length = int(right)
        if length <= 0:
            raise ValueError("invalid suffix range")
        return ByteRange(max(0, size - length), size - 1)
    start = int(left)
    if start >= size:
        raise ValueError("range start beyond size")
    end = int(right) if right else size - 1
    if end < start:
        raise ValueError("range end before start")
    return ByteRange(start, min(end, size - 1))


def intersect_ranges(left: ByteRange, right: ByteRange) -> ByteRange | None:
    start = max(left.start, right.start)
    end = min(left.end, right.end)
    if end < start:
        return None
    return ByteRange(start, end)


def content_range_header(byte_range: ByteRange, *, size: int) -> str:
    return f"bytes {byte_range.start}-{byte_range.end}/{size}"
```

- [ ] **Step 5: Implement request parser and redaction**

Create `src/emby_range_cache_proxy/requests.py`:

```python
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
```

Create `src/emby_range_cache_proxy/sanitize.py`:

```python
from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SENSITIVE_QUERY_KEYS = {"api_key", "PlaySessionId", "DeviceId", "X-Emby-Token", "token"}


def stable_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def redact_url(url: str) -> str:
    parsed = urlsplit(url)
    redacted_pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key in SENSITIVE_QUERY_KEYS:
            redacted_pairs.append((key, "[REDACTED]"))
        else:
            redacted_pairs.append((key, value))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(redacted_pairs), parsed.fragment))
```

- [ ] **Step 6: Run parser and redaction tests**

Run:

```bash
python -m pytest tests/test_ranges.py tests/test_requests.py tests/test_sanitize.py -q
```

Expected: `10 passed`.

- [ ] **Step 7: Commit parser utilities**

Run:

```bash
git add src/emby_range_cache_proxy/models.py src/emby_range_cache_proxy/ranges.py src/emby_range_cache_proxy/sanitize.py src/emby_range_cache_proxy/requests.py tests/test_ranges.py tests/test_requests.py tests/test_sanitize.py
git commit -m "feat: parse Emby original range requests"
```

Expected: commit succeeds.

## Task 3: Emby PlaybackInfo Authorization

**Files:**
- Create: `src/emby_range_cache_proxy/auth.py`
- Create: `tests/test_auth.py`

- [ ] **Step 1: Write failing auth tests**

Create `tests/test_auth.py`:

```python
from aiohttp import web
import pytest

from emby_range_cache_proxy.auth import AuthorizationError, EmbyAuthClient
from emby_range_cache_proxy.models import RequestContext


def _ctx(token: str = "user-token") -> RequestContext:
    return RequestContext(
        method="GET",
        raw_path="/emby/videos/151357/original.mkv?MediaSourceId=mediasource_151357&api_key=user-token",
        item_id="151357",
        media_source_id="mediasource_151357",
        token=token,
        extension="mkv",
    )


async def test_authorize_selects_exact_media_source(aiohttp_client):
    async def playback_info(request):
        assert request.query["api_key"] == "user-token"
        assert request.match_info["item_id"] == "151357"
        return web.json_response(
            {
                "MediaSources": [
                    {"Id": "other", "Path": "http://origin/other.mkv", "Protocol": "Http", "Size": 1},
                    {
                        "Id": "mediasource_151357",
                        "Path": "http://origin/movie.mkv",
                        "Protocol": "Http",
                        "Size": 88513978283,
                        "Container": "mkv",
                        "Bitrate": 78740027,
                    },
                ]
            }
        )

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        source = await client.authorize(_ctx())

    assert source.media_source_id == "mediasource_151357"
    assert source.path == "http://origin/movie.mkv"
    assert source.size == 88513978283


async def test_authorize_rejects_missing_media_source(aiohttp_client):
    async def playback_info(request):
        return web.json_response({"MediaSources": []})

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="media source not allowed"):
            await client.authorize(_ctx())


async def test_authorize_rejects_emby_403(aiohttp_client):
    async def playback_info(request):
        return web.Response(status=403)

    app = web.Application()
    app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    server = await aiohttp_client(app)

    async with EmbyAuthClient(str(server.make_url(""))) as client:
        with pytest.raises(AuthorizationError, match="Emby authorization failed"):
            await client.authorize(_ctx())
```

- [ ] **Step 2: Add aiohttp test fixture dependency**

Modify `pyproject.toml` dev dependencies:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8,<9",
    "pytest-asyncio>=0.23,<1",
    "pytest-aiohttp>=1.0,<2",
]
```

Run:

```bash
python -m pip install -e ".[dev]"
python -m pytest tests/test_auth.py -q
```

Expected: FAIL with missing `emby_range_cache_proxy.auth`.

- [ ] **Step 3: Implement Emby auth client**

Create `src/emby_range_cache_proxy/auth.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

from aiohttp import ClientSession, ClientTimeout

from .models import MediaSource, RequestContext


class AuthorizationError(Exception):
    pass


@dataclass
class EmbyAuthClient:
    base_url: str
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self._session: ClientSession | None = None

    async def __aenter__(self) -> "EmbyAuthClient":
        self._session = ClientSession(timeout=ClientTimeout(total=self.timeout_seconds))
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()

    async def authorize(self, ctx: RequestContext) -> MediaSource:
        if self._session is None:
            raise RuntimeError("EmbyAuthClient must be used as an async context manager")
        url = f"{self.base_url}/Items/{ctx.item_id}/PlaybackInfo"
        async with self._session.get(
            url,
            params={"MediaSourceId": ctx.media_source_id, "api_key": ctx.token},
        ) as response:
            if response.status != 200:
                raise AuthorizationError(f"Emby authorization failed: status={response.status}")
            payload = await response.json()
        for source in payload.get("MediaSources", []):
            if source.get("Id") == ctx.media_source_id:
                path = source.get("Path")
                if not path:
                    raise AuthorizationError("media source path is empty")
                return MediaSource(
                    item_id=ctx.item_id,
                    media_source_id=ctx.media_source_id,
                    path=path,
                    protocol=str(source.get("Protocol", "")),
                    size=int(source["Size"]) if source.get("Size") is not None else None,
                    container=source.get("Container"),
                    bitrate=int(source["Bitrate"]) if source.get("Bitrate") is not None else None,
                )
        raise AuthorizationError("media source not allowed")
```

- [ ] **Step 4: Run auth tests**

Run:

```bash
python -m pytest tests/test_auth.py -q
```

Expected: `3 passed`.

- [ ] **Step 5: Commit auth client**

Run:

```bash
git add pyproject.toml src/emby_range_cache_proxy/auth.py tests/test_auth.py
git commit -m "feat: validate playback through Emby"
```

Expected: commit succeeds.

## Task 4: HTTP Origin Client

**Files:**
- Create: `src/emby_range_cache_proxy/origin.py`
- Create: `tests/test_origin.py`

- [ ] **Step 1: Write failing origin tests**

Create `tests/test_origin.py`:

```python
from aiohttp import web

from emby_range_cache_proxy.models import ByteRange
from emby_range_cache_proxy.origin import OriginClient


async def test_origin_head_reads_size_and_validators(aiohttp_client):
    async def handler(request):
        return web.Response(
            status=200,
            headers={"Content-Length": "100", "ETag": "abc", "Last-Modified": "Fri, 03 Jul 2026 00:00:00 GMT"},
        )

    app = web.Application()
    app.router.add_head("/movie.mkv", handler)
    server = await aiohttp_client(app)

    async with OriginClient() as client:
        metadata = await client.head(str(server.make_url("/movie.mkv")))

    assert metadata.size == 100
    assert metadata.etag == "abc"
    assert metadata.last_modified == "Fri, 03 Jul 2026 00:00:00 GMT"


async def test_stream_range_requests_exact_bytes(aiohttp_client):
    body = b"0123456789"

    async def handler(request):
        assert request.headers["Range"] == "bytes=2-5"
        return web.Response(status=206, body=body[2:6], headers={"Content-Range": "bytes 2-5/10"})

    app = web.Application()
    app.router.add_get("/movie.mkv", handler)
    server = await aiohttp_client(app)

    chunks = []
    async with OriginClient(chunk_bytes=2) as client:
        async for chunk in client.stream_range(str(server.make_url("/movie.mkv")), ByteRange(2, 5)):
            chunks.append(chunk)

    assert b"".join(chunks) == b"2345"
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_origin.py -q
```

Expected: FAIL with missing `emby_range_cache_proxy.origin`.

- [ ] **Step 3: Implement origin client**

Create `src/emby_range_cache_proxy/origin.py`:

```python
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
```

- [ ] **Step 4: Run origin tests**

Run:

```bash
python -m pytest tests/test_origin.py -q
```

Expected: `2 passed`.

- [ ] **Step 5: Commit origin client**

Run:

```bash
git add src/emby_range_cache_proxy/origin.py tests/test_origin.py
git commit -m "feat: add HTTP range origin client"
```

Expected: commit succeeds.

## Task 5: Head/Tail Cache Manager

**Files:**
- Create: `src/emby_range_cache_proxy/cache.py`
- Create: `tests/test_cache.py`

- [ ] **Step 1: Write failing cache tests**

Create `tests/test_cache.py`:

```python
from emby_range_cache_proxy.cache import HeadTailCache, adaptive_head_tail, cache_key
from emby_range_cache_proxy.models import ByteRange, MediaSource, SourceMetadata


def _source(size: int = 100) -> MediaSource:
    return MediaSource(
        item_id="151357",
        media_source_id="mediasource_151357",
        path="http://origin/movie.mkv",
        protocol="Http",
        size=size,
        container="mkv",
    )


def _metadata(size: int = 100) -> SourceMetadata:
    return SourceMetadata(url="http://origin/movie.mkv", size=size, etag="etag", last_modified="date")


def test_adaptive_sizes():
    assert adaptive_head_tail(1024**3) == (16 * 1024**2, 4 * 1024**2)
    assert adaptive_head_tail(4 * 1024**3) == (32 * 1024**2, 8 * 1024**2)
    assert adaptive_head_tail(12 * 1024**3) == (64 * 1024**2, 8 * 1024**2)
    assert adaptive_head_tail(80 * 1024**3) == (128 * 1024**2, 16 * 1024**2)


def test_cache_key_changes_when_size_changes():
    left = cache_key(_source(100), _metadata(100))
    right = cache_key(_source(101), _metadata(101))

    assert left != right
    assert "http://origin" not in left


def test_store_and_read_head_block(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=1024 * 1024)
    key = cache_key(_source(100), _metadata(100))

    cache.store_block(key, "head", ByteRange(0, 9), b"0123456789")

    assert cache.read_block(key, "head", ByteRange(2, 5)) == b"2345"
    assert cache.read_block(key, "head", ByteRange(10, 11)) is None


def test_evict_lru(tmp_path):
    cache = HeadTailCache(tmp_path, max_bytes=12)

    cache.store_block("a", "head", ByteRange(0, 9), b"0123456789")
    cache.store_block("b", "head", ByteRange(0, 9), b"abcdefghij")
    cache.evict_if_needed()

    remaining = sorted(path.name for path in tmp_path.glob("*/*.bin"))
    assert len(remaining) == 1
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_cache.py -q
```

Expected: FAIL with missing `emby_range_cache_proxy.cache`.

- [ ] **Step 3: Implement cache manager**

Create `src/emby_range_cache_proxy/cache.py`:

```python
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
```

- [ ] **Step 4: Run cache tests**

Run:

```bash
python -m pytest tests/test_cache.py -q
```

Expected: `4 passed`.

- [ ] **Step 5: Commit cache manager**

Run:

```bash
git add src/emby_range_cache_proxy/cache.py tests/test_cache.py
git commit -m "feat: add adaptive head tail cache"
```

Expected: commit succeeds.

## Task 6: Proxy Web App With Safe Fallback

**Files:**
- Create: `src/emby_range_cache_proxy/app.py`
- Create: `tests/test_app.py`

- [ ] **Step 1: Write failing app tests**

Create `tests/test_app.py`:

```python
from aiohttp import web

from emby_range_cache_proxy.app import create_app
from emby_range_cache_proxy.config import Config, RolloutConfig


async def test_healthz(aiohttp_client, tmp_path):
    app = create_app(Config(emby_base_url="http://emby", fallback_base_url="http://emby", cache_dir=str(tmp_path)))
    client = await aiohttp_client(app)

    response = await client.get("/healthz")

    assert response.status == 200
    assert await response.text() == "ok\n"


async def test_out_of_scope_falls_back_to_emby(aiohttp_client, tmp_path):
    async def fallback(request):
        return web.Response(status=206, body=b"emby", headers={"Content-Range": "bytes 0-3/4"})

    fallback_app = web.Application()
    fallback_app.router.add_get("/emby/videos/{item_id}/original.mkv", fallback)
    fallback_server = await aiohttp_client(fallback_app)

    app = create_app(
        Config(
            emby_base_url=str(fallback_server.make_url("")),
            fallback_base_url=str(fallback_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=False),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-3"})

    assert response.status == 206
    assert await response.read() == b"emby"


async def test_authorized_head_range_is_served_and_cached(aiohttp_client, tmp_path):
    async def playback_info(request):
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        assert request.headers["Range"] == "bytes=0-9"
        return web.Response(status=206, body=b"0123456789", headers={"Content-Range": "bytes 0-9/100"})

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_app.router.add_get("/emby/videos/{item_id}/original.mkv", lambda request: web.Response(body=b"fallback"))
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin, allow_head=False)
    origin_app.router.add_head("/movie.mkv", lambda request: web.Response(headers={"Content-Length": "100"}))
    origin_server = await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=t", headers={"Range": "bytes=0-9"})

    assert response.status == 206
    assert await response.read() == b"0123456789"
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_app.py -q
```

Expected: FAIL with missing `emby_range_cache_proxy.app`.

- [ ] **Step 3: Implement minimal app**

Create `src/emby_range_cache_proxy/app.py`:

```python
from __future__ import annotations

from aiohttp import ClientSession, web

from .auth import AuthorizationError, EmbyAuthClient
from .cache import HeadTailCache, adaptive_head_tail, cache_key
from .config import Config
from .models import ByteRange, MediaSource, SourceMetadata
from .origin import OriginClient
from .ranges import content_range_header, parse_range_header
from .requests import parse_original_request


def create_app(config: Config) -> web.Application:
    app = web.Application()
    app["config"] = config
    app["cache"] = HeadTailCache(config.cache_dir, max_bytes=config.cache.max_bytes)
    app.router.add_get("/healthz", healthz, allow_head=False)
    app.router.add_head("/healthz", healthz)
    app.router.add_route("*", "/{tail:.*}", proxy_handler)
    return app


async def healthz(request: web.Request) -> web.Response:
    return web.Response(text="ok\n")


async def proxy_handler(request: web.Request) -> web.StreamResponse:
    config: Config = request.app["config"]
    ctx = parse_original_request(request.method, request.raw_path, dict(request.headers))
    if ctx is None:
        return await fallback_to_emby(request, config)
    if not config.rollout.in_scope(item_id=ctx.item_id, media_source_id=ctx.media_source_id):
        return await fallback_to_emby(request, config)

    try:
        async with EmbyAuthClient(config.emby_base_url) as auth:
            source = await auth.authorize(ctx)
        if not config.rollout.in_scope(
            item_id=ctx.item_id,
            media_source_id=ctx.media_source_id,
            path=source.path,
        ):
            return await fallback_to_emby(request, config)
        if not source.path.startswith(("http://", "https://")):
            return await fallback_to_emby(request, config)
        return await serve_authorized_range(request, source, config)
    except AuthorizationError:
        return web.Response(status=403, text="forbidden\n")
    except Exception:
        return await fallback_to_emby(request, config)


async def serve_authorized_range(request: web.Request, source: MediaSource, config: Config) -> web.StreamResponse:
    cache: HeadTailCache = request.app["cache"]
    async with OriginClient(chunk_bytes=config.cache.chunk_bytes) as origin:
        if source.size is None:
            metadata = await origin.head(source.path)
        else:
            metadata = SourceMetadata(url=source.path, size=source.size)
        byte_range = parse_range_header(request.headers.get("Range"), size=metadata.size)
        if request.method == "HEAD":
            return _empty_range_response(byte_range, metadata.size)
        key = cache_key(source, metadata)
        cached = _read_cached_if_available(cache, key, byte_range, metadata.size)
        if cached is not None:
            return _bytes_response(cached, byte_range, metadata.size)
        return await _stream_origin_range(request, origin, cache, key, metadata.url, byte_range, metadata.size)


async def _stream_origin_range(
    request: web.Request,
    origin: OriginClient,
    cache: HeadTailCache,
    key: str,
    url: str,
    byte_range: ByteRange,
    size: int,
) -> web.StreamResponse:
    cacheable = _is_complete_head_or_tail(byte_range, size)
    captured = bytearray() if cacheable else None
    response = web.StreamResponse(
        status=206,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Range": content_range_header(byte_range, size=size),
        },
    )
    await response.prepare(request)
    bytes_written = 0
    try:
        async for chunk in origin.stream_range(url, byte_range):
            bytes_written += len(chunk)
            if captured is not None:
                captured.extend(chunk)
            await response.write(chunk)
    finally:
        await response.write_eof()
    if captured is not None and bytes_written == byte_range.length:
        _store_if_head_or_tail(cache, key, byte_range, size, bytes(captured))
        cache.evict_if_needed()
    return response


def _read_cached_if_available(cache: HeadTailCache, key: str, byte_range: ByteRange, size: int) -> bytes | None:
    head_size, tail_size = adaptive_head_tail(size)
    if byte_range.start < head_size:
        return cache.read_block(key, "head", byte_range)
    if byte_range.start >= max(0, size - tail_size):
        return cache.read_block(key, "tail", byte_range)
    return None


def _store_if_head_or_tail(cache: HeadTailCache, key: str, byte_range: ByteRange, size: int, data: bytes) -> None:
    head_size, tail_size = adaptive_head_tail(size)
    if byte_range.start == 0 and byte_range.end < head_size:
        cache.store_block(key, "head", byte_range, data)
    elif byte_range.start >= max(0, size - tail_size):
        cache.store_block(key, "tail", byte_range, data)


def _is_complete_head_or_tail(byte_range: ByteRange, size: int) -> bool:
    head_size, tail_size = adaptive_head_tail(size)
    return (byte_range.start == 0 and byte_range.end < head_size) or byte_range.start >= max(0, size - tail_size)


def _bytes_response(data: bytes, byte_range: ByteRange, size: int) -> web.Response:
    return web.Response(
        status=206,
        body=data,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(len(data)),
            "Content-Range": content_range_header(byte_range, size=size),
        },
    )


def _empty_range_response(byte_range: ByteRange, size: int) -> web.Response:
    return web.Response(
        status=206,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(byte_range.length),
            "Content-Range": content_range_header(byte_range, size=size),
        },
    )


async def fallback_to_emby(request: web.Request, config: Config) -> web.StreamResponse:
    url = f"{config.fallback_base_url}{request.raw_path}"
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    async with ClientSession() as session:
        async with session.request(request.method, url, headers=headers, allow_redirects=False) as response:
            copied_headers = {
                key: value
                for key, value in response.headers.items()
                if key.lower() not in {"transfer-encoding", "connection", "content-encoding"}
            }
            downstream = web.StreamResponse(status=response.status, headers=copied_headers)
            await downstream.prepare(request)
            async for chunk in response.content.iter_chunked(config.cache.chunk_bytes):
                if chunk:
                    await downstream.write(chunk)
            await downstream.write_eof()
            return downstream
```

- [ ] **Step 4: Run app tests**

Run:

```bash
python -m pytest tests/test_app.py -q
```

Expected: `3 passed`.

- [ ] **Step 5: Run full test suite**

Run:

```bash
python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit app integration**

Run:

```bash
git add src/emby_range_cache_proxy/app.py tests/test_app.py
git commit -m "feat: add authorized range proxy app"
```

Expected: commit succeeds.

## Task 7: Prewarm Worker

**Files:**
- Create: `src/emby_range_cache_proxy/prewarm.py`
- Create: `tests/test_prewarm.py`

- [ ] **Step 1: Write failing prewarm tests**

Create `tests/test_prewarm.py`:

```python
from aiohttp import web

from emby_range_cache_proxy.config import Config, PrewarmConfig, RolloutConfig
from emby_range_cache_proxy.prewarm import PrewarmWorker


async def test_prewarm_uses_internal_key_and_builds_head_tail(aiohttp_client, tmp_path):
    async def items(request):
        assert request.query["api_key"] == "internal"
        return web.json_response({"Items": [{"Id": "1"}]})

    async def playback_info(request):
        assert request.query["api_key"] == "internal"
        return web.json_response(
            {
                "MediaSources": [
                    {
                        "Id": "ms1",
                        "Path": str(origin_server.make_url("/movie.mkv")),
                        "Protocol": "Http",
                        "Size": 100,
                        "Container": "mkv",
                    }
                ]
            }
        )

    async def origin(request):
        range_header = request.headers["Range"]
        if range_header == "bytes=0-15":
            return web.Response(status=206, body=b"0123456789abcdef")
        if range_header == "bytes=96-99":
            return web.Response(status=206, body=b"wxyz")
        return web.Response(status=416)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin)
    origin_server = await aiohttp_client(origin_app)

    emby_app = web.Application()
    emby_app.router.add_get("/Items", items)
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    config = Config(
        emby_base_url=str(emby_server.make_url("")),
        fallback_base_url=str(emby_server.make_url("")),
        cache_dir=str(tmp_path),
        prewarm_api_key="internal",
        rollout=RolloutConfig(enabled=True, item_allowlist={"1"}, media_source_allowlist={"ms1"}),
        prewarm=PrewarmConfig(enabled=True, max_items_per_scan=1),
    )
    worker = PrewarmWorker(config)

    result = await worker.run_once()

    assert result.scanned == 1
    assert result.prewarmed == 1
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
python -m pytest tests/test_prewarm.py -q
```

Expected: FAIL with missing `emby_range_cache_proxy.prewarm`.

- [ ] **Step 3: Implement prewarm worker**

Create `src/emby_range_cache_proxy/prewarm.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

from aiohttp import ClientSession

from .cache import HeadTailCache, adaptive_head_tail, cache_key
from .config import Config
from .models import ByteRange, MediaSource, SourceMetadata
from .origin import OriginClient


@dataclass(frozen=True)
class PrewarmResult:
    scanned: int
    prewarmed: int
    skipped: int


class PrewarmWorker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.cache = HeadTailCache(config.cache_dir, max_bytes=config.cache.max_bytes)

    async def run_once(self) -> PrewarmResult:
        if not self.config.prewarm.enabled or not self.config.prewarm_api_key:
            return PrewarmResult(scanned=0, prewarmed=0, skipped=0)
        items = await self._recent_items()
        prewarmed = 0
        skipped = 0
        for item in items:
            item_id = str(item.get("Id", ""))
            if not item_id:
                skipped += 1
                continue
            sources = await self._media_sources(item_id)
            for source in sources:
                if not self.config.rollout.in_scope(
                    item_id=item_id,
                    media_source_id=source.media_source_id,
                    path=source.path,
                ):
                    skipped += 1
                    continue
                if not source.path.startswith(("http://", "https://")):
                    skipped += 1
                    continue
                await self._prewarm_source(source)
                prewarmed += 1
        return PrewarmResult(scanned=len(items), prewarmed=prewarmed, skipped=skipped)

    async def _recent_items(self) -> list[dict]:
        assert self.config.prewarm_api_key is not None
        url = f"{self.config.emby_base_url}/Items"
        params = {
            "api_key": self.config.prewarm_api_key,
            "SortBy": "DateCreated",
            "SortOrder": "Descending",
            "IncludeItemTypes": "Movie,Episode",
            "Recursive": "true",
            "Limit": str(self.config.prewarm.max_items_per_scan),
        }
        async with ClientSession() as session:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                payload = await response.json()
        return list(payload.get("Items", []))

    async def _media_sources(self, item_id: str) -> list[MediaSource]:
        assert self.config.prewarm_api_key is not None
        url = f"{self.config.emby_base_url}/Items/{item_id}/PlaybackInfo"
        async with ClientSession() as session:
            async with session.get(url, params={"api_key": self.config.prewarm_api_key}) as response:
                response.raise_for_status()
                payload = await response.json()
        sources: list[MediaSource] = []
        for raw in payload.get("MediaSources", []):
            if not raw.get("Id") or not raw.get("Path"):
                continue
            sources.append(
                MediaSource(
                    item_id=item_id,
                    media_source_id=str(raw["Id"]),
                    path=str(raw["Path"]),
                    protocol=str(raw.get("Protocol", "")),
                    size=int(raw["Size"]) if raw.get("Size") is not None else None,
                    container=raw.get("Container"),
                    bitrate=int(raw["Bitrate"]) if raw.get("Bitrate") is not None else None,
                )
            )
        return sources

    async def _prewarm_source(self, source: MediaSource) -> None:
        async with OriginClient(chunk_bytes=self.config.cache.chunk_bytes) as origin:
            metadata = SourceMetadata(url=source.path, size=source.size) if source.size else await origin.head(source.path)
            key = cache_key(source, metadata)
            head_bytes, tail_bytes = adaptive_head_tail(metadata.size)
            head_range = ByteRange(0, min(head_bytes, metadata.size) - 1)
            tail_start = max(0, metadata.size - tail_bytes)
            tail_range = ByteRange(tail_start, metadata.size - 1)
            head = await _read_range(origin, metadata.url, head_range)
            tail = await _read_range(origin, metadata.url, tail_range)
            self.cache.store_block(key, "head", head_range, head)
            self.cache.store_block(key, "tail", tail_range, tail)
            self.cache.evict_if_needed()


async def _read_range(origin: OriginClient, url: str, byte_range: ByteRange) -> bytes:
    chunks: list[bytes] = []
    async for chunk in origin.stream_range(url, byte_range):
        chunks.append(chunk)
    return b"".join(chunks)
```

- [ ] **Step 4: Run prewarm tests**

Run:

```bash
python -m pytest tests/test_prewarm.py -q
```

Expected: `1 passed`.

- [ ] **Step 5: Commit prewarm worker**

Run:

```bash
git add src/emby_range_cache_proxy/prewarm.py tests/test_prewarm.py
git commit -m "feat: add head tail prewarm worker"
```

Expected: commit succeeds.

## Task 8: CLI, Service Files, And Caddy Example

**Files:**
- Create: `src/emby_range_cache_proxy/cli.py`
- Create: `deploy/emby-range-cache-proxy.service`
- Create: `deploy/Caddyfile.range-cache.example`
- Create: `tests/test_cli.py`

- [ ] **Step 1: Write failing CLI tests**

Create `tests/test_cli.py`:

```python
from emby_range_cache_proxy.cli import build_arg_parser


def test_arg_parser_requires_config():
    parser = build_arg_parser()
    args = parser.parse_args(["--config", "/etc/emby-range-cache-proxy/config.json"])

    assert args.config == "/etc/emby-range-cache-proxy/config.json"
```

- [ ] **Step 2: Run CLI test and verify failure**

Run:

```bash
python -m pytest tests/test_cli.py -q
```

Expected: FAIL with missing `emby_range_cache_proxy.cli`.

- [ ] **Step 3: Implement CLI**

Create `src/emby_range_cache_proxy/cli.py`:

```python
from __future__ import annotations

import argparse

from aiohttp import web

from .app import create_app
from .config import load_config


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emby Range Cache Proxy")
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    config = load_config(args.config)
    web.run_app(create_app(config), host=config.listen_host, port=config.listen_port)
```

Create `deploy/emby-range-cache-proxy.service`:

```ini
[Unit]
Description=Emby Range Cache Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=emby-cache
Group=emby-cache
WorkingDirectory=/opt/emby-range-cache-proxy
ExecStart=/opt/emby-range-cache-proxy/.venv/bin/emby-range-cache-proxy --config /etc/emby-range-cache-proxy/config.json
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=/home/nax/emby/cache/range-proxy

[Install]
WantedBy=multi-user.target
```

Create `deploy/Caddyfile.range-cache.example`:

```caddy
@emby_original {
    path_regexp emby_original ^/emby/videos/[0-9]+/original\.(mkv|mp4|ts|mov|avi)$
    query MediaSourceId=*
}

handle @emby_original {
    reverse_proxy 127.0.0.1:18180 127.0.0.1:8096 {
        lb_policy first
        lb_try_duration 2s
        lb_try_interval 100ms
        fail_duration 10s
        flush_interval -1
    }
}
```

- [ ] **Step 4: Run CLI test**

Run:

```bash
python -m pytest tests/test_cli.py -q
```

Expected: `1 passed`.

- [ ] **Step 5: Validate Caddy example syntax on the test server before applying it**

Run on the target server in a temporary file:

```bash
caddy adapt --config /tmp/Caddyfile.range-cache.example --pretty >/tmp/caddy-range-cache.json
```

Expected: command exits 0. Do not replace `/etc/caddy/Caddyfile` in this task.

- [ ] **Step 6: Commit deploy files**

Run:

```bash
git add src/emby_range_cache_proxy/cli.py deploy/emby-range-cache-proxy.service deploy/Caddyfile.range-cache.example tests/test_cli.py
git commit -m "feat: add proxy CLI and deploy examples"
```

Expected: commit succeeds.

## Task 9: Hardening, Logging, And Full Verification

**Files:**
- Modify: `src/emby_range_cache_proxy/app.py`
- Modify: `src/emby_range_cache_proxy/auth.py`
- Modify: `src/emby_range_cache_proxy/origin.py`
- Modify: `src/emby_range_cache_proxy/prewarm.py`
- Create: `tests/test_security_behavior.py`
- Create: `README.md`

- [ ] **Step 1: Write security behavior tests**

Create `tests/test_security_behavior.py`:

```python
from aiohttp import web

from emby_range_cache_proxy.app import create_app
from emby_range_cache_proxy.config import Config, RolloutConfig


async def test_auth_failure_does_not_touch_origin(aiohttp_client, tmp_path):
    origin_hits = 0

    async def playback_info(request):
        return web.Response(status=403)

    async def origin(request):
        nonlocal origin_hits
        origin_hits += 1
        return web.Response(body=b"origin")

    emby_app = web.Application()
    emby_app.router.add_get("/Items/{item_id}/PlaybackInfo", playback_info)
    emby_server = await aiohttp_client(emby_app)

    origin_app = web.Application()
    origin_app.router.add_get("/movie.mkv", origin)
    await aiohttp_client(origin_app)

    app = create_app(
        Config(
            emby_base_url=str(emby_server.make_url("")),
            fallback_base_url=str(emby_server.make_url("")),
            cache_dir=str(tmp_path),
            rollout=RolloutConfig(enabled=True, item_allowlist={"1"}),
        )
    )
    client = await aiohttp_client(app)

    response = await client.get("/emby/videos/1/original.mkv?MediaSourceId=ms1&api_key=bad", headers={"Range": "bytes=0-3"})

    assert response.status == 403
    assert origin_hits == 0


def test_readme_mentions_no_middle_cache():
    text = open("README.md", encoding="utf-8").read()

    assert "does not actively cache arbitrary middle playback ranges" in text
```

- [ ] **Step 2: Run security tests and verify failure**

Run:

```bash
python -m pytest tests/test_security_behavior.py -q
```

Expected: FAIL because `README.md` does not exist.

- [ ] **Step 3: Add README**

Create `README.md`:

```markdown
# Emby Range Cache Proxy

Unified local cache proxy for Emby original-media direct-play requests.

V1 behavior:

- Validates user playback requests with the user's own Emby token.
- Accepts only HTTP/HTTPS `MediaSource` origins from Emby `PlaybackInfo`.
- Caches adaptive head/tail ranges for startup and container probing.
- Prewarms head/tail for newly added media in rollout scope using a separate internal Emby API key.
- Does not actively cache arbitrary middle playback ranges.
- Falls back to Emby for out-of-scope requests and authorized internal proxy failures.

Security boundary:

- The internal prewarm API key is not used to authorize user playback.
- Authorization failures never read origin or cache.
- Logs must redact `api_key`, `X-Emby-Token`, `PlaySessionId`, and `DeviceId`.

Local development:

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
emby-range-cache-proxy --config config.example.json
```
```

- [ ] **Step 4: Add structured redacted logging hooks**

Modify `src/emby_range_cache_proxy/app.py` imports:

```python
import logging
```

Add module logger near imports:

```python
LOGGER = logging.getLogger(__name__)
```

Inside `proxy_handler`, add concise decision logs without plaintext token:

```python
    if ctx is None:
        LOGGER.info("fallback reason=not_eligible path=%s", request.path)
        return await fallback_to_emby(request, config)
```

Before authorization failure response:

```python
    except AuthorizationError:
        LOGGER.info("deny reason=authorization_failed item_id=%s media_source_id=%s", ctx.item_id, ctx.media_source_id)
        return web.Response(status=403, text="forbidden\n")
```

Before generic fallback:

```python
    except Exception:
        LOGGER.exception("fallback reason=proxy_error item_id=%s media_source_id=%s", ctx.item_id, ctx.media_source_id)
        return await fallback_to_emby(request, config)
```

Keep logs free of `ctx.token`, raw query strings, and origin URLs with query parameters.

- [ ] **Step 5: Run full test suite**

Run:

```bash
python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 6: Run local smoke command**

Run:

```bash
python -m emby_range_cache_proxy.cli --config config.example.json
```

Expected: command starts and binds `127.0.0.1:18180`. Stop it with Ctrl-C. With rollout and prewarm disabled in `config.example.json`, `/healthz` should still return `ok`.

- [ ] **Step 7: Commit hardening and docs**

Run:

```bash
git add README.md src/emby_range_cache_proxy/app.py tests/test_security_behavior.py
git commit -m "docs: document proxy security boundary"
```

Expected: commit succeeds.

## Task 10: Test Server Grey Release Checklist

**Files:**
- Create: `docs/deploy-test-server.md`

- [ ] **Step 1: Write deployment checklist**

Create `docs/deploy-test-server.md`:

```markdown
# Test Server Grey Release Checklist

This checklist is for the test Emby server only. Do not apply it to production without a separate production review.

## Preflight

- Confirm current Caddy config backup exists.
- Confirm current per-item range-cache services are still available for rollback.
- Confirm cache-proxy config uses rollout allowlist.
- Confirm cache-proxy listens only on `127.0.0.1`.
- Confirm logs redact tokens and session identifiers.

## Service

```bash
python -m pip install -e ".[dev]"
emby-range-cache-proxy --config /etc/emby-range-cache-proxy/config.json
curl -fsS http://127.0.0.1:18180/healthz
```

Expected: `ok`.

## Caddy Validation

```bash
caddy adapt --config /etc/caddy/Caddyfile --pretty >/tmp/caddy-current.json
```

Expected: command exits 0 before and after adding the grey-release route.

## Playback Tests

- Valid token, allowlisted item, first head range: returns 206 and builds head cache.
- Valid token, allowlisted item, second head range: returns 206 and hits cache.
- Valid token, allowlisted item, tail range: returns 206 and builds tail cache.
- Valid token, allowlisted item, middle range: returns 206 and does not write middle cache.
- Invalid token: does not read cache or origin.
- Stop cache-proxy: Caddy falls back to Emby.

## Rollback

```bash
cp /etc/caddy/Caddyfile.bak-no-ip-whitelist-20260703-050143 /etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile
systemctl reload caddy
systemctl stop emby-range-cache-proxy
```
```

- [ ] **Step 2: Commit checklist**

Run:

```bash
git add docs/deploy-test-server.md
git commit -m "docs: add test server grey release checklist"
```

Expected: commit succeeds.

## Final Verification

- [ ] **Run full tests**

```bash
python -m pytest -q
```

Expected: all tests pass.

- [ ] **Inspect git history**

```bash
git log --oneline --decorate --max-count=12
```

Expected: shows one commit per task after the design and plan commits.

- [ ] **Inspect working tree**

```bash
git status --short --branch
```

Expected: clean working tree on `main`.

## Spec Coverage Review

This plan covers:

- One unified service instead of per-movie services: Tasks 1, 6, 8.
- User-token-only authorization for playback: Tasks 3, 6, 9.
- HTTP/HTTPS `PlaybackInfo` media source resolution: Tasks 3, 4, 6.
- Adaptive head/tail cache only: Tasks 5, 6, 7.
- New media prewarm with internal API key: Task 7.
- Safe fallback: Tasks 6, 8, 9.
- Grey release controls and deployment checklist: Tasks 1, 8, 10.
- Redacted observability: Tasks 2, 9.
- Verification before rollout: Tasks 6, 9, 10.
