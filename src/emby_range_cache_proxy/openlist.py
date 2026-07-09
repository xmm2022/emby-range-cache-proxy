from __future__ import annotations

from dataclasses import replace
from pathlib import PurePosixPath
from urllib.parse import quote, unquote, urlencode, urlsplit

from aiohttp import ClientError, ClientSession, ClientTimeout, ContentTypeError

from .config import OpenListConfig
from .models import MediaSource


class OpenListError(Exception):
    pass


async def resolve_openlist_media_source(source: MediaSource, config: OpenListConfig) -> MediaSource:
    if not config.enabled:
        return source
    openlist_path = openlist_path_from_source(source.path, config.base_url)
    if openlist_path is None:
        return source
    try:
        async with OpenListClient(config) as client:
            resolved_url = await client.resolve_file_url(openlist_path)
    except OpenListError:
        return source
    return replace(source, path=resolved_url, protocol="Http")


class OpenListClient:
    def __init__(self, config: OpenListConfig) -> None:
        self.config = config
        self._session: ClientSession | None = None

    async def __aenter__(self) -> "OpenListClient":
        self._session = ClientSession(timeout=ClientTimeout(total=self.config.timeout_seconds))
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            finally:
                self._session = None

    async def resolve_file_url(self, path: str) -> str:
        if self._session is None:
            raise RuntimeError("OpenListClient must be used as an async context manager")
        headers = {"Content-Type": "application/json"}
        if self.config.token:
            headers["Authorization"] = self.config.token
        endpoint = f"{self.config.base_url}/api/fs/get"
        try:
            async with self._session.post(
                endpoint,
                json={"path": path, "password": self.config.password},
                headers=headers,
            ) as response:
                if response.status >= 400:
                    raise OpenListError(f"openlist fs/get failed: status={response.status}")
                try:
                    payload = await response.json()
                except (ContentTypeError, ValueError):
                    raise OpenListError("invalid openlist fs/get response") from None
        except OpenListError:
            raise
        except (ClientError, TimeoutError, OSError):
            raise OpenListError("openlist fs/get failed") from None

        data = _response_data(payload)
        if data.get("is_dir") is True:
            raise OpenListError("openlist path is a directory")
        sign = _optional_string(data.get("sign"))
        if sign:
            return _signed_download_url(self.config.base_url, path, sign)
        raw_url = _optional_string(data.get("raw_url"))
        if raw_url:
            return _absolute_openlist_url(self.config.base_url, raw_url)
        raise OpenListError("openlist fs/get response has no downloadable URL")


def openlist_path_from_source(value: str, base_url: str) -> str | None:
    parsed = urlsplit(value)
    if parsed.scheme.lower() == "openlist":
        path = f"/{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
        return _normalize_openlist_path(path)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None

    base = urlsplit(base_url)
    if parsed.scheme.lower() != base.scheme.lower() or parsed.netloc.lower() != base.netloc.lower():
        return None
    base_path = base.path.rstrip("/")
    if base_path:
        if parsed.path != base_path and not parsed.path.startswith(f"{base_path}/"):
            return None
        relative = parsed.path[len(base_path) :]
    else:
        relative = parsed.path
    for prefix in ("/d", "/p"):
        if relative.startswith(f"{prefix}/"):
            return _normalize_openlist_path(relative[len(prefix) :])
    return None


def is_openlist_source(value: str) -> bool:
    return urlsplit(value).scheme.lower() == "openlist"


def _response_data(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise OpenListError("invalid openlist fs/get response")
    code = payload.get("code")
    if code not in {None, 200}:
        raise OpenListError("openlist fs/get returned non-success code")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise OpenListError("invalid openlist fs/get response")
    return data


def _normalize_openlist_path(value: str) -> str | None:
    if not value:
        return None
    path = unquote(value if value.startswith("/") else f"/{value}")
    parts = PurePosixPath(path).parts
    if len(parts) <= 1 or any(part in {"", ".", ".."} for part in parts):
        return None
    return "/" + "/".join(parts[1:])


def _signed_download_url(base_url: str, path: str, sign: str) -> str:
    encoded_path = quote(path, safe="/")
    return f"{base_url}/d{encoded_path}?{urlencode({'sign': sign})}"


def _absolute_openlist_url(base_url: str, raw_url: str) -> str:
    if raw_url.startswith(("http://", "https://")):
        return raw_url
    if raw_url.startswith("/"):
        return f"{base_url}{raw_url}"
    return f"{base_url}/{raw_url}"


def _optional_string(value: object) -> str:
    if value is None:
        return ""
    return str(value)
