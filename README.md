# Emby Range Cache Proxy

Unified local cache proxy for Emby original media direct-play requests.

## V1 Behavior

- Caddy forwards eligible `/emby/videos/.../original.*` requests to this proxy during a controlled rollout.
- User playback requests are authorized with the user's own Emby token by calling Emby `PlaybackInfo`.
- The internal prewarm key is only used by the prewarm worker. It is not used to authorize user playback requests.
- The proxy accepts HTTP and HTTPS media source paths returned by Emby after authorization.
- `.strm` media source paths can be resolved through configured path mappings such as `/strm/` to `/home/nax/emby/strm`, then cached from the HTTP URL inside the `.strm` file when that URL also matches `rollout.path_prefix_allowlist`.
- The cache stores adaptive head and tail ranges for startup, probing, and container metadata reads.
- The proxy does not actively cache arbitrary middle playback ranges.
- Out-of-scope requests fall back to the normal Emby proxy path.

## Security Boundary

- Explicit Emby authorization failures return `403` and do not read origin, cache, or fallback.
- Logs must not include user tokens, `api_key`, `X-Emby-Token`, `PlaySessionId`, `DeviceId`, origin URLs, or raw query strings.
- Cache entries are shared by media source and origin metadata, not by user. Authorization remains per request.
- Local `.strm` reads are limited to configured path mappings, and resolved `.strm` URLs are limited by `rollout.path_prefix_allowlist`.
- The prewarm API key only discovers and warms rollout-scoped media. It does not replace user token checks.

## Caddy Fallback Boundary

The deployment example uses Caddy upstream fallback so Emby can receive the request quickly if the cache proxy cannot be connected to or is unavailable before a proxy response exists.

That boundary is limited. If the cache proxy has already returned `403` or `5xx`, or if it has already started a response and the downstream stream later breaks, Caddy will not transparently replay that same client response through Emby. The proxy keeps pre-response internal proxy errors eligible for fallback and avoids falling back after it has begun streaming proxy data.

## Cache Scope

V1 keeps the cache intentionally narrow:

- Adaptive head cache for startup and initial probe reads.
- Adaptive tail cache for end-of-file metadata reads.
- No active arbitrary middle range cache during normal playback.
- Prewarm builds the same head and tail blocks for rollout-scoped media.

## Local Development

```bash
python3 -m pip install -e ".[dev]"
python3 -m pytest -q
python3 -m emby_range_cache_proxy.cli --config config.example.json
curl -fsS http://127.0.0.1:18180/healthz
```

The console entry point is also available after editable install:

```bash
emby-range-cache-proxy --config config.example.json
```
