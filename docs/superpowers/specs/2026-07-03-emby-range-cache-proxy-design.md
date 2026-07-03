# Emby Range Cache Proxy Design

Date: 2026-07-03

## Goal

Build a unified Emby Range Cache Proxy for the production Emby servers.

The first version focuses on direct-play startup speed and safe rollout:

- Replace per-movie range-cache services with one cache-proxy service.
- Keep Emby as the source of authorization truth.
- Cache only adaptive head/tail ranges for startup and container probing.
- Prewarm head/tail for newly added media in the rollout scope.
- Do not actively cache middle playback ranges in v1.
- Provide fast fallback to Emby's original proxy.
- Roll out by allowlist before enabling wider coverage.

## Current Test Server Findings

Observed host: Debian 12, Emby Docker container using host networking, Emby listening on `*:8096`, Caddy listening on `80/443`.

Important paths:

- Caddy config: `/etc/caddy/Caddyfile`
- Emby config: `/home/nax/emby/config`
- Emby DB: `/home/nax/emby/config/data/library.db`
- Existing range-cache tools: `/home/nax/emby/tools/`
- Existing cache dir: `/home/nax/emby/cache/range/`

Current test setup:

- Caddy has hardcoded matchers for selected `/emby/videos/.../original.mkv` paths.
- Each test item has a separate `emby-range-cache-*.service`.
- Existing scripts cache head and tail; the newer resume script can also build per-item segment cache.
- Current scripts do not validate Emby permissions.
- Current logs include full request lines and can expose `api_key`, `PlaySessionId`, and `DeviceId`.

Existing scripts:

- `single_range_cache.py`: one file, one service, head/tail cache only.
- `single_range_cache_resume.py`: one file, one service, head/tail plus per-item segment cache.

The production design should not continue the per-movie service model.

## Architecture

The proxy runs as one local service, for example:

```text
127.0.0.1:18180
```

Request flow:

```text
Client
  -> Caddy
    -> cache-proxy
      -> Emby PlaybackInfo authorization
      -> HTTP/HTTPS Range origin
      -> adaptive head/tail cache
```

Core components:

- Caddy route: forwards only eligible Emby original media requests to cache-proxy.
- Cache proxy: handles authorization, media resolution, Range serving, cache lookup, and fallback.
- Auth validator: validates each playback request using the user's original Emby token.
- Media resolver: selects the exact requested `MediaSourceId` from Emby `PlaybackInfo`.
- Range fetcher: reads HTTP/HTTPS origin with Range requests.
- Cache manager: stores adaptive head/tail cache blocks and evicts by LRU when capacity is reached.
- Prewarm worker: uses an internal Emby API key to discover new media and prewarm head/tail only.
- Fallback handler: returns to Emby's original proxy for authorized requests when cache-proxy cannot safely serve.

V1 intentionally does not include:

- Active middle-range caching.
- Idle prefetch after playback stops.
- CloudFS/local-file path mapping.
- Full-library prewarm.
- Admin-token based user request authorization.

## Request Eligibility

The proxy only considers requests shaped like Emby direct-play original media requests:

```text
/emby/videos/{ItemId}/original.{ext}
```

Initial supported methods:

- `GET`
- `HEAD`

Required request data:

- `ItemId` from the path.
- `MediaSourceId` from query string.
- User token from `api_key` query param or `X-Emby-Token` header.

Useful but not authorization-critical:

- `PlaySessionId`
- `DeviceId`
- `UserId`, if visible in validated Emby response/session data

Requests outside rollout scope are proxied back to Emby without reading origin or writing cache.

## Authorization

User playback requests are authorized only with the user's original token.

Authorization flow:

1. Extract `ItemId`, `MediaSourceId`, and user token.
2. Call Emby on localhost:

   ```text
   http://127.0.0.1:8096/Items/{ItemId}/PlaybackInfo?MediaSourceId={MediaSourceId}
   ```

3. Use the same user token from the client request.
4. Continue only when Emby returns HTTP 200.
5. Find an exact `MediaSources[].Id == MediaSourceId` match.
6. Use only that matched media source.

The proxy must not:

- Use an internal or admin token to authorize a user's playback request.
- Infer permission from file paths, source URLs, or cached files.
- Read cache or origin when authorization fails.

Authorization cache:

- Key: `sha256(user_token) + ItemId + MediaSourceId`
- TTL: 60-180 seconds
- Stores only allow result and selected media-source metadata.
- Does not store or log the plaintext token.
- Failed authorization is not cached, or is cached only for a very short negative TTL.

## Media Source Resolution

V1 resolves source from Emby `PlaybackInfo`.

From the matching media source, read:

- `Id`
- `Path`
- `Protocol`
- `Size`
- `Container`
- `Bitrate`

V1 accepts only:

- `http://...`
- `https://...`

V1 rejects:

- Non-HTTP sources.
- Empty source path.
- Sources pointing back to cache-proxy itself.
- Media source size that cannot be obtained from Emby metadata or origin HEAD.
- Mismatched `MediaSourceId`.

`.strm` and CloudFS local-path mapping can be added later, but the first production version prioritizes HTTP Range stability and runtime consistency.

## Cache Model

V1 caches only head and tail ranges.

Head/tail cache is used for:

- Startup reads from the beginning of a file.
- Container metadata probing.
- Tail reads from players or containers that inspect end-of-file data.

V1 does not actively cache arbitrary middle playback ranges.

Adaptive cache sizing:

```text
file size < 2GiB       head 16MiB    tail 4MiB
2GiB - 8GiB            head 32MiB    tail 8MiB
8GiB - 30GiB           head 64MiB    tail 8MiB
file size > 30GiB      head 128MiB   tail 16MiB
```

Head/tail lifecycle:

- No TTL by default because media files are effectively immutable.
- Evicted only under capacity pressure by LRU.
- Cache entries are invalidated by key changes when source identity or source metadata changes.

Cache key should include:

- `MediaSourceId`
- Origin URL hash
- `Content-Length`
- `ETag`, if provided
- `Last-Modified`, if provided

Cache file names should be hash-based and must not expose media names, source URLs, tokens, or user IDs.

## Cache Serving Behavior

For a requested range:

- If it falls fully inside a valid head/tail cache block, serve from cache.
- If it partially overlaps head/tail cache, serve the cached portion from cache and the rest from origin.
- If it is outside head/tail, proxy from origin without writing middle cache in v1.

Cache writing:

- When a client request overlaps head/tail and cache is missing, the proxy can stream from origin to client while also building the cache block.
- Writes use a temporary file and atomic rename after full-size validation.
- A per-cache-entry build lock prevents duplicate builders.
- Concurrent requests may either wait briefly for the builder or stream directly from origin.
- Corrupt or short cache files are deleted and rebuilt.

Range behavior:

- Preserve `Accept-Ranges: bytes`.
- Return correct `Content-Range` and `Content-Length`.
- Clamp open-ended ranges conservatively.
- Avoid expanding client requests into large unexpected origin reads.
- Handle client disconnects without noisy tracebacks.

## Prewarm

Newly added media in rollout scope should be prewarmed.

Prewarm scope:

- Do not prewarm the full library.
- Do not prewarm every middle segment.
- Do not require high bitrate as a condition.
- Prewarm newly added eligible media with adaptive head/tail only.

Prewarm data source:

- Use an internal Emby API key for the prewarm worker.
- The internal key is not used for user request authorization.
- The internal key is not used as fallback for failed user authorization.

Prewarm worker behavior:

- Periodically query recently added media through Emby API.
- Resolve media sources through Emby API.
- Enqueue only eligible HTTP/HTTPS sources.
- Deduplicate tasks by cache key.
- Prewarm adaptive head and tail blocks.
- Run as a low-priority background worker.
- Configurable concurrency, initially 1.
- Configurable bandwidth limit.
- Optional low-traffic schedule window.
- Drop or deprioritize stale queued tasks if backlog grows.

Prewarm safety:

- Never log the internal API key.
- Store config with restrictive permissions, preferably `0600`.
- Run under a dedicated service user when practical.
- Expose no public prewarm control endpoint in v1.

## Fallback

Fallback must preserve the security boundary.

Rules:

- Authorization failure: do not read cache, do not read origin, do not use internal prewarm key.
- Authorization validator error or timeout: proxy the original request to Emby, but do not read cache or origin.
- Request not in rollout scope: proxy to Emby original endpoint.
- Authorized request but cache-proxy cannot serve due to internal error: proxy to Emby original endpoint.
- Cache-proxy service unavailable: Caddy should quickly fall back to Emby.

Caddy fallback pattern, subject to final config validation:

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

The proxy should also support an explicit bypass mode, so operators can disable cache serving without editing many Caddy matchers.

## Rollout Controls

V1 should start with a small grey release.

Supported rollout controls:

- Item allowlist.
- MediaSourceId allowlist.
- Library or path prefix allowlist, evaluated after Emby authorization.
- Optional user allowlist, evaluated after Emby authorization or session lookup.
- Global enable/disable flag.
- Separate enable flags for request-time cache serving and prewarm.

Recommended rollout order:

1. Deploy cache-proxy service disabled for traffic.
2. Run local health checks and curl tests.
3. Enable proxy for one or two known test items.
4. Verify authorization, cache hits, fallback, and playback behavior.
5. Enable prewarm only for the same rollout scope.
6. Expand by allowlist after observing logs and cache hit rate.

## Capacity

Production has enough disk to use a larger cache, but v1 should still start with explicit limits.

Recommended initial limits:

- Head/tail cache pool: start at 512GiB to 1TiB if production disk is comfortable.
- Prewarm concurrency: 1.
- Request-time origin concurrency: bounded globally and per origin host.
- Bandwidth limit: configurable, with separate limits for prewarm and foreground requests.

Because head/tail entries have no TTL, capacity pressure must be handled by LRU. Recently played or recently prewarmed entries stay warm; old entries are evicted when the pool is full.

## Observability

Logs must be useful without leaking credentials.

Log fields:

- request id
- item id
- media source id
- method
- range
- cache decision: hit, miss, partial, bypass, fallback
- served bytes
- cache bytes
- origin bytes
- elapsed time
- fallback reason
- error class

Sensitive fields must be redacted:

- `api_key`
- `X-Emby-Token`
- `PlaySessionId`
- `DeviceId`
- internal prewarm API key
- full origin URL query strings if they may contain tokens

Useful counters:

- auth success/failure
- cache hit/miss/partial
- fallback count by reason
- prewarm queued/running/succeeded/failed
- origin errors by host
- cache bytes by pool
- LRU evictions

## Verification Plan

Before Caddy integration:

- Unit test Range parsing and clamping.
- Unit test adaptive cache sizing.
- Unit test cache key generation.
- Unit test token redaction.
- Unit test fallback decisions.
- Unit test media-source selection by exact `MediaSourceId`.
- Integration test with a local HTTP origin that supports Range.

On the test server:

- Start cache-proxy on localhost only.
- Verify `/healthz`.
- Use curl with valid and invalid user tokens.
- Verify unauthorized requests never read origin/cache.
- Verify first head request builds cache.
- Verify second head request hits cache.
- Verify tail request builds and hits tail cache.
- Verify non-head/tail middle request proxies without middle cache writes.
- Stop cache-proxy and verify Caddy falls back to Emby.
- Break origin and verify authorized fallback to Emby.
- Confirm logs redact tokens and session identifiers.

Grey release:

- Enable one known large MKV.
- Test first play, second play, seek, resume, and stop/start.
- Enable one smaller file to verify adaptive sizing.
- Enable prewarm for the same scope.
- Observe cache growth and CloudFS/OpenList pressure.

## Open Implementation Choices

These are not design blockers:

- Python async service versus Go binary.
- Exact config file format.
- Whether to expose Prometheus metrics in v1 or start with structured logs.
- Whether Caddy should check only `MediaSourceId` or also a token-looking query/header before routing.

The implementation should keep these choices isolated so they can change without rewriting the authorization or cache model.
