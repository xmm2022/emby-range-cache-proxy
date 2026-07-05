# Emby Range Cache Proxy

Unified local cache proxy for Emby original media direct-play requests.

## Usage

The Go implementation under `go/` is the recommended runtime for production. It
uses the same config file, cache directory, and SQLite state layout as the Python
implementation, so the Python service can remain available for rollback.

### 1. Prepare the config

Copy `config.example.json` to the host config path, usually
`/etc/emby-range-cache-proxy/config.json`, then set at least:

```json
{
  "emby_base_url": "http://127.0.0.1:8096",
  "fallback_base_url": "http://127.0.0.1:8096",
  "listen_host": "127.0.0.1",
  "listen_port": 18180,
  "cache_dir": "/home/nax/emby/cache/range-proxy",
  "prewarm_api_key": "replace-with-a-long-random-secret",
  "rollout": {
    "enabled": true,
    "item_allowlist": ["10535"],
    "media_source_allowlist": ["mediasource_10535"],
    "path_prefix_allowlist": ["http://127.0.0.1:18096/"]
  }
}
```

Keep `listen_host` on `127.0.0.1` unless the service is protected by a trusted
local firewall and reverse proxy rules. `path_prefix_allowlist` values should be
full URL prefixes with a trailing slash, for example `http://127.0.0.1:18096/`,
so adjacent hostnames or ports are not accidentally included.

### 2. Build and test the Go binary

```bash
cd /opt/emby-range-cache-proxy/go
go test ./...
go build -o bin/emby-range-cache-proxy ./cmd/emby-range-cache-proxy
```

Run it on an unused port first if Python is still serving `18180`:

```bash
cp /etc/emby-range-cache-proxy/config.json /tmp/range-cache-go.json
# edit /tmp/range-cache-go.json and set listen_port to an unused local port
./bin/emby-range-cache-proxy --config /tmp/range-cache-go.json
curl -fsS http://127.0.0.1:<port>/healthz
curl -fsS http://127.0.0.1:<port>/internal/stats
```

### 3. Install the service

The Go systemd unit is provided at
`go/deploy/emby-range-cache-proxy-go.service`.

```bash
install -m 0644 go/deploy/emby-range-cache-proxy-go.service /etc/systemd/system/emby-range-cache-proxy.service
systemctl daemon-reload
systemctl restart emby-range-cache-proxy.service
systemctl status emby-range-cache-proxy.service --no-pager
curl -fsS http://127.0.0.1:18180/healthz
curl -fsS http://127.0.0.1:18180/internal/stats
```

Keep the previous Python unit before cutover if rollback is needed:

```bash
systemctl cat emby-range-cache-proxy.service > /root/emby-range-cache-proxy-python-unit.backup
```

### 4. Route only rollout traffic through the proxy

Use Caddy to send only selected original media requests to the range cache proxy,
with Emby as the fallback upstream:

```caddyfile
@unified_range_proxy_10535 {
	path /emby/videos/10535/original.mkv
	query MediaSourceId=mediasource_10535
}

handle @unified_range_proxy_10535 {
	reverse_proxy 127.0.0.1:18180 127.0.0.1:8096 {
		lb_policy first
		lb_try_duration 2s
		lb_try_interval 100ms
		fail_duration 10s
		flush_interval -1
	}
}

handle {
	reverse_proxy 127.0.0.1:8096 {
		flush_interval -1
	}
}
```

Do not expose `/internal/prewarm` through the public reverse proxy. It is for
loopback callers such as MediaInfoKeeper.

### 5. Trigger a prewarm

Event-driven prewarm does not require `prewarm.enabled=true`; it only requires
`prewarm_api_key`.

```bash
curl -fsS -X POST http://127.0.0.1:18180/internal/prewarm \
  -H 'Content-Type: application/json' \
  -H "X-Range-Cache-Prewarm-Key: ${RANGE_CACHE_PREWARM_KEY}" \
  --data '{"itemId":"10535","mediaSourceId":"mediasource_10535"}'
```

Expected responses:

- `{"status":"queued",...}` when a new task is accepted.
- `{"status":"existing",...}` when the same item/source is already queued or running.

### 6. Roll back to Python

The Go service writes the same cache files and SQLite schema as Python. To roll
back, restore the Python systemd unit and restart:

```bash
cp /root/emby-range-cache-proxy-python-unit.backup /etc/systemd/system/emby-range-cache-proxy.service
systemctl daemon-reload
systemctl restart emby-range-cache-proxy.service
curl -fsS http://127.0.0.1:18180/healthz
```

## V1 Behavior

- Caddy forwards eligible `/emby/videos/.../original.*` requests to this proxy during a controlled rollout.
- User playback requests are authorized with the user's own Emby token by calling Emby `PlaybackInfo`.
- The internal prewarm key is only used by the prewarm worker. It is not used to authorize user playback requests.
- The proxy accepts HTTP and HTTPS media source paths returned by Emby after authorization.
- `.strm` media source paths can be resolved through configured path mappings such as `/strm/` to `/home/nax/emby/strm`, then cached from the HTTP URL inside the `.strm` file when that URL also matches `rollout.path_prefix_allowlist`.
- `.strm` support is not tied to a hard-coded port. The current test server allowlists `http://127.0.0.1:18096/` as its local `.strm` origin; `.strm` files pointing elsewhere fall back to Emby unless that origin prefix is explicitly allowlisted.
- The cache stores adaptive head and tail ranges for startup, probing, and container metadata reads.
- The proxy does not actively cache arbitrary middle playback ranges.
- `POST /internal/prewarm` accepts `itemId` and `mediaSourceId`, then performs the same Emby `PlaybackInfo`, `.strm`, and rollout allowlist checks before warming only head and tail blocks.
- Out-of-scope requests fall back to the normal Emby proxy path.

## Security Boundary

- Explicit Emby authorization failures return `403` and do not read origin, cache, or fallback.
- Logs must not include user tokens, `api_key`, `X-Emby-Token`, `PlaySessionId`, `DeviceId`, origin URLs, or raw query strings.
- Cache entries are shared by media source and origin metadata, not by user. Authorization remains per request.
- Local `.strm` reads are limited to configured path mappings, and resolved `.strm` URLs are limited by `rollout.path_prefix_allowlist`.
- The prewarm API key only discovers and warms rollout-scoped media. It does not replace user token checks.

## Internal Prewarm Endpoint

`POST /internal/prewarm` is intended for loopback-only callers such as MediaInfoKeeper after media information extraction succeeds. Authenticate with `X-Range-Cache-Prewarm-Key: <prewarm_api_key>` or `Authorization: Bearer <prewarm_api_key>`.

Request body:

```json
{"itemId":"12345","mediaSourceId":"mediasource_12345"}
```

The endpoint returns `202` with `queued` for a new in-process prewarm task and `existing` when the same item/source is already queued or running. The task queries Emby PlaybackInfo with the internal key, resolves mapped `.strm` files only when the resolved URL matches `rollout.path_prefix_allowlist`, skips already-complete head/tail cache blocks, uses `prewarm.concurrency` for in-process concurrency, throttles downloads with `prefetch.bandwidth_bytes_per_second`, and evicts through the head/tail cache capacity policy.

`prewarm.enabled` only controls the periodic recent-item scanner. Event-triggered prewarm through `/internal/prewarm` requires `prewarm_api_key` but does not require enabling periodic scans.

## Caddy Fallback Boundary

The deployment example uses Caddy upstream fallback so Emby can receive the request quickly if the cache proxy cannot be connected to or is unavailable before a proxy response exists.

That boundary is limited. If the cache proxy has already returned `403` or `5xx`, or if it has already started a response and the downstream stream later breaks, Caddy will not transparently replay that same client response through Emby. The proxy keeps pre-response internal proxy errors eligible for fallback and avoids falling back after it has begun streaming proxy data.

## Cache Scope

V1 keeps the cache intentionally narrow:

- Adaptive head cache for startup and initial probe reads.
- Adaptive tail cache for end-of-file metadata reads.
- No active arbitrary middle range cache during normal playback.
- Prewarm builds the same head and tail blocks for rollout-scoped media.

## Phase 2

Phase 2 adds disabled-by-default playback session recording and idle/stop-driven middle-range prefetch.

Safe defaults:

- `session.enabled=false`
- `middle_cache.enabled=false`
- `prefetch.enabled=false`
- `prefetch.poll_interval_seconds=5`
- `prefetch.error_backoff_seconds=300`

The prefetch worker polls an empty queue with `prefetch.poll_interval_seconds` so newly idle sessions are picked up quickly. Fetch or probe failures still use `prefetch.error_backoff_seconds` for retry/backoff.

The internal API key is not used for user playback authorization. User playback requests continue to be authorized with the user's own Emby token through `PlaybackInfo`. The internal key is only for read-only session observation and background work when those features are explicitly enabled.

Recommended rollout order:

1. Deploy code with Phase 2 disabled.
2. Enable `session.enabled=true` for logging and state observation.
3. Enable `session.observer_enabled=true` after configuring the internal key.
4. Enable `middle_cache.enabled=true` with `prefetch.enabled=false`.
5. Enable `prefetch.enabled=true` for one or two allowlisted items.

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
