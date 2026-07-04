# Test Server Grey Release Checklist

This checklist is only for the test Emby server at `a.inemby.pp.ua`. Do not use it as a production runbook without a separate production review, capacity plan, and rollback drill.

The goal is a small grey release: keep the existing per-item range-cache services available, route only allowlisted original media requests through the unified proxy, and verify that fallback remains quick.

## Preflight

- Confirm the current Caddy backup exists:

```bash
test -f /etc/caddy/Caddyfile.bak-no-ip-whitelist-20260703-050143
```

- Confirm the existing per-item test services are still present and can be used for rollback or comparison:

```bash
systemctl status emby-range-cache-151357.service emby-range-cache-164958.service emby-range-cache-151355.service emby-range-cache-10535.service emby-range-cache-159769.service emby-range-cache-151358.service
```

- Confirm the proxy config enables rollout allowlists before Caddy routes traffic to it. For the first test, `item_allowlist` or `media_source_allowlist` must be narrow. Do not use `path_prefix_allowlist` as the only first-stage grey-release boundary because path checks happen after Emby authorization.

```bash
jq '.rollout' /etc/emby-range-cache-proxy/config.json
```

- Confirm the proxy listens only on loopback:

```bash
jq -r '.listen_host' /etc/emby-range-cache-proxy/config.json
```

Expected: `127.0.0.1`.

- If Emby stores media as `.strm` files, confirm container paths are mapped to host paths. On the current test server, Emby mounts `/home/nax/emby/strm` as `/strm`, so the proxy config should include:

```json
"path_mappings": [
  {
    "from": "/strm/",
    "to": "/home/nax/emby/strm"
  }
]
```

- The resolved URL inside the `.strm` file must also be allowlisted. On the current test server those URLs are served locally by `127.0.0.1:18096`, so keep rollout narrow and include:

```json
"rollout": {
  "enabled": true,
  "item_allowlist": ["10535"],
  "media_source_allowlist": ["mediasource_10535"],
  "path_prefix_allowlist": ["http://127.0.0.1:18096/"]
}
```

This means the test deployment caches only `.strm` entries whose resolved URL starts with `http://127.0.0.1:18096/`. Other `.strm` URLs are not cached by the range proxy and should fall back to the normal Emby path. Port `18096` is a test-server origin convention, not a code requirement; add another trusted local origin prefix only after confirming reachability, authorization scope, and resource limits.

- Confirm logs are token-safe before enabling traffic. Proxy decision logs must not include raw query strings, `api_key`, `X-Emby-Token`, `PlaySessionId`, `DeviceId`, origin URLs, or internal prewarm keys. The service disables the default aiohttp access log.
- Keep the config readable only by root and the service group because it may contain `prewarm_api_key`. The service runs as `emby-cache`, so group read is required:

```bash
chown root:emby-cache /etc/emby-range-cache-proxy/config.json
chmod 0640 /etc/emby-range-cache-proxy/config.json
```

- Confirm the cache directory owner matches the service account:

```bash
install -d -o emby-cache -g emby-cache -m 0750 /home/nax/emby/cache/range-proxy
```

## Service

Install from this repository on the test server:

```bash
cd /opt/emby-range-cache-proxy
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -U pip
python3 -m pip install .
```

Create the service user and required directories:

```bash
useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin emby-cache || true
install -d -o root -g emby-cache -m 0750 /etc/emby-range-cache-proxy
install -d -o emby-cache -g emby-cache -m 0750 /home/nax/emby/cache/range-proxy
```

For an interactive smoke test, run the proxy in the background and stop it on exit:

```bash
(
set -eu
/opt/emby-range-cache-proxy/.venv/bin/emby-range-cache-proxy --config /etc/emby-range-cache-proxy/config.json &
pid=$!
trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true' EXIT
curl -fsS http://127.0.0.1:18180/healthz
)
```

Expected: `ok`.

For systemd, use `deploy/emby-range-cache-proxy.service` as the example unit. Review paths first, then install and start it:

```bash
install -m 0644 deploy/emby-range-cache-proxy.service /etc/systemd/system/emby-range-cache-proxy.service
systemctl daemon-reload
systemctl enable --now emby-range-cache-proxy.service
systemctl status emby-range-cache-proxy.service
curl -fsS http://127.0.0.1:18180/healthz
```

## Caddy Validation

Do not directly overwrite `/etc/caddy/Caddyfile` during validation. Validate either a temporary candidate file or the current file before and after a controlled edit.

Validate the current live config first:

```bash
caddy adapt --config /etc/caddy/Caddyfile --pretty >/tmp/caddy-current.json
caddy validate --config /etc/caddy/Caddyfile
```

Build a temporary candidate from the current config and the grey-release route. Use `deploy/Caddyfile.range-cache.example` only as a reference; adapt it to the current test-server site block.

```bash
cp /etc/caddy/Caddyfile /tmp/Caddyfile.range-cache-candidate
vi /tmp/Caddyfile.range-cache-candidate
caddy adapt --config /tmp/Caddyfile.range-cache-candidate --pretty >/tmp/caddy-range-cache-candidate.json
caddy validate --config /tmp/Caddyfile.range-cache-candidate
```

Before editing the live Caddyfile, create a deployment-specific rollback backup. Only after the candidate validates should the same minimal route be applied to `/etc/caddy/Caddyfile`. Validate again before reload:

```bash
backup="/etc/caddy/Caddyfile.bak-range-proxy-$(date +%Y%m%d-%H%M%S)"
cp /etc/caddy/Caddyfile "$backup"
printf '%s\n' "$backup" >/tmp/emby-range-cache-proxy-caddy-backup
printf 'backup=%s\n' "$backup"
caddy adapt --config /etc/caddy/Caddyfile --pretty >/tmp/caddy-after-range-cache.json
caddy validate --config /etc/caddy/Caddyfile
systemctl reload caddy
```

Caddy fallback is limited to cache-proxy connection failure or upstream unavailability before the proxy has produced a response. It does not promise transparent fallback after the proxy has returned `403` or `5xx`, and it cannot replay a response that already started streaming and then broke.

## Playback Tests

Use one rollout-allowlisted item and one non-allowlisted item. Keep real user tokens out of docs and shell history where possible.

Set local shell variables for the test session. Read the token without storing it in shell history, and prefer `X-Emby-Token` over query `api_key` in manual curl commands. Command-line arguments and environment variables can still be visible to local administrators while the command runs.

```bash
BASE='https://a.inemby.pp.ua'
ITEM_ID='<allowlisted-item-id>'
MEDIA_SOURCE_ID='<allowlisted-media-source-id>'
read -rsp 'TOKEN: ' TOKEN
printf '\n'
```

Valid token, head miss then cache build:

```bash
curl -sS -D /tmp/head-miss.headers -o /tmp/head-miss.bin \
  -H 'Range: bytes=0-1048575' \
  -H "X-Emby-Token: $TOKEN" \
  "$BASE/emby/videos/$ITEM_ID/original.mkv?MediaSourceId=$MEDIA_SOURCE_ID"
```

Expected: `206 Partial Content`; cache files should appear under `/home/nax/emby/cache/range-proxy`.

Valid token, same head range hit:

```bash
curl -sS -D /tmp/head-hit.headers -o /tmp/head-hit.bin \
  -H 'Range: bytes=0-1048575' \
  -H "X-Emby-Token: $TOKEN" \
  "$BASE/emby/videos/$ITEM_ID/original.mkv?MediaSourceId=$MEDIA_SOURCE_ID"
```

Expected: `206 Partial Content`; response body matches the first head request. Use file count, file size, and unchanged cache file mtimes as the cache evidence because V1 does not emit hit/miss logs.

Valid token, tail range:

```bash
SIZE='<media-size-bytes>'
START=$((SIZE - 1048576))
curl -sS -D /tmp/tail.headers -o /tmp/tail.bin \
  -H "Range: bytes=$START-" \
  -H "X-Emby-Token: $TOKEN" \
  "$BASE/emby/videos/$ITEM_ID/original.mkv?MediaSourceId=$MEDIA_SOURCE_ID"
```

Expected: `206 Partial Content`; tail cache entry is built or reused.

Valid token, middle range does not create middle cache:

```bash
MID=$((SIZE / 2))
END=$((MID + 1048575))
before=$(find /home/nax/emby/cache/range-proxy -type f | wc -l)
curl -sS -D /tmp/middle.headers -o /tmp/middle.bin \
  -H "Range: bytes=$MID-$END" \
  -H "X-Emby-Token: $TOKEN" \
  "$BASE/emby/videos/$ITEM_ID/original.mkv?MediaSourceId=$MEDIA_SOURCE_ID"
after=$(find /home/nax/emby/cache/range-proxy -type f | wc -l)
printf 'before=%s after=%s\n' "$before" "$after"
```

Expected: `206 Partial Content`; no new arbitrary middle cache entry is written. A small count change from unrelated concurrent head/tail prewarm should be investigated before continuing.

Invalid token:

```bash
curl -sS -D /tmp/invalid-token.headers -o /tmp/invalid-token.bin \
  -H 'Range: bytes=0-1048575' \
  -H 'X-Emby-Token: invalid-test-token' \
  "$BASE/emby/videos/$ITEM_ID/original.mkv?MediaSourceId=$MEDIA_SOURCE_ID"
```

Expected: `403`; proxy must not read origin, cache, or fallback for explicit authorization failure.

Auth unavailable fallback:

- Temporarily point the proxy's `emby_base_url` to an unavailable local port in a temporary config or controlled test unit.
- Keep `fallback_base_url` pointed at the real Emby listener.
- Request an allowlisted item with a valid token.

Expected: Caddy reaches the proxy, proxy cannot contact Emby auth, and the proxy internally falls back to Emby before reading origin/cache. The response should match normal Emby behavior for that request.

Cache-proxy stopped Caddy fallback:

```bash
(
set -eu
systemctl stop emby-range-cache-proxy.service
trap 'systemctl start emby-range-cache-proxy.service' EXIT
curl -sS -D /tmp/proxy-stopped.headers -o /tmp/proxy-stopped.bin \
  -H 'Range: bytes=0-1048575' \
  -H "X-Emby-Token: $TOKEN" \
  "$BASE/emby/videos/$ITEM_ID/original.mkv?MediaSourceId=$MEDIA_SOURCE_ID"
)
```

Expected: Caddy falls back to `127.0.0.1:8096` because `127.0.0.1:18180` is unavailable. This proves only the connection-failure fallback path.

## Prewarm Tests

Prewarm uses `prewarm_api_key` only for Emby item discovery, PlaybackInfo lookup, and the loopback internal prewarm endpoint. It must not be accepted as a replacement for a user's playback token.

Confirm the internal key is configured with a narrow rollout allowlist. `prewarm.enabled` controls only the periodic recent-item scanner; MediaInfoKeeper-triggered prewarm through `POST /internal/prewarm` only requires `prewarm_api_key`.

```bash
jq '.prewarm, .rollout, (.prewarm_api_key != null)' /etc/emby-range-cache-proxy/config.json
```

For a direct endpoint smoke test, trigger one allowlisted media source over loopback:

```bash
PREWARM_KEY=$(jq -r '.prewarm_api_key' /etc/emby-range-cache-proxy/config.json)
curl -fsS -X POST http://127.0.0.1:18180/internal/prewarm \
  -H "X-Range-Cache-Prewarm-Key: $PREWARM_KEY" \
  -H 'Content-Type: application/json' \
  --data "{\"itemId\":\"$ITEM_ID\",\"mediaSourceId\":\"$MEDIA_SOURCE_ID\"}"
```

Expected: JSON with `status` set to `queued`, or `existing` if the same item/source is already queued or running. The proxy then queries Emby PlaybackInfo itself, resolves `.strm` only through configured path mappings and `rollout.path_prefix_allowlist`, and warms only adaptive head and tail ranges.

When wiring MediaInfoKeeper, call the same `POST /internal/prewarm` endpoint only after media information extraction succeeds. Keep the call loopback-local to `127.0.0.1:18180`, send `itemId` and `mediaSourceId` in the JSON body, and send the secret only in `X-Range-Cache-Prewarm-Key` or a Bearer `Authorization` header.

If periodic prewarm is intentionally enabled, trigger or wait for the next scan after adding a new test item or selecting a recently added item. The worker should scan recent media and prewarm only adaptive head and tail ranges.

Checks:

- New or recent rollout-allowlisted media creates head/tail cache entries only.
- No arbitrary middle cache entry is created by the prewarm worker.
- Non-allowlisted media is skipped.
- A repeated `POST /internal/prewarm` for the same item/source returns `existing` while the first task is queued or running.
- A playback request using the internal `prewarm_api_key` as `api_key` is rejected by the proxy before Emby authorization.
- User playback authorization still calls Emby with the user's own token on each playback request.

## Rollback

Prefer restoring the deployment-specific backup created immediately before the grey-release edit. Use the older known backup only when intentionally returning to that exact earlier test state. Validate, reload, and disable the unified proxy:

```bash
backup=$(cat /tmp/emby-range-cache-proxy-caddy-backup)
cp "$backup" /etc/caddy/Caddyfile
# Fallback only when intentionally returning to that exact earlier state:
# cp /etc/caddy/Caddyfile.bak-no-ip-whitelist-20260703-050143 /etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile
systemctl reload caddy
systemctl disable --now emby-range-cache-proxy.service
```

After rollback:

- Normal Emby requests should still go to `127.0.0.1:8096`.
- Existing per-item range-cache services remain available for the previous test routes if those routes are restored.
- Keep `/home/nax/emby/cache/range-proxy` intact until logs confirm no rollback analysis is needed.
