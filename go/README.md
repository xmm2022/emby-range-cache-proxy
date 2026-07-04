# Go Emby Range Cache Proxy

This is the Go v1 implementation of the existing Python `emby-range-cache-proxy`.

It is an independent Go module under `go/` so the Python service can stay in place for rollback. The binary reads the same `/etc/emby-range-cache-proxy/config.json`, ignores unknown future fields, and reuses the Python cache/state layout:

- `{cache_dir}/{key}/head.bin`
- `{cache_dir}/{key}/tail.bin`
- `{cache_dir}/{key}/mid/{start}-{end}.bin`
- `{cache_dir}/state/phase2.sqlite3`

## Build

```bash
cd /opt/emby-range-cache-proxy/go
go test ./...
go build -o bin/emby-range-cache-proxy ./cmd/emby-range-cache-proxy
```

## Smoke Test

Run next to the Python service only if the configured port is free, or copy the config and change `listen_port`:

```bash
./bin/emby-range-cache-proxy --config /etc/emby-range-cache-proxy/config.json
curl -fsS http://127.0.0.1:18180/healthz
curl -fsS http://127.0.0.1:18180/internal/stats
```

## systemd Cutover

Build the binary into `/opt/emby-range-cache-proxy/go/bin/emby-range-cache-proxy`, then replace the service unit with `go/deploy/emby-range-cache-proxy-go.service`:

```bash
install -m 0644 go/deploy/emby-range-cache-proxy-go.service /etc/systemd/system/emby-range-cache-proxy.service
systemctl daemon-reload
systemctl restart emby-range-cache-proxy.service
systemctl status emby-range-cache-proxy.service --no-pager
curl -fsS http://127.0.0.1:18180/healthz
curl -fsS http://127.0.0.1:18180/internal/stats
```

## SQLite Migration Strategy

The Go service opens the existing `session.state_db` and runs additive migrations only:

- create missing `playback_sessions`, `source_metadata`, `prefetch_tasks`, and `middle_blocks` tables
- add `prefetch_tasks.next_attempt_at` if it is missing
- backfill retryable failed/skipped prefetch tasks into the queue

It also enables SQLite WAL mode for steadier planner/worker concurrency. It does not drop or rewrite Python rows. The cache key, head/tail files, middle block files, and current table names stay compatible with the Python phase2 layout.

## Rollback To Python

Keep the previous Python unit content before replacing it:

```bash
systemctl cat emby-range-cache-proxy.service > /root/emby-range-cache-proxy-python-unit.backup
```

Rollback restores the Python `ExecStart`:

```bash
cat >/etc/systemd/system/emby-range-cache-proxy.service <<'UNIT'
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
ProtectHome=read-only
ReadWritePaths=/home/nax/emby/cache/range-proxy

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl restart emby-range-cache-proxy.service
curl -fsS http://127.0.0.1:18180/healthz
```

The Go service uses the same cache directory and SQLite schema, so rollback does not require cache deletion. If a clean rollback is desired, stop the service first and move only the Go-created files aside after inspecting timestamps.
