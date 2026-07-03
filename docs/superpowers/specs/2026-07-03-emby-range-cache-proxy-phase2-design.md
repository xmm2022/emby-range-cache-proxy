# Emby Range Cache Proxy Phase 2 Design

Date: 2026-07-03

## Goal

Phase 2 adds playback session state tracking and idle/stop-driven middle-range prefetch to the existing unified Emby Range Cache Proxy.

The intent is to improve resume and later-playback smoothness without changing the successful Phase 1 startup path:

- Keep normal playback requests fast and non-blocking.
- Continue to authorize every user request through Emby with the user's token.
- Use the internal Emby API key only for read-only playback session observation and background work.
- Cache middle ranges only from background prefetch after a playback session becomes idle or stops.
- Bound the feature with global middle-cache capacity, TTL, LRU eviction, concurrency limits, bandwidth limits, and allowlist rollout.

## Current Baseline

Phase 1 is deployed on the test server as one local service:

```text
127.0.0.1:18180
```

Current verified behavior:

- Caddy gray-routes selected Emby original media requests to the unified proxy.
- The service is `emby-range-cache-proxy` and runs as `emby-cache`.
- User playback requests are authorized through Emby `PlaybackInfo` with the user token.
- The internal prewarm key is not used for user playback authorization.
- Head/tail cache sizes are adaptive:

```text
<2GiB:   head 16MiB / tail 8MiB
2-8GiB:  head 32MiB / tail 8MiB
8-30GiB: head 64MiB / tail 8MiB
>30GiB:  head 128MiB / tail 16MiB
```

- Open-ended head responses are capped with `open_head_response_bytes=32MiB` on the test server.
- 800MiB, 5GiB, 20GiB, 29.55GiB, and large Remux startup/tail paths have been validated.
- The 29.55GiB item starts in seconds after head/tail cache is warm.

Phase 1 intentionally does not cache arbitrary middle playback ranges. Phase 2 should preserve that safety property for foreground misses: a middle-range miss proxies origin directly and does not build a middle cache synchronously.

## Approved Direction

The approved Phase 2 approach is:

1. Record playback session state from authorized foreground Range requests.
2. Poll Emby sessions with the internal API key to detect stop/disappear events.
3. Also detect idle from proxy-observed inactivity when Emby session state is unavailable or delayed.
4. Queue middle-range prefetch only after idle or stop.
5. Serve a middle-range request from cache only when the requested range is already fully covered by a completed middle block.
6. On a middle-cache miss or partial overlap, proxy from origin without waiting for or starting a foreground cache build.

The user has explicitly allowed using the internal Emby API key for read-only playback session status. That key remains out of the user authorization path.

The user has also explicitly allowed using subagents during implementation. The implementation plan may split independent work such as config/model changes, middle-cache storage, session tracking, app integration, and deployment docs across subagents.

## Non-Goals

Phase 2 does not include:

- Replacing Emby authorization with an internal or admin token.
- Building middle cache from the active foreground playback stream.
- Making playback wait for a background prefetch task.
- Full-library middle prefetch.
- Wide production rollout before test-server session observation and prefetch behavior are verified.
- Logging raw tokens, raw origin URLs, raw query strings, raw `PlaySessionId`, or raw `DeviceId`.

## Architecture

Phase 2 adds five bounded components:

- `SessionRecorder`: updates session state after a request is authorized and source metadata is known.
- `EmbySessionObserver`: polls Emby session state with the internal API key and marks sessions stopped when they disappear or report stopped.
- `PrefetchPlanner`: converts idle/stopped sessions into bounded prefetch tasks.
- `MiddleRangeCache`: stores completed middle blocks under the same cache key namespace but in an independent middle-cache pool.
- `PrefetchWorker`: executes queued background prefetch tasks with concurrency, bandwidth, and disk-capacity limits.

Request flow becomes:

```text
Client
  -> Caddy
    -> cache-proxy
      -> user-token PlaybackInfo authorization
      -> origin HEAD metadata
      -> session record update
      -> head/tail cache check
      -> middle cache check for completed full-block coverage
      -> origin stream on miss
```

Background flow becomes:

```text
Authorized playback ranges
  -> session state
  -> idle/stop decision
  -> prefetch task queue
  -> bounded origin Range fetch
  -> complete middle block commit
  -> middle-cache LRU/TTL janitor
```

## State Storage

Use SQLite from the Python standard library for durable state and queue metadata. The default path should be under the cache directory:

```text
{cache_dir}/state/phase2.sqlite3
```

SQLite keeps the queue and block metadata reproducible across service restarts without adding a runtime dependency.

Recommended tables:

- `playback_sessions`
  - hashed session id
  - item id
  - media source id
  - hashed device id, if present
  - cache key
  - origin metadata hash
  - media size
  - last observed range start/end
  - max observed offset
  - first seen timestamp
  - last seen timestamp
  - last Emby-observed timestamp
  - status: `active`, `idle`, `stopped`, `expired`
  - queued prefetch boundary
- `prefetch_tasks`
  - task id
  - item id
  - media source id
  - cache key
  - start/end byte range
  - priority
  - status: `queued`, `running`, `done`, `failed`, `skipped`
  - attempts
  - created/updated timestamps
  - last error class, not full error text
- `middle_blocks`
  - cache key
  - block start/end
  - path
  - size
  - created timestamp
  - last access timestamp
  - expires timestamp

Raw user tokens are never stored. Raw `PlaySessionId` and `DeviceId` are not stored; store stable SHA-256 hashes instead. Logs may include short hash prefixes for correlation.

## Session Recording

The proxy records a session only after:

1. The request shape is eligible.
2. Rollout allowlist passes.
3. User-token authorization succeeds through Emby.
4. The media source resolves to an allowed HTTP/HTTPS origin.
5. Origin metadata is known and a cache key has been computed.

The recorder should capture:

- `ItemId`
- `MediaSourceId`
- hashed `PlaySessionId`, if supplied
- hashed `DeviceId`, if supplied
- request method
- planned byte range after open-ended range clamping
- source size
- cache key
- wall-clock update time

If `PlaySessionId` is absent, the recorder may use a synthetic key derived from `ItemId`, `MediaSourceId`, hashed `DeviceId`, and a short time bucket. Synthetic sessions are lower confidence and can be used for idle prefetch only, not Emby stop correlation.

Session updates must be lightweight. SQLite writes should be short and should not happen while holding streaming backpressure on the response body. A small in-memory queue may batch writes, but dropping a session update is preferable to delaying playback.

## Emby Session Observation

When `session.observer_enabled` is true and `prewarm_api_key` is configured, poll Emby sessions periodically with the internal key.

Initial endpoint:

```text
GET {emby_base_url}/Sessions?api_key={prewarm_api_key}
```

The observer should extract only the fields needed to correlate playback:

- `PlaySessionId`, hashed before storing or logging
- `DeviceId`, hashed before storing or logging
- current item id, if present
- playback stopped/paused/playing indicators, if present
- last activity date, if present

If a previously observed session disappears from Emby's active session list, mark it stopped after `session.stop_grace_seconds`. If the API call fails, keep existing sessions unchanged and let idle detection handle old activity.

The internal key is only used by the observer and by existing background workers. It must never be accepted as a user playback token; the existing internal-key denial behavior for foreground requests stays in place.

## Idle And Stop Decisions

Default thresholds:

```text
session.idle_seconds = 180
session.stop_grace_seconds = 60
session.expire_seconds = 86400
```

A session becomes idle when no authorized foreground range for that session has been observed for `idle_seconds`.

A session becomes stopped when:

- Emby reports a stopped state, or
- the session disappears from Emby's session list and remains absent for `stop_grace_seconds`.

Stopped has higher confidence than idle. Both can queue prefetch, but stopped sessions can receive slightly higher task priority because they are less likely to compete with active playback.

Repeated idle checks should not enqueue duplicate work. Store the highest queued byte boundary per session and only enqueue later ranges when playback advances.

## Prefetch Planning

Prefetch only operates on rollout-scoped media. It reuses the same source resolution and path-prefix allowlist concepts as foreground requests.

Recommended defaults:

```text
prefetch.enabled = false
prefetch.window_bytes = 2147483648        # 2GiB per idle/stop event
prefetch.resume_overlap_bytes = 134217728 # 128MiB behind last observed offset
prefetch.max_session_bytes = 4294967296   # 4GiB total queued per session
prefetch.max_queue_depth = 200
middle_cache.segment_bytes = 67108864     # 64MiB
```

Planning algorithm:

1. Start near the observed playback position:
   - `start = max(head_end + 1, max_observed_offset - resume_overlap_bytes)`
   - align `start` down to `middle_cache.segment_bytes`
2. End at:
   - `start + prefetch.window_bytes - 1`
   - capped by `tail_start - 1`
   - capped by file size
3. Split the range into fixed middle segments.
4. Skip segments that already have a complete middle block.
5. Skip segments that overlap head or tail blocks.
6. Skip sessions that have already queued `prefetch.max_session_bytes`.
7. Insert deduplicated queued tasks by `(cache_key, block_start, block_end)`.

For files smaller than the combined head/tail plus one segment, no middle prefetch is queued.

## Middle Cache Model

Middle blocks are separate from head/tail blocks.

Recommended filesystem layout:

```text
{cache_dir}/{cache_key}/head.bin
{cache_dir}/{cache_key}/head.range
{cache_dir}/{cache_key}/tail.bin
{cache_dir}/{cache_key}/tail.range
{cache_dir}/{cache_key}/mid/{start}-{end}.bin
{cache_dir}/{cache_key}/mid/{start}-{end}.range
```

Middle block names must be generated from numeric byte ranges only. They must not include media names, origin URLs, tokens, user ids, session ids, or device ids.

Write behavior:

- Fetch exactly one middle segment per task.
- Write to a UUID temp file.
- Validate full byte count.
- Atomically rename `.bin` and `.range` metadata.
- Insert or update `middle_blocks` metadata only after the file commit succeeds.
- Delete temp files on failure or cancellation.

Read behavior:

- Foreground requests may read middle cache only after successful user authorization.
- A middle cache hit requires the requested planned range to be fully contained inside one completed middle block.
- On miss or partial overlap, proxy from origin without waiting and without building a middle block synchronously.
- A corrupt, truncated, or metadata-mismatched middle block is deleted and treated as a miss.

## Capacity, LRU, And TTL

Keep two capacity concepts:

- `cache.max_bytes`: total safety ceiling for the whole cache directory.
- `middle_cache.max_bytes`: independent ceiling for middle blocks.

Recommended test-server default:

```text
middle_cache.enabled = false
middle_cache.max_bytes = 137438953472 # 128GiB
middle_cache.ttl_seconds = 604800     # 7 days
middle_cache.min_free_bytes = 53687091200 # 50GiB
```

The middle-cache janitor order is:

1. Delete expired middle blocks.
2. Delete least-recently-used middle blocks until `middle_cache.max_bytes` is satisfied.
3. If the whole cache directory is still above `cache.max_bytes`, delete middle blocks first.
4. Delete head/tail blocks only as the existing final safety behavior when total cache pressure remains after middle blocks are gone.

This preserves Phase 1 startup benefits under pressure.

LRU is based on middle block access time recorded in SQLite and mirrored to file mtime for simple inspection. TTL is extended on read by updating last-access time and recomputing expiry.

## Concurrency And Bandwidth Limits

Recommended defaults:

```text
prefetch.concurrency = 1
prefetch.per_origin_concurrency = 1
prefetch.bandwidth_bytes_per_second = 31457280 # 30MiB/s
prefetch.pause_when_rollout_session_active = true
prefetch.error_backoff_seconds = 300
```

The worker should:

- Run at low priority relative to foreground requests.
- Never hold foreground request locks.
- Pause task starts while a rollout-scoped session is active when `pause_when_rollout_session_active` is true.
- Enforce a global async token-bucket or leaky-bucket bandwidth limiter across all prefetch workers.
- Keep a per-origin semaphore so one origin cannot be overrun by multiple tasks.
- Back off a media source after repeated origin errors.
- Stop starting new tasks when disk free space is below `middle_cache.min_free_bytes`.

A running prefetch task may finish its current segment unless the service is shutting down, disk pressure becomes severe, or the origin returns an error.

## Configuration

Add three new config sections.

Recommended example:

```json
{
  "session": {
    "enabled": true,
    "state_db": null,
    "observer_enabled": false,
    "observer_interval_seconds": 30,
    "idle_seconds": 180,
    "stop_grace_seconds": 60,
    "expire_seconds": 86400
  },
  "middle_cache": {
    "enabled": false,
    "max_bytes": 137438953472,
    "ttl_seconds": 604800,
    "segment_bytes": 67108864,
    "min_free_bytes": 53687091200
  },
  "prefetch": {
    "enabled": false,
    "window_bytes": 2147483648,
    "resume_overlap_bytes": 134217728,
    "max_session_bytes": 4294967296,
    "max_queue_depth": 200,
    "concurrency": 1,
    "per_origin_concurrency": 1,
    "bandwidth_bytes_per_second": 31457280,
    "pause_when_rollout_session_active": true,
    "error_backoff_seconds": 300
  }
}
```

Default behavior must be backwards-compatible:

- If `session.enabled` is false, no session DB is opened.
- If `middle_cache.enabled` is false, middle cache is neither read nor written.
- If `prefetch.enabled` is false, sessions may still be recorded but no prefetch tasks are queued or executed.
- If `session.observer_enabled` is false, no Emby `/Sessions` polling occurs.

## Observability

Add concise sanitized log events:

- `session_update`
- `session_idle`
- `session_stopped`
- `prefetch_queued`
- `prefetch_started`
- `prefetch_complete`
- `prefetch_skipped`
- `prefetch_failed`
- `middle_cache_hit`
- `middle_cache_miss`
- `middle_cache_evict`

Logs should include:

- item id
- media source id
- short session hash, if available
- cache key prefix
- byte range
- size
- reason
- elapsed milliseconds
- queue depth

Logs must not include raw tokens, raw `PlaySessionId`, raw `DeviceId`, raw origin URLs, or raw query strings.

## Testing

Add focused tests before implementation:

- Config parsing for `session`, `middle_cache`, and `prefetch`.
- Session recorder hashes session/device ids and stores range progress.
- Idle detector marks sessions idle after the configured threshold.
- Emby observer marks disappeared sessions stopped after grace time.
- Prefetch planner aligns ranges, skips head/tail, caps window size, and deduplicates tasks.
- Middle cache stores, reads, rejects malformed names, detects truncated files, updates access time, expires by TTL, and evicts by LRU.
- App integration authorizes before reading middle cache.
- App integration serves a full middle-cache hit without touching origin.
- App integration proxies origin on middle-cache miss and does not synchronously create a middle block.
- Worker enforces concurrency and bandwidth limits with deterministic fake clocks.
- Worker skips tasks when disk free space is below the configured floor.

Keep deployment tests updated so disabled Phase 2 config remains valid and Phase 1 defaults remain unchanged.

## Test-Server Rollout

Stage 1: deploy code with all Phase 2 runtime features disabled.

- `session.enabled=false`
- `middle_cache.enabled=false`
- `prefetch.enabled=false`
- Confirm Phase 1 head/tail behavior is unchanged.

Stage 2: enable session recording only.

- `session.enabled=true`
- `session.observer_enabled=false`
- `middle_cache.enabled=false`
- `prefetch.enabled=false`
- Verify sanitized `session_update` logs for the existing Caddy allowlist.
- Confirm no middle files are created.

Stage 3: enable Emby session observation.

- Set `prewarm_api_key`.
- `session.observer_enabled=true`
- Verify stopped/disappeared sessions are detected.
- Confirm internal key is rejected if used as a foreground playback token.

Stage 4: enable middle-cache read path with no prefetch.

- `middle_cache.enabled=true`
- `prefetch.enabled=false`
- Verify no background downloads occur.
- Manually seed a small middle block in a test fixture or unit integration path and confirm authorized reads can hit it.

Stage 5: enable prefetch for one or two rollout items.

- `prefetch.enabled=true`
- `prefetch.concurrency=1`
- `prefetch.bandwidth_bytes_per_second=20971520` to `31457280`
- `middle_cache.max_bytes=68719476736` to `137438953472`
- Confirm idle/stop queues are bounded.
- Confirm playback startup remains head/tail cached.
- Confirm active playback is not slowed by worker activity.

Stage 6: expand the test allowlist gradually.

- Watch origin errors, disk use, queue depth, evictions, and middle-cache hit rate.
- Keep Caddy gray routing at the item/media-source level.

## Production Rollout

Formal production rollout should mirror test-server stages:

1. Deploy code with Phase 2 disabled.
2. Enable session recording for the existing Caddy gray list.
3. Enable Emby session observation with the internal key.
4. Enable middle-cache read path.
5. Enable prefetch for one low-risk item and one high-value Remux item.
6. Expand by item/media-source allowlist after at least one stable observation window.

Rollback is config-only for each stage:

- Disable `prefetch.enabled` to stop background downloads.
- Disable `middle_cache.enabled` to stop middle-cache reads.
- Disable `session.enabled` to stop state writes.
- Revert Caddy gray routes to send traffic directly to Emby.

Existing head/tail cache files remain valid throughout rollback.

## Risks And Mitigations

- Emby session API fields may differ by version.
  - Mitigation: observer is best-effort and idle detection remains available.
- Some clients may omit `PlaySessionId`.
  - Mitigation: use lower-confidence synthetic sessions for idle only.
- Prefetch may compete with active playback.
  - Mitigation: default concurrency 1, bandwidth cap, active-session pause, and disk free-space checks.
- Middle cache may evict useful startup cache if capacity is not separated.
  - Mitigation: middle eviction runs first and does not delete head/tail unless total cache safety pressure remains.
- Large files can generate too many blocks.
  - Mitigation: fixed segment size, per-session byte cap, queue depth cap, deduplication.
- Stale origin metadata can make prefetched blocks invalid.
  - Mitigation: cache key includes media source id, final origin URL, size, ETag, and Last-Modified as in Phase 1.
