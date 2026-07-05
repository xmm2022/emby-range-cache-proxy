# Local HDD Resume Window Design

Date: 2026-07-05

## Goal

Use local HDD middle cache to make resume after idle/stop fast, without moving a large fraction of a movie into cache and without depending on remote startup/cache providers.

## Scope

In scope:

- Local HDD middle cache.
- Idle/stop-driven resume prefetch.
- Adjacent middle block stitching.
- Go canary rollout on `127.0.0.1:18181`.

Out of scope:

- Remote startup or middle-cache providers.
- Changing the production Python service on `127.0.0.1:18180`.

## Evidence

The relevant measured behavior is byte/block based, not proportional to whole-file size:

- Real middle-block reads from local cache are fast even across adjacent blocks:
  - 16MiB inside one block: about 0.009s.
  - 16MiB crossing two blocks: about 0.006s.
  - 128MiB across two blocks: about 0.056s.
  - 128MiB unaligned across three blocks: about 0.038s.
  - 256MiB unaligned across five blocks: about 0.084s.
- Isolated Go HTTP E2E with copied real 64MiB blocks confirmed adjacent stitching avoids origin:
  - 16MiB within one block: origin delta 0.
  - 16MiB crossing two blocks: origin delta 0.
  - 128MiB aligned two blocks: origin delta 0.
  - 128MiB unaligned three blocks: origin delta 0.
  - 256MiB unaligned five blocks: origin delta 0.
- Formal-service log samples show most middle playback requests are 16MiB:
  - 46 requests needed one 64MiB block.
  - 14 requests needed two 64MiB blocks.
  - No observed foreground middle request needed three or more blocks.
- Sequential-ish playback runs after middle requests were usually short:
  - 1 request: 13 runs.
  - 2 requests: 9 runs.
  - 3 requests: 2 runs.
  - 5 requests: 2 runs.
  - 6 requests: 1 run.
  - 7 requests: 1 run, about 112MiB over about 49s.
- Bitrate samples show one 64MiB block covers very different wall-clock time:
  - 65.9Mbps: about 8.1s per 64MiB.
  - 24.9Mbps: about 21.5s per 64MiB.
  - 23.3Mbps: about 23.1s per 64MiB.
  - 8.4Mbps: about 63.8s per 64MiB.
  - 1.3Mbps: about 422.8s per 64MiB.

The previous miss on the formal Python service was caused by planner geometry: the effective resume overlap was capped too low by the head-sized window, so the prefetched range started too far ahead of the actual resume request. That does not justify a 2GiB per-session window.

## Design

### Head/Tail

Keep the canary startup head/tail at 8MiB/8MiB for this resume experiment.

High-bitrate startup head enlargement is a separate startup experiment. It should not be mixed into the resume-window test, because resume misses are middle-cache planning/stitching problems and startup misses are head-cache coverage problems.

### Resume Window

Use block-count planning for the canary:

```text
segment_bytes = 64MiB
resume_back_blocks = 1
resume_forward_blocks = 2
total target window = previous block + current block + next two blocks = 256MiB
```

This is intentionally not a 2GiB per-movie or per-session cache. The canary target is enough to cover measured resume behavior while keeping the blast radius small.

The block-count mode should remain configurable:

```json
{
  "prefetch": {
    "window_bytes": 268435456,
    "max_session_bytes": 536870912,
    "resume_overlap_bytes": 134217728,
    "resume_back_blocks": 1,
    "resume_forward_blocks": 2
  }
}
```

If both `resume_back_blocks` and `resume_forward_blocks` are set to 0, the older byte-window algorithm can be used as a compatibility fallback.

### Bitrate Policy

Do not scale the canary window linearly by bitrate yet. The first canary should collect hit/miss data with the measured 4-block window.

If canary data shows the 4-block window is too wide for low-bitrate files or too narrow for high-bitrate files, use this bounded mapping:

```text
< 15Mbps:  back=1, forward=1, total=192MiB
15-50Mbps: back=1, forward=2, total=256MiB
> 50Mbps:  back=1, forward=3, total=320MiB
```

The upper bound should stay at 320MiB unless real canary misses show that adjacent stitching works but the planned window is still too short.

### Segment Size

Keep `middle_cache.segment_bytes = 64MiB`.

Reasons:

- Real requests are commonly 16MiB and often straddle at most two 64MiB blocks.
- 64MiB gives better reuse and lower waste than 128MiB for small resume windows.
- Adjacent stitching already makes cross-block reads fast, so larger segment size is not needed to avoid request fragmentation.

Use 128MiB only if future measurements show metadata/task overhead or filesystem overhead dominates. Current measurements do not show that.

### Adjacent Block Stitching

Adjacent middle block stitching is required and must stay enabled.

A foreground middle-cache hit is valid only when the requested byte range is fully covered by completed adjacent middle blocks. Partial coverage must still miss and proxy origin directly.

This preserves the current safety property: foreground playback never waits for a cache build.

### Local HDD Limits

For the current canary server, disk free space is about 14.6GB on the root filesystem. Use conservative canary limits:

```text
middle_cache.max_bytes = 4GiB
middle_cache.min_free_bytes = 8GiB
middle_cache.ttl_seconds = 172800
prefetch.concurrency = 1
prefetch.per_origin_concurrency = 1
prefetch.bandwidth_bytes_per_second = 31457280
prefetch.max_queue_depth = 50
```

This means the canary can hold roughly sixteen 256MiB resume windows before LRU pressure, while keeping enough disk headroom.

When production has more confirmed HDD space, increase only the total pool size first. Do not increase the per-resume window unless canary data shows a miss pattern that needs it.

## Canary State

The Go canary on the test server is configured as:

```text
listen = 127.0.0.1:18181
cache_dir = /home/nax/emby/cache/range-proxy-go-canary/cache
state_db = /home/nax/emby/cache/range-proxy-go-canary/state/state.sqlite3
session.enabled = true
session.observer_enabled = true
middle_cache.enabled = true
prefetch.enabled = true
```

The formal Python service on `127.0.0.1:18180` is unchanged.

## Verification

Already verified:

- `go test ./...`
- `make test-go`
- Canary process restarted on `127.0.0.1:18181`.
- `/internal/stats` reports session, middle cache, and prefetch enabled.
- `/internal/stats` reports no current prefetch failures.

Next real-playback verification:

1. Route one known rollout item through `127.0.0.1:18181`.
2. Play past the 8MiB head region into middle content.
3. Stop or idle for longer than `session.idle_seconds`.
4. Watch `/internal/stats` for prefetch queue/done and `middle_blocks_bytes`.
5. Resume near the stopped position.
6. Confirm `middle_hit` increments and origin delay is avoided.
