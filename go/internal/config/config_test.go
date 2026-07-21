package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, value map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigDefaultsAndUnknownFields(t *testing.T) {
	path := writeConfig(t, map[string]any{
		"emby_base_url":     "http://127.0.0.1:8096/",
		"fallback_base_url": "http://127.0.0.1:8096/",
		"cache_dir":         filepath.Join(t.TempDir(), "cache"),
		"ctl":               map[string]any{"future": true},
	})

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}

	if cfg.EmbyBaseURL != "http://127.0.0.1:8096" {
		t.Fatalf("EmbyBaseURL = %q", cfg.EmbyBaseURL)
	}
	if cfg.FallbackBaseURL != "http://127.0.0.1:8096" {
		t.Fatalf("FallbackBaseURL = %q", cfg.FallbackBaseURL)
	}
	if cfg.ListenHost != "127.0.0.1" || cfg.ListenPort != 18180 {
		t.Fatalf("listen = %s:%d", cfg.ListenHost, cfg.ListenPort)
	}
	if cfg.PlaybackInfoTimeoutSeconds != 15 {
		t.Fatalf("playback info timeout = %d", cfg.PlaybackInfoTimeoutSeconds)
	}
	if cfg.PlaybackAuthCacheTTLSeconds != 30 {
		t.Fatalf("playback auth cache ttl = %d", cfg.PlaybackAuthCacheTTLSeconds)
	}
	if cfg.OpenList.Enabled || cfg.OpenList.TimeoutSeconds != 10 {
		t.Fatalf("openlist defaults = %+v", cfg.OpenList)
	}
	if cfg.DirectOpenList.Enabled || cfg.DirectOpenList.PathPrefix != "/openlist/" {
		t.Fatalf("direct openlist defaults = %+v", cfg.DirectOpenList)
	}
	if cfg.DirectHTTP.Enabled || cfg.DirectHTTP.PathPrefix != "/http/" {
		t.Fatalf("direct http defaults = %+v", cfg.DirectHTTP)
	}
	if cfg.DirectCache.RequireEligibility {
		t.Fatalf("direct cache defaults = %+v", cfg.DirectCache)
	}
	if cfg.Cache.MaxBytes != 512*1024*1024*1024 {
		t.Fatalf("cache max bytes = %d", cfg.Cache.MaxBytes)
	}
	if cfg.Cache.HeadBytes != 8*1024*1024 || cfg.Cache.TailBytes != 8*1024*1024 {
		t.Fatalf("cache head/tail = %d/%d", cfg.Cache.HeadBytes, cfg.Cache.TailBytes)
	}
	if cfg.Cache.AdaptiveTailMaxBytes != 0 {
		t.Fatalf("adaptive tail max = %d", cfg.Cache.AdaptiveTailMaxBytes)
	}
	if len(cfg.Cache.OpenHeadResponseBytesByExtension) != 0 {
		t.Fatalf("open head response by extension = %+v", cfg.Cache.OpenHeadResponseBytesByExtension)
	}
	if len(cfg.Cache.OpenInitialResponseBytesByExtension) != 0 {
		t.Fatalf("open initial response by extension = %+v", cfg.Cache.OpenInitialResponseBytesByExtension)
	}
	if cfg.Cache.DefaultOpenRangeBytes != 16*1024*1024 {
		t.Fatalf("default open range = %d", cfg.Cache.DefaultOpenRangeBytes)
	}
	if cfg.Prewarm.IntervalSeconds != 900 || cfg.Prewarm.Concurrency != 1 || cfg.Prewarm.PlaybackInfoTimeoutSeconds != 15 {
		t.Fatalf("prewarm defaults = %+v", cfg.Prewarm)
	}
	if cfg.Session.Enabled || cfg.MiddleCache.Enabled || cfg.Prefetch.Enabled {
		t.Fatalf("phase2 features should default disabled: %+v %+v %+v", cfg.Session, cfg.MiddleCache, cfg.Prefetch)
	}
	if cfg.Prefetch.PollIntervalSeconds != 5 || cfg.Prefetch.ErrorBackoffSeconds != 300 {
		t.Fatalf("prefetch polling defaults = %+v", cfg.Prefetch)
	}
	if cfg.Prefetch.WindowBytes != 256*1024*1024 || cfg.Prefetch.MaxSessionBytes != 512*1024*1024 {
		t.Fatalf("prefetch window defaults = %+v", cfg.Prefetch)
	}
	if cfg.Prefetch.ResumeBackBlocks != 1 || cfg.Prefetch.ResumeForwardBlocks != 2 {
		t.Fatalf("prefetch resume block defaults = %+v", cfg.Prefetch)
	}
}

func TestLoadConfigParsesExplicitPhase2AndPathMappings(t *testing.T) {
	path := writeConfig(t, map[string]any{
		"emby_base_url":                   "http://emby.local/",
		"fallback_base_url":               "http://fallback.local/",
		"listen_host":                     "127.0.0.2",
		"listen_port":                     19090,
		"cache_dir":                       filepath.Join(t.TempDir(), "cache"),
		"prewarm_api_key":                 "secret",
		"playback_info_timeout_seconds":   11,
		"playback_auth_cache_ttl_seconds": 7,
		"path_mappings": []map[string]any{
			{"from": "/strm", "to": "/srv/strm"},
			{"source_prefix": "/media/", "target_prefix": "/srv/media"},
		},
		"openlist": map[string]any{
			"enabled":         true,
			"base_url":        "https://openlist.example/",
			"token":           "openlist-token",
			"password":        "path-password",
			"timeout_seconds": 3,
		},
		"direct_openlist": map[string]any{
			"enabled":     true,
			"path_prefix": "edge-openlist",
			"token":       "direct-token",
		},
		"direct_http": map[string]any{
			"enabled":           true,
			"path_prefix":       "google",
			"upstream_base_url": "http://127.0.0.1:18096/",
		},
		"direct_cache": map[string]any{
			"require_eligibility": true,
		},
		"rollout": map[string]any{
			"enabled":                    true,
			"item_allowlist":             []string{"1"},
			"media_source_allowlist":     []string{"ms1"},
			"path_prefix_allowlist":      []string{"http://127.0.0.1:18096/"},
			"ignored_future_rollout_key": true,
		},
		"cache": map[string]any{
			"max_bytes":                                "123",
			"build_wait_seconds":                       1.5,
			"head_bytes":                               32,
			"tail_bytes":                               64,
			"adaptive_tail_max_bytes":                  128,
			"chunk_bytes":                              4096,
			"default_open_range_bytes":                 8192,
			"open_head_response_bytes":                 16384,
			"open_head_response_bytes_by_extension":    map[string]any{".MP4": 4096, "mkv": "8192"},
			"open_initial_response_bytes_by_extension": map[string]any{".MP4": 256},
		},
		"prewarm": map[string]any{
			"enabled":                       true,
			"interval_seconds":              60,
			"max_items_per_scan":            9,
			"concurrency":                   2,
			"playback_info_timeout_seconds": 17,
		},
		"session": map[string]any{
			"enabled":                   true,
			"state_db":                  "/tmp/state.sqlite3",
			"observer_enabled":          true,
			"observer_interval_seconds": "45",
			"idle_seconds":              240,
			"stop_grace_seconds":        90,
			"expire_seconds":            7200,
		},
		"middle_cache": map[string]any{
			"enabled":        true,
			"max_bytes":      1234,
			"ttl_seconds":    456,
			"segment_bytes":  789,
			"min_free_bytes": 321,
		},
		"prefetch": map[string]any{
			"enabled":                           true,
			"window_bytes":                      111,
			"resume_overlap_bytes":              222,
			"max_session_bytes":                 333,
			"max_queue_depth":                   44,
			"concurrency":                       2,
			"per_origin_concurrency":            3,
			"bandwidth_bytes_per_second":        555,
			"pause_when_rollout_session_active": false,
			"poll_interval_seconds":             6,
			"error_backoff_seconds":             66,
			"resume_back_blocks":                2,
			"resume_forward_blocks":             3,
		},
	})

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}

	if cfg.EmbyBaseURL != "http://emby.local" || cfg.FallbackBaseURL != "http://fallback.local" {
		t.Fatalf("base URLs = %q %q", cfg.EmbyBaseURL, cfg.FallbackBaseURL)
	}
	if cfg.PlaybackInfoTimeoutSeconds != 11 {
		t.Fatalf("playback info timeout = %d", cfg.PlaybackInfoTimeoutSeconds)
	}
	if cfg.PlaybackAuthCacheTTLSeconds != 7 {
		t.Fatalf("playback auth cache ttl = %d", cfg.PlaybackAuthCacheTTLSeconds)
	}
	if len(cfg.PathMappings) != 2 || cfg.PathMappings[0].SourcePrefix != "/strm/" || cfg.PathMappings[1].SourcePrefix != "/media/" {
		t.Fatalf("path mappings = %+v", cfg.PathMappings)
	}
	if !cfg.OpenList.Enabled || cfg.OpenList.BaseURL != "https://openlist.example" || cfg.OpenList.Token != "openlist-token" || cfg.OpenList.Password != "path-password" || cfg.OpenList.TimeoutSeconds != 3 {
		t.Fatalf("openlist = %+v", cfg.OpenList)
	}
	if !cfg.DirectOpenList.Enabled || cfg.DirectOpenList.PathPrefix != "/edge-openlist/" || cfg.DirectOpenList.Token != "direct-token" {
		t.Fatalf("direct openlist = %+v", cfg.DirectOpenList)
	}
	if !cfg.DirectHTTP.Enabled || cfg.DirectHTTP.PathPrefix != "/google/" || cfg.DirectHTTP.UpstreamBaseURL != "http://127.0.0.1:18096" {
		t.Fatalf("direct http = %+v", cfg.DirectHTTP)
	}
	if !cfg.DirectCache.RequireEligibility {
		t.Fatalf("direct cache = %+v", cfg.DirectCache)
	}
	if !cfg.Rollout.InScope("1", "ms1", "http://127.0.0.1:18096/a.mkv") {
		t.Fatalf("expected rollout in scope")
	}
	if cfg.Cache.MaxBytes != 123 || cfg.Cache.HeadBytes != 32 || cfg.Cache.TailBytes != 64 || cfg.Cache.AdaptiveTailMaxBytes != 128 || cfg.Cache.OpenHeadResponseBytes == nil || *cfg.Cache.OpenHeadResponseBytes != 16384 {
		t.Fatalf("cache = %+v", cfg.Cache)
	}
	if cfg.Cache.OpenHeadResponseBytesByExtension["mp4"] != 4096 || cfg.Cache.OpenHeadResponseBytesByExtension["mkv"] != 8192 {
		t.Fatalf("open head response by extension = %+v", cfg.Cache.OpenHeadResponseBytesByExtension)
	}
	if cfg.Cache.OpenInitialResponseBytesByExtension["mp4"] != 256 {
		t.Fatalf("open initial response by extension = %+v", cfg.Cache.OpenInitialResponseBytesByExtension)
	}
	if !cfg.Prewarm.Enabled || cfg.Prewarm.IntervalSeconds != 60 || cfg.Prewarm.Concurrency != 2 || cfg.Prewarm.PlaybackInfoTimeoutSeconds != 17 {
		t.Fatalf("prewarm = %+v", cfg.Prewarm)
	}
	if !cfg.Session.Enabled || cfg.Session.StateDB != "/tmp/state.sqlite3" || cfg.Session.ObserverIntervalSeconds != 45 {
		t.Fatalf("session = %+v", cfg.Session)
	}
	if !cfg.MiddleCache.Enabled || cfg.MiddleCache.MinFreeBytes != 321 {
		t.Fatalf("middle cache = %+v", cfg.MiddleCache)
	}
	if !cfg.Prefetch.Enabled || cfg.Prefetch.PerOriginConcurrency != 3 || cfg.Prefetch.PauseWhenRolloutSessionActive {
		t.Fatalf("prefetch = %+v", cfg.Prefetch)
	}
	if cfg.Prefetch.ResumeBackBlocks != 2 || cfg.Prefetch.ResumeForwardBlocks != 3 {
		t.Fatalf("prefetch resume blocks = %+v", cfg.Prefetch)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name  string
		patch map[string]any
	}{
		{"missing emby", map[string]any{"emby_base_url": ""}},
		{"missing cache", map[string]any{"cache_dir": ""}},
		{"bad playbackinfo timeout", map[string]any{"playback_info_timeout_seconds": 0}},
		{"bad playback auth cache ttl", map[string]any{"playback_auth_cache_ttl_seconds": -1}},
		{"bad openlist missing base", map[string]any{"openlist": map[string]any{"enabled": true}}},
		{"bad openlist timeout", map[string]any{"openlist": map[string]any{"timeout_seconds": 0}}},
		{"bad direct openlist without openlist", map[string]any{"direct_openlist": map[string]any{"enabled": true, "token": "secret"}}},
		{"bad direct openlist without token", map[string]any{"openlist": map[string]any{"enabled": true, "base_url": "http://openlist.local"}, "direct_openlist": map[string]any{"enabled": true}}},
		{"bad direct http without upstream", map[string]any{"direct_http": map[string]any{"enabled": true}}},
		{"bad direct http upstream scheme", map[string]any{"direct_http": map[string]any{"enabled": true, "upstream_base_url": "file:///tmp/media"}}},
		{"bad cache head", map[string]any{"cache": map[string]any{"head_bytes": 0}}},
		{"bad cache tail", map[string]any{"cache": map[string]any{"tail_bytes": 0}}},
		{"bad adaptive tail max", map[string]any{"cache": map[string]any{"adaptive_tail_max_bytes": 1}}},
		{"bad mapping root", map[string]any{"path_mappings": []map[string]any{{"from": "/", "to": "/tmp"}}}},
		{"string bool", map[string]any{"session": map[string]any{"enabled": "false"}}},
		{"short prewarm", map[string]any{"prewarm": map[string]any{"interval_seconds": 59}}},
		{"bad prewarm playbackinfo timeout", map[string]any{"prewarm": map[string]any{"playback_info_timeout_seconds": 0}}},
		{"bad middle free", map[string]any{"middle_cache": map[string]any{"min_free_bytes": -1}}},
		{"bad prefetch concurrency", map[string]any{"prefetch": map[string]any{"per_origin_concurrency": 0}}},
		{"bad prefetch back blocks", map[string]any{"prefetch": map[string]any{"resume_back_blocks": -1}}},
		{"bad prefetch forward blocks", map[string]any{"prefetch": map[string]any{"resume_forward_blocks": -1}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := map[string]any{
				"emby_base_url": "http://127.0.0.1:8096",
				"cache_dir":     filepath.Join(t.TempDir(), "cache"),
			}
			for k, v := range tc.patch {
				raw[k] = v
			}
			if _, err := LoadFile(writeConfig(t, raw)); err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}
