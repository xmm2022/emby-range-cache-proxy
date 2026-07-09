package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/emby"
)

func testConfig(t *testing.T, embyBase, fallbackBase string) config.Config {
	t.Helper()
	return config.Config{
		EmbyBaseURL:                 embyBase,
		FallbackBaseURL:             fallbackBase,
		ListenHost:                  "127.0.0.1",
		ListenPort:                  18180,
		CacheDir:                    filepath.Join(t.TempDir(), "cache"),
		PrewarmAPIKey:               "internal-secret",
		PlaybackInfoTimeoutSeconds:  15,
		PlaybackAuthCacheTTLSeconds: 30,
		Rollout: config.RolloutConfig{
			Enabled:              true,
			ItemAllowlist:        map[string]struct{}{"10535": {}},
			MediaSourceAllowlist: map[string]struct{}{"ms1": {}},
			PathPrefixAllowlist:  []string{"http://127.0.0.1:"},
		},
		Cache: config.CacheConfig{
			MaxBytes:              1024 * 1024,
			BuildWaitSeconds:      0.01,
			HeadBytes:             8 * 1024 * 1024,
			TailBytes:             8 * 1024 * 1024,
			ChunkBytes:            4,
			DefaultOpenRangeBytes: 16,
		},
		Prewarm: config.PrewarmConfig{IntervalSeconds: 900, MaxItemsPerScan: 100, Concurrency: 1, PlaybackInfoTimeoutSeconds: 15},
		Session: config.SessionConfig{
			ObserverIntervalSeconds: 30,
			IdleSeconds:             180,
			StopGraceSeconds:        60,
			ExpireSeconds:           86400,
		},
		MiddleCache: config.MiddleCacheConfig{MaxBytes: 1024 * 1024, TTLSeconds: 60, SegmentBytes: 64, MinFreeBytes: 0},
		Prefetch: config.PrefetchConfig{
			WindowBytes:                   128,
			ResumeOverlapBytes:            16,
			MaxSessionBytes:               256,
			MaxQueueDepth:                 10,
			Concurrency:                   1,
			PerOriginConcurrency:          1,
			BandwidthBytesPerSecond:       1024 * 1024,
			PauseWhenRolloutSessionActive: true,
			PollIntervalSeconds:           5,
			ErrorBackoffSeconds:           30,
		},
	}
}

func TestNewUsesSeparatePrewarmAuthTimeout(t *testing.T) {
	cfg := testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1")
	cfg.PlaybackInfoTimeoutSeconds = 11
	cfg.Prewarm.PlaybackInfoTimeoutSeconds = 17
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if server.auth.HTTP.Timeout != 11*time.Second {
		t.Fatalf("playback auth timeout = %s", server.auth.HTTP.Timeout)
	}
	if server.prewarmAuth.HTTP.Timeout != 17*time.Second {
		t.Fatalf("prewarm auth timeout = %s", server.prewarmAuth.HTTP.Timeout)
	}
	if server.httpClient.Timeout != 17*time.Second {
		t.Fatalf("prewarm scan timeout = %s", server.httpClient.Timeout)
	}
	if server.fallbackClient.Timeout != 0 {
		t.Fatalf("fallback timeout = %s", server.fallbackClient.Timeout)
	}
}

func TestHealthzAndStats(t *testing.T) {
	server, err := New(testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Fatalf("healthz code=%d body=%q", rec.Code, rec.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/internal/stats", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats code=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("stats json: %v", err)
	}
	if payload["uptime_seconds"] == nil || payload["config"] == nil {
		t.Fatalf("stats payload = %+v", payload)
	}
}

func TestInternalMetricsExposesStatsForLoopback(t *testing.T) {
	server, err := New(testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.addStat(func(stats *Stats) {
		stats.Counters.CacheHit = 2
		stats.Counters.Fallback = 1
		stats.Counters.Denied = 3
		stats.Prewarm.Queued = 4
		stats.Prefetch.Running = 5
	})
	req := httptest.NewRequest(http.MethodGet, "/internal/metrics", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Fatalf("content-type=%q", got)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"emby_range_cache_proxy_cache_hit_total 2",
		"emby_range_cache_proxy_fallback_total 1",
		"emby_range_cache_proxy_denied_total 3",
		"emby_range_cache_proxy_prewarm_queued 4",
		"emby_range_cache_proxy_prefetch_running 5",
		"emby_range_cache_proxy_rollout_enabled 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, body)
		}
	}
}

func TestInternalMetricsRejectsNonLoopbackRemote(t *testing.T) {
	server, err := New(testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	req := httptest.NewRequest(http.MethodGet, "/internal/metrics", nil)
	req.RemoteAddr = "203.0.113.10:12345"
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInternalMetricsRejectsForwardedNonLoopbackClient(t *testing.T) {
	server, err := New(testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	req := httptest.NewRequest(http.MethodGet, "/internal/metrics", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestProxyRejectsInternalKeyBeforeFallback(t *testing.T) {
	cases := []struct {
		name string
		path string
		set  func(*http.Request)
	}{
		{
			name: "query api key",
			path: "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=internal-secret",
		},
		{
			name: "x emby token",
			path: "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token",
			set:  func(req *http.Request) { req.Header.Set("X-Emby-Token", "internal-secret") },
		},
		{
			name: "prewarm header",
			path: "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token",
			set:  func(req *http.Request) { req.Header.Set("X-Range-Cache-Prewarm-Key", "internal-secret") },
		},
		{
			name: "authorization bearer case insensitive",
			path: "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token",
			set:  func(req *http.Request) { req.Header.Set("Authorization", "bearer internal-secret") },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fallbackHit := false
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fallbackHit = true
			}))
			defer fallback.Close()
			server, err := New(testConfig(t, fallback.URL, fallback.URL))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if tc.set != nil {
				tc.set(req)
			}
			rec := httptest.NewRecorder()
			server.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden || fallbackHit {
				t.Fatalf("code=%d fallbackHit=%v", rec.Code, fallbackHit)
			}
		})
	}
}

func TestDirBytesSkipsMiddleCacheDirectories(t *testing.T) {
	root := t.TempDir()
	keyDir := filepath.Join(root, strings.Repeat("a", 64))
	if err := os.MkdirAll(filepath.Join(keyDir, "mid"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "head.bin"), []byte("head"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "mid", "0-3.bin"), []byte("middle"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := dirBytes(root, func(path string) bool {
		return strings.HasSuffix(path, ".bin")
	})
	if got != 4 {
		t.Fatalf("dir bytes = %d", got)
	}
}

func TestRecentErrorEventsIncludeTimestampAndMessage(t *testing.T) {
	server, err := New(testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.addError("example failure")
	stats := server.SnapshotStats()
	if len(stats.RecentErrors) != 1 || stats.RecentErrors[0] != "example failure" {
		t.Fatalf("recent errors = %+v", stats.RecentErrors)
	}
	if len(stats.ErrorEvents) != 1 || stats.ErrorEvents[0].Message != "example failure" {
		t.Fatalf("error events = %+v", stats.ErrorEvents)
	}
	if _, err := time.Parse(time.RFC3339Nano, stats.ErrorEvents[0].Timestamp); err != nil {
		t.Fatalf("timestamp = %q", stats.ErrorEvents[0].Timestamp)
	}
}

func TestErrorClassRecognizesWrappedErrors(t *testing.T) {
	if got := errorClass(fmt.Errorf("outer: %w", context.DeadlineExceeded)); got != "DeadlineExceeded" {
		t.Fatalf("deadline class = %s", got)
	}
	if got := errorClass(fmt.Errorf("outer: %w", emby.AuthUnavailable{Reason: "slow"})); got != "AuthUnavailable" {
		t.Fatalf("auth class = %s", got)
	}
}

func TestFallbackStreamIgnoresShortAPITimeout(t *testing.T) {
	chunks := []string{"aaaaaaaa", "bbbbbbbb", "cccccccc", "dddddddd", "eeeeeeee", "ffffffff"}
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(strings.Join(chunks, ""))))
		flusher, _ := w.(http.Flusher)
		for _, chunk := range chunks {
			_, _ = w.Write([]byte(chunk))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(300 * time.Millisecond)
		}
	}))
	defer fallback.Close()

	cfg := testConfig(t, "http://127.0.0.1:1", fallback.URL)
	cfg.Prewarm.PlaybackInfoTimeoutSeconds = 1
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/not-an-original-media-path", nil)
	server.ServeHTTP(rec, req)

	want := strings.Join(chunks, "")
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("fallback code=%d body_len=%d want_len=%d body=%q", rec.Code, rec.Body.Len(), len(want), rec.Body.String())
	}
}

func TestAuthorizedHeadRangeBuildsAndHitsHeadCache(t *testing.T) {
	originGets := 0
	originBody := []byte("abcdefghijklmnopqrstuvwxyz")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "26")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			originGets++
			if r.Header.Get("Range") != "bytes=0-25" {
				t.Fatalf("origin range = %q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Range", "bytes 0-25/26")
			w.Header().Set("Content-Length", "26")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(originBody)
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Items/10535/PlaybackInfo" || r.URL.Query().Get("api_key") != "user-token" {
			t.Fatalf("unexpected emby request: %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http"}]}`))
	}))
	defer emby.Close()

	server, err := New(testConfig(t, emby.URL, emby.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
		req.Header.Set("Range", "bytes=0-9")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent || rec.Body.String() != "abcdefghij" {
			t.Fatalf("iter=%d code=%d body=%q headers=%v", i, rec.Code, rec.Body.String(), rec.Header())
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 0-9/26" {
			t.Fatalf("content-range=%q", got)
		}
	}
	if originGets != 1 {
		t.Fatalf("originGets=%d", originGets)
	}
	stats := server.SnapshotStats()
	if stats.Counters.CacheBuild == 0 || stats.Counters.CacheHit == 0 {
		t.Fatalf("stats = %+v", stats.Counters)
	}
}

func TestAuthorizedHeadRangeCacheHitSkipsOriginHead(t *testing.T) {
	originHeads := 0
	originGets := 0
	originBody := []byte("abcdefghijklmnopqrstuvwxyz")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			originHeads++
			w.Header().Set("Content-Length", "26")
			w.Header().Set("Content-Type", "video/x-matroska")
			w.Header().Set("ETag", `"v1"`)
			w.Header().Set("Last-Modified", "Thu, 09 Jul 2026 01:00:00 GMT")
		case http.MethodGet:
			originGets++
			if r.Header.Get("Range") != "bytes=0-25" {
				t.Fatalf("origin range = %q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Range", "bytes 0-25/26")
			w.Header().Set("Content-Length", "26")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(originBody)
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http","Size":26}]}`))
	}))
	defer emby.Close()

	server, err := New(testConfig(t, emby.URL, emby.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
		req.Header.Set("Range", "bytes=0-9")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent || rec.Body.String() != "abcdefghij" {
			t.Fatalf("iter=%d code=%d body=%q headers=%v", i, rec.Code, rec.Body.String(), rec.Header())
		}
		if got := rec.Header().Get("Content-Type"); got != "video/x-matroska" {
			t.Fatalf("content-type=%q", got)
		}
	}
	if originHeads != 1 || originGets != 1 {
		t.Fatalf("originHeads=%d originGets=%d", originHeads, originGets)
	}
}

func TestDirectOpenListEndpointBuildsAndHitsCache(t *testing.T) {
	var openListAPICalls int32
	originBody := []byte("abcdefghijklmnopqrstuvwxyz")
	openList := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/fs/get":
			atomic.AddInt32(&openListAPICalls, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":200,"data":{"is_dir":false,"sign":"sig"}}`))
		case "/d/movie.mkv":
			if r.URL.Query().Get("sign") != "sig" {
				t.Fatalf("missing sign: %s", r.URL.RawQuery)
			}
			switch r.Method {
			case http.MethodHead:
				w.Header().Set("Content-Length", "26")
				w.Header().Set("Content-Type", "video/x-matroska")
				w.Header().Set("ETag", `"v1"`)
			case http.MethodGet:
				if got := r.Header.Get("Range"); got != "bytes=0-25" {
					t.Fatalf("origin range = %q", got)
				}
				w.Header().Set("Content-Range", "bytes 0-25/26")
				w.Header().Set("Content-Length", "26")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(originBody)
			default:
				t.Fatalf("method = %s", r.Method)
			}
		default:
			t.Fatalf("unexpected openlist request: %s", r.URL.String())
		}
	}))
	defer openList.Close()

	cfg := testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1")
	cfg.OpenList = config.OpenListConfig{Enabled: true, BaseURL: openList.URL, TimeoutSeconds: 1}
	cfg.DirectOpenList = config.DirectOpenListConfig{Enabled: true, PathPrefix: "/openlist/", Token: "direct-secret"}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/openlist/movie.mkv?token=direct-secret", nil)
		req.Header.Set("Range", "bytes=0-3")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent || rec.Body.String() != "abcd" {
			t.Fatalf("iter=%d code=%d body=%q headers=%v", i, rec.Code, rec.Body.String(), rec.Header())
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 0-3/26" {
			t.Fatalf("content-range=%q", got)
		}
	}
	if got := atomic.LoadInt32(&openListAPICalls); got != 1 {
		t.Fatalf("openListAPICalls=%d", got)
	}
	stats := server.SnapshotStats()
	if stats.Counters.CacheBuild != 1 || stats.Counters.CacheHit != 1 {
		t.Fatalf("counters=%+v", stats.Counters)
	}
}

func TestDirectOpenListEndpointRequiresToken(t *testing.T) {
	cfg := testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1")
	cfg.OpenList = config.OpenListConfig{Enabled: true, BaseURL: "http://openlist.local", TimeoutSeconds: 1}
	cfg.DirectOpenList = config.DirectOpenListConfig{Enabled: true, PathPrefix: "/openlist/", Token: "direct-secret"}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	req := httptest.NewRequest(http.MethodGet, "/openlist/movie.mkv", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("code=%d body=%q", rec.Code, rec.Body.String())
	}
	if stats := server.SnapshotStats().Counters; stats.Denied != 1 || stats.ProxyErrors != 0 {
		t.Fatalf("counters=%+v", stats)
	}
}

func TestCachedOpenListHeadRangeSkipsOpenListResolve(t *testing.T) {
	var openListAPICalls int32
	originBody := []byte("abcdefghijklmnopqrstuvwxyz")
	openList := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/fs/get":
			atomic.AddInt32(&openListAPICalls, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":200,"data":{"is_dir":false,"sign":"sig"}}`))
		case "/d/movie.mkv":
			if r.URL.Query().Get("sign") != "sig" {
				t.Fatalf("missing sign: %s", r.URL.RawQuery)
			}
			switch r.Method {
			case http.MethodHead:
				w.Header().Set("Content-Length", "26")
				w.Header().Set("Content-Type", "video/x-matroska")
				w.Header().Set("ETag", `"v1"`)
			case http.MethodGet:
				if got := r.Header.Get("Range"); got != "bytes=0-25" {
					t.Fatalf("origin range = %q", got)
				}
				w.Header().Set("Content-Range", "bytes 0-25/26")
				w.Header().Set("Content-Length", "26")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(originBody)
			default:
				t.Fatalf("method = %s", r.Method)
			}
		default:
			t.Fatalf("unexpected openlist request: %s", r.URL.String())
		}
	}))
	defer openList.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"openlist:///movie.mkv","Protocol":"Http","Size":26}]}`))
	}))
	defer emby.Close()

	cfg := testConfig(t, emby.URL, emby.URL)
	cfg.OpenList = config.OpenListConfig{Enabled: true, BaseURL: openList.URL, TimeoutSeconds: 1}
	cfg.Rollout.PathPrefixAllowlist = []string{openList.URL + "/"}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
		req.Header.Set("Range", "bytes=0-9")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent || rec.Body.String() != "abcdefghij" {
			t.Fatalf("iter=%d code=%d body=%q headers=%v", i, rec.Code, rec.Body.String(), rec.Header())
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 0-9/26" {
			t.Fatalf("content-range=%q", got)
		}
	}
	if got := atomic.LoadInt32(&openListAPICalls); got != 1 {
		t.Fatalf("openListAPICalls=%d", got)
	}
}

func TestAuthorizedNoRangeBuildsAndHitsHeadCache(t *testing.T) {
	originGets := 0
	originBody := strings.Repeat("0123456789", 10)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			originGets++
			if r.Header.Get("Range") != "bytes=0-15" {
				t.Fatalf("origin range = %q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Range", "bytes 0-15/100")
			w.Header().Set("Content-Length", "16")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(originBody[:16]))
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http"}]}`))
	}))
	defer emby.Close()

	cfg := testConfig(t, emby.URL, emby.URL)
	cfg.Cache.HeadBytes = 16
	cfg.Cache.TailBytes = 8
	cfg.Cache.ChunkBytes = 8
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent || rec.Body.String() != originBody[:16] {
			t.Fatalf("iter=%d code=%d body=%q headers=%v", i, rec.Code, rec.Body.String(), rec.Header())
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 0-15/100" {
			t.Fatalf("content-range=%q", got)
		}
	}
	if originGets != 1 {
		t.Fatalf("originGets=%d", originGets)
	}
}

func TestAuthorizedOpenMiddleRangeStreamsToEOF(t *testing.T) {
	originGets := 0
	originBody := strings.Repeat("0123456789", 10)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			originGets++
			if r.Header.Get("Range") != "bytes=16-99" {
				t.Fatalf("origin range = %q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Range", "bytes 16-99/100")
			w.Header().Set("Content-Length", "84")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(originBody[16:]))
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http"}]}`))
	}))
	defer emby.Close()

	cfg := testConfig(t, emby.URL, emby.URL)
	cfg.Cache.HeadBytes = 16
	cfg.Cache.TailBytes = 8
	cfg.Cache.DefaultOpenRangeBytes = 20
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
	req.Header.Set("Range", "bytes=16-")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent || rec.Body.String() != originBody[16:] {
		t.Fatalf("code=%d body=%q headers=%v", rec.Code, rec.Body.String(), rec.Header())
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes 16-99/100" {
		t.Fatalf("content-range=%q", got)
	}
	if originGets != 1 {
		t.Fatalf("originGets=%d", originGets)
	}
}

func TestUnsatisfiablePlaybackRangeReturns416WithoutFallback(t *testing.T) {
	originGets := 0
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			originGets++
			http.Error(w, "origin should not be read", http.StatusInternalServerError)
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mp4","Protocol":"Http"}]}`))
	}))
	defer emby.Close()
	fallbackHits := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits++
		http.Error(w, "fallback should not be used", http.StatusInternalServerError)
	}))
	defer fallback.Close()

	server, err := New(testConfig(t, emby.URL, fallback.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mp4?MediaSourceId=ms1&api_key=user-token", nil)
	req.Header.Set("Range", "bytes=100-")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("code=%d body=%q headers=%v", rec.Code, rec.Body.String(), rec.Header())
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes */100" {
		t.Fatalf("content-range=%q", got)
	}
	if fallbackHits != 0 {
		t.Fatalf("fallbackHits=%d", fallbackHits)
	}
	if originGets != 0 {
		t.Fatalf("originGets=%d", originGets)
	}
	stats := server.SnapshotStats()
	if stats.Counters.Fallback != 0 {
		t.Fatalf("fallback counter=%d", stats.Counters.Fallback)
	}
}

func TestUnsatisfiablePlaybackRangeWithCachedMetadataReturns416WithoutFallback(t *testing.T) {
	originHeads := 0
	originGets := 0
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			originHeads++
			w.Header().Set("Content-Length", "100")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			originGets++
			http.Error(w, "origin should not be read", http.StatusInternalServerError)
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mp4","Protocol":"Http"}]}`))
	}))
	defer emby.Close()
	fallbackHits := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits++
		http.Error(w, "fallback should not be used", http.StatusInternalServerError)
	}))
	defer fallback.Close()

	server, err := New(testConfig(t, emby.URL, fallback.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	warmReq := httptest.NewRequest(http.MethodHead, "/emby/videos/10535/original.mp4?MediaSourceId=ms1&api_key=user-token", nil)
	warmReq.Header.Set("Range", "bytes=0-0")
	warmRec := httptest.NewRecorder()
	server.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusPartialContent {
		t.Fatalf("warm code=%d headers=%v", warmRec.Code, warmRec.Header())
	}
	if originHeads != 1 {
		t.Fatalf("originHeads after warm=%d", originHeads)
	}

	req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mp4?MediaSourceId=ms1&api_key=user-token", nil)
	req.Header.Set("Range", "bytes=100-")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("code=%d body=%q headers=%v", rec.Code, rec.Body.String(), rec.Header())
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes */100" {
		t.Fatalf("content-range=%q", got)
	}
	if fallbackHits != 0 {
		t.Fatalf("fallbackHits=%d", fallbackHits)
	}
	if originGets != 0 {
		t.Fatalf("originGets=%d", originGets)
	}
	if originHeads != 1 {
		t.Fatalf("originHeads=%d", originHeads)
	}
}

func TestPlaybackAuthorizationCacheReusesSuccessfulResult(t *testing.T) {
	var authRequests int32
	originBody := []byte("abcdefghijklmnopqrstuvwxyz")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "26")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			w.Header().Set("Content-Range", "bytes 0-25/26")
			w.Header().Set("Content-Length", "26")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(originBody)
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&authRequests, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http","Size":26}]}`))
	}))
	defer emby.Close()

	server, err := New(testConfig(t, emby.URL, emby.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
		req.Header.Set("Range", "bytes=0-9")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusPartialContent || rec.Body.String() != "abcdefghij" {
			t.Fatalf("iter=%d code=%d body=%q", i, rec.Code, rec.Body.String())
		}
	}
	if got := atomic.LoadInt32(&authRequests); got != 1 {
		t.Fatalf("authRequests=%d", got)
	}
}

func TestProxyAccessLogIncludesTimingAndRedactsToken(t *testing.T) {
	var logs bytes.Buffer
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "26")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			w.Header().Set("Content-Range", "bytes 0-25/26")
			w.Header().Set("Content-Length", "26")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http"}]}`))
	}))
	defer emby.Close()

	server, err := New(testConfig(t, emby.URL, emby.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
	req.Header.Set("Range", "bytes=0-9")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("code=%d body=%q", rec.Code, rec.Body.String())
	}

	body := logs.String()
	for _, want := range []string{"event=access", "method=GET", "status=206", "item_id=10535", "media_source_id=ms1", "range=\"bytes=0-9\"", "auth_ms=", "duration_ms=", "bytes=10"} {
		if !strings.Contains(body, want) {
			t.Fatalf("access log missing %q in:\n%s", want, body)
		}
	}
	if strings.Contains(body, "user-token") {
		t.Fatalf("access log leaked token:\n%s", body)
	}
}

func TestConcurrentHeadRangeRequestsShareInProgressBuild(t *testing.T) {
	var originGets int32
	firstGetStarted := make(chan struct{})
	releaseOrigin := make(chan struct{})
	originBody := strings.Repeat("x", 64)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "64")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			if got := r.Header.Get("Range"); got != "bytes=0-63" {
				t.Errorf("origin range = %q", got)
			}
			if atomic.AddInt32(&originGets, 1) == 1 {
				close(firstGetStarted)
			}
			<-releaseOrigin
			w.Header().Set("Content-Range", "bytes 0-63/64")
			w.Header().Set("Content-Length", "64")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(originBody))
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http"}]}`))
	}))
	defer emby.Close()

	cfg := testConfig(t, emby.URL, emby.URL)
	cfg.Cache.ChunkBytes = 8
	cfg.Cache.BuildWaitSeconds = 2
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	type response struct {
		code int
		body string
	}
	requestRange := func(done chan<- response) {
		req := httptest.NewRequest(http.MethodGet, "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user-token", nil)
		req.Header.Set("Range", "bytes=0-15")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		done <- response{code: rec.Code, body: rec.Body.String()}
	}

	done1 := make(chan response, 1)
	done2 := make(chan response, 1)
	go requestRange(done1)
	select {
	case <-firstGetStarted:
	case <-time.After(time.Second):
		t.Fatal("first origin GET did not start")
	}
	go requestRange(done2)
	time.Sleep(100 * time.Millisecond)
	close(releaseOrigin)

	for i, done := range []chan response{done1, done2} {
		select {
		case got := <-done:
			if got.code != http.StatusPartialContent || got.body != strings.Repeat("x", 16) {
				t.Fatalf("response %d code=%d body=%q", i+1, got.code, got.body)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("response %d did not complete", i+1)
		}
	}
	if got := atomic.LoadInt32(&originGets); got != 1 {
		t.Fatalf("originGets=%d", got)
	}
}

func TestInternalPrewarmQueuesHeadTail(t *testing.T) {
	originBody := strings.Repeat("x", 32)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "32")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			w.Header().Set("Content-Range", "bytes 0-31/32")
			w.Header().Set("Content-Length", "32")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(originBody))
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http"}]}`))
	}))
	defer emby.Close()
	server, err := New(testConfig(t, emby.URL, emby.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	req := httptest.NewRequest(http.MethodPost, "/internal/prewarm", strings.NewReader(`{"itemId":"10535","mediaSourceId":"ms1"}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("X-Range-Cache-Prewarm-Key", "internal-secret")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if server.SnapshotStats().Prewarm.Completed > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("prewarm did not complete, stats=%+v", server.SnapshotStats().Prewarm)
}

func TestInternalPrewarmAlreadyCachedDoesNotRecordError(t *testing.T) {
	originBody := strings.Repeat("x", 32)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "32")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			w.Header().Set("Content-Range", "bytes 0-31/32")
			w.Header().Set("Content-Length", "32")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(originBody))
		}
	}))
	defer origin.Close()
	emby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"` + origin.URL + `/movie.mkv","Protocol":"Http"}]}`))
	}))
	defer emby.Close()
	server, err := New(testConfig(t, emby.URL, emby.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	queuePrewarm := func() {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/internal/prewarm", strings.NewReader(`{"itemId":"10535","mediaSourceId":"ms1"}`))
		req.RemoteAddr = "127.0.0.1:12345"
		req.Header.Set("X-Range-Cache-Prewarm-Key", "internal-secret")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	}
	waitFor := func(done func(Stats) bool) Stats {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			stats := server.SnapshotStats()
			if done(stats) {
				return stats
			}
			time.Sleep(10 * time.Millisecond)
		}
		return server.SnapshotStats()
	}

	queuePrewarm()
	stats := waitFor(func(stats Stats) bool { return stats.Prewarm.Completed == 1 })
	if stats.Prewarm.Completed != 1 {
		t.Fatalf("first prewarm did not complete, stats=%+v", stats.Prewarm)
	}
	queuePrewarm()
	stats = waitFor(func(stats Stats) bool { return stats.Prewarm.Skipped == 1 })
	if stats.Prewarm.Skipped != 1 {
		t.Fatalf("second prewarm did not skip cached item, stats=%+v", stats.Prewarm)
	}
	if len(stats.RecentErrors) != 0 {
		t.Fatalf("cached prewarm recorded errors: %+v", stats.RecentErrors)
	}
}

func TestInternalPrewarmRejectsNonLoopbackRemote(t *testing.T) {
	server, err := New(testConfig(t, "http://127.0.0.1:1", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	req := httptest.NewRequest(http.MethodPost, "/internal/prewarm", strings.NewReader(`{"itemId":"10535","mediaSourceId":"ms1"}`))
	req.RemoteAddr = "203.0.113.10:12345"
	req.Header.Set("X-Range-Cache-Prewarm-Key", "internal-secret")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if stats := server.SnapshotStats().Prewarm; stats.Queued != 0 || stats.Running != 0 || stats.Completed != 0 {
		t.Fatalf("prewarm unexpectedly queued: %+v", stats)
	}
}
