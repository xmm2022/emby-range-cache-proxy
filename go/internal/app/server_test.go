package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
)

func testConfig(t *testing.T, embyBase, fallbackBase string) config.Config {
	t.Helper()
	return config.Config{
		EmbyBaseURL:     embyBase,
		FallbackBaseURL: fallbackBase,
		ListenHost:      "127.0.0.1",
		ListenPort:      18180,
		CacheDir:        filepath.Join(t.TempDir(), "cache"),
		PrewarmAPIKey:   "internal-secret",
		Rollout: config.RolloutConfig{
			Enabled:              true,
			ItemAllowlist:        map[string]struct{}{"10535": {}},
			MediaSourceAllowlist: map[string]struct{}{"ms1": {}},
			PathPrefixAllowlist:  []string{"http://127.0.0.1:"},
		},
		Cache: config.CacheConfig{
			MaxBytes:              1024 * 1024,
			BuildWaitSeconds:      0.01,
			ChunkBytes:            4,
			DefaultOpenRangeBytes: 16,
		},
		Prewarm: config.PrewarmConfig{IntervalSeconds: 900, MaxItemsPerScan: 100, Concurrency: 1},
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
