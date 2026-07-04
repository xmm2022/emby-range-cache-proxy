package prefetch

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/cache"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/middle"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/state"
)

func TestPlanMiddleRangesSkipsHeadTailAndUsesOverlap(t *testing.T) {
	cfg := config.PrefetchConfig{WindowBytes: 256, ResumeOverlapBytes: 64, MaxSessionBytes: 512}
	mid := config.MiddleCacheConfig{SegmentBytes: 64}
	got := PlanMiddleRanges(1000, 512, 100, 700, nil, cfg, mid)
	want := []model.ByteRange{
		{Start: 576, End: 639},
		{Start: 640, End: 703},
		{Start: 704, End: 767},
		{Start: 768, End: 831},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %+v want %+v", got, want)
		}
	}
}

func TestEnqueuePrefetchForSessionDeduplicatesReusableRanges(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	session := state.PlaybackSessionRecord{
		SessionHash:       state.HashIdentifier("session"),
		ItemID:            "item",
		MediaSourceID:     "ms",
		CacheKey:          strings.Repeat("a", 64),
		MediaSize:         100 * 1024 * 1024,
		LastRangeEnd:      20 * 1024 * 1024,
		Status:            "idle",
		MaxObservedOffset: 300,
	}
	cfg := config.PrefetchConfig{WindowBytes: 4 * 1024 * 1024, ResumeOverlapBytes: 1024 * 1024, MaxSessionBytes: 8 * 1024 * 1024, MaxQueueDepth: 10}
	mid := config.MiddleCacheConfig{SegmentBytes: 1024 * 1024}
	inserted, err := EnqueueForSession(store, session, cfg, mid, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	again, err := EnqueueForSession(store, session, cfg, mid, 11, 10)
	if err != nil {
		t.Fatal(err)
	}
	if inserted != 4 || again != 0 {
		t.Fatalf("inserted=%d again=%d", inserted, again)
	}
}

func TestWorkerFetchesClaimedTaskIntoMiddleCache(t *testing.T) {
	body := []byte("0123456789")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			if r.Header.Get("Range") != "bytes=10-19" {
				t.Fatalf("Range = %q", r.Header.Get("Range"))
			}
			w.Header().Set("Content-Range", "bytes 10-19/100")
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body)
		default:
			t.Fatalf("method = %s", r.Method)
		}
	}))
	defer srv.Close()

	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	metadata := model.SourceMetadata{URL: srv.URL, Size: 100, ETag: `"v1"`}
	key := cache.Key(model.MediaSource{ItemID: "item", MediaSourceID: "ms", Path: srv.URL, Protocol: "Http"}, metadata)
	if err := store.UpsertSourceMetadata("item", "ms", key, srv.URL, "sig", 100, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueuePrefetchTask("item", "ms", key, 10, 19, 10, 2, 10); err != nil {
		t.Fatal(err)
	}
	midCache := middle.NewCache(root, store, 1024*1024, 60)
	worker := NewWorker(config.PrefetchConfig{
		Enabled:                 true,
		Concurrency:             1,
		PerOriginConcurrency:    1,
		BandwidthBytesPerSecond: 1024 * 1024,
		ErrorBackoffSeconds:     30,
	}, config.CacheConfig{ChunkBytes: 4}, store, midCache)
	result, err := worker.RunOnce(3)
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 1 || result.Failed != 0 || result.Skipped != 0 {
		t.Fatalf("result = %+v", result)
	}
	chunks, err := midCache.IterBlock(key, model.ByteRange{Start: 10, End: 19}, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for chunk := range chunks {
		_, _ = io.WriteString(&got, string(chunk))
	}
	if got.String() != string(body) {
		t.Fatalf("got %q", got.String())
	}
}

func TestWorkerRunningHookTracksClaimedTasks(t *testing.T) {
	body := []byte("0123456789")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("ETag", `"v1"`)
		case http.MethodGet:
			w.Header().Set("Content-Range", "bytes 10-19/100")
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body)
		default:
			t.Fatalf("method = %s", r.Method)
		}
	}))
	defer srv.Close()

	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	metadata := model.SourceMetadata{URL: srv.URL, Size: 100, ETag: `"v1"`}
	key := cache.Key(model.MediaSource{ItemID: "item", MediaSourceID: "ms", Path: srv.URL, Protocol: "Http"}, metadata)
	if err := store.UpsertSourceMetadata("item", "ms", key, srv.URL, "sig", 100, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueuePrefetchTask("item", "ms", key, 10, 19, 10, 2, 10); err != nil {
		t.Fatal(err)
	}
	midCache := middle.NewCache(root, store, 1024*1024, 60)
	worker := NewWorker(config.PrefetchConfig{
		Enabled:                 true,
		Concurrency:             1,
		PerOriginConcurrency:    1,
		BandwidthBytesPerSecond: 1024 * 1024,
		ErrorBackoffSeconds:     30,
	}, config.CacheConfig{ChunkBytes: 4}, store, midCache)
	var deltas []int
	worker.RunningHook = func(delta int) {
		deltas = append(deltas, delta)
	}
	if _, err := worker.RunOnce(3); err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 2 || deltas[0] != 1 || deltas[1] != -1 {
		t.Fatalf("deltas = %+v", deltas)
	}
}
