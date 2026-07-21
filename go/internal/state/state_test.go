package state

import (
	"path/filepath"
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "phase2.sqlite3"))
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestOpenCreatesPythonCompatibleSchema(t *testing.T) {
	store := openTestStore(t)
	for _, table := range []string{"playback_sessions", "prefetch_tasks", "middle_blocks", "source_metadata", "runtime_settings"} {
		var name string
		err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
		if err != nil {
			t.Fatalf("missing table %s: %v", table, err)
		}
	}

	if err := store.SetRuntimeSetting("cache_mode", "bypass", 123); err != nil {
		t.Fatal(err)
	}
	value, ok, err := store.RuntimeSetting("cache_mode")
	if err != nil || !ok || value != "bypass" {
		t.Fatalf("runtime setting value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestRecordPlaybackUpsertsAndKeepsMaxObservedOffset(t *testing.T) {
	store := openTestStore(t)
	update := PlaybackSessionUpdate{
		SessionHash:     HashIdentifier("play-session"),
		DeviceHash:      HashIdentifier("device"),
		ItemID:          "item",
		MediaSourceID:   "ms",
		CacheKey:        "key",
		OriginSignature: "sig",
		MediaSize:       1000,
		ByteRange:       model.ByteRange{Start: 100, End: 200},
		ObservedAt:      10,
	}
	if err := store.RecordPlayback(update); err != nil {
		t.Fatalf("RecordPlayback error: %v", err)
	}
	update.ByteRange = model.ByteRange{Start: 50, End: 60}
	update.ObservedAt = 20
	if err := store.RecordPlayback(update); err != nil {
		t.Fatalf("RecordPlayback update error: %v", err)
	}

	session, err := store.GetSession(HashIdentifier("play-session"))
	if err != nil {
		t.Fatalf("GetSession error: %v", err)
	}
	if session.LastRangeStart != 50 || session.LastRangeEnd != 60 || session.MaxObservedOffset != 200 || session.Status != "active" {
		t.Fatalf("session = %+v", session)
	}
}

func TestIdleStoppedAndPrefetchTaskLifecycle(t *testing.T) {
	store := openTestStore(t)
	update := PlaybackSessionUpdate{
		SessionHash:     HashIdentifier("session"),
		ItemID:          "item",
		MediaSourceID:   "ms",
		CacheKey:        "cache",
		OriginSignature: "sig",
		MediaSize:       1000,
		ByteRange:       model.ByteRange{Start: 400, End: 500},
		ObservedAt:      10,
	}
	if err := store.RecordPlayback(update); err != nil {
		t.Fatal(err)
	}
	idle, err := store.MarkIdleSessions(200, 60)
	if err != nil {
		t.Fatal(err)
	}
	if len(idle) != 1 || idle[0].Status != "idle" {
		t.Fatalf("idle = %+v", idle)
	}
	if err := store.RecordObservedSessions(map[string]struct{}{update.SessionHash: {}}, 220); err != nil {
		t.Fatal(err)
	}
	stopped, err := store.MarkMissingObservedSessionsStopped(300, 60)
	if err != nil {
		t.Fatal(err)
	}
	if len(stopped) != 1 || stopped[0].Status != "stopped" {
		t.Fatalf("stopped = %+v", stopped)
	}

	task, err := store.EnqueuePrefetchTask("item", "ms", "cache", 512, 575, 20, 300, 10)
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}
	dup, err := store.EnqueuePrefetchTask("item", "ms", "cache", 512, 575, 20, 301, 10)
	if err != nil {
		t.Fatalf("dup enqueue error: %v", err)
	}
	if task == nil || dup != nil {
		t.Fatalf("task=%+v dup=%+v", task, dup)
	}
	claimed, err := store.ClaimPrefetchTasks(1, 310, 300)
	if err != nil {
		t.Fatalf("claim error: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Attempts != 1 || claimed[0].Status != "running" {
		t.Fatalf("claimed = %+v", claimed)
	}
	if err := store.FailPrefetchTask(claimed[0].ID, "OriginError", 320, 30, claimed[0].Attempts); err != nil {
		t.Fatalf("fail error: %v", err)
	}
	early, err := store.ClaimPrefetchTasks(1, 349, 300)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.ClaimPrefetchTasks(1, 350, 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(early) != 0 || len(retry) != 1 || retry[0].Attempts != 2 {
		t.Fatalf("early=%+v retry=%+v", early, retry)
	}
}

func TestMiddleBlockAndSourceMetadataLifecycle(t *testing.T) {
	store := openTestStore(t)
	if err := store.UpsertSourceMetadata("item", "ms", "cache", "http://origin/movie.mkv", "sig", 1000, 10); err != nil {
		t.Fatal(err)
	}
	meta, err := store.GetSourceMetadata("item", "ms", "cache")
	if err != nil {
		t.Fatal(err)
	}
	if meta == nil || meta.OriginURL != "http://origin/movie.mkv" {
		t.Fatalf("meta = %+v", meta)
	}
	block := MiddleBlockRecord{CacheKey: "cache", Start: 512, End: 575, Path: "cache/mid/512-575.bin", Size: 64, CreatedAt: 20, LastAccessAt: 20, ExpiresAt: 120}
	if err := store.UpsertMiddleBlock(block); err != nil {
		t.Fatal(err)
	}
	found, err := store.FindMiddleBlock("cache", model.ByteRange{Start: 520, End: 530})
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.Start != 512 || found.End != 575 {
		t.Fatalf("found = %+v", found)
	}
	if bytes, err := store.MiddleCacheBytes(); err != nil || bytes != 64 {
		t.Fatalf("bytes=%d err=%v", bytes, err)
	}
}

func TestFindMiddleBlocksReturnsOverlappingBlocksInOrder(t *testing.T) {
	store := openTestStore(t)
	blocks := []MiddleBlockRecord{
		{CacheKey: "cache", Start: 120, End: 129, Path: "cache/mid/120-129.bin", Size: 10, CreatedAt: 20, LastAccessAt: 20, ExpiresAt: 120},
		{CacheKey: "other", Start: 110, End: 119, Path: "other/mid/110-119.bin", Size: 10, CreatedAt: 20, LastAccessAt: 20, ExpiresAt: 120},
		{CacheKey: "cache", Start: 100, End: 109, Path: "cache/mid/100-109.bin", Size: 10, CreatedAt: 20, LastAccessAt: 20, ExpiresAt: 120},
		{CacheKey: "cache", Start: 110, End: 119, Path: "cache/mid/110-119.bin", Size: 10, CreatedAt: 20, LastAccessAt: 20, ExpiresAt: 120},
	}
	for _, block := range blocks {
		if err := store.UpsertMiddleBlock(block); err != nil {
			t.Fatal(err)
		}
	}

	found, err := store.FindMiddleBlocks("cache", model.ByteRange{Start: 105, End: 124})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 {
		t.Fatalf("len(found) = %d, found=%+v", len(found), found)
	}
	for i, wantStart := range []int64{100, 110, 120} {
		if found[i].Start != wantStart {
			t.Fatalf("found[%d].Start = %d, want %d", i, found[i].Start, wantStart)
		}
	}
}
