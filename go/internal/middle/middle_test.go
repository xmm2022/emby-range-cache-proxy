package middle

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/state"
)

func newTestCache(t *testing.T) (*Cache, *state.Store) {
	t.Helper()
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state", "phase2.sqlite3"))
	if err != nil {
		t.Fatalf("state open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewCache(root, store, 1024*1024, 60), store
}

func TestStorePrefetchBlockStreamsAndPublishesPythonLayout(t *testing.T) {
	cache, store := newTestCache(t)
	key := strings.Repeat("c", 64)
	task, err := store.EnqueuePrefetchTask("item", "ms", key, 1024, 1033, 10, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimPrefetchTasks(1, 2, 300)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := cache.StorePrefetchBlockFromReader(claimed[0].ID, claimed[0].Attempts, key, model.ByteRange{Start: 1024, End: 1033}, bytes.NewBufferString("0123456789"), 3)
	if err != nil {
		t.Fatalf("store error: %v", err)
	}
	if !ok || task == nil {
		t.Fatalf("stored=%v task=%+v", ok, task)
	}
	path := filepath.Join(cache.Root, key, "mid", "1024-1033.bin")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "0123456789" {
		t.Fatalf("data = %q", data)
	}
	sidecar, err := os.ReadFile(filepath.Join(cache.Root, key, "mid", "1024-1033.range"))
	if err != nil {
		t.Fatal(err)
	}
	if string(sidecar) != "1024-1033\n" {
		t.Fatalf("sidecar = %q", sidecar)
	}
}

func TestIterBlockRequiresFullCoverageAndRemovesBrokenFiles(t *testing.T) {
	cache, _ := newTestCache(t)
	key := strings.Repeat("d", 64)
	if err := cache.StoreBlock(key, model.ByteRange{Start: 100, End: 109}, []byte("abcdefghij"), 10); err != nil {
		t.Fatal(err)
	}
	chunks, err := cache.IterBlock(key, model.ByteRange{Start: 102, End: 106}, 2, 11)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	for chunk := range chunks {
		got.Write(chunk)
	}
	if got.String() != "cdefg" {
		t.Fatalf("got %q", got.String())
	}
	chunks, err = cache.IterBlock(key, model.ByteRange{Start: 108, End: 112}, 2, 11)
	if err != nil {
		t.Fatal(err)
	}
	if chunks != nil {
		t.Fatalf("partial coverage should miss")
	}
	if err := os.Truncate(filepath.Join(cache.Root, key, "mid", "100-109.bin"), 3); err != nil {
		t.Fatal(err)
	}
	chunks, err = cache.IterBlock(key, model.ByteRange{Start: 102, End: 106}, 2, 12)
	if err != nil {
		t.Fatal(err)
	}
	if chunks != nil {
		t.Fatalf("truncated file should miss")
	}
	if found, err := cache.Store.FindMiddleBlock(key, model.ByteRange{Start: 102, End: 106}); err != nil || found != nil {
		t.Fatalf("metadata should be removed, found=%+v err=%v", found, err)
	}
}

func TestEvictExpiredAndLRU(t *testing.T) {
	cache, store := newTestCache(t)
	cache.MaxBytes = 15
	if err := cache.StoreBlock(strings.Repeat("e", 64), model.ByteRange{Start: 0, End: 9}, []byte("aaaaaaaaaa"), 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.StoreBlock(strings.Repeat("f", 64), model.ByteRange{Start: 0, End: 9}, []byte("bbbbbbbbbb"), 1); err != nil {
		t.Fatal(err)
	}
	if removed, err := cache.EvictExpired(61); err != nil || removed != 2 {
		t.Fatalf("expired removed=%d err=%v", removed, err)
	}
	if bytes, err := store.MiddleCacheBytes(); err != nil || bytes != 0 {
		t.Fatalf("bytes=%d err=%v", bytes, err)
	}
	if err := cache.StoreBlock(strings.Repeat("a", 64), model.ByteRange{Start: 0, End: 9}, []byte("aaaaaaaaaa"), 100); err != nil {
		t.Fatal(err)
	}
	if err := cache.StoreBlock(strings.Repeat("b", 64), model.ByteRange{Start: 0, End: 9}, []byte("bbbbbbbbbb"), 101); err != nil {
		t.Fatal(err)
	}
	if removed, err := cache.EvictLRUIfNeeded(); err != nil || removed != 1 {
		t.Fatalf("lru removed=%d err=%v", removed, err)
	}
}

func TestStorePrefetchBlockHonorsMinFreeBytes(t *testing.T) {
	cache, store := newTestCache(t)
	cache.MinFreeBytes = 1 << 60
	key := strings.Repeat("9", 64)
	if _, err := store.EnqueuePrefetchTask("item", "ms", key, 0, 3, 10, 1, 10); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimPrefetchTasks(1, 2, 300)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := cache.StorePrefetchBlockFromReader(claimed[0].ID, claimed[0].Attempts, key, model.ByteRange{Start: 0, End: 3}, bytes.NewBufferString("data"), 3)
	if err == nil || ok {
		t.Fatalf("expected min free error, ok=%v err=%v", ok, err)
	}
}
