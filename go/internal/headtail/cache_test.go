package headtail

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestStageBlockPublishesPythonCompatibleFiles(t *testing.T) {
	cache := NewCache(t.TempDir(), 1024*1024)
	key := strings.Repeat("a", 64)
	writer, err := cache.StageBlock(key, "head", model.ByteRange{Start: 0, End: 9})
	if err != nil {
		t.Fatalf("StageBlock error: %v", err)
	}
	if _, err := writer.Write([]byte("0123456789")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("commit error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(cache.Root, key, "head.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "0123456789" {
		t.Fatalf("data = %q", data)
	}
	sidecar, err := os.ReadFile(filepath.Join(cache.Root, key, "head.range"))
	if err != nil {
		t.Fatal(err)
	}
	if string(sidecar) != "0-9\n" {
		t.Fatalf("sidecar = %q", sidecar)
	}
}

func TestIterBlockStreamsSubrangeAndRejectsTruncatedFile(t *testing.T) {
	cache := NewCache(t.TempDir(), 1024*1024)
	key := strings.Repeat("b", 64)
	if err := cache.StoreBlock(key, "tail", model.ByteRange{Start: 90, End: 99}, []byte("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	chunks, err := cache.IterBlock(key, "tail", model.ByteRange{Start: 92, End: 96}, 2)
	if err != nil {
		t.Fatalf("IterBlock error: %v", err)
	}
	var got bytes.Buffer
	for chunk := range chunks {
		got.Write(chunk)
	}
	if got.String() != "cdefg" {
		t.Fatalf("got %q", got.String())
	}

	if err := os.Truncate(filepath.Join(cache.Root, key, "tail.bin"), 3); err != nil {
		t.Fatal(err)
	}
	chunks, err = cache.IterBlock(key, "tail", model.ByteRange{Start: 92, End: 96}, 2)
	if err != nil {
		t.Fatalf("IterBlock after truncate error: %v", err)
	}
	if chunks != nil {
		t.Fatalf("expected truncated block to miss")
	}
	if _, err := os.Stat(filepath.Join(cache.Root, key, "tail.bin")); !os.IsNotExist(err) {
		t.Fatalf("expected truncated block to be removed, stat err=%v", err)
	}
}

func TestCacheBlockForRequestRequiresFullContainment(t *testing.T) {
	block, blockRange := BlockForRequest(model.ByteRange{Start: 0, End: 10}, 100, 16, 8)
	if block != "head" || blockRange != (model.ByteRange{Start: 0, End: 15}) {
		t.Fatalf("head block = %q %+v", block, blockRange)
	}
	block, blockRange = BlockForRequest(model.ByteRange{Start: 95, End: 99}, 100, 16, 8)
	if block != "tail" || blockRange != (model.ByteRange{Start: 92, End: 99}) {
		t.Fatalf("tail block = %q %+v", block, blockRange)
	}
	block, _ = BlockForRequest(model.ByteRange{Start: 10, End: 20}, 100, 16, 8)
	if block != "" {
		t.Fatalf("partial overlap should miss, got %q", block)
	}
}
