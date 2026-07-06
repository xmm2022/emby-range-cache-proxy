package session

import (
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestBuildSessionUpdateUsesPlaySessionWhenPresent(t *testing.T) {
	meta := model.SourceMetadata{URL: "http://origin/movie.mkv", Size: 100, ETag: `"v1"`}
	update := BuildUpdate(model.RequestContext{
		ItemID:        "item",
		MediaSourceID: "ms",
		Token:         "user-token",
		PlaySessionID: "play-session",
		DeviceID:      "device",
	}, "cache-key", meta, model.ByteRange{Start: 10, End: 19}, 900)

	if update.SessionHash != Hash("play-session") {
		t.Fatalf("session hash = %s", update.SessionHash)
	}
	if update.DeviceHash != Hash("device") || update.OriginSignature == "" {
		t.Fatalf("update = %+v", update)
	}
}

func TestBuildSessionUpdateSyntheticUsesTimeBucket(t *testing.T) {
	meta := model.SourceMetadata{URL: "http://origin/movie.mkv", Size: 100}
	ctx := model.RequestContext{ItemID: "item", MediaSourceID: "ms", Token: "user-token"}
	a := BuildUpdate(ctx, "cache-key", meta, model.ByteRange{Start: 10, End: 19}, 100)
	b := BuildUpdate(ctx, "cache-key", meta, model.ByteRange{Start: 20, End: 29}, 899)
	if a.SessionHash != b.SessionHash {
		t.Fatalf("same 15m bucket should match: %s %s", a.SessionHash, b.SessionHash)
	}
	c := BuildUpdate(ctx, "cache-key", meta, model.ByteRange{Start: 20, End: 29}, 901)
	if c.SessionHash == a.SessionHash {
		t.Fatalf("different bucket should differ")
	}
}

func TestIsTailMetadataRange(t *testing.T) {
	size := int64(100 * 1024 * 1024)
	if !IsTailMetadataRange(size, model.ByteRange{Start: size - 1024, End: size - 1}, 8*1024*1024, 8*1024*1024) {
		t.Fatalf("expected tail metadata")
	}
	if IsTailMetadataRange(size, model.ByteRange{Start: 20 * 1024 * 1024, End: 21 * 1024 * 1024}, 8*1024*1024, 8*1024*1024) {
		t.Fatalf("middle range should not be tail metadata")
	}
}
