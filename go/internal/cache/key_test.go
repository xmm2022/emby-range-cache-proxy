package cache

import (
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestCacheKeyMatchesPythonMaterial(t *testing.T) {
	source := model.MediaSource{MediaSourceID: "ms1"}
	meta := model.SourceMetadata{
		URL:          "http://127.0.0.1:18096/movie.mkv?sig=abc",
		Size:         12345,
		ETag:         `"etag"`,
		LastModified: "Sat, 04 Jul 2026 01:02:03 GMT",
	}
	got := Key(source, meta)
	want := "35aa7b842fc74339ae2f1d032a6697b1abf8edcbc7a6c363e28e1c5b93292b73"
	if got != want {
		t.Fatalf("key = %s, want %s", got, want)
	}
}
