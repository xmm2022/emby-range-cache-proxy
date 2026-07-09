package source

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestResolveHTTPSourceReturnsOriginal(t *testing.T) {
	src := model.MediaSource{Path: "https://example.invalid/movie.mkv", Protocol: "Http"}
	got := ResolveMediaSource(src, nil, []string{"http://127.0.0.1:18096/"})
	if got.Path != src.Path || got.Protocol != src.Protocol {
		t.Fatalf("got %+v", got)
	}
}

func TestResolveSTRMThroughMappingAndAllowlist(t *testing.T) {
	root := t.TempDir()
	strmPath := filepath.Join(root, "movies", "a.strm")
	if err := os.MkdirAll(filepath.Dir(strmPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(strmPath, []byte("# comment\n\nhttp://127.0.0.1:18096/media/a.mkv\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	src := model.MediaSource{Path: "/strm/movies/a.strm", Protocol: "File"}
	got := ResolveMediaSource(src, []config.PathMapping{{SourcePrefix: "/strm/", TargetPrefix: root}}, []string{"http://127.0.0.1:18096/"})

	if got.Path != "http://127.0.0.1:18096/media/a.mkv" || got.Protocol != "Http" {
		t.Fatalf("got %+v", got)
	}
}

func TestResolveSTRMWithOpenListPseudoURL(t *testing.T) {
	root := t.TempDir()
	strmPath := filepath.Join(root, "movie.strm")
	if err := os.WriteFile(strmPath, []byte("openlist:///Movies/movie.mkv\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	src := model.MediaSource{Path: "/strm/movie.strm", Protocol: "File"}
	got := ResolveMediaSource(src, []config.PathMapping{{SourcePrefix: "/strm/", TargetPrefix: root}}, nil)

	if got.Path != "openlist:///Movies/movie.mkv" || got.Protocol != "OpenList" {
		t.Fatalf("got %+v", got)
	}
}

func TestResolveSTRMRejectsTraversalAndNonAllowlistedURL(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "evil.strm")
	if err := os.WriteFile(outside, []byte("http://127.0.0.1:18096/evil.mkv\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	badURLPath := filepath.Join(root, "bad.strm")
	if err := os.WriteFile(badURLPath, []byte("http://other.invalid/movie.mkv\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	mapping := []config.PathMapping{{SourcePrefix: "/strm/", TargetPrefix: root}}
	traversal := ResolveMediaSource(model.MediaSource{Path: "/strm/../evil.strm"}, mapping, []string{"http://127.0.0.1:18096/"})
	if traversal.Path != "/strm/../evil.strm" {
		t.Fatalf("traversal resolved unexpectedly: %+v", traversal)
	}

	disallowed := ResolveMediaSource(model.MediaSource{Path: "/strm/bad.strm"}, mapping, []string{"http://127.0.0.1:18096/"})
	if disallowed.Path != "/strm/bad.strm" {
		t.Fatalf("disallowed URL resolved unexpectedly: %+v", disallowed)
	}
}
