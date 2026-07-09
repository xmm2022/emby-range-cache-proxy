package openlist

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestPathFromPseudoURL(t *testing.T) {
	got, ok := PathFromSource("openlist:///Movies/a.mkv", "https://openlist.example")
	if !ok || got != "/Movies/a.mkv" {
		t.Fatalf("path = %q ok=%v", got, ok)
	}

	got, ok = PathFromSource("openlist://Movies/a.mkv", "https://openlist.example")
	if !ok || got != "/Movies/a.mkv" {
		t.Fatalf("path = %q ok=%v", got, ok)
	}
}

func TestPathFromDownloadURLWithBasePath(t *testing.T) {
	got, ok := PathFromSource("https://example.test/list/d/Movies/%E7%89%87.mkv?sign=old", "https://example.test/list")
	if !ok || got != "/Movies/片.mkv" {
		t.Fatalf("path = %q ok=%v", got, ok)
	}
}

func TestPathRejectsTraversalAndOtherHost(t *testing.T) {
	if got, ok := PathFromSource("openlist:///Movies/../secret.mkv", "https://openlist.example"); ok {
		t.Fatalf("traversal resolved to %q", got)
	}
	if got, ok := PathFromSource("https://evil.example/d/movie.mkv", "https://openlist.example"); ok {
		t.Fatalf("other host resolved to %q", got)
	}
}

func TestResolveUsesFSGetSign(t *testing.T) {
	var gotAuth string
	var gotPayload map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/fs/get" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200,
			"data": map[string]any{
				"name":    "a.mkv",
				"size":    123,
				"is_dir":  false,
				"sign":    "abc=:0",
				"raw_url": "https://temporary.example/a.mkv",
			},
		})
	}))
	defer server.Close()

	resolver := NewResolver(config.OpenListConfig{Enabled: true, BaseURL: server.URL, Token: "openlist-token", TimeoutSeconds: 10})
	source := model.MediaSource{Path: "openlist:///Movies/a.mkv", Protocol: "OpenList"}

	resolved := resolver.Resolve(context.Background(), source)

	if gotAuth != "openlist-token" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotPayload["path"] != "/Movies/a.mkv" || gotPayload["password"] != "" {
		t.Fatalf("payload = %+v", gotPayload)
	}
	want := server.URL + "/d/Movies/a.mkv?sign=" + url.QueryEscape("abc=:0")
	if resolved.Path != want || resolved.Protocol != "Http" {
		t.Fatalf("resolved = %+v want path %q", resolved, want)
	}
}

func TestResolveFallsBackToRawURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200,
			"data": map[string]any{
				"name":    "a.mkv",
				"size":    123,
				"is_dir":  false,
				"raw_url": "/p/Movies/a.mkv?sign=proxy",
			},
		})
	}))
	defer server.Close()

	resolver := NewResolver(config.OpenListConfig{Enabled: true, BaseURL: server.URL, TimeoutSeconds: 10})
	source := model.MediaSource{Path: server.URL + "/d/Movies/a.mkv?sign=old", Protocol: "Http"}

	resolved := resolver.Resolve(context.Background(), source)

	if resolved.Path != server.URL+"/p/Movies/a.mkv?sign=proxy" {
		t.Fatalf("resolved = %+v", resolved)
	}
}
