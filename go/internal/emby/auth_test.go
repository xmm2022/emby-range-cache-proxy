package emby

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestAuthorizeSelectsExactMediaSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Items/123/PlaybackInfo" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("MediaSourceId") != "ms2" || r.URL.Query().Get("api_key") != "user-token" {
			t.Fatalf("query = %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaSources":[{"Id":"ms1","Path":"http://wrong"},{"Id":"ms2","Path":"http://origin/movie.mkv","Protocol":"Http","Size":"123","Container":"mkv","Bitrate":456}]}`))
	}))
	defer srv.Close()

	client := NewAuthClient(srv.URL)
	source, err := client.Authorize(model.RequestContext{ItemID: "123", MediaSourceID: "ms2", Token: "user-token"})
	if err != nil {
		t.Fatalf("Authorize error: %v", err)
	}
	if source.Path != "http://origin/movie.mkv" || source.MediaSourceID != "ms2" || source.Size == nil || *source.Size != 123 {
		t.Fatalf("source = %+v", source)
	}
}

func TestNewAuthClientWithTimeoutConfiguresHTTPTimeout(t *testing.T) {
	client := NewAuthClientWithTimeout("http://emby.local/", 17*time.Second)
	if client.BaseURL != "http://emby.local" {
		t.Fatalf("BaseURL = %q", client.BaseURL)
	}
	if client.HTTP.Timeout != 17*time.Second {
		t.Fatalf("timeout = %s", client.HTTP.Timeout)
	}
}

func TestAuthorizeRejectsForbiddenAndMalformed(t *testing.T) {
	for _, body := range []string{`not-json`, `{"MediaSources":[]}`, `{"MediaSources":[{"Id":"ms","Path":""}]}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		_, err := NewAuthClient(srv.URL).Authorize(model.RequestContext{ItemID: "1", MediaSourceID: "ms", Token: "t"})
		srv.Close()
		if err == nil {
			t.Fatalf("expected error for body %q", body)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()
	if _, err := NewAuthClient(srv.URL).Authorize(model.RequestContext{ItemID: "1", MediaSourceID: "ms", Token: "t"}); err == nil {
		t.Fatalf("expected forbidden error")
	}
}
