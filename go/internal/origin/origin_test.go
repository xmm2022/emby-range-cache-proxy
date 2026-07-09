package origin

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestHeadFollowsRedirectAndReadsValidators(t *testing.T) {
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s", r.Method)
		}
		w.Header().Set("Content-Length", "123")
		w.Header().Set("Content-Type", "video/x-matroska")
		w.Header().Set("ETag", `"abc"`)
		w.Header().Set("Last-Modified", "Sat, 04 Jul 2026 01:02:03 GMT")
	}))
	defer final.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL+"/movie.mkv", http.StatusFound)
	}))
	defer redirect.Close()

	client := NewClient(1024)
	meta, err := client.Head(redirect.URL + "/redirect")
	if err != nil {
		t.Fatalf("Head error: %v", err)
	}
	if meta.URL != final.URL+"/movie.mkv" || meta.Size != 123 || meta.ETag != `"abc"` {
		t.Fatalf("metadata = %+v", meta)
	}
}

func TestHeadFallsBackToRangeProbeWhenHeadForbidden(t *testing.T) {
	var methods []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		switch r.Method {
		case http.MethodHead:
			http.Error(w, "head is forbidden", http.StatusForbidden)
		case http.MethodGet:
			if got := r.Header.Get("Range"); got != "bytes=0-0" {
				t.Fatalf("Range = %q", got)
			}
			w.Header().Set("Content-Range", "bytes 0-0/456")
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Content-Type", "video/x-matroska")
			w.Header().Set("ETag", `"range-etag"`)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("x"))
		default:
			t.Fatalf("method = %s", r.Method)
		}
	}))
	defer srv.Close()

	meta, err := NewClient(1024).Head(srv.URL + "/movie.mkv")
	if err != nil {
		t.Fatalf("Head error: %v", err)
	}
	if meta.Size != 456 || meta.ContentType != "video/x-matroska" || meta.ETag != `"range-etag"` {
		t.Fatalf("metadata = %+v", meta)
	}
	if fmt.Sprint(methods) != "[HEAD GET]" {
		t.Fatalf("methods = %v", methods)
	}
}

func TestOpenRangeRequires206AndMatchingContentRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=10-19" {
			t.Fatalf("Range = %q", got)
		}
		w.Header().Set("Content-Range", "bytes 10-19/100")
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer srv.Close()

	client := NewClient(4)
	resp, err := client.OpenRange(srv.URL, model.ByteRange{Start: 10, End: 19}, 100)
	if err != nil {
		t.Fatalf("OpenRange error: %v", err)
	}
	defer resp.Close()
	body, err := io.ReadAll(resp)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "0123456789" {
		t.Fatalf("body = %q", body)
	}
}

func TestOpenRangeSlicesStatusOKBody(t *testing.T) {
	body := "abcdefghijklmnopqrstuvwxyz"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=10-19" {
			t.Fatalf("Range = %q", got)
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, body)
	}))
	defer srv.Close()

	resp, err := NewClient(4).OpenRange(srv.URL, model.ByteRange{Start: 10, End: 19}, int64(len(body)))
	if err != nil {
		t.Fatalf("OpenRange error: %v", err)
	}
	defer resp.Close()
	got, err := io.ReadAll(resp)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "klmnopqrst" {
		t.Fatalf("body = %q", got)
	}
}

func TestOpenRangeRejectsBadOriginResponses(t *testing.T) {
	cases := []struct {
		name   string
		status int
		crange string
	}{
		{"missing content range", http.StatusPartialContent, ""},
		{"mismatched content range", http.StatusPartialContent, "bytes 10-18/100"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.crange != "" {
					w.Header().Set("Content-Range", tc.crange)
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, "bad")
			}))
			defer srv.Close()
			resp, err := NewClient(1024).OpenRange(srv.URL, model.ByteRange{Start: 10, End: 19}, 100)
			if err == nil {
				resp.Close()
				t.Fatalf("expected error")
			}
		})
	}
}
