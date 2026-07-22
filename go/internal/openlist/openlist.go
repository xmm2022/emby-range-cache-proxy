package openlist

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

type Resolver struct {
	cfg  config.OpenListConfig
	http *http.Client
}

func NewResolver(cfg config.OpenListConfig) *Resolver {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Resolver{
		cfg: cfg,
		http: &http.Client{
			Timeout: timeout,
		},
	}
}

func (r *Resolver) Resolve(ctx context.Context, source model.MediaSource) model.MediaSource {
	if r == nil || !r.cfg.Enabled {
		return source
	}
	openListPath, ok := PathFromSource(source.Path, r.cfg.BaseURL)
	if !ok {
		return source
	}
	resolvedURL, size, ok := r.resolveURL(ctx, openListPath)
	if !ok {
		return source
	}
	source.Path = resolvedURL
	source.Protocol = "Http"
	if size > 0 {
		source.Size = &size
		source.SizeTrusted = true
	}
	return source
}

func PathFromSource(value, baseURL string) (string, bool) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", false
	}
	if strings.EqualFold(parsed.Scheme, "openlist") {
		rawPath := parsed.EscapedPath()
		if parsed.Host != "" {
			rawPath = "/" + parsed.Host + rawPath
		}
		return normalizePath(rawPath)
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return "", false
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", false
	}
	if !strings.EqualFold(parsed.Scheme, base.Scheme) || !strings.EqualFold(parsed.Host, base.Host) {
		return "", false
	}
	basePath := strings.TrimRight(base.EscapedPath(), "/")
	requestPath := parsed.EscapedPath()
	relative := requestPath
	if basePath != "" {
		if requestPath != basePath && !strings.HasPrefix(requestPath, basePath+"/") {
			return "", false
		}
		relative = strings.TrimPrefix(requestPath, basePath)
	}
	for _, prefix := range []string{"/d", "/p"} {
		if strings.HasPrefix(relative, prefix+"/") {
			return normalizePath(relative[len(prefix):])
		}
	}
	return "", false
}

func (r *Resolver) resolveURL(ctx context.Context, openListPath string) (string, int64, bool) {
	body, err := json.Marshal(map[string]string{
		"path":     openListPath,
		"password": r.cfg.Password,
	})
	if err != nil {
		return "", 0, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(r.cfg.BaseURL, "/")+"/api/fs/get", bytes.NewReader(body))
	if err != nil {
		return "", 0, false
	}
	req.Header.Set("Content-Type", "application/json")
	if r.cfg.Token != "" {
		req.Header.Set("Authorization", r.cfg.Token)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return "", 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", 0, false
	}
	var payload struct {
		Code int `json:"code"`
		Data struct {
			IsDir  bool   `json:"is_dir"`
			Sign   string `json:"sign"`
			RawURL string `json:"raw_url"`
			Size   int64  `json:"size"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", 0, false
	}
	if payload.Code != 0 && payload.Code != http.StatusOK {
		return "", 0, false
	}
	if payload.Data.IsDir {
		return "", 0, false
	}
	if payload.Data.Sign != "" {
		return signedDownloadURL(r.cfg.BaseURL, openListPath, payload.Data.Sign), payload.Data.Size, true
	}
	if payload.Data.RawURL != "" {
		return absoluteOpenListURL(r.cfg.BaseURL, payload.Data.RawURL), payload.Data.Size, true
	}
	return "", 0, false
}

func normalizePath(value string) (string, bool) {
	if value == "" {
		return "", false
	}
	unescaped, err := url.PathUnescape(value)
	if err != nil {
		return "", false
	}
	if !strings.HasPrefix(unescaped, "/") {
		unescaped = "/" + unescaped
	}
	parts := strings.Split(unescaped, "/")
	for _, part := range parts {
		if part == "." || part == ".." {
			return "", false
		}
	}
	cleaned := path.Clean(unescaped)
	if cleaned == "/" {
		return "", false
	}
	return cleaned, true
}

func signedDownloadURL(baseURL, openListPath, sign string) string {
	query := url.Values{}
	query.Set("sign", sign)
	return strings.TrimRight(baseURL, "/") + "/d" + encodePath(openListPath) + "?" + query.Encode()
}

func encodePath(value string) string {
	parts := strings.Split(value, "/")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func absoluteOpenListURL(baseURL, rawURL string) string {
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		return rawURL
	}
	base := strings.TrimRight(baseURL, "/")
	if strings.HasPrefix(rawURL, "/") {
		return base + rawURL
	}
	return base + "/" + rawURL
}
