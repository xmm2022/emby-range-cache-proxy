package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/cache"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/diskfree"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/emby"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/headtail"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/middle"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/openlist"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/origin"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/prefetch"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/ranges"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/request"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/session"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/source"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/state"
)

type Server struct {
	cfg       config.Config
	startedAt time.Time

	cache  *headtail.Cache
	store  *state.Store
	middle *middle.Cache

	httpClient     *http.Client
	fallbackClient *http.Client
	origin         *origin.Client
	openList       *openlist.Resolver
	auth           *emby.AuthClient
	prewarmAuth    *emby.AuthClient
	authCacheTTL   time.Duration
	authCacheMu    sync.Mutex
	authCache      map[authCacheKey]authCacheEntry

	originSem chan struct{}

	statsMu sync.Mutex
	stats   Stats

	cacheBuildMu    sync.Mutex
	cacheBuildLocks map[string]*cacheBuildLock

	prewarmMu    sync.Mutex
	prewarmTasks map[string]struct{}
	prewarmSem   chan struct{}
}

type Stats struct {
	UptimeSeconds int64         `json:"uptime_seconds"`
	Config        ConfigSummary `json:"config"`
	CacheBytes    int64         `json:"cache_bytes"`
	DiskFreeBytes int64         `json:"disk_free_bytes"`
	Counters      Counters      `json:"counters"`
	Prewarm       PrewarmStats  `json:"prewarm"`
	Prefetch      PrefetchStats `json:"prefetch"`
	MiddleBytes   int64         `json:"middle_blocks_bytes"`
	RecentErrors  []string      `json:"recent_errors"`
	ErrorEvents   []ErrorEvent  `json:"recent_error_events"`
}

type ErrorEvent struct {
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
}

type ConfigSummary struct {
	Listen                 string `json:"listen"`
	CacheDir               string `json:"cache_dir"`
	RolloutEnabled         bool   `json:"rollout_enabled"`
	DirectOpenListEnabled  bool   `json:"direct_openlist_enabled"`
	DirectHTTPEnabled      bool   `json:"direct_http_enabled"`
	DirectCacheEligibility bool   `json:"direct_cache_require_eligibility"`
	MiddleCacheEnabled     bool   `json:"middle_cache_enabled"`
	PrefetchEnabled        bool   `json:"prefetch_enabled"`
	SessionEnabled         bool   `json:"session_enabled"`
	PrewarmEnabled         bool   `json:"prewarm_enabled"`
	PrefetchConcurrency    int    `json:"prefetch_concurrency"`
	PrewarmConcurrency     int    `json:"prewarm_concurrency"`
	PerOriginConcurrency   int    `json:"per_origin_concurrency"`
	PauseWhenActiveSession bool   `json:"pause_when_rollout_session_active"`
}

type Counters struct {
	CacheHit    int64 `json:"cache_hit"`
	CacheBuild  int64 `json:"cache_build"`
	Origin      int64 `json:"origin"`
	Fallback    int64 `json:"fallback"`
	Denied      int64 `json:"denied"`
	MiddleHit   int64 `json:"middle_hit"`
	MiddleMiss  int64 `json:"middle_miss"`
	ProxyErrors int64 `json:"proxy_errors"`
}

type PrewarmStats struct {
	Queued    int64 `json:"queued"`
	Running   int64 `json:"running"`
	Completed int64 `json:"completed"`
	Skipped   int64 `json:"skipped"`
}

type PrefetchStats struct {
	Queue   int64 `json:"queue"`
	Running int64 `json:"running"`
	Done    int64 `json:"done"`
	Failed  int64 `json:"failed"`
}

var errPrewarmAlreadyCached = errors.New("prewarm already cached")

const directCacheEligibilityHeader = "X-Range-Cache-Eligible"

type cacheBuildResponseError struct {
	err error
}

func (e *cacheBuildResponseError) Error() string {
	return e.err.Error()
}

func (e *cacheBuildResponseError) Unwrap() error {
	return e.err
}

var accessLogSeq uint64

type authCacheKey struct {
	itemID        string
	mediaSourceID string
	token         string
}

type authCacheEntry struct {
	source    model.MediaSource
	expiresAt time.Time
}

type accessLogEvent struct {
	requestID     uint64
	startedAt     time.Time
	method        string
	path          string
	itemID        string
	mediaSourceID string
	rangeHeader   string
	userAgent     string
	clientIP      string
	authCache     string
	authMS        int64
	route         string
	err           string
}

type accessLogResponseWriter struct {
	http.ResponseWriter
	status       int
	bytes        int64
	firstWriteAt time.Time
}

func (w *accessLogResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.firstWriteAt = time.Now()
	w.ResponseWriter.WriteHeader(status)
}

func (w *accessLogResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
		w.firstWriteAt = time.Now()
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *accessLogResponseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *accessLogResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func New(cfg config.Config) (*Server, error) {
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, err
	}
	statePath := cfg.Session.StateDB
	if statePath == "" {
		statePath = filepath.Join(cfg.CacheDir, "state", "phase2.sqlite3")
	}
	store, err := state.Open(statePath)
	if err != nil {
		return nil, err
	}
	prewarmConcurrency := cfg.Prewarm.Concurrency
	if prewarmConcurrency <= 0 {
		prewarmConcurrency = 1
	}
	playbackInfoTimeout := time.Duration(cfg.PlaybackInfoTimeoutSeconds) * time.Second
	prewarmTimeout := time.Duration(cfg.Prewarm.PlaybackInfoTimeoutSeconds) * time.Second
	server := &Server{
		cfg:             cfg,
		startedAt:       time.Now(),
		cache:           headtail.NewCache(cfg.CacheDir, cfg.Cache.MaxBytes),
		store:           store,
		httpClient:      &http.Client{Timeout: prewarmTimeout},
		fallbackClient:  &http.Client{Timeout: 0},
		origin:          origin.NewClient(cfg.Cache.ChunkBytes),
		openList:        openlist.NewResolver(cfg.OpenList),
		auth:            emby.NewAuthClientWithTimeout(cfg.EmbyBaseURL, playbackInfoTimeout),
		prewarmAuth:     emby.NewAuthClientWithTimeout(cfg.EmbyBaseURL, prewarmTimeout),
		authCacheTTL:    time.Duration(cfg.PlaybackAuthCacheTTLSeconds) * time.Second,
		authCache:       make(map[authCacheKey]authCacheEntry),
		originSem:       make(chan struct{}, 32),
		cacheBuildLocks: make(map[string]*cacheBuildLock),
		prewarmTasks:    make(map[string]struct{}),
		prewarmSem:      make(chan struct{}, prewarmConcurrency),
	}
	server.middle = middle.NewCache(cfg.CacheDir, store, cfg.MiddleCache.MaxBytes, cfg.MiddleCache.TTLSeconds, cfg.MiddleCache.MinFreeBytes)
	return server, nil
}

func (s *Server) Close() error {
	return s.store.Close()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		_, _ = w.Write([]byte("ok\n"))
	case r.Method == http.MethodGet && r.URL.Path == "/internal/stats":
		s.handleStats(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/internal/metrics":
		s.handleMetrics(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/internal/prewarm":
		s.handleInternalPrewarm(w, r)
	case s.isDirectOpenListRequest(r):
		s.handleDirectOpenList(w, r)
	case s.isDirectHTTPRequest(r):
		s.handleDirectHTTP(w, r)
	default:
		s.proxyHandler(w, r)
	}
}

func (s *Server) isDirectHTTPRequest(r *http.Request) bool {
	if !s.cfg.DirectHTTP.Enabled {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	return strings.HasPrefix(r.URL.Path, s.cfg.DirectHTTP.PathPrefix)
}

func (s *Server) isDirectOpenListRequest(r *http.Request) bool {
	if !s.cfg.DirectOpenList.Enabled {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	return strings.HasPrefix(r.URL.Path, s.cfg.DirectOpenList.PathPrefix)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if !isInternalCaller(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.SnapshotStats())
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !isInternalCaller(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writeMetrics(w, s.SnapshotStats())
}

func writeMetrics(w io.Writer, stats Stats) {
	writeMetric(w, "emby_range_cache_proxy_uptime_seconds", stats.UptimeSeconds)
	writeMetric(w, "emby_range_cache_proxy_cache_bytes", stats.CacheBytes)
	writeMetric(w, "emby_range_cache_proxy_middle_blocks_bytes", stats.MiddleBytes)
	writeMetric(w, "emby_range_cache_proxy_disk_free_bytes", stats.DiskFreeBytes)

	writeMetric(w, "emby_range_cache_proxy_cache_hit_total", stats.Counters.CacheHit)
	writeMetric(w, "emby_range_cache_proxy_cache_build_total", stats.Counters.CacheBuild)
	writeMetric(w, "emby_range_cache_proxy_origin_total", stats.Counters.Origin)
	writeMetric(w, "emby_range_cache_proxy_fallback_total", stats.Counters.Fallback)
	writeMetric(w, "emby_range_cache_proxy_denied_total", stats.Counters.Denied)
	writeMetric(w, "emby_range_cache_proxy_middle_hit_total", stats.Counters.MiddleHit)
	writeMetric(w, "emby_range_cache_proxy_middle_miss_total", stats.Counters.MiddleMiss)
	writeMetric(w, "emby_range_cache_proxy_proxy_errors_total", stats.Counters.ProxyErrors)

	writeMetric(w, "emby_range_cache_proxy_prewarm_queued", stats.Prewarm.Queued)
	writeMetric(w, "emby_range_cache_proxy_prewarm_running", stats.Prewarm.Running)
	writeMetric(w, "emby_range_cache_proxy_prewarm_completed_total", stats.Prewarm.Completed)
	writeMetric(w, "emby_range_cache_proxy_prewarm_skipped_total", stats.Prewarm.Skipped)

	writeMetric(w, "emby_range_cache_proxy_prefetch_queue", stats.Prefetch.Queue)
	writeMetric(w, "emby_range_cache_proxy_prefetch_running", stats.Prefetch.Running)
	writeMetric(w, "emby_range_cache_proxy_prefetch_done_total", stats.Prefetch.Done)
	writeMetric(w, "emby_range_cache_proxy_prefetch_failed_total", stats.Prefetch.Failed)

	writeMetric(w, "emby_range_cache_proxy_rollout_enabled", boolMetric(stats.Config.RolloutEnabled))
	writeMetric(w, "emby_range_cache_proxy_direct_openlist_enabled", boolMetric(stats.Config.DirectOpenListEnabled))
	writeMetric(w, "emby_range_cache_proxy_direct_http_enabled", boolMetric(stats.Config.DirectHTTPEnabled))
	writeMetric(w, "emby_range_cache_proxy_middle_cache_enabled", boolMetric(stats.Config.MiddleCacheEnabled))
	writeMetric(w, "emby_range_cache_proxy_prefetch_enabled", boolMetric(stats.Config.PrefetchEnabled))
	writeMetric(w, "emby_range_cache_proxy_session_enabled", boolMetric(stats.Config.SessionEnabled))
	writeMetric(w, "emby_range_cache_proxy_prewarm_enabled", boolMetric(stats.Config.PrewarmEnabled))
	writeMetric(w, "emby_range_cache_proxy_pause_when_rollout_session_active", boolMetric(stats.Config.PauseWhenActiveSession))
	writeMetric(w, "emby_range_cache_proxy_prefetch_concurrency", int64(stats.Config.PrefetchConcurrency))
	writeMetric(w, "emby_range_cache_proxy_prewarm_concurrency", int64(stats.Config.PrewarmConcurrency))
	writeMetric(w, "emby_range_cache_proxy_per_origin_concurrency", int64(stats.Config.PerOriginConcurrency))
}

func writeMetric(w io.Writer, name string, value int64) {
	_, _ = fmt.Fprintf(w, "%s %d\n", name, value)
}

func boolMetric(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func (s *Server) SnapshotStats() Stats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	stats := s.stats
	stats.UptimeSeconds = int64(time.Since(s.startedAt).Seconds())
	stats.Config = ConfigSummary{
		Listen:                 fmt.Sprintf("%s:%d", s.cfg.ListenHost, s.cfg.ListenPort),
		CacheDir:               s.cfg.CacheDir,
		RolloutEnabled:         s.cfg.Rollout.Enabled,
		DirectOpenListEnabled:  s.cfg.DirectOpenList.Enabled,
		DirectHTTPEnabled:      s.cfg.DirectHTTP.Enabled,
		DirectCacheEligibility: s.cfg.DirectCache.RequireEligibility,
		MiddleCacheEnabled:     s.cfg.MiddleCache.Enabled,
		PrefetchEnabled:        s.cfg.Prefetch.Enabled,
		SessionEnabled:         s.cfg.Session.Enabled,
		PrewarmEnabled:         s.cfg.Prewarm.Enabled,
		PrefetchConcurrency:    s.cfg.Prefetch.Concurrency,
		PrewarmConcurrency:     s.cfg.Prewarm.Concurrency,
		PerOriginConcurrency:   s.cfg.Prefetch.PerOriginConcurrency,
		PauseWhenActiveSession: s.cfg.Prefetch.PauseWhenRolloutSessionActive,
	}
	stats.CacheBytes = dirBytes(s.cfg.CacheDir, func(path string) bool {
		return strings.HasSuffix(path, ".bin") && !strings.Contains(path, string(filepath.Separator)+"mid"+string(filepath.Separator))
	})
	if bytes, err := s.store.MiddleCacheBytes(); err == nil {
		stats.MiddleBytes = bytes
	}
	if queue, err := s.store.QueueDepth(); err == nil {
		stats.Prefetch.Queue = int64(queue)
	}
	stats.DiskFreeBytes = diskfree.FreeBytes(s.cfg.CacheDir)
	stats.RecentErrors = append([]string(nil), s.stats.RecentErrors...)
	stats.ErrorEvents = append([]ErrorEvent(nil), s.stats.ErrorEvents...)
	return stats
}

func (s *Server) handleInternalPrewarm(w http.ResponseWriter, r *http.Request) {
	if s.cfg.PrewarmAPIKey == "" {
		http.NotFound(w, r)
		return
	}
	if !isInternalCaller(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !requestContainsPrewarmKey(r, s.cfg.PrewarmAPIKey) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var payload struct {
		ItemID        string `json:"itemId"`
		ItemIDAlt     string `json:"item_id"`
		MediaSourceID string `json:"mediaSourceId"`
		MediaAlt      string `json:"media_source_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	itemID := firstNonEmpty(payload.ItemID, payload.ItemIDAlt)
	mediaSourceID := firstNonEmpty(payload.MediaSourceID, payload.MediaAlt)
	if itemID == "" || mediaSourceID == "" {
		http.Error(w, "itemId and mediaSourceId are required", http.StatusBadRequest)
		return
	}
	status := s.enqueuePrewarm(itemID, mediaSourceID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "itemId": itemID, "mediaSourceId": mediaSourceID})
}

func (s *Server) handleDirectOpenList(w http.ResponseWriter, r *http.Request) {
	trace := newAccessLogEvent(r)
	aw := &accessLogResponseWriter{ResponseWriter: w}
	defer s.logAccess(trace, aw)
	if !requestContainsDirectOpenListToken(r, s.cfg.DirectOpenList.Token) {
		trace.route = "denied"
		s.addStat(func(stats *Stats) { stats.Counters.Denied++ })
		http.Error(aw, "forbidden", http.StatusForbidden)
		return
	}
	openListPath, err := s.directOpenListPath(r)
	if err != nil {
		trace.route = "direct_openlist_error"
		trace.err = errorClass(err)
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
		http.Error(aw, "invalid openlist path", http.StatusBadRequest)
		return
	}
	ctx := directOpenListRequestContext(r, openListPath)
	trace.itemID = ctx.ItemID
	trace.mediaSourceID = ctx.MediaSourceID
	sourceMedia := model.MediaSource{
		ItemID:        ctx.ItemID,
		MediaSourceID: ctx.MediaSourceID,
		Path:          "openlist://" + openListPath,
		Protocol:      "OpenList",
	}
	cacheEligible := s.directCacheEligible(r)
	if cacheEligible {
		if served, err := s.serveCachedHeadTailIfAvailable(aw, r, sourceMedia, ctx, trace); served {
			return
		} else if err != nil {
			trace.route = "direct_openlist_error"
			trace.err = errorClass(err)
			s.addError("direct openlist cache probe failed: " + errorClass(err))
			s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			http.Error(aw, "cache unavailable", http.StatusBadGateway)
			return
		}
	}
	sourceMedia = s.openList.Resolve(r.Context(), sourceMedia)
	if !isHTTP(sourceMedia.Path) {
		trace.route = "direct_openlist_error"
		trace.err = "OpenListResolveFailed"
		s.addError("direct openlist resolve failed")
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
		http.Error(aw, "openlist unavailable", http.StatusBadGateway)
		return
	}
	if err := s.serveAuthorizedRange(aw, r, sourceMedia, ctx, trace, cacheEligible); err != nil {
		trace.err = errorClass(err)
		s.addError("direct openlist proxy failed: " + errorClass(err))
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
		http.Error(aw, "proxy error", http.StatusBadGateway)
		return
	}
}

func (s *Server) handleDirectHTTP(w http.ResponseWriter, r *http.Request) {
	trace := newAccessLogEvent(r)
	aw := &accessLogResponseWriter{ResponseWriter: w}
	defer s.logAccess(trace, aw)

	upstreamURL, err := s.directHTTPUpstreamURL(r)
	if err != nil {
		trace.route = "direct_http_error"
		trace.err = errorClass(err)
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
		http.Error(aw, "invalid source path", http.StatusBadRequest)
		return
	}
	ctx := directHTTPRequestContext(r, upstreamURL)
	trace.itemID = ctx.ItemID
	trace.mediaSourceID = ctx.MediaSourceID
	sourceMedia := model.MediaSource{
		ItemID:        ctx.ItemID,
		MediaSourceID: ctx.MediaSourceID,
		Path:          upstreamURL,
		Protocol:      "Http",
	}
	cacheEligible := s.directCacheEligible(r)
	if cacheEligible {
		if served, err := s.serveCachedHeadTailIfAvailable(aw, r, sourceMedia, ctx, trace); served {
			return
		} else if err != nil {
			trace.route = "direct_http_error"
			trace.err = errorClass(err)
			s.addError("direct http cache probe failed: " + errorClass(err))
			s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			http.Error(aw, "cache unavailable", http.StatusBadGateway)
			return
		}
	}
	if err := s.serveAuthorizedRange(aw, r, sourceMedia, ctx, trace, cacheEligible); err != nil {
		trace.err = errorClass(err)
		s.addError("direct http proxy failed: " + errorClass(err))
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
		http.Error(aw, "proxy error", http.StatusBadGateway)
	}
}

func (s *Server) directHTTPUpstreamURL(r *http.Request) (string, error) {
	prefix := s.cfg.DirectHTTP.PathPrefix
	escapedPath := r.URL.EscapedPath()
	if !strings.HasPrefix(escapedPath, prefix) {
		return "", fmt.Errorf("path prefix mismatch")
	}
	relative, err := url.PathUnescape(strings.TrimPrefix(escapedPath, prefix))
	if err != nil {
		return "", err
	}
	if relative == "" {
		return "", fmt.Errorf("empty source path")
	}
	for _, segment := range strings.Split(relative, "/") {
		if segment == "." || segment == ".." {
			return "", fmt.Errorf("invalid source path")
		}
	}
	base, err := url.Parse(s.cfg.DirectHTTP.UpstreamBaseURL)
	if err != nil {
		return "", err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/" + strings.TrimLeft(relative, "/")
	base.RawPath = ""
	base.RawQuery = r.URL.RawQuery
	return base.String(), nil
}

func directHTTPRequestContext(r *http.Request, upstreamURL string) model.RequestContext {
	sum := sha256.Sum256([]byte(upstreamURL))
	mediaSourceID := "direct_http_" + hex.EncodeToString(sum[:8])
	parsed, _ := url.Parse(upstreamURL)
	ext := strings.TrimPrefix(pathpkg.Ext(parsed.Path), ".")
	return model.RequestContext{
		Method:        r.Method,
		RawPath:       r.URL.RequestURI(),
		ItemID:        "direct_http",
		MediaSourceID: mediaSourceID,
		Token:         "direct_http",
		Extension:     strings.ToLower(ext),
	}
}

func (s *Server) directCacheEligible(r *http.Request) bool {
	if !s.cfg.DirectCache.RequireEligibility {
		return true
	}
	if strings.TrimSpace(r.Header.Get(directCacheEligibilityHeader)) == "1" {
		return true
	}
	return s.cfg.PrewarmAPIKey != "" && requestContainsInternalKey(r, s.cfg.PrewarmAPIKey)
}

func requestContainsDirectOpenListToken(r *http.Request, token string) bool {
	if token == "" {
		return false
	}
	if subtleEqual(r.URL.Query().Get("token"), token) {
		return true
	}
	if subtleEqual(r.Header.Get("X-Direct-OpenList-Token"), token) {
		return true
	}
	return authorizationBearerMatches(r.Header.Get("Authorization"), token)
}

func (s *Server) directOpenListPath(r *http.Request) (string, error) {
	prefix := s.cfg.DirectOpenList.PathPrefix
	escapedPath := r.URL.EscapedPath()
	if !strings.HasPrefix(escapedPath, prefix) {
		return "", fmt.Errorf("path prefix mismatch")
	}
	relative, err := url.PathUnescape(strings.TrimPrefix(escapedPath, prefix))
	if err != nil {
		return "", err
	}
	if relative == "" {
		return "", fmt.Errorf("empty openlist path")
	}
	for _, segment := range strings.Split(relative, "/") {
		if segment == "." || segment == ".." {
			return "", fmt.Errorf("invalid openlist path")
		}
	}
	cleaned := pathpkg.Clean("/" + relative)
	if cleaned == "/" {
		return "", fmt.Errorf("empty openlist path")
	}
	return cleaned, nil
}

func directOpenListRequestContext(r *http.Request, openListPath string) model.RequestContext {
	sum := sha256.Sum256([]byte(openListPath))
	mediaSourceID := "direct_openlist_" + hex.EncodeToString(sum[:8])
	ext := strings.TrimPrefix(pathpkg.Ext(openListPath), ".")
	return model.RequestContext{
		Method:        r.Method,
		RawPath:       r.URL.RequestURI(),
		ItemID:        "direct_openlist",
		MediaSourceID: mediaSourceID,
		Token:         "direct_openlist",
		Extension:     strings.ToLower(ext),
	}
}

func (s *Server) enqueuePrewarm(itemID, mediaSourceID string) string {
	key := itemID + "\x00" + mediaSourceID
	s.prewarmMu.Lock()
	if _, exists := s.prewarmTasks[key]; exists {
		s.prewarmMu.Unlock()
		return "existing"
	}
	s.prewarmTasks[key] = struct{}{}
	s.prewarmMu.Unlock()
	s.addStat(func(stats *Stats) { stats.Prewarm.Queued++ })
	go func() {
		s.prewarmSem <- struct{}{}
		s.addStat(func(stats *Stats) {
			stats.Prewarm.Queued--
			stats.Prewarm.Running++
		})
		defer func() {
			<-s.prewarmSem
			s.prewarmMu.Lock()
			delete(s.prewarmTasks, key)
			s.prewarmMu.Unlock()
			s.addStat(func(stats *Stats) { stats.Prewarm.Running-- })
		}()
		if err := s.prewarmItem(itemID, mediaSourceID); err != nil {
			if errors.Is(err, errPrewarmAlreadyCached) {
				s.addStat(func(stats *Stats) { stats.Prewarm.Skipped++ })
				return
			}
			s.addError("prewarm failed: " + errorClass(err))
			s.addStat(func(stats *Stats) { stats.Prewarm.Skipped++ })
			return
		}
		s.addStat(func(stats *Stats) { stats.Prewarm.Completed++ })
	}()
	return "queued"
}

func (s *Server) prewarmItem(itemID, mediaSourceID string) error {
	ctx := model.RequestContext{Method: http.MethodGet, ItemID: itemID, MediaSourceID: mediaSourceID, Token: s.cfg.PrewarmAPIKey}
	sourceMedia, err := s.prewarmAuth.Authorize(ctx)
	if err != nil {
		return err
	}
	sourceMedia = source.ResolveMediaSource(sourceMedia, s.cfg.PathMappings, s.cfg.Rollout.PathPrefixAllowlist)
	sourceMedia = s.openList.Resolve(context.Background(), sourceMedia)
	if !isHTTP(sourceMedia.Path) || !s.cfg.Rollout.InScope(itemID, mediaSourceID, sourceMedia.Path) {
		return fmt.Errorf("source out of scope")
	}
	meta, err := s.sourceMetadata(sourceMedia)
	if err != nil {
		return err
	}
	key := cache.Key(sourceMedia, meta)
	headSize, tailSize := s.headTailBytes(meta.Size)
	toWarm := []struct {
		name string
		rng  model.ByteRange
	}{
		{"head", model.ByteRange{Start: 0, End: minInt64(headSize, meta.Size) - 1}},
		{"tail", model.ByteRange{Start: maxInt64(0, meta.Size-tailSize), End: meta.Size - 1}},
	}
	warmed := false
	for _, item := range toWarm {
		cached, err := s.cache.IterBlock(key, item.name, item.rng, s.cfg.Cache.ChunkBytes)
		if err != nil {
			return err
		}
		if cached != nil {
			drain(cached)
			continue
		}
		if err := s.buildHeadTailBlock(sourceMedia.Path, meta, key, item.name, item.rng, model.ByteRange{}); err != nil {
			return err
		}
		warmed = true
	}
	if !warmed {
		return errPrewarmAlreadyCached
	}
	return nil
}

func (s *Server) proxyHandler(w http.ResponseWriter, r *http.Request) {
	trace := newAccessLogEvent(r)
	aw := &accessLogResponseWriter{ResponseWriter: w}
	defer s.logAccess(trace, aw)
	if s.cfg.PrewarmAPIKey != "" && requestContainsInternalKey(r, s.cfg.PrewarmAPIKey) {
		trace.route = "denied"
		s.addStat(func(stats *Stats) { stats.Counters.Denied++ })
		http.Error(aw, "forbidden", http.StatusForbidden)
		return
	}
	ctx, ok := request.ParseOriginal(r.Method, r.URL.RequestURI(), r.Header)
	if ok {
		trace.itemID = ctx.ItemID
		trace.mediaSourceID = ctx.MediaSourceID
	}
	if !ok || !s.preAuthRollout(ctx) {
		trace.route = "fallback"
		s.addStat(func(stats *Stats) { stats.Counters.Fallback++ })
		s.streamFallback(aw, r)
		return
	}
	sourceMedia, err := s.authorizePlayback(ctx, trace)
	if err != nil {
		trace.err = errorClass(err)
		var authErr emby.AuthorizationError
		if errors.As(err, &authErr) {
			trace.route = "denied"
			s.addStat(func(stats *Stats) { stats.Counters.Denied++ })
			http.Error(aw, "forbidden", http.StatusForbidden)
			return
		}
		trace.route = "fallback"
		s.addStat(func(stats *Stats) { stats.Counters.Fallback++ })
		s.streamFallback(aw, r)
		return
	}
	sourceMedia = source.ResolveMediaSource(sourceMedia, s.cfg.PathMappings, s.cfg.Rollout.PathPrefixAllowlist)
	if s.canServeCachedBeforeOriginResolution(ctx, sourceMedia) {
		served, err := s.serveCachedHeadTailIfAvailable(aw, r, sourceMedia, ctx, trace)
		if served {
			return
		}
		if err != nil {
			trace.err = errorClass(err)
			s.addError("cache probe failed: " + errorClass(err))
			s.addStat(func(stats *Stats) {
				stats.Counters.ProxyErrors++
				stats.Counters.Fallback++
			})
			trace.route = "fallback"
			s.streamFallback(aw, r)
			return
		}
	}
	sourceMedia = s.openList.Resolve(r.Context(), sourceMedia)
	if !isHTTP(sourceMedia.Path) || !s.cfg.Rollout.InScope(ctx.ItemID, ctx.MediaSourceID, sourceMedia.Path) {
		trace.route = "fallback"
		s.addStat(func(stats *Stats) { stats.Counters.Fallback++ })
		s.streamFallback(aw, r)
		return
	}
	if err := s.serveAuthorizedRange(aw, r, sourceMedia, ctx, trace, true); err != nil {
		trace.err = errorClass(err)
		s.addError("proxy failed: " + errorClass(err))
		s.addStat(func(stats *Stats) {
			stats.Counters.ProxyErrors++
			stats.Counters.Fallback++
		})
		trace.route = "fallback"
		s.streamFallback(aw, r)
	}
}

func (s *Server) canServeCachedBeforeOriginResolution(ctx model.RequestContext, sourceMedia model.MediaSource) bool {
	if isHTTP(sourceMedia.Path) {
		return s.cfg.Rollout.InScope(ctx.ItemID, ctx.MediaSourceID, sourceMedia.Path)
	}
	if !s.cfg.OpenList.Enabled {
		return false
	}
	_, ok := openlist.PathFromSource(sourceMedia.Path, s.cfg.OpenList.BaseURL)
	return ok
}

func (s *Server) preAuthRollout(ctx model.RequestContext) bool {
	return s.cfg.Rollout.Enabled && s.cfg.Rollout.ItemAllowed(ctx.ItemID) && s.cfg.Rollout.MediaSourceAllowed(ctx.MediaSourceID)
}

func (s *Server) authorizePlayback(ctx model.RequestContext, trace *accessLogEvent) (model.MediaSource, error) {
	if s.authCacheTTL > 0 {
		key := authCacheKey{itemID: ctx.ItemID, mediaSourceID: ctx.MediaSourceID, token: ctx.Token}
		now := time.Now()
		s.authCacheMu.Lock()
		entry, ok := s.authCache[key]
		if ok && now.Before(entry.expiresAt) {
			s.authCacheMu.Unlock()
			if trace != nil {
				trace.authCache = "hit"
			}
			return entry.source, nil
		}
		if ok {
			delete(s.authCache, key)
		}
		s.authCacheMu.Unlock()
	}
	if trace != nil {
		if s.authCacheTTL > 0 {
			trace.authCache = "miss"
		} else {
			trace.authCache = "disabled"
		}
	}
	start := time.Now()
	sourceMedia, err := s.auth.Authorize(ctx)
	if trace != nil {
		trace.authMS = time.Since(start).Milliseconds()
	}
	if err != nil || s.authCacheTTL <= 0 {
		return sourceMedia, err
	}
	key := authCacheKey{itemID: ctx.ItemID, mediaSourceID: ctx.MediaSourceID, token: ctx.Token}
	s.authCacheMu.Lock()
	s.authCache[key] = authCacheEntry{source: sourceMedia, expiresAt: time.Now().Add(s.authCacheTTL)}
	s.authCacheMu.Unlock()
	return sourceMedia, nil
}

func (s *Server) serveAuthorizedRange(w http.ResponseWriter, r *http.Request, sourceMedia model.MediaSource, ctx model.RequestContext, trace *accessLogEvent, cacheEligible bool) error {
	if cacheEligible {
		if served, err := s.serveCachedHeadTailIfAvailable(w, r, sourceMedia, ctx, trace); served || err != nil {
			return err
		}
	}
	meta, err := s.sourceMetadata(sourceMedia)
	if err != nil {
		return err
	}
	headSize, tailSize := s.headTailBytes(meta.Size)
	byteRange, err := ranges.PlanPlaybackRange(r.Header.Get("Range"), meta.Size, headSize, tailSize, s.cfg.Cache.DefaultOpenRangeBytes, s.openHeadResponseBytes(ctx.Extension, r.Header.Get("Range")))
	if err != nil {
		if errors.Is(err, ranges.ErrRangeNotSatisfiable) {
			setAccessRoute(trace, "range_unsatisfiable")
			writeUnsatisfiableRange(w, meta.Size)
			return nil
		}
		return err
	}
	status := http.StatusOK
	if r.Header.Get("Range") != "" || byteRange.Start != 0 || byteRange.End != meta.Size-1 {
		status = http.StatusPartialContent
	}
	key := ""
	if cacheEligible {
		key = cache.Key(sourceMedia, meta)
		_ = s.store.UpsertSourceMetadataRecord(sourceMetadataRecord(ctx.ItemID, ctx.MediaSourceID, key, meta, float64(time.Now().Unix())))
	}
	writeRangeHeaders(w, status, byteRange, meta)
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return nil
	}
	if !cacheEligible {
		setAccessRoute(trace, "origin_no_cache")
		w.WriteHeader(status)
		if err := s.streamOriginRange(meta.URL, meta, byteRange, w); err != nil {
			setAccessError(trace, err)
			s.addError("no-cache origin stream failed: " + errorClass(err))
			s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			return nil
		}
		s.addStat(func(stats *Stats) { stats.Counters.Origin++ })
		return nil
	}
	if s.cfg.MiddleCache.Enabled {
		chunks, err := s.middle.IterBlock(key, byteRange, s.cfg.Cache.ChunkBytes, float64(time.Now().Unix()))
		if err == nil && chunks != nil {
			s.addStat(func(stats *Stats) {
				stats.Counters.MiddleHit++
				stats.Counters.CacheHit++
			})
			setAccessRoute(trace, "middle_cache")
			w.WriteHeader(status)
			if err := writeChunks(w, chunks, func() {
				s.recordSession(ctx, key, meta, byteRange)
			}); err != nil {
				setAccessError(trace, err)
				s.addError("middle cache response failed: " + errorClass(err))
				s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			}
			return nil
		}
		s.addStat(func(stats *Stats) { stats.Counters.MiddleMiss++ })
	}
	blockName, blockRange, adaptiveTail := s.cacheBlockForRequest(byteRange, meta.Size, headSize, tailSize)
	cacheRoute := "head_tail_cache"
	buildRoute := "head_tail_build"
	if adaptiveTail {
		cacheRoute = "adaptive_tail_cache"
		buildRoute = "adaptive_tail_build"
	}
	if blockName != "" {
		chunks, err := s.cache.IterBlock(key, blockName, byteRange, s.cfg.Cache.ChunkBytes)
		if err != nil {
			return err
		}
		if chunks != nil {
			s.addStat(func(stats *Stats) { stats.Counters.CacheHit++ })
			setAccessRoute(trace, cacheRoute)
			w.WriteHeader(status)
			if err := writeChunks(w, chunks, func() {
				s.recordSession(ctx, key, meta, byteRange)
			}); err != nil {
				setAccessError(trace, err)
				s.addError("cache response failed: " + errorClass(err))
				s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			}
			return nil
		}
		buildLock := s.cacheBuildLock(key, blockName)
		if !buildLock.tryAcquire() {
			buildLock.wait(time.Duration(s.cfg.Cache.BuildWaitSeconds * float64(time.Second)))
			chunks, err = s.cache.IterBlock(key, blockName, byteRange, s.cfg.Cache.ChunkBytes)
			if err != nil {
				return err
			}
			if chunks != nil {
				s.addStat(func(stats *Stats) { stats.Counters.CacheHit++ })
				setAccessRoute(trace, cacheRoute)
				w.WriteHeader(status)
				if err := writeChunks(w, chunks, func() {
					s.recordSession(ctx, key, meta, byteRange)
				}); err != nil {
					setAccessError(trace, err)
					s.addError("cache response failed: " + errorClass(err))
					s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
				}
				return nil
			}
			if !buildLock.tryAcquire() {
				setAccessRoute(trace, "origin")
				w.WriteHeader(status)
				if err := s.streamOriginRange(meta.URL, meta, byteRange, w); err != nil {
					setAccessError(trace, err)
					s.addError("origin stream failed: " + errorClass(err))
					s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
					return nil
				}
				s.addStat(func(stats *Stats) { stats.Counters.Origin++ })
				s.recordSession(ctx, key, meta, byteRange)
				return nil
			}
			chunks, err := s.cache.IterBlock(key, blockName, byteRange, s.cfg.Cache.ChunkBytes)
			if err != nil {
				buildLock.release()
				return err
			}
			if chunks != nil {
				buildLock.release()
				s.addStat(func(stats *Stats) { stats.Counters.CacheHit++ })
				setAccessRoute(trace, cacheRoute)
				w.WriteHeader(status)
				if err := writeChunks(w, chunks, func() {
					s.recordSession(ctx, key, meta, byteRange)
				}); err != nil {
					setAccessError(trace, err)
					s.addError("cache response failed: " + errorClass(err))
					s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
				}
				return nil
			}
		}
		defer buildLock.release()
		setAccessRoute(trace, buildRoute)
		w.WriteHeader(status)
		if err := s.buildHeadTailBlock(sourceMedia.Path, meta, key, blockName, blockRange, byteRange, w); err != nil {
			var responseErr *cacheBuildResponseError
			if errors.As(err, &responseErr) {
				setAccessError(trace, responseErr.err)
				s.addError("cache build response failed: " + errorClass(responseErr.err))
				s.addStat(func(stats *Stats) {
					stats.Counters.CacheBuild++
					stats.Counters.ProxyErrors++
				})
				return nil
			}
			setAccessError(trace, err)
			s.addError("cache build stream failed: " + errorClass(err))
			s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			return nil
		}
		s.addStat(func(stats *Stats) { stats.Counters.CacheBuild++ })
		s.recordSession(ctx, key, meta, byteRange)
		return nil
	}
	setAccessRoute(trace, "origin")
	w.WriteHeader(status)
	if err := s.streamOriginRange(meta.URL, meta, byteRange, w); err != nil {
		setAccessError(trace, err)
		s.addError("origin stream failed: " + errorClass(err))
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
		return nil
	}
	s.addStat(func(stats *Stats) { stats.Counters.Origin++ })
	s.recordSession(ctx, key, meta, byteRange)
	return nil
}

func (s *Server) serveCachedHeadTailIfAvailable(w http.ResponseWriter, r *http.Request, sourceMedia model.MediaSource, ctx model.RequestContext, trace *accessLogEvent) (bool, error) {
	record, err := s.store.LatestSourceMetadata(ctx.ItemID, ctx.MediaSourceID)
	if err != nil || record == nil {
		return false, err
	}
	if sourceMedia.Size != nil && *sourceMedia.Size > 0 && *sourceMedia.Size != record.MediaSize {
		return false, nil
	}
	meta := sourceMetadataFromRecord(record)
	headSize, tailSize := s.headTailBytes(meta.Size)
	byteRange, err := ranges.PlanPlaybackRange(r.Header.Get("Range"), meta.Size, headSize, tailSize, s.cfg.Cache.DefaultOpenRangeBytes, s.openHeadResponseBytes(ctx.Extension, r.Header.Get("Range")))
	if err != nil {
		if errors.Is(err, ranges.ErrRangeNotSatisfiable) {
			setAccessRoute(trace, "range_unsatisfiable")
			writeUnsatisfiableRange(w, meta.Size)
			return true, nil
		}
		return false, err
	}
	blockName, _, adaptiveTail := s.cacheBlockForRequest(byteRange, meta.Size, headSize, tailSize)
	if blockName == "" {
		return false, nil
	}
	cacheRoute := "head_tail_cache"
	if adaptiveTail {
		cacheRoute = "adaptive_tail_cache"
	}
	status := responseStatus(r, byteRange, meta)
	if r.Method == http.MethodHead {
		ok, err := s.cache.HasBlockRange(record.CacheKey, blockName, byteRange)
		if err != nil || !ok {
			return false, err
		}
		s.addStat(func(stats *Stats) { stats.Counters.CacheHit++ })
		setAccessRoute(trace, cacheRoute)
		writeRangeHeaders(w, status, byteRange, meta)
		w.WriteHeader(status)
		return true, nil
	}
	chunks, err := s.cache.IterBlock(record.CacheKey, blockName, byteRange, s.cfg.Cache.ChunkBytes)
	if err != nil || chunks == nil {
		return false, err
	}
	s.addStat(func(stats *Stats) { stats.Counters.CacheHit++ })
	setAccessRoute(trace, cacheRoute)
	writeRangeHeaders(w, status, byteRange, meta)
	w.WriteHeader(status)
	if err := writeChunks(w, chunks, func() {
		s.recordSession(ctx, record.CacheKey, meta, byteRange)
	}); err != nil {
		setAccessError(trace, err)
		s.addError("cache response failed: " + errorClass(err))
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
	}
	return true, nil
}

func (s *Server) cacheBlockForRequest(byteRange model.ByteRange, size, headSize, tailSize int64) (string, model.ByteRange, bool) {
	blockName, blockRange := headtail.BlockForRequest(byteRange, size, headSize, tailSize)
	if blockName != "" {
		return blockName, blockRange, false
	}
	maxBytes := s.cfg.Cache.AdaptiveTailMaxBytes
	if maxBytes <= tailSize || byteRange.End != size-1 {
		return "", model.ByteRange{}, false
	}
	maxStart := size - maxBytes
	if maxStart < 0 {
		maxStart = 0
	}
	if byteRange.Start < maxStart {
		return "", model.ByteRange{}, false
	}
	baseStart := size - tailSize
	if baseStart < 0 {
		baseStart = 0
	}
	if byteRange.Start < baseStart {
		baseStart = byteRange.Start
	}
	return "tail", model.ByteRange{Start: baseStart, End: size - 1}, true
}

func responseStatus(r *http.Request, byteRange model.ByteRange, meta model.SourceMetadata) int {
	if r.Header.Get("Range") != "" || byteRange.Start != 0 || byteRange.End != meta.Size-1 {
		return http.StatusPartialContent
	}
	return http.StatusOK
}

func writeUnsatisfiableRange(w http.ResponseWriter, size int64) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
	w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
}

func sourceMetadataRecord(itemID, mediaSourceID, key string, meta model.SourceMetadata, updatedAt float64) state.SourceMetadataRecord {
	return state.SourceMetadataRecord{
		ItemID:          itemID,
		MediaSourceID:   mediaSourceID,
		CacheKey:        key,
		OriginURL:       meta.URL,
		OriginSignature: session.OriginSignature(meta),
		MediaSize:       meta.Size,
		ContentType:     meta.ContentType,
		ETag:            meta.ETag,
		LastModified:    meta.LastModified,
		UpdatedAt:       updatedAt,
	}
}

func sourceMetadataFromRecord(record *state.SourceMetadataRecord) model.SourceMetadata {
	return model.SourceMetadata{
		URL:          record.OriginURL,
		Size:         record.MediaSize,
		ContentType:  record.ContentType,
		ETag:         record.ETag,
		LastModified: record.LastModified,
	}
}

func (s *Server) buildHeadTailBlock(sourceURL string, meta model.SourceMetadata, key, blockName string, blockRange, responseRange model.ByteRange, writers ...io.Writer) error {
	if s.cfg.MiddleCache.MinFreeBytes > 0 {
		free := diskfree.FreeBytes(s.cfg.CacheDir)
		if free >= 0 && free-blockRange.Length() < s.cfg.MiddleCache.MinFreeBytes {
			return fmt.Errorf("insufficient disk free space")
		}
	}
	writer, err := s.cache.StageBlock(key, blockName, blockRange)
	if err != nil {
		return err
	}
	defer writer.Abort()
	buf := make([]byte, int(s.cfg.Cache.ChunkBytes))
	offset := blockRange.Start
	resumeAttempts := 0
	var responseWriter io.Writer
	var responseErr error
	if len(writers) > 0 {
		responseWriter = writers[0]
	}
	for offset <= blockRange.End {
		body, openErr := s.origin.OpenRange(meta.URL, model.ByteRange{Start: offset, End: blockRange.End}, meta.Size)
		if openErr != nil {
			resumeAttempts++
			if resumeAttempts > 3 {
				return openErr
			}
			time.Sleep(time.Duration(resumeAttempts) * 250 * time.Millisecond)
			continue
		}
		startOffset := offset
		var readErr error
		for offset <= blockRange.End {
			n, err := body.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				if responseWriter == nil && s.cfg.Prefetch.BandwidthBytesPerSecond > 0 {
					time.Sleep(time.Duration(int64(n) * int64(time.Second) / s.cfg.Prefetch.BandwidthBytesPerSecond))
				}
				if _, err := writer.Write(chunk); err != nil {
					return err
				}
				if responseWriter != nil && responseRange.End >= responseRange.Start {
					chunkStart := offset
					chunkEnd := offset + int64(n) - 1
					overlapStart := maxInt64(chunkStart, responseRange.Start)
					overlapEnd := minInt64(chunkEnd, responseRange.End)
					if overlapStart <= overlapEnd {
						start := overlapStart - chunkStart
						end := overlapEnd - chunkStart + 1
						if _, err := responseWriter.Write(chunk[start:end]); err != nil {
							responseErr = err
							responseWriter = nil
						} else if flusher, ok := responseWriter.(http.Flusher); ok {
							flusher.Flush()
						}
					}
				}
				offset += int64(n)
			}
			if err != nil {
				readErr = err
				break
			}
		}
		_ = body.Close()
		if offset > blockRange.End {
			break
		}
		if readErr == nil || readErr == io.EOF {
			readErr = io.ErrUnexpectedEOF
		}
		if offset > startOffset {
			resumeAttempts = 0
		} else {
			resumeAttempts++
		}
		if resumeAttempts > 3 {
			return readErr
		}
		time.Sleep(time.Duration(resumeAttempts+1) * 250 * time.Millisecond)
	}
	if err := writer.Commit(); err != nil {
		return err
	}
	if err := s.cache.EvictIfNeeded(); err != nil {
		return err
	}
	if responseErr != nil {
		return &cacheBuildResponseError{err: responseErr}
	}
	return nil
}

func (s *Server) sourceMetadata(source model.MediaSource) (model.SourceMetadata, error) {
	if source.SizeTrusted && source.Size != nil && *source.Size > 0 {
		return model.SourceMetadata{
			URL:         source.Path,
			Size:        *source.Size,
			ContentType: inferredContentType(source.Path),
		}, nil
	}
	s.originSem <- struct{}{}
	meta, err := s.origin.Head(source.Path)
	<-s.originSem
	return meta, err
}

func inferredContentType(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	return mime.TypeByExtension(pathpkg.Ext(parsed.Path))
}

func (s *Server) streamOriginRange(url string, meta model.SourceMetadata, byteRange model.ByteRange, w io.Writer) error {
	body, err := s.origin.OpenRange(url, byteRange, meta.Size)
	if err != nil {
		return err
	}
	defer body.Close()
	_, err = io.CopyBuffer(w, body, make([]byte, int(s.cfg.Cache.ChunkBytes)))
	return err
}

func (s *Server) headTailBytes(size int64) (int64, int64) {
	return ranges.ConfiguredHeadTail(size, s.cfg.Cache.HeadBytes, s.cfg.Cache.TailBytes)
}

func (s *Server) openHeadResponseBytes(extension, rangeHeader string) *int64 {
	normalized := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(extension)), ".")
	if strings.TrimSpace(rangeHeader) == "bytes=0-" {
		if value, ok := s.cfg.Cache.OpenInitialResponseBytesByExtension[normalized]; ok {
			return &value
		}
	}
	if value, ok := s.cfg.Cache.OpenHeadResponseBytesByExtension[normalized]; ok {
		return &value
	}
	return s.cfg.Cache.OpenHeadResponseBytes
}

type cacheBuildLock struct {
	token chan struct{}
}

func newCacheBuildLock() *cacheBuildLock {
	lock := &cacheBuildLock{token: make(chan struct{}, 1)}
	lock.token <- struct{}{}
	return lock
}

func (l *cacheBuildLock) tryAcquire() bool {
	select {
	case <-l.token:
		return true
	default:
		return false
	}
}

func (l *cacheBuildLock) wait(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-l.token:
		l.release()
	case <-timer.C:
	}
}

func (l *cacheBuildLock) release() {
	select {
	case l.token <- struct{}{}:
	default:
	}
}

func (s *Server) cacheBuildLock(key, blockName string) *cacheBuildLock {
	lockKey := key + "\x00" + blockName
	s.cacheBuildMu.Lock()
	defer s.cacheBuildMu.Unlock()
	lock := s.cacheBuildLocks[lockKey]
	if lock == nil {
		lock = newCacheBuildLock()
		s.cacheBuildLocks[lockKey] = lock
	}
	return lock
}

func (s *Server) recordSession(ctx model.RequestContext, key string, meta model.SourceMetadata, byteRange model.ByteRange) {
	headSize, tailSize := s.headTailBytes(meta.Size)
	if !s.cfg.Session.Enabled || session.IsTailMetadataRange(meta.Size, byteRange, headSize, tailSize) {
		return
	}
	update := session.BuildUpdate(ctx, key, meta, byteRange, float64(time.Now().Unix()))
	if err := s.store.RecordPlayback(update); err != nil {
		s.addError("session record failed: " + errorClass(err))
	}
}

func (s *Server) streamFallback(w http.ResponseWriter, r *http.Request) {
	target := strings.TrimRight(s.cfg.FallbackBaseURL, "/") + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
	if err != nil {
		http.Error(w, "fallback request failed", http.StatusBadGateway)
		return
	}
	copyForwardHeaders(req.Header, r.Header)
	resp, err := s.fallbackClient.Do(req)
	if err != nil {
		http.Error(w, "fallback unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = io.CopyBuffer(w, resp.Body, make([]byte, int(s.cfg.Cache.ChunkBytes)))
	}
}

func writeRangeHeaders(w http.ResponseWriter, status int, byteRange model.ByteRange, meta model.SourceMetadata) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", byteRange.Length()))
	if status == http.StatusPartialContent {
		w.Header().Set("Content-Range", ranges.ContentRangeHeader(byteRange, meta.Size))
	}
	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
	}
	if meta.ETag != "" {
		w.Header().Set("ETag", meta.ETag)
	}
	if meta.LastModified != "" {
		w.Header().Set("Last-Modified", meta.LastModified)
	}
}

func writeChunks(w io.Writer, chunks <-chan []byte, after func()) error {
	for chunk := range chunks {
		if len(chunk) > 0 {
			if _, err := w.Write(chunk); err != nil {
				return err
			}
		}
	}
	if after != nil {
		after()
	}
	return nil
}

func drain(chunks <-chan []byte) {
	for range chunks {
	}
}

func (s *Server) addStat(fn func(*Stats)) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	fn(&s.stats)
}

func (s *Server) addError(message string) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	log.Print(message)
	s.stats.RecentErrors = append(s.stats.RecentErrors, message)
	if len(s.stats.RecentErrors) > 20 {
		s.stats.RecentErrors = s.stats.RecentErrors[len(s.stats.RecentErrors)-20:]
	}
	s.stats.ErrorEvents = append(s.stats.ErrorEvents, ErrorEvent{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Message: message})
	if len(s.stats.ErrorEvents) > 20 {
		s.stats.ErrorEvents = s.stats.ErrorEvents[len(s.stats.ErrorEvents)-20:]
	}
}

func newAccessLogEvent(r *http.Request) *accessLogEvent {
	return &accessLogEvent{
		requestID:   atomic.AddUint64(&accessLogSeq, 1),
		startedAt:   time.Now(),
		method:      r.Method,
		path:        r.URL.Path,
		rangeHeader: r.Header.Get("Range"),
		userAgent:   r.UserAgent(),
		clientIP:    requestClientIP(r),
		authCache:   "none",
		route:       "unknown",
		err:         "none",
	}
}

func (s *Server) logAccess(event *accessLogEvent, w *accessLogResponseWriter) {
	if event == nil || w == nil {
		return
	}
	durationMS := time.Since(event.startedAt).Milliseconds()
	ttfbMS := int64(-1)
	if !w.firstWriteAt.IsZero() {
		ttfbMS = w.firstWriteAt.Sub(event.startedAt).Milliseconds()
	}
	log.Printf(
		"event=access request_id=%d method=%s path=%q item_id=%s media_source_id=%s range=%q status=%d bytes=%d duration_ms=%d ttfb_ms=%d auth_ms=%d auth_cache=%s route=%s error=%s client_ip=%s user_agent=%q",
		event.requestID,
		event.method,
		event.path,
		event.itemID,
		event.mediaSourceID,
		event.rangeHeader,
		w.statusCode(),
		w.bytes,
		durationMS,
		ttfbMS,
		event.authMS,
		event.authCache,
		event.route,
		event.err,
		event.clientIP,
		event.userAgent,
	)
}

func setAccessRoute(event *accessLogEvent, route string) {
	if event != nil {
		event.route = route
	}
}

func setAccessError(event *accessLogEvent, err error) {
	if event != nil && err != nil {
		event.err = errorClass(err)
	}
}

func requestClientIP(r *http.Request) string {
	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if first = strings.TrimSpace(first); first != "" {
			return first
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func requestContainsPrewarmKey(r *http.Request, internalKey string) bool {
	if subtleEqual(r.Header.Get("X-Range-Cache-Prewarm-Key"), internalKey) {
		return true
	}
	auth := r.Header.Get("Authorization")
	scheme, value, ok := strings.Cut(auth, " ")
	return ok && strings.EqualFold(scheme, "Bearer") && subtleEqual(value, internalKey)
}

func requestContainsInternalKey(r *http.Request, internalKey string) bool {
	for key, values := range r.URL.Query() {
		lower := strings.ToLower(key)
		if lower == "api_key" || lower == "token" || lower == "x-emby-token" {
			for _, value := range values {
				if subtleEqual(value, internalKey) {
					return true
				}
			}
		}
	}
	for key, values := range r.Header {
		lower := strings.ToLower(key)
		if lower == "x-emby-token" || lower == "x-range-cache-prewarm-key" || lower == "authorization" {
			for _, value := range values {
				if subtleEqual(value, internalKey) || authorizationBearerMatches(value, internalKey) {
					return true
				}
			}
		}
	}
	return false
}

func authorizationBearerMatches(value, internalKey string) bool {
	scheme, token, ok := strings.Cut(value, " ")
	return ok && strings.EqualFold(scheme, "Bearer") && subtleEqual(token, internalKey)
}

func subtleEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var out byte
	for i := range a {
		out |= a[i] ^ b[i]
	}
	return out == 0
}

func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isInternalCaller(r *http.Request) bool {
	if !isLoopback(r.RemoteAddr) {
		return false
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if !isLoopback(strings.TrimSpace(first)) {
			return false
		}
	}
	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" && !isLoopback(realIP) {
		return false
	}
	return true
}

func isHTTP(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func errorClass(err error) string {
	if err == nil {
		return "none"
	}
	var authErr emby.AuthorizationError
	if errors.As(err, &authErr) {
		return "AuthorizationError"
	}
	var authUnavailable emby.AuthUnavailable
	if errors.As(err, &authUnavailable) {
		return "AuthUnavailable"
	}
	if errors.Is(err, context.Canceled) {
		return "Canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DeadlineExceeded"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "Timeout"
		}
		return "NetError"
	}
	for {
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			break
		}
		err = unwrapped
	}
	name := fmt.Sprintf("%T", err)
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		return name[idx+1:]
	}
	return name
}

func dirBytes(root string, include func(string) bool) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			if entry.Name() == "mid" {
				return filepath.SkipDir
			}
			return nil
		}
		if !include(path) {
			return nil
		}
		if info, err := entry.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

var hopByHopHeaders = map[string]struct{}{
	"connection":          {},
	"keep-alive":          {},
	"proxy-authenticate":  {},
	"proxy-authorization": {},
	"te":                  {},
	"trailer":             {},
	"transfer-encoding":   {},
	"upgrade":             {},
}

func copyForwardHeaders(dst, src http.Header) {
	for name, values := range src {
		lower := strings.ToLower(name)
		if lower == "host" {
			continue
		}
		if _, skip := hopByHopHeaders[lower]; skip {
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for name, values := range src {
		if _, skip := hopByHopHeaders[strings.ToLower(name)]; skip {
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *Server) StartBackground(ctx context.Context) {
	if s.cfg.Session.Enabled {
		go s.sessionPlannerLoop(ctx)
	}
	if s.cfg.Prefetch.Enabled && s.cfg.MiddleCache.Enabled {
		go s.prefetchLoop(ctx)
	}
	if s.cfg.Prewarm.Enabled && s.cfg.PrewarmAPIKey != "" {
		go s.prewarmScanLoop(ctx)
	}
}

func (s *Server) sessionPlannerLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Session.ObserverIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := float64(time.Now().Unix())
			if s.cfg.Session.ObserverEnabled {
				_ = s.observeSessions(now)
			}
			_, _ = s.store.MarkIdleSessions(now, s.cfg.Session.IdleSeconds)
			_, _ = s.store.ExpireOldSessions(now, s.cfg.Session.ExpireSeconds)
			candidates, err := s.store.PrefetchCandidateSessions()
			if err != nil {
				s.addError("prefetch candidates failed: " + errorClass(err))
				continue
			}
			for _, candidate := range candidates {
				priority := 10
				if candidate.Status == "stopped" {
					priority = 20
				}
				_, _ = prefetch.EnqueueForSession(s.store, candidate, s.cfg.Cache, s.cfg.Prefetch, s.cfg.MiddleCache, now, priority)
			}
		}
	}
}

func (s *Server) prefetchLoop(ctx context.Context) {
	worker := prefetch.NewWorker(s.cfg.Prefetch, s.cfg.Cache, s.store, s.middle)
	worker.RunningHook = func(delta int) {
		s.addStat(func(stats *Stats) {
			stats.Prefetch.Running += int64(delta)
			if stats.Prefetch.Running < 0 {
				stats.Prefetch.Running = 0
			}
		})
	}
	ticker := time.NewTicker(time.Duration(s.cfg.Prefetch.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.cfg.Prefetch.PauseWhenRolloutSessionActive {
				active, _ := s.store.RecentActiveSessions(float64(time.Now().Unix()), s.cfg.Session.IdleSeconds)
				if len(active) > 0 {
					continue
				}
			}
			result, err := worker.RunOnce(float64(time.Now().Unix()))
			if err != nil {
				s.addError("prefetch failed: " + errorClass(err))
				continue
			}
			s.addStat(func(stats *Stats) {
				stats.Prefetch.Done += int64(result.Completed)
				stats.Prefetch.Failed += int64(result.Failed)
			})
		}
	}
}

func (s *Server) prewarmScanLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Prewarm.IntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.prewarmRecentItems(); err != nil {
				s.addError("prewarm scan failed: " + errorClass(err))
			}
		}
	}
}

func (s *Server) prewarmRecentItems() error {
	if s.cfg.PrewarmAPIKey == "" {
		return nil
	}
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(s.cfg.EmbyBaseURL, "/")+"/Items", nil)
	if err != nil {
		return err
	}
	query := req.URL.Query()
	query.Set("api_key", s.cfg.PrewarmAPIKey)
	query.Set("SortBy", "DateCreated")
	query.Set("SortOrder", "Descending")
	query.Set("IncludeItemTypes", "Movie,Episode")
	query.Set("Recursive", "true")
	query.Set("Limit", fmt.Sprintf("%d", s.cfg.Prewarm.MaxItemsPerScan))
	req.URL.RawQuery = query.Encode()
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("items status %d", resp.StatusCode)
	}
	var payload struct {
		Items []struct {
			ID string `json:"Id"`
		} `json:"Items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	for _, item := range payload.Items {
		if item.ID == "" {
			continue
		}
		sourceIDs, err := s.playbackMediaSourceIDs(item.ID)
		if err != nil {
			s.addStat(func(stats *Stats) { stats.Prewarm.Skipped++ })
			continue
		}
		for _, mediaSourceID := range sourceIDs {
			if s.cfg.Rollout.ItemAllowed(item.ID) && s.cfg.Rollout.MediaSourceAllowed(mediaSourceID) {
				s.enqueuePrewarm(item.ID, mediaSourceID)
			}
		}
	}
	return nil
}

func (s *Server) playbackMediaSourceIDs(itemID string) ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(s.cfg.EmbyBaseURL, "/")+"/Items/"+url.PathEscape(itemID)+"/PlaybackInfo", nil)
	if err != nil {
		return nil, err
	}
	query := req.URL.Query()
	query.Set("api_key", s.cfg.PrewarmAPIKey)
	req.URL.RawQuery = query.Encode()
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("playbackinfo status %d", resp.StatusCode)
	}
	var payload struct {
		MediaSources []struct {
			ID string `json:"Id"`
		} `json:"MediaSources"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(payload.MediaSources))
	for _, source := range payload.MediaSources {
		if source.ID != "" {
			ids = append(ids, source.ID)
		}
	}
	return ids, nil
}

func (s *Server) observeSessions(now float64) error {
	if s.cfg.PrewarmAPIKey == "" {
		return nil
	}
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(s.cfg.EmbyBaseURL, "/")+"/Sessions", nil)
	if err != nil {
		return err
	}
	query := req.URL.Query()
	query.Set("api_key", s.cfg.PrewarmAPIKey)
	req.URL.RawQuery = query.Encode()
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("sessions status %d", resp.StatusCode)
	}
	var payload []struct {
		PlaySessionID string `json:"PlaySessionId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	observed := map[string]struct{}{}
	for _, entry := range payload {
		if entry.PlaySessionID != "" {
			observed[session.Hash(entry.PlaySessionID)] = struct{}{}
		}
	}
	if err := s.store.RecordObservedSessions(observed, now); err != nil {
		return err
	}
	_, err = s.store.MarkMissingObservedSessionsStopped(now, s.cfg.Session.StopGraceSeconds)
	return err
}
