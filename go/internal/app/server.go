package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/cache"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/emby"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/headtail"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/middle"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
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

	httpClient  *http.Client
	origin      *origin.Client
	auth        *emby.AuthClient
	prewarmAuth *emby.AuthClient

	originSem chan struct{}

	statsMu sync.Mutex
	stats   Stats

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
}

type ConfigSummary struct {
	Listen                 string `json:"listen"`
	CacheDir               string `json:"cache_dir"`
	RolloutEnabled         bool   `json:"rollout_enabled"`
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
	prewarmTimeout := time.Duration(cfg.Prewarm.PlaybackInfoTimeoutSeconds) * time.Second
	server := &Server{
		cfg:          cfg,
		startedAt:    time.Now(),
		cache:        headtail.NewCache(cfg.CacheDir, cfg.Cache.MaxBytes),
		store:        store,
		httpClient:   &http.Client{Timeout: prewarmTimeout},
		origin:       origin.NewClient(cfg.Cache.ChunkBytes),
		auth:         emby.NewAuthClient(cfg.EmbyBaseURL),
		prewarmAuth:  emby.NewAuthClientWithTimeout(cfg.EmbyBaseURL, prewarmTimeout),
		originSem:    make(chan struct{}, 32),
		prewarmTasks: make(map[string]struct{}),
		prewarmSem:   make(chan struct{}, prewarmConcurrency),
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
	default:
		s.proxyHandler(w, r)
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if !isLoopback(r.RemoteAddr) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.SnapshotStats())
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !isLoopback(r.RemoteAddr) {
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
	stats.DiskFreeBytes = diskFree(s.cfg.CacheDir)
	stats.RecentErrors = append([]string(nil), s.stats.RecentErrors...)
	return stats
}

func (s *Server) handleInternalPrewarm(w http.ResponseWriter, r *http.Request) {
	if s.cfg.PrewarmAPIKey == "" {
		http.NotFound(w, r)
		return
	}
	if !isLoopback(r.RemoteAddr) {
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
	if !isHTTP(sourceMedia.Path) || !s.cfg.Rollout.InScope(itemID, mediaSourceID, sourceMedia.Path) {
		return fmt.Errorf("source out of scope")
	}
	meta, err := s.origin.Head(sourceMedia.Path)
	if err != nil {
		return err
	}
	key := cache.Key(sourceMedia, meta)
	headSize, tailSize := ranges.AdaptiveHeadTail(meta.Size)
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
		return fmt.Errorf("already cached")
	}
	return nil
}

func (s *Server) proxyHandler(w http.ResponseWriter, r *http.Request) {
	if s.cfg.PrewarmAPIKey != "" && requestContainsInternalKey(r, s.cfg.PrewarmAPIKey) {
		s.addStat(func(stats *Stats) { stats.Counters.Denied++ })
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	ctx, ok := request.ParseOriginal(r.Method, r.URL.RequestURI(), r.Header)
	if !ok || !s.preAuthRollout(ctx) {
		s.addStat(func(stats *Stats) { stats.Counters.Fallback++ })
		s.streamFallback(w, r)
		return
	}
	sourceMedia, err := s.auth.Authorize(ctx)
	if err != nil {
		var authErr emby.AuthorizationError
		if errors.As(err, &authErr) {
			s.addStat(func(stats *Stats) { stats.Counters.Denied++ })
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		s.addStat(func(stats *Stats) { stats.Counters.Fallback++ })
		s.streamFallback(w, r)
		return
	}
	sourceMedia = source.ResolveMediaSource(sourceMedia, s.cfg.PathMappings, s.cfg.Rollout.PathPrefixAllowlist)
	if !isHTTP(sourceMedia.Path) || !s.cfg.Rollout.InScope(ctx.ItemID, ctx.MediaSourceID, sourceMedia.Path) {
		s.addStat(func(stats *Stats) { stats.Counters.Fallback++ })
		s.streamFallback(w, r)
		return
	}
	if err := s.serveAuthorizedRange(w, r, sourceMedia, ctx); err != nil {
		s.addError("proxy failed: " + errorClass(err))
		s.addStat(func(stats *Stats) {
			stats.Counters.ProxyErrors++
			stats.Counters.Fallback++
		})
		s.streamFallback(w, r)
	}
}

func (s *Server) preAuthRollout(ctx model.RequestContext) bool {
	return s.cfg.Rollout.Enabled && s.cfg.Rollout.ItemAllowed(ctx.ItemID) && s.cfg.Rollout.MediaSourceAllowed(ctx.MediaSourceID)
}

func (s *Server) serveAuthorizedRange(w http.ResponseWriter, r *http.Request, sourceMedia model.MediaSource, ctx model.RequestContext) error {
	s.originSem <- struct{}{}
	meta, err := s.origin.Head(sourceMedia.Path)
	<-s.originSem
	if err != nil {
		return err
	}
	headSize, tailSize := ranges.AdaptiveHeadTail(meta.Size)
	byteRange, err := ranges.PlanPlaybackRange(r.Header.Get("Range"), meta.Size, headSize, tailSize, s.cfg.Cache.DefaultOpenRangeBytes, s.cfg.Cache.OpenHeadResponseBytes)
	if err != nil {
		return err
	}
	key := cache.Key(sourceMedia, meta)
	_ = s.store.UpsertSourceMetadata(ctx.ItemID, ctx.MediaSourceID, key, meta.URL, session.OriginSignature(meta), meta.Size, float64(time.Now().Unix()))
	status := http.StatusOK
	if r.Header.Get("Range") != "" {
		status = http.StatusPartialContent
	}
	writeRangeHeaders(w, status, byteRange, meta)
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return nil
	}
	if s.cfg.MiddleCache.Enabled {
		chunks, err := s.middle.IterBlock(key, byteRange, s.cfg.Cache.ChunkBytes, float64(time.Now().Unix()))
		if err == nil && chunks != nil {
			s.addStat(func(stats *Stats) {
				stats.Counters.MiddleHit++
				stats.Counters.CacheHit++
			})
			w.WriteHeader(status)
			if err := writeChunks(w, chunks, func() {
				s.recordSession(ctx, key, meta, byteRange)
			}); err != nil {
				s.addError("middle cache response failed: " + errorClass(err))
				s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			}
			return nil
		}
		s.addStat(func(stats *Stats) { stats.Counters.MiddleMiss++ })
	}
	blockName, blockRange := headtail.BlockForRequest(byteRange, meta.Size, headSize, tailSize)
	if blockName != "" {
		chunks, err := s.cache.IterBlock(key, blockName, byteRange, s.cfg.Cache.ChunkBytes)
		if err != nil {
			return err
		}
		if chunks != nil {
			s.addStat(func(stats *Stats) { stats.Counters.CacheHit++ })
			w.WriteHeader(status)
			if err := writeChunks(w, chunks, func() {
				s.recordSession(ctx, key, meta, byteRange)
			}); err != nil {
				s.addError("cache response failed: " + errorClass(err))
				s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			}
			return nil
		}
		w.WriteHeader(status)
		if err := s.buildHeadTailBlock(sourceMedia.Path, meta, key, blockName, blockRange, byteRange, w); err != nil {
			s.addError("cache build stream failed: " + errorClass(err))
			s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
			return nil
		}
		s.addStat(func(stats *Stats) { stats.Counters.CacheBuild++ })
		s.recordSession(ctx, key, meta, byteRange)
		return nil
	}
	w.WriteHeader(status)
	if err := s.streamOriginRange(meta.URL, meta, byteRange, w); err != nil {
		s.addError("origin stream failed: " + errorClass(err))
		s.addStat(func(stats *Stats) { stats.Counters.ProxyErrors++ })
		return nil
	}
	s.addStat(func(stats *Stats) { stats.Counters.Origin++ })
	s.recordSession(ctx, key, meta, byteRange)
	return nil
}

func (s *Server) buildHeadTailBlock(sourceURL string, meta model.SourceMetadata, key, blockName string, blockRange, responseRange model.ByteRange, writers ...io.Writer) error {
	if s.cfg.MiddleCache.MinFreeBytes > 0 {
		free := diskFree(s.cfg.CacheDir)
		if free >= 0 && free-blockRange.Length() < s.cfg.MiddleCache.MinFreeBytes {
			return fmt.Errorf("insufficient disk free space")
		}
	}
	body, err := s.origin.OpenRange(meta.URL, blockRange, meta.Size)
	if err != nil {
		return err
	}
	defer body.Close()
	writer, err := s.cache.StageBlock(key, blockName, blockRange)
	if err != nil {
		return err
	}
	defer writer.Abort()
	buf := make([]byte, int(s.cfg.Cache.ChunkBytes))
	offset := blockRange.Start
	var responseWriter io.Writer
	if len(writers) > 0 {
		responseWriter = writers[0]
	}
	for {
		n, readErr := body.Read(buf)
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
						return err
					}
				}
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if err := writer.Commit(); err != nil {
		return err
	}
	return s.cache.EvictIfNeeded()
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

func (s *Server) recordSession(ctx model.RequestContext, key string, meta model.SourceMetadata, byteRange model.ByteRange) {
	if !s.cfg.Session.Enabled || session.IsTailMetadataRange(meta.Size, byteRange) {
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
	resp, err := s.httpClient.Do(req)
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
	name := fmt.Sprintf("%T", err)
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		return name[idx+1:]
	}
	return name
}

func dirBytes(root string, include func(string) bool) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !include(path) {
			return nil
		}
		if info, err := entry.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

func diskFree(path string) int64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return -1
	}
	return int64(stat.Bavail) * int64(stat.Bsize)
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
				_, _ = prefetch.EnqueueForSession(s.store, candidate, s.cfg.Prefetch, s.cfg.MiddleCache, now, priority)
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
