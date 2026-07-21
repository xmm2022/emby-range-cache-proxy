package config

import (
	"encoding/json"
	"path/filepath"
	"sort"
)

type EffectiveConfig struct {
	EmbyBaseURL                 string                  `json:"emby_base_url"`
	FallbackBaseURL             string                  `json:"fallback_base_url"`
	ListenHost                  string                  `json:"listen_host"`
	ListenPort                  int                     `json:"listen_port"`
	CacheDir                    string                  `json:"cache_dir"`
	PrewarmAPIKey               any                     `json:"prewarm_api_key"`
	PlaybackInfoTimeoutSeconds  int                     `json:"playback_info_timeout_seconds"`
	PlaybackAuthCacheTTLSeconds int                     `json:"playback_auth_cache_ttl_seconds"`
	PathMappings                []EffectivePathMapping  `json:"path_mappings"`
	OpenList                    EffectiveOpenList       `json:"openlist"`
	DirectOpenList              EffectiveDirectOpenList `json:"direct_openlist"`
	DirectHTTP                  EffectiveDirectHTTP     `json:"direct_http"`
	DirectCache                 EffectiveDirectCache    `json:"direct_cache"`
	Rollout                     EffectiveRollout        `json:"rollout"`
	Cache                       EffectiveCache          `json:"cache"`
	Prewarm                     EffectivePrewarm        `json:"prewarm"`
	Session                     EffectiveSession        `json:"session"`
	MiddleCache                 EffectiveMiddleCache    `json:"middle_cache"`
	Prefetch                    EffectivePrefetch       `json:"prefetch"`
}

type EffectivePathMapping struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type EffectiveOpenList struct {
	Enabled        bool   `json:"enabled"`
	BaseURL        string `json:"base_url"`
	Token          any    `json:"token"`
	Password       any    `json:"password"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type EffectiveDirectOpenList struct {
	Enabled    bool   `json:"enabled"`
	PathPrefix string `json:"path_prefix"`
	Token      any    `json:"token"`
}

type EffectiveDirectHTTP struct {
	Enabled         bool   `json:"enabled"`
	PathPrefix      string `json:"path_prefix"`
	UpstreamBaseURL string `json:"upstream_base_url"`
}

type EffectiveDirectCache struct {
	RequireEligibility bool `json:"require_eligibility"`
}

type EffectiveRollout struct {
	Enabled              bool     `json:"enabled"`
	ItemAllowlist        []string `json:"item_allowlist"`
	MediaSourceAllowlist []string `json:"media_source_allowlist"`
	PathPrefixAllowlist  []string `json:"path_prefix_allowlist"`
}

type EffectiveCache struct {
	MaxBytes                            int64            `json:"max_bytes"`
	BuildWaitSeconds                    float64          `json:"build_wait_seconds"`
	HeadBytes                           int64            `json:"head_bytes"`
	TailBytes                           int64            `json:"tail_bytes"`
	AdaptiveTailMaxBytes                int64            `json:"adaptive_tail_max_bytes"`
	ChunkBytes                          int64            `json:"chunk_bytes"`
	DefaultOpenRangeBytes               int64            `json:"default_open_range_bytes"`
	OpenHeadResponseBytes               *int64           `json:"open_head_response_bytes"`
	OpenHeadResponseBytesByExtension    map[string]int64 `json:"open_head_response_bytes_by_extension"`
	OpenInitialResponseBytesByExtension map[string]int64 `json:"open_initial_response_bytes_by_extension"`
}

type EffectivePrewarm struct {
	Enabled                    bool `json:"enabled"`
	IntervalSeconds            int  `json:"interval_seconds"`
	MaxItemsPerScan            int  `json:"max_items_per_scan"`
	Concurrency                int  `json:"concurrency"`
	PlaybackInfoTimeoutSeconds int  `json:"playback_info_timeout_seconds"`
}

type EffectiveSession struct {
	Enabled                 bool   `json:"enabled"`
	StateDB                 string `json:"state_db"`
	ObserverEnabled         bool   `json:"observer_enabled"`
	ObserverIntervalSeconds int    `json:"observer_interval_seconds"`
	IdleSeconds             int    `json:"idle_seconds"`
	StopGraceSeconds        int    `json:"stop_grace_seconds"`
	ExpireSeconds           int    `json:"expire_seconds"`
}

type EffectiveMiddleCache struct {
	Enabled      bool  `json:"enabled"`
	MaxBytes     int64 `json:"max_bytes"`
	TTLSeconds   int   `json:"ttl_seconds"`
	SegmentBytes int64 `json:"segment_bytes"`
	MinFreeBytes int64 `json:"min_free_bytes"`
}

type EffectivePrefetch struct {
	Enabled                       bool  `json:"enabled"`
	WindowBytes                   int64 `json:"window_bytes"`
	ResumeOverlapBytes            int64 `json:"resume_overlap_bytes"`
	MaxSessionBytes               int64 `json:"max_session_bytes"`
	ResumeBackBlocks              int   `json:"resume_back_blocks"`
	ResumeForwardBlocks           int   `json:"resume_forward_blocks"`
	MaxQueueDepth                 int   `json:"max_queue_depth"`
	Concurrency                   int   `json:"concurrency"`
	PerOriginConcurrency          int   `json:"per_origin_concurrency"`
	BandwidthBytesPerSecond       int64 `json:"bandwidth_bytes_per_second"`
	PauseWhenRolloutSessionActive bool  `json:"pause_when_rollout_session_active"`
	PollIntervalSeconds           int   `json:"poll_interval_seconds"`
	ErrorBackoffSeconds           int   `json:"error_backoff_seconds"`
}

func MarshalEffectiveJSON(cfg Config, showSecrets bool) ([]byte, error) {
	return json.MarshalIndent(Effective(cfg, showSecrets), "", "  ")
}

func Effective(cfg Config, showSecrets bool) EffectiveConfig {
	prewarmKey := any(nil)
	if cfg.PrewarmAPIKey != "" {
		if showSecrets {
			prewarmKey = cfg.PrewarmAPIKey
		} else {
			prewarmKey = "REDACTED"
		}
	}
	openListToken := any(nil)
	if cfg.OpenList.Token != "" {
		if showSecrets {
			openListToken = cfg.OpenList.Token
		} else {
			openListToken = "REDACTED"
		}
	}
	openListPassword := any(nil)
	if cfg.OpenList.Password != "" {
		if showSecrets {
			openListPassword = cfg.OpenList.Password
		} else {
			openListPassword = "REDACTED"
		}
	}
	directOpenListToken := any(nil)
	if cfg.DirectOpenList.Token != "" {
		if showSecrets {
			directOpenListToken = cfg.DirectOpenList.Token
		} else {
			directOpenListToken = "REDACTED"
		}
	}
	pathMappings := make([]EffectivePathMapping, 0, len(cfg.PathMappings))
	for _, mapping := range cfg.PathMappings {
		pathMappings = append(pathMappings, EffectivePathMapping{From: mapping.SourcePrefix, To: mapping.TargetPrefix})
	}
	stateDB := cfg.Session.StateDB
	if stateDB == "" {
		stateDB = filepath.Join(cfg.CacheDir, "state", "phase2.sqlite3")
	}
	return EffectiveConfig{
		EmbyBaseURL:                 cfg.EmbyBaseURL,
		FallbackBaseURL:             cfg.FallbackBaseURL,
		ListenHost:                  cfg.ListenHost,
		ListenPort:                  cfg.ListenPort,
		CacheDir:                    cfg.CacheDir,
		PrewarmAPIKey:               prewarmKey,
		PlaybackInfoTimeoutSeconds:  cfg.PlaybackInfoTimeoutSeconds,
		PlaybackAuthCacheTTLSeconds: cfg.PlaybackAuthCacheTTLSeconds,
		PathMappings:                pathMappings,
		OpenList: EffectiveOpenList{
			Enabled:        cfg.OpenList.Enabled,
			BaseURL:        cfg.OpenList.BaseURL,
			Token:          openListToken,
			Password:       openListPassword,
			TimeoutSeconds: cfg.OpenList.TimeoutSeconds,
		},
		DirectOpenList: EffectiveDirectOpenList{
			Enabled:    cfg.DirectOpenList.Enabled,
			PathPrefix: cfg.DirectOpenList.PathPrefix,
			Token:      directOpenListToken,
		},
		DirectHTTP: EffectiveDirectHTTP{
			Enabled:         cfg.DirectHTTP.Enabled,
			PathPrefix:      cfg.DirectHTTP.PathPrefix,
			UpstreamBaseURL: cfg.DirectHTTP.UpstreamBaseURL,
		},
		DirectCache: EffectiveDirectCache{
			RequireEligibility: cfg.DirectCache.RequireEligibility,
		},
		Rollout: EffectiveRollout{
			Enabled:              cfg.Rollout.Enabled,
			ItemAllowlist:        sortedSet(cfg.Rollout.ItemAllowlist),
			MediaSourceAllowlist: sortedSet(cfg.Rollout.MediaSourceAllowlist),
			PathPrefixAllowlist:  append([]string(nil), cfg.Rollout.PathPrefixAllowlist...),
		},
		Cache: EffectiveCache{
			MaxBytes:                            cfg.Cache.MaxBytes,
			BuildWaitSeconds:                    cfg.Cache.BuildWaitSeconds,
			HeadBytes:                           cfg.Cache.HeadBytes,
			TailBytes:                           cfg.Cache.TailBytes,
			AdaptiveTailMaxBytes:                cfg.Cache.AdaptiveTailMaxBytes,
			ChunkBytes:                          cfg.Cache.ChunkBytes,
			DefaultOpenRangeBytes:               cfg.Cache.DefaultOpenRangeBytes,
			OpenHeadResponseBytes:               cfg.Cache.OpenHeadResponseBytes,
			OpenHeadResponseBytesByExtension:    cfg.Cache.OpenHeadResponseBytesByExtension,
			OpenInitialResponseBytesByExtension: cfg.Cache.OpenInitialResponseBytesByExtension,
		},
		Prewarm: EffectivePrewarm{
			Enabled:                    cfg.Prewarm.Enabled,
			IntervalSeconds:            cfg.Prewarm.IntervalSeconds,
			MaxItemsPerScan:            cfg.Prewarm.MaxItemsPerScan,
			Concurrency:                cfg.Prewarm.Concurrency,
			PlaybackInfoTimeoutSeconds: cfg.Prewarm.PlaybackInfoTimeoutSeconds,
		},
		Session: EffectiveSession{
			Enabled:                 cfg.Session.Enabled,
			StateDB:                 stateDB,
			ObserverEnabled:         cfg.Session.ObserverEnabled,
			ObserverIntervalSeconds: cfg.Session.ObserverIntervalSeconds,
			IdleSeconds:             cfg.Session.IdleSeconds,
			StopGraceSeconds:        cfg.Session.StopGraceSeconds,
			ExpireSeconds:           cfg.Session.ExpireSeconds,
		},
		MiddleCache: EffectiveMiddleCache{
			Enabled:      cfg.MiddleCache.Enabled,
			MaxBytes:     cfg.MiddleCache.MaxBytes,
			TTLSeconds:   cfg.MiddleCache.TTLSeconds,
			SegmentBytes: cfg.MiddleCache.SegmentBytes,
			MinFreeBytes: cfg.MiddleCache.MinFreeBytes,
		},
		Prefetch: EffectivePrefetch{
			Enabled:                       cfg.Prefetch.Enabled,
			WindowBytes:                   cfg.Prefetch.WindowBytes,
			ResumeOverlapBytes:            cfg.Prefetch.ResumeOverlapBytes,
			MaxSessionBytes:               cfg.Prefetch.MaxSessionBytes,
			ResumeBackBlocks:              cfg.Prefetch.ResumeBackBlocks,
			ResumeForwardBlocks:           cfg.Prefetch.ResumeForwardBlocks,
			MaxQueueDepth:                 cfg.Prefetch.MaxQueueDepth,
			Concurrency:                   cfg.Prefetch.Concurrency,
			PerOriginConcurrency:          cfg.Prefetch.PerOriginConcurrency,
			BandwidthBytesPerSecond:       cfg.Prefetch.BandwidthBytesPerSecond,
			PauseWhenRolloutSessionActive: cfg.Prefetch.PauseWhenRolloutSessionActive,
			PollIntervalSeconds:           cfg.Prefetch.PollIntervalSeconds,
			ErrorBackoffSeconds:           cfg.Prefetch.ErrorBackoffSeconds,
		},
	}
}

func sortedSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
