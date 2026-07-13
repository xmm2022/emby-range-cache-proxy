package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
)

const (
	mib int64 = 1024 * 1024
	gib int64 = 1024 * 1024 * 1024
)

type Config struct {
	EmbyBaseURL                 string
	FallbackBaseURL             string
	ListenHost                  string
	ListenPort                  int
	CacheDir                    string
	PrewarmAPIKey               string
	PlaybackInfoTimeoutSeconds  int
	PlaybackAuthCacheTTLSeconds int
	PathMappings                []PathMapping
	OpenList                    OpenListConfig
	DirectOpenList              DirectOpenListConfig
	DirectHTTP                  DirectHTTPConfig
	Rollout                     RolloutConfig
	Cache                       CacheConfig
	Prewarm                     PrewarmConfig
	Session                     SessionConfig
	MiddleCache                 MiddleCacheConfig
	Prefetch                    PrefetchConfig
}

type PathMapping struct {
	SourcePrefix string
	TargetPrefix string
}

type OpenListConfig struct {
	Enabled        bool
	BaseURL        string
	Token          string
	Password       string
	TimeoutSeconds int
}

type DirectOpenListConfig struct {
	Enabled    bool
	PathPrefix string
	Token      string
}

type DirectHTTPConfig struct {
	Enabled         bool
	PathPrefix      string
	UpstreamBaseURL string
}

type RolloutConfig struct {
	Enabled              bool
	ItemAllowlist        map[string]struct{}
	MediaSourceAllowlist map[string]struct{}
	PathPrefixAllowlist  []string
}

func (r RolloutConfig) ItemAllowed(itemID string) bool {
	return len(r.ItemAllowlist) == 0 || hasString(r.ItemAllowlist, itemID)
}

func (r RolloutConfig) MediaSourceAllowed(mediaSourceID string) bool {
	return len(r.MediaSourceAllowlist) == 0 || hasString(r.MediaSourceAllowlist, mediaSourceID)
}

func (r RolloutConfig) PathAllowed(value string) bool {
	if len(r.PathPrefixAllowlist) == 0 {
		return true
	}
	if value == "" {
		return false
	}
	for _, prefix := range r.PathPrefixAllowlist {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func (r RolloutConfig) InScope(itemID, mediaSourceID, path string) bool {
	return r.Enabled && r.ItemAllowed(itemID) && r.MediaSourceAllowed(mediaSourceID) && r.PathAllowed(path)
}

type CacheConfig struct {
	MaxBytes                            int64
	BuildWaitSeconds                    float64
	HeadBytes                           int64
	TailBytes                           int64
	AdaptiveTailMaxBytes                int64
	ChunkBytes                          int64
	DefaultOpenRangeBytes               int64
	OpenHeadResponseBytes               *int64
	OpenHeadResponseBytesByExtension    map[string]int64
	OpenInitialResponseBytesByExtension map[string]int64
}

type PrewarmConfig struct {
	Enabled                    bool
	IntervalSeconds            int
	MaxItemsPerScan            int
	Concurrency                int
	PlaybackInfoTimeoutSeconds int
}

type SessionConfig struct {
	Enabled                 bool
	StateDB                 string
	ObserverEnabled         bool
	ObserverIntervalSeconds int
	IdleSeconds             int
	StopGraceSeconds        int
	ExpireSeconds           int
}

type MiddleCacheConfig struct {
	Enabled      bool
	MaxBytes     int64
	TTLSeconds   int
	SegmentBytes int64
	MinFreeBytes int64
}

type PrefetchConfig struct {
	Enabled                       bool
	WindowBytes                   int64
	ResumeOverlapBytes            int64
	MaxSessionBytes               int64
	ResumeBackBlocks              int
	ResumeForwardBlocks           int
	MaxQueueDepth                 int
	Concurrency                   int
	PerOriginConcurrency          int
	BandwidthBytesPerSecond       int64
	PauseWhenRolloutSessionActive bool
	PollIntervalSeconds           int
	ErrorBackoffSeconds           int
}

func LoadFile(filePath string) (Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return Config{}, err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return Config{}, err
	}
	return parseRaw(raw)
}

func parseRaw(raw map[string]any) (Config, error) {
	cfg := Config{
		ListenHost:                  "127.0.0.1",
		ListenPort:                  18180,
		PlaybackInfoTimeoutSeconds:  15,
		PlaybackAuthCacheTTLSeconds: 30,
		Cache: CacheConfig{
			MaxBytes:              512 * gib,
			BuildWaitSeconds:      0.25,
			HeadBytes:             8 * mib,
			TailBytes:             8 * mib,
			ChunkBytes:            mib,
			DefaultOpenRangeBytes: 16 * mib,
		},
		Prewarm: PrewarmConfig{
			IntervalSeconds:            900,
			MaxItemsPerScan:            100,
			Concurrency:                1,
			PlaybackInfoTimeoutSeconds: 15,
		},
		Session: SessionConfig{
			ObserverIntervalSeconds: 30,
			IdleSeconds:             180,
			StopGraceSeconds:        60,
			ExpireSeconds:           86400,
		},
		MiddleCache: MiddleCacheConfig{
			MaxBytes:     128 * gib,
			TTLSeconds:   7 * 24 * 60 * 60,
			SegmentBytes: 64 * mib,
			MinFreeBytes: 50 * gib,
		},
		Prefetch: PrefetchConfig{
			WindowBytes:                   256 * mib,
			ResumeOverlapBytes:            128 * mib,
			MaxSessionBytes:               512 * mib,
			ResumeBackBlocks:              1,
			ResumeForwardBlocks:           2,
			MaxQueueDepth:                 200,
			Concurrency:                   1,
			PerOriginConcurrency:          1,
			BandwidthBytesPerSecond:       30 * mib,
			PauseWhenRolloutSessionActive: true,
			PollIntervalSeconds:           5,
			ErrorBackoffSeconds:           300,
		},
		OpenList: OpenListConfig{
			TimeoutSeconds: 10,
		},
		DirectOpenList: DirectOpenListConfig{
			PathPrefix: "/openlist/",
		},
		DirectHTTP: DirectHTTPConfig{
			PathPrefix: "/http/",
		},
	}

	var err error
	if cfg.EmbyBaseURL, err = stringField(raw, "emby_base_url", true); err != nil {
		return Config{}, err
	}
	cfg.EmbyBaseURL = strings.TrimRight(cfg.EmbyBaseURL, "/")
	if cfg.FallbackBaseURL, err = stringField(raw, "fallback_base_url", false); err != nil {
		return Config{}, err
	}
	if cfg.FallbackBaseURL == "" {
		cfg.FallbackBaseURL = cfg.EmbyBaseURL
	} else {
		cfg.FallbackBaseURL = strings.TrimRight(cfg.FallbackBaseURL, "/")
	}
	if cfg.CacheDir, err = stringField(raw, "cache_dir", true); err != nil {
		return Config{}, err
	}
	if cfg.ListenHost, err = stringFieldDefault(raw, "listen_host", cfg.ListenHost); err != nil {
		return Config{}, err
	}
	if cfg.ListenPort, err = intFieldDefault(raw, "listen_port", cfg.ListenPort); err != nil {
		return Config{}, err
	}
	if cfg.PrewarmAPIKey, err = stringField(raw, "prewarm_api_key", false); err != nil {
		return Config{}, err
	}
	if cfg.PlaybackInfoTimeoutSeconds, err = intFieldDefault(raw, "playback_info_timeout_seconds", cfg.PlaybackInfoTimeoutSeconds); err != nil {
		return Config{}, err
	}
	if cfg.PlaybackAuthCacheTTLSeconds, err = intFieldDefault(raw, "playback_auth_cache_ttl_seconds", cfg.PlaybackAuthCacheTTLSeconds); err != nil {
		return Config{}, err
	}
	if cfg.PathMappings, err = parsePathMappings(raw["path_mappings"]); err != nil {
		return Config{}, err
	}
	if err := parseOpenList(raw["openlist"], &cfg.OpenList); err != nil {
		return Config{}, err
	}
	if err := parseDirectOpenList(raw["direct_openlist"], &cfg.DirectOpenList); err != nil {
		return Config{}, err
	}
	if err := parseDirectHTTP(raw["direct_http"], &cfg.DirectHTTP); err != nil {
		return Config{}, err
	}
	if cfg.Rollout, err = parseRollout(raw["rollout"]); err != nil {
		return Config{}, err
	}
	if err := parseCache(raw["cache"], &cfg.Cache); err != nil {
		return Config{}, err
	}
	if err := parsePrewarm(raw["prewarm"], &cfg.Prewarm); err != nil {
		return Config{}, err
	}
	if err := parseSession(raw["session"], &cfg.Session); err != nil {
		return Config{}, err
	}
	if err := parseMiddleCache(raw["middle_cache"], &cfg.MiddleCache); err != nil {
		return Config{}, err
	}
	if err := parsePrefetch(raw["prefetch"], &cfg.Prefetch); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.EmbyBaseURL == "" {
		return fmt.Errorf("emby_base_url is required")
	}
	if c.CacheDir == "" {
		return fmt.Errorf("cache_dir is required")
	}
	if c.ListenPort <= 0 || c.ListenPort > 65535 {
		return fmt.Errorf("listen_port must be valid")
	}
	if c.PlaybackInfoTimeoutSeconds <= 0 {
		return fmt.Errorf("playback_info_timeout_seconds must be positive")
	}
	if c.PlaybackAuthCacheTTLSeconds < 0 {
		return fmt.Errorf("playback_auth_cache_ttl_seconds must be >= 0")
	}
	if c.OpenList.Enabled && c.OpenList.BaseURL == "" {
		return fmt.Errorf("openlist.base_url is required when openlist.enabled=true")
	}
	if c.OpenList.TimeoutSeconds <= 0 {
		return fmt.Errorf("openlist.timeout_seconds must be positive")
	}
	if c.DirectOpenList.Enabled {
		if !c.OpenList.Enabled {
			return fmt.Errorf("openlist.enabled must be true when direct_openlist.enabled=true")
		}
		if c.DirectOpenList.PathPrefix == "" || !strings.HasPrefix(c.DirectOpenList.PathPrefix, "/") || !strings.HasSuffix(c.DirectOpenList.PathPrefix, "/") {
			return fmt.Errorf("direct_openlist.path_prefix must start and end with /")
		}
		if c.DirectOpenList.Token == "" {
			return fmt.Errorf("direct_openlist.token is required when direct_openlist.enabled=true")
		}
	}
	if c.DirectHTTP.Enabled {
		if c.DirectHTTP.PathPrefix == "" || !strings.HasPrefix(c.DirectHTTP.PathPrefix, "/") || !strings.HasSuffix(c.DirectHTTP.PathPrefix, "/") {
			return fmt.Errorf("direct_http.path_prefix must start and end with /")
		}
		if !strings.HasPrefix(c.DirectHTTP.UpstreamBaseURL, "http://") && !strings.HasPrefix(c.DirectHTTP.UpstreamBaseURL, "https://") {
			return fmt.Errorf("direct_http.upstream_base_url must be http or https")
		}
	}
	if c.Cache.MaxBytes <= 0 || c.Cache.BuildWaitSeconds < 0 || c.Cache.HeadBytes <= 0 || c.Cache.TailBytes <= 0 || c.Cache.ChunkBytes <= 0 || c.Cache.DefaultOpenRangeBytes <= 0 {
		return fmt.Errorf("cache values must be positive")
	}
	if c.Cache.AdaptiveTailMaxBytes < 0 || (c.Cache.AdaptiveTailMaxBytes > 0 && c.Cache.AdaptiveTailMaxBytes < c.Cache.TailBytes) {
		return fmt.Errorf("cache.adaptive_tail_max_bytes must be zero or at least cache.tail_bytes")
	}
	if c.Cache.OpenHeadResponseBytes != nil && *c.Cache.OpenHeadResponseBytes <= 0 {
		return fmt.Errorf("cache.open_head_response_bytes must be positive")
	}
	for extension, value := range c.Cache.OpenHeadResponseBytesByExtension {
		if extension == "" || value <= 0 {
			return fmt.Errorf("cache.open_head_response_bytes_by_extension values must use non-empty extensions and positive sizes")
		}
	}
	for extension, value := range c.Cache.OpenInitialResponseBytesByExtension {
		if extension == "" || value <= 0 {
			return fmt.Errorf("cache.open_initial_response_bytes_by_extension values must use non-empty extensions and positive sizes")
		}
	}
	if c.Prewarm.IntervalSeconds < 60 {
		return fmt.Errorf("prewarm.interval_seconds must be >= 60")
	}
	if c.Prewarm.Concurrency <= 0 {
		return fmt.Errorf("prewarm.concurrency must be positive")
	}
	if c.Prewarm.PlaybackInfoTimeoutSeconds <= 0 {
		return fmt.Errorf("prewarm.playback_info_timeout_seconds must be positive")
	}
	if c.Session.ObserverIntervalSeconds <= 0 || c.Session.IdleSeconds <= 0 || c.Session.StopGraceSeconds <= 0 || c.Session.ExpireSeconds <= 0 {
		return fmt.Errorf("session values must be positive")
	}
	if c.MiddleCache.MaxBytes <= 0 || c.MiddleCache.TTLSeconds <= 0 || c.MiddleCache.SegmentBytes <= 0 || c.MiddleCache.MinFreeBytes < 0 {
		return fmt.Errorf("middle_cache values must be valid")
	}
	if c.Prefetch.WindowBytes <= 0 || c.Prefetch.ResumeOverlapBytes < 0 || c.Prefetch.MaxSessionBytes <= 0 || c.Prefetch.ResumeBackBlocks < 0 || c.Prefetch.ResumeForwardBlocks < 0 || c.Prefetch.MaxQueueDepth <= 0 || c.Prefetch.Concurrency <= 0 || c.Prefetch.PerOriginConcurrency <= 0 || c.Prefetch.BandwidthBytesPerSecond <= 0 || c.Prefetch.PollIntervalSeconds <= 0 || c.Prefetch.ErrorBackoffSeconds <= 0 {
		return fmt.Errorf("prefetch values must be valid")
	}
	return nil
}

func parseRollout(value any) (RolloutConfig, error) {
	data, err := object(value, "rollout")
	if err != nil {
		return RolloutConfig{}, err
	}
	enabled, err := boolFieldDefault(data, "enabled", false)
	if err != nil {
		return RolloutConfig{}, err
	}
	items, err := stringSetField(data, "item_allowlist")
	if err != nil {
		return RolloutConfig{}, err
	}
	media, err := stringSetField(data, "media_source_allowlist")
	if err != nil {
		return RolloutConfig{}, err
	}
	prefixes, err := stringListField(data, "path_prefix_allowlist")
	if err != nil {
		return RolloutConfig{}, err
	}
	return RolloutConfig{Enabled: enabled, ItemAllowlist: items, MediaSourceAllowlist: media, PathPrefixAllowlist: prefixes}, nil
}

func parsePathMappings(value any) ([]PathMapping, error) {
	if value == nil {
		return nil, nil
	}
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("path_mappings must be a list")
	}
	out := make([]PathMapping, 0, len(values))
	for i, item := range values {
		obj, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path_mappings[%d] must be an object", i)
		}
		from := firstString(obj, "from", "source_prefix")
		to := firstString(obj, "to", "target_prefix")
		if from == "" || to == "" {
			return nil, fmt.Errorf("path_mappings[%d] must include from and to", i)
		}
		normalized, err := normalizeSourcePrefix(from)
		if err != nil {
			return nil, err
		}
		out = append(out, PathMapping{SourcePrefix: normalized, TargetPrefix: to})
	}
	return out, nil
}

func parseOpenList(value any, cfg *OpenListConfig) error {
	data, err := object(value, "openlist")
	if err != nil {
		return err
	}
	if cfg.Enabled, err = boolFieldDefault(data, "enabled", cfg.Enabled); err != nil {
		return err
	}
	if cfg.BaseURL, err = stringFieldDefault(data, "base_url", cfg.BaseURL); err != nil {
		return err
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.Token, err = stringFieldDefault(data, "token", cfg.Token); err != nil {
		return err
	}
	if cfg.Password, err = stringFieldDefault(data, "password", cfg.Password); err != nil {
		return err
	}
	if cfg.TimeoutSeconds, err = intFieldDefault(data, "timeout_seconds", cfg.TimeoutSeconds); err != nil {
		return err
	}
	return nil
}

func parseDirectOpenList(value any, cfg *DirectOpenListConfig) error {
	data, err := object(value, "direct_openlist")
	if err != nil {
		return err
	}
	if cfg.Enabled, err = boolFieldDefault(data, "enabled", cfg.Enabled); err != nil {
		return err
	}
	if cfg.PathPrefix, err = stringFieldDefault(data, "path_prefix", cfg.PathPrefix); err != nil {
		return err
	}
	if cfg.PathPrefix == "" {
		cfg.PathPrefix = "/openlist/"
	}
	if !strings.HasPrefix(cfg.PathPrefix, "/") {
		cfg.PathPrefix = "/" + cfg.PathPrefix
	}
	if !strings.HasSuffix(cfg.PathPrefix, "/") {
		cfg.PathPrefix += "/"
	}
	if cfg.Token, err = stringFieldDefault(data, "token", cfg.Token); err != nil {
		return err
	}
	return nil
}

func parseDirectHTTP(value any, cfg *DirectHTTPConfig) error {
	data, err := object(value, "direct_http")
	if err != nil {
		return err
	}
	if cfg.Enabled, err = boolFieldDefault(data, "enabled", cfg.Enabled); err != nil {
		return err
	}
	if cfg.PathPrefix, err = stringFieldDefault(data, "path_prefix", cfg.PathPrefix); err != nil {
		return err
	}
	if cfg.PathPrefix == "" {
		cfg.PathPrefix = "/http/"
	}
	if !strings.HasPrefix(cfg.PathPrefix, "/") {
		cfg.PathPrefix = "/" + cfg.PathPrefix
	}
	if !strings.HasSuffix(cfg.PathPrefix, "/") {
		cfg.PathPrefix += "/"
	}
	if cfg.UpstreamBaseURL, err = stringFieldDefault(data, "upstream_base_url", cfg.UpstreamBaseURL); err != nil {
		return err
	}
	cfg.UpstreamBaseURL = strings.TrimRight(cfg.UpstreamBaseURL, "/")
	return nil
}

func parseCache(value any, cfg *CacheConfig) error {
	data, err := object(value, "cache")
	if err != nil {
		return err
	}
	if cfg.MaxBytes, err = int64FieldDefault(data, "max_bytes", cfg.MaxBytes); err != nil {
		return err
	}
	if cfg.BuildWaitSeconds, err = floatFieldDefault(data, "build_wait_seconds", cfg.BuildWaitSeconds); err != nil {
		return err
	}
	if cfg.HeadBytes, err = int64FieldDefault(data, "head_bytes", cfg.HeadBytes); err != nil {
		return err
	}
	if cfg.TailBytes, err = int64FieldDefault(data, "tail_bytes", cfg.TailBytes); err != nil {
		return err
	}
	if cfg.AdaptiveTailMaxBytes, err = int64FieldDefault(data, "adaptive_tail_max_bytes", cfg.AdaptiveTailMaxBytes); err != nil {
		return err
	}
	if cfg.ChunkBytes, err = int64FieldDefault(data, "chunk_bytes", cfg.ChunkBytes); err != nil {
		return err
	}
	if cfg.DefaultOpenRangeBytes, err = int64FieldDefault(data, "default_open_range_bytes", cfg.DefaultOpenRangeBytes); err != nil {
		return err
	}
	if value, ok := data["open_head_response_bytes"]; ok && value != nil {
		parsed, err := int64FromAny(value, "cache.open_head_response_bytes")
		if err != nil {
			return err
		}
		cfg.OpenHeadResponseBytes = &parsed
	}
	if value, ok := data["open_head_response_bytes_by_extension"]; ok && value != nil {
		cfg.OpenHeadResponseBytesByExtension, err = parseExtensionByteMap(value, "cache.open_head_response_bytes_by_extension")
		if err != nil {
			return err
		}
	}
	if value, ok := data["open_initial_response_bytes_by_extension"]; ok && value != nil {
		cfg.OpenInitialResponseBytesByExtension, err = parseExtensionByteMap(value, "cache.open_initial_response_bytes_by_extension")
		if err != nil {
			return err
		}
	}
	return nil
}

func parseExtensionByteMap(value any, name string) (map[string]int64, error) {
	values, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", name)
	}
	out := make(map[string]int64, len(values))
	for extension, raw := range values {
		normalized := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(extension)), ".")
		if normalized == "" {
			return nil, fmt.Errorf("%s contains an empty extension", name)
		}
		parsed, err := int64FromAny(raw, name+"."+extension)
		if err != nil {
			return nil, err
		}
		out[normalized] = parsed
	}
	return out, nil
}

func parsePrewarm(value any, cfg *PrewarmConfig) error {
	data, err := object(value, "prewarm")
	if err != nil {
		return err
	}
	if cfg.Enabled, err = boolFieldDefault(data, "enabled", cfg.Enabled); err != nil {
		return err
	}
	if cfg.IntervalSeconds, err = intFieldDefault(data, "interval_seconds", cfg.IntervalSeconds); err != nil {
		return err
	}
	if cfg.MaxItemsPerScan, err = intFieldDefault(data, "max_items_per_scan", cfg.MaxItemsPerScan); err != nil {
		return err
	}
	if cfg.Concurrency, err = intFieldDefault(data, "concurrency", cfg.Concurrency); err != nil {
		return err
	}
	if cfg.PlaybackInfoTimeoutSeconds, err = intFieldDefault(data, "playback_info_timeout_seconds", cfg.PlaybackInfoTimeoutSeconds); err != nil {
		return err
	}
	return nil
}

func parseSession(value any, cfg *SessionConfig) error {
	data, err := object(value, "session")
	if err != nil {
		return err
	}
	if cfg.Enabled, err = boolFieldDefault(data, "enabled", cfg.Enabled); err != nil {
		return err
	}
	if cfg.StateDB, err = stringFieldDefault(data, "state_db", cfg.StateDB); err != nil {
		return err
	}
	if cfg.ObserverEnabled, err = boolFieldDefault(data, "observer_enabled", cfg.ObserverEnabled); err != nil {
		return err
	}
	if cfg.ObserverIntervalSeconds, err = intFieldDefault(data, "observer_interval_seconds", cfg.ObserverIntervalSeconds); err != nil {
		return err
	}
	if cfg.IdleSeconds, err = intFieldDefault(data, "idle_seconds", cfg.IdleSeconds); err != nil {
		return err
	}
	if cfg.StopGraceSeconds, err = intFieldDefault(data, "stop_grace_seconds", cfg.StopGraceSeconds); err != nil {
		return err
	}
	if cfg.ExpireSeconds, err = intFieldDefault(data, "expire_seconds", cfg.ExpireSeconds); err != nil {
		return err
	}
	return nil
}

func parseMiddleCache(value any, cfg *MiddleCacheConfig) error {
	data, err := object(value, "middle_cache")
	if err != nil {
		return err
	}
	if cfg.Enabled, err = boolFieldDefault(data, "enabled", cfg.Enabled); err != nil {
		return err
	}
	if cfg.MaxBytes, err = int64FieldDefault(data, "max_bytes", cfg.MaxBytes); err != nil {
		return err
	}
	if cfg.TTLSeconds, err = intFieldDefault(data, "ttl_seconds", cfg.TTLSeconds); err != nil {
		return err
	}
	if cfg.SegmentBytes, err = int64FieldDefault(data, "segment_bytes", cfg.SegmentBytes); err != nil {
		return err
	}
	if cfg.MinFreeBytes, err = int64FieldDefault(data, "min_free_bytes", cfg.MinFreeBytes); err != nil {
		return err
	}
	return nil
}

func parsePrefetch(value any, cfg *PrefetchConfig) error {
	data, err := object(value, "prefetch")
	if err != nil {
		return err
	}
	if cfg.Enabled, err = boolFieldDefault(data, "enabled", cfg.Enabled); err != nil {
		return err
	}
	if cfg.WindowBytes, err = int64FieldDefault(data, "window_bytes", cfg.WindowBytes); err != nil {
		return err
	}
	if cfg.ResumeOverlapBytes, err = int64FieldDefault(data, "resume_overlap_bytes", cfg.ResumeOverlapBytes); err != nil {
		return err
	}
	if cfg.MaxSessionBytes, err = int64FieldDefault(data, "max_session_bytes", cfg.MaxSessionBytes); err != nil {
		return err
	}
	if cfg.ResumeBackBlocks, err = intFieldDefault(data, "resume_back_blocks", cfg.ResumeBackBlocks); err != nil {
		return err
	}
	if cfg.ResumeForwardBlocks, err = intFieldDefault(data, "resume_forward_blocks", cfg.ResumeForwardBlocks); err != nil {
		return err
	}
	if cfg.MaxQueueDepth, err = intFieldDefault(data, "max_queue_depth", cfg.MaxQueueDepth); err != nil {
		return err
	}
	if cfg.Concurrency, err = intFieldDefault(data, "concurrency", cfg.Concurrency); err != nil {
		return err
	}
	if cfg.PerOriginConcurrency, err = intFieldDefault(data, "per_origin_concurrency", cfg.PerOriginConcurrency); err != nil {
		return err
	}
	if cfg.BandwidthBytesPerSecond, err = int64FieldDefault(data, "bandwidth_bytes_per_second", cfg.BandwidthBytesPerSecond); err != nil {
		return err
	}
	if cfg.PauseWhenRolloutSessionActive, err = boolFieldDefault(data, "pause_when_rollout_session_active", cfg.PauseWhenRolloutSessionActive); err != nil {
		return err
	}
	if cfg.PollIntervalSeconds, err = intFieldDefault(data, "poll_interval_seconds", cfg.PollIntervalSeconds); err != nil {
		return err
	}
	if cfg.ErrorBackoffSeconds, err = intFieldDefault(data, "error_backoff_seconds", cfg.ErrorBackoffSeconds); err != nil {
		return err
	}
	return nil
}

func normalizeSourcePrefix(value string) (string, error) {
	prefix := strings.TrimSpace(value)
	if !strings.HasPrefix(prefix, "/") {
		return "", fmt.Errorf("path_mappings source prefix must be absolute")
	}
	cleaned := path.Clean(prefix)
	if cleaned == "/" || strings.Contains(cleaned, "/../") || strings.Contains(cleaned, "/./") {
		return "", fmt.Errorf("path_mappings source prefix must be a non-root directory")
	}
	parts := strings.Split(cleaned, "/")
	for _, part := range parts {
		if part == "." || part == ".." {
			return "", fmt.Errorf("path_mappings source prefix must be a non-root directory")
		}
	}
	if !strings.HasSuffix(cleaned, "/") {
		cleaned += "/"
	}
	return cleaned, nil
}

func object(value any, name string) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	data, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", name)
	}
	return data, nil
}

func stringField(data map[string]any, key string, required bool) (string, error) {
	value, ok := data[key]
	if !ok || value == nil {
		if required {
			return "", fmt.Errorf("%s is required", key)
		}
		return "", nil
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	if required && text == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return text, nil
}

func stringFieldDefault(data map[string]any, key, fallback string) (string, error) {
	value, ok := data[key]
	if !ok || value == nil {
		return fallback, nil
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return text, nil
}

func boolFieldDefault(data map[string]any, key string, fallback bool) (bool, error) {
	value, ok := data[key]
	if !ok || value == nil {
		return fallback, nil
	}
	parsed, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return parsed, nil
}

func intFieldDefault(data map[string]any, key string, fallback int) (int, error) {
	value, ok := data[key]
	if !ok || value == nil {
		return fallback, nil
	}
	parsed, err := int64FromAny(value, key)
	return int(parsed), err
}

func int64FieldDefault(data map[string]any, key string, fallback int64) (int64, error) {
	value, ok := data[key]
	if !ok || value == nil {
		return fallback, nil
	}
	return int64FromAny(value, key)
}

func floatFieldDefault(data map[string]any, key string, fallback float64) (float64, error) {
	value, ok := data[key]
	if !ok || value == nil {
		return fallback, nil
	}
	switch typed := value.(type) {
	case float64:
		return typed, nil
	case string:
		var parsed float64
		if _, err := fmt.Sscan(typed, &parsed); err != nil {
			return 0, fmt.Errorf("%s must be a number", key)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("%s must be a number", key)
	}
}

func int64FromAny(value any, key string) (int64, error) {
	switch typed := value.(type) {
	case float64:
		if typed != float64(int64(typed)) {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return int64(typed), nil
	case string:
		var parsed int64
		if _, err := fmt.Sscan(typed, &parsed); err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
}

func stringListField(data map[string]any, key string) ([]string, error) {
	value, ok := data[key]
	if !ok || value == nil {
		return nil, nil
	}
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a list", key)
	}
	out := make([]string, 0, len(values))
	for _, item := range values {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s must contain strings", key)
		}
		out = append(out, text)
	}
	return out, nil
}

func stringSetField(data map[string]any, key string) (map[string]struct{}, error) {
	values, err := stringListField(data, key)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out, nil
}

func firstString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key].(string); ok {
			return value
		}
	}
	return ""
}

func hasString(values map[string]struct{}, value string) bool {
	_, ok := values[value]
	return ok
}
