package prefetch

import (
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/cache"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/middle"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/origin"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/ranges"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/state"
)

type RunResult struct {
	Completed int
	Failed    int
	Skipped   int
}

type Worker struct {
	Prefetch    config.PrefetchConfig
	CacheConfig config.CacheConfig
	Store       *state.Store
	Middle      *middle.Cache
	Origin      *origin.Client
	RunningHook func(delta int)
	limiter     *BandwidthLimiter
	originLocks *originLimiter
}

func NewWorker(prefetch config.PrefetchConfig, cacheConfig config.CacheConfig, store *state.Store, middleCache *middle.Cache) *Worker {
	limiter := NewBandwidthLimiter(prefetch.BandwidthBytesPerSecond)
	if prefetch.BandwidthBytesPerSecond <= 0 {
		limiter = NewBandwidthLimiter(1)
	}
	return &Worker{
		Prefetch:    prefetch,
		CacheConfig: cacheConfig,
		Store:       store,
		Middle:      middleCache,
		Origin:      origin.NewClient(cacheConfig.ChunkBytes),
		limiter:     limiter,
		originLocks: newOriginLimiter(prefetch.PerOriginConcurrency),
	}
}

func (w *Worker) RunOnce(now float64) (RunResult, error) {
	if !w.Prefetch.Enabled {
		return RunResult{}, nil
	}
	concurrency := w.Prefetch.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	tasks, err := w.Store.ClaimPrefetchTasks(concurrency, now, w.Prefetch.ErrorBackoffSeconds)
	if err != nil {
		return RunResult{}, err
	}
	if len(tasks) == 0 {
		return RunResult{}, nil
	}
	if w.RunningHook != nil {
		w.RunningHook(len(tasks))
		defer w.RunningHook(-len(tasks))
	}
	var result RunResult
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(task state.PrefetchTaskRecord) {
			defer wg.Done()
			taskResult := w.runTask(task, now)
			mu.Lock()
			result.Completed += taskResult.Completed
			result.Failed += taskResult.Failed
			result.Skipped += taskResult.Skipped
			mu.Unlock()
		}(task)
	}
	wg.Wait()
	return result, nil
}

func (w *Worker) runTask(task state.PrefetchTaskRecord, now float64) RunResult {
	byteRange := model.ByteRange{Start: task.Start, End: task.End}
	if byteRange.Length() > w.Middle.MaxBytes {
		_ = w.Store.SkipPrefetchTask(task.ID, "RangeTooLarge", now, 0, task.Attempts)
		return RunResult{Skipped: 1}
	}
	sourceMeta, err := w.Store.GetSourceMetadata(task.ItemID, task.MediaSourceID, task.CacheKey)
	if err != nil || sourceMeta == nil {
		_ = w.Store.SkipPrefetchTask(task.ID, "SourceUnavailable", now, w.Prefetch.ErrorBackoffSeconds, task.Attempts)
		return RunResult{Skipped: 1}
	}
	release := w.originLocks.acquire(sourceMeta.OriginURL)
	defer release()
	meta, err := w.Origin.Head(sourceMeta.OriginURL)
	if err != nil {
		_ = w.Store.FailPrefetchTask(task.ID, "OriginError", now, w.Prefetch.ErrorBackoffSeconds, task.Attempts)
		return RunResult{Failed: 1}
	}
	expectedKey := cache.Key(model.MediaSource{
		ItemID:        task.ItemID,
		MediaSourceID: task.MediaSourceID,
		Path:          sourceMeta.OriginURL,
		Protocol:      "Http",
	}, meta)
	if expectedKey != task.CacheKey {
		_ = w.Store.FailPrefetchTask(task.ID, "PrefetchSourceMismatch", now, 0, task.Attempts)
		return RunResult{Failed: 1}
	}
	body, err := w.Origin.OpenRange(meta.URL, byteRange, meta.Size)
	if err != nil {
		_ = w.Store.FailPrefetchTask(task.ID, "OriginError", now, w.Prefetch.ErrorBackoffSeconds, task.Attempts)
		return RunResult{Failed: 1}
	}
	defer body.Close()
	reader := io.Reader(body)
	if w.limiter != nil {
		reader = &limitedReader{reader: body, limiter: w.limiter}
	}
	ok, err := w.Middle.StorePrefetchBlockFromReader(task.ID, task.Attempts, task.CacheKey, byteRange, reader, now)
	if err != nil {
		_ = w.Store.FailPrefetchTask(task.ID, "StoreError", now, w.Prefetch.ErrorBackoffSeconds, task.Attempts)
		return RunResult{Failed: 1}
	}
	if !ok {
		return RunResult{Skipped: 1}
	}
	_, _ = w.Middle.EvictExpired(now)
	_, _ = w.Middle.EvictLRUIfNeeded()
	return RunResult{Completed: 1}
}

func PlanMiddleRanges(mediaSize, headSize, tailSize, anchorOffset int64, queuedUntil *int64, prefetch config.PrefetchConfig, middleCache config.MiddleCacheConfig) []model.ByteRange {
	segment := middleCache.SegmentBytes
	if segment <= 0 {
		return nil
	}
	headEnd := headSize - 1
	if headEnd >= mediaSize {
		headEnd = mediaSize - 1
	}
	tailStart := mediaSize - tailSize
	if tailStart < 0 {
		tailStart = 0
	}
	middleStart := headEnd + 1
	middleEnd := tailStart - 1
	if middleStart > middleEnd {
		return nil
	}
	start, sessionEnd, ok := planMiddleWindow(middleStart, middleEnd, segment, headEnd, anchorOffset, queuedUntil, prefetch)
	if !ok {
		return nil
	}
	if start < middleStart {
		start = middleStart
	}
	if sessionEnd > middleEnd {
		sessionEnd = middleEnd
	}
	if start > sessionEnd {
		return nil
	}
	var out []model.ByteRange
	current := start
	for current <= sessionEnd {
		segmentEnd := current + segment - 1
		if current%segment != 0 {
			segmentEnd = alignUp(current, segment) - 1
		}
		end := segmentEnd
		if end > sessionEnd {
			end = sessionEnd
		}
		if end > middleEnd {
			end = middleEnd
		}
		if end >= middleStart && current <= middleEnd {
			startRange := current
			if startRange < middleStart {
				startRange = middleStart
			}
			out = append(out, model.ByteRange{Start: startRange, End: end})
		}
		current = end + 1
	}
	return out
}

func planMiddleWindow(middleStart, middleEnd, segment, headEnd, anchorOffset int64, queuedUntil *int64, prefetch config.PrefetchConfig) (int64, int64, bool) {
	if prefetch.ResumeBackBlocks > 0 || prefetch.ResumeForwardBlocks > 0 {
		if anchorOffset <= headEnd {
			return 0, 0, false
		}
		anchorBlockStart := alignDown(anchorOffset, segment)
		start := anchorBlockStart - int64(prefetch.ResumeBackBlocks)*segment
		sessionEnd := anchorBlockStart + int64(prefetch.ResumeForwardBlocks+1)*segment - 1
		if queuedUntil != nil {
			start = *queuedUntil + 1
		}
		return start, sessionEnd, true
	}

	windowBytes := minPositive(headEnd+1, prefetch.WindowBytes, prefetch.MaxSessionBytes)
	if windowBytes <= 0 {
		return 0, 0, false
	}
	var start int64
	if queuedUntil == nil {
		if anchorOffset <= headEnd {
			return 0, 0, false
		}
		overlap := prefetch.ResumeOverlapBytes
		if half := windowBytes / 2; overlap > half {
			overlap = half
		}
		start = anchorOffset - overlap
		if start < middleStart {
			start = middleStart
		}
		if windowBytes > segment {
			start = alignDown(start, segment)
			if start < middleStart {
				start = middleStart
			}
		}
	} else {
		start = *queuedUntil + 1
		if start < middleStart {
			start = middleStart
		}
	}
	sessionEnd := start + windowBytes - 1
	if sessionEnd > middleEnd {
		sessionEnd = middleEnd
	}
	return start, sessionEnd, true
}

func EnqueueForSession(store *state.Store, session state.PlaybackSessionRecord, prefetch config.PrefetchConfig, middleCache config.MiddleCacheConfig, now float64, priority int) (int, error) {
	headSize, tailSize := ranges.AdaptiveHeadTail(session.MediaSize)
	rangesToQueue := PlanMiddleRanges(session.MediaSize, headSize, tailSize, session.LastRangeEnd, nil, prefetch, middleCache)
	if len(rangesToQueue) == 0 {
		return 0, nil
	}
	targetEnd := rangesToQueue[len(rangesToQueue)-1].End
	var queuedUntil *int64
	if session.QueuedUntil.Valid && rangesToQueue[0].Start <= session.QueuedUntil.Int64 && session.QueuedUntil.Int64 < targetEnd {
		value := session.QueuedUntil.Int64
		queuedUntil = &value
		rangesToQueue = PlanMiddleRanges(session.MediaSize, headSize, tailSize, session.LastRangeEnd, queuedUntil, prefetch, middleCache)
	}
	inserted := 0
	var highestEnd *int64
	for _, byteRange := range rangesToQueue {
		if byteRange.End > targetEnd {
			byteRange.End = targetEnd
		}
		existing, err := store.ReusablePrefetchRanges(session.CacheKey, byteRange)
		if err != nil {
			return inserted, err
		}
		missing := subtractRanges(byteRange, existing)
		if len(missing) == 0 {
			value := byteRange.End
			highestEnd = &value
			continue
		}
		for _, gap := range missing {
			task, err := store.EnqueuePrefetchTask(session.ItemID, session.MediaSourceID, session.CacheKey, gap.Start, gap.End, priority, now, prefetch.MaxQueueDepth)
			if err != nil {
				return inserted, err
			}
			if task == nil {
				exists, err := store.PrefetchTaskExists(session.CacheKey, gap.Start, gap.End)
				if err != nil {
					return inserted, err
				}
				if exists {
					value := gap.End
					highestEnd = &value
					continue
				}
				if highestEnd != nil {
					return inserted, store.UpdateSessionQueuedUntil(session.SessionHash, *highestEnd, now)
				}
				return inserted, nil
			}
			inserted++
			value := gap.End
			highestEnd = &value
		}
	}
	if highestEnd != nil {
		if err := store.UpdateSessionQueuedUntil(session.SessionHash, *highestEnd, now); err != nil {
			return inserted, err
		}
	}
	return inserted, nil
}

func subtractRanges(byteRange model.ByteRange, existing []model.ByteRange) []model.ByteRange {
	var missing []model.ByteRange
	current := byteRange.Start
	for _, ex := range existing {
		if ex.End < current {
			continue
		}
		if ex.Start > byteRange.End {
			break
		}
		if ex.Start > current {
			end := ex.Start - 1
			if end > byteRange.End {
				end = byteRange.End
			}
			missing = append(missing, model.ByteRange{Start: current, End: end})
		}
		if ex.End+1 > current {
			current = ex.End + 1
		}
		if current > byteRange.End {
			break
		}
	}
	if current <= byteRange.End {
		missing = append(missing, model.ByteRange{Start: current, End: byteRange.End})
	}
	return missing
}

func alignDown(value, alignment int64) int64 {
	return value - value%alignment
}

func alignUp(value, alignment int64) int64 {
	if value%alignment == 0 {
		return value
	}
	return value + alignment - value%alignment
}

func minPositive(values ...int64) int64 {
	min := int64(0)
	for _, value := range values {
		if value <= 0 {
			return 0
		}
		if min == 0 || value < min {
			min = value
		}
	}
	return min
}

type BandwidthLimiter struct {
	bytesPerSecond int64
}

func NewBandwidthLimiter(bytesPerSecond int64) *BandwidthLimiter {
	return &BandwidthLimiter{bytesPerSecond: bytesPerSecond}
}

func (l *BandwidthLimiter) Consume(byteCount int) {
	if l == nil || l.bytesPerSecond <= 0 || byteCount <= 0 {
		return
	}
	time.Sleep(time.Duration(int64(byteCount) * int64(time.Second) / l.bytesPerSecond))
}

type limitedReader struct {
	reader  io.Reader
	limiter *BandwidthLimiter
}

func (r *limitedReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.limiter.Consume(n)
	return n, err
}

type originLimiter struct {
	limit int
	mu    sync.Mutex
	sems  map[string]chan struct{}
}

func newOriginLimiter(limit int) *originLimiter {
	if limit <= 0 {
		limit = 1
	}
	return &originLimiter{limit: limit, sems: make(map[string]chan struct{})}
}

func (l *originLimiter) acquire(rawURL string) func() {
	host := rawURL
	if parsed, err := url.Parse(rawURL); err == nil && parsed.Host != "" {
		host = parsed.Host
	}
	l.mu.Lock()
	sem := l.sems[host]
	if sem == nil {
		sem = make(chan struct{}, l.limit)
		l.sems[host] = sem
	}
	l.mu.Unlock()
	sem <- struct{}{}
	return func() { <-sem }
}

func ShortHash(value string) string {
	if value == "" {
		return "none"
	}
	if len(value) < 12 {
		return value
	}
	return value[:12]
}

var ErrSourceMismatch = fmt.Errorf("prefetch source metadata mismatch")
