package middle

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/diskfree"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/state"
)

var keyRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Cache struct {
	Root         string
	Store        *state.Store
	MaxBytes     int64
	TTLSeconds   int
	MinFreeBytes int64
}

type middleBlockSpan struct {
	record    state.MiddleBlockRecord
	byteRange model.ByteRange
}

func NewCache(root string, store *state.Store, maxBytes int64, ttlSeconds int, minFreeBytes ...int64) *Cache {
	_ = os.MkdirAll(root, 0o755)
	cache := &Cache{Root: root, Store: store, MaxBytes: maxBytes, TTLSeconds: ttlSeconds}
	if len(minFreeBytes) > 0 {
		cache.MinFreeBytes = minFreeBytes[0]
	}
	return cache
}

func (c *Cache) StoreBlock(key string, byteRange model.ByteRange, data []byte, now float64) error {
	if int64(len(data)) != byteRange.Length() {
		return fmt.Errorf("data length must match byte range")
	}
	if err := c.validate(key, byteRange); err != nil {
		return err
	}
	if err := c.ensureFree(byteRange.Length()); err != nil {
		return err
	}
	path, sidecar := c.paths(key, byteRange)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		if writeErr != nil {
			return writeErr
		}
		return closeErr
	}
	sidecarTmp := sidecar + "." + filepath.Base(tmpPath) + ".tmp"
	if err := os.WriteFile(sidecarTmp, []byte(fmt.Sprintf("%d-%d\n", byteRange.Start, byteRange.End)), 0o644); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		_ = os.Remove(sidecarTmp)
		return err
	}
	if err := os.Rename(sidecarTmp, sidecar); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(sidecarTmp)
		return err
	}
	return c.Store.UpsertMiddleBlock(c.record(key, byteRange, now, byteRange.Length()))
}

func (c *Cache) StorePrefetchBlockFromReader(taskID int64, expectedAttempts int, key string, byteRange model.ByteRange, reader io.Reader, now float64) (bool, error) {
	if err := c.validate(key, byteRange); err != nil {
		return false, err
	}
	if err := c.ensureFree(byteRange.Length()); err != nil {
		return false, err
	}
	path, sidecar := c.paths(key, byteRange)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return false, err
	}
	tmpPath := tmp.Name()
	written, copyErr := io.Copy(tmp, io.LimitReader(reader, byteRange.Length()+1))
	closeErr := tmp.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		if copyErr != nil {
			return false, copyErr
		}
		return false, closeErr
	}
	if written != byteRange.Length() {
		_ = os.Remove(tmpPath)
		return false, fmt.Errorf("prefetch data length mismatch")
	}
	sidecarTmp := sidecar + "." + filepath.Base(tmpPath) + ".tmp"
	if err := os.WriteFile(sidecarTmp, []byte(fmt.Sprintf("%d-%d\n", byteRange.Start, byteRange.End)), 0o644); err != nil {
		_ = os.Remove(tmpPath)
		return false, err
	}
	publish := func() error {
		if err := os.Rename(tmpPath, path); err != nil {
			return err
		}
		if err := os.Rename(sidecarTmp, sidecar); err != nil {
			_ = os.Remove(path)
			return err
		}
		return nil
	}
	ok, err := c.Store.PublishMiddleBlockAndCompletePrefetchTask(
		taskID,
		expectedAttempts,
		c.record(key, byteRange, now, byteRange.Length()),
		now,
		publish,
	)
	if err != nil || !ok {
		_ = os.Remove(tmpPath)
		_ = os.Remove(sidecarTmp)
		return ok, err
	}
	return true, nil
}

func (c *Cache) IterBlock(key string, requested model.ByteRange, chunkBytes int64, now float64) (<-chan []byte, error) {
	if chunkBytes <= 0 {
		return nil, fmt.Errorf("chunkBytes must be positive")
	}
	if !keyRE.MatchString(key) || requested.Start < 0 || requested.End < requested.Start {
		return nil, fmt.Errorf("invalid middle cache request")
	}
	records, err := c.Store.FindMiddleBlocks(key, requested)
	if err != nil || len(records) == 0 {
		return nil, err
	}
	spans, ok := c.coveringSpans(records, requested, now)
	if !ok {
		return nil, nil
	}
	for _, span := range spans {
		_ = c.Store.TouchMiddleBlock(key, span.record.Start, span.record.End, now, c.TTLSeconds)
	}
	out := make(chan []byte)
	go func() {
		defer close(out)
		buf := make([]byte, int(chunkBytes))
		for _, span := range spans {
			if !c.streamSpan(out, span, buf) {
				return
			}
		}
	}()
	return out, nil
}

func (c *Cache) coveringSpans(records []state.MiddleBlockRecord, requested model.ByteRange, now float64) ([]middleBlockSpan, bool) {
	expectedStart := requested.Start
	var spans []middleBlockSpan
	for _, record := range records {
		if record.End < expectedStart {
			continue
		}
		if record.Start > expectedStart {
			return nil, false
		}
		if !c.validRecord(record) {
			_ = c.Store.DeleteMiddleBlockRecord(record.CacheKey, record.Start, record.End)
			return nil, false
		}
		if record.ExpiresAt <= now {
			_ = c.RemoveBlock(record)
			return nil, false
		}
		path, sidecar := c.recordPaths(record)
		if !validFiles(record, path, sidecar) {
			_ = c.RemoveBlock(record)
			return nil, false
		}
		spanEnd := record.End
		if spanEnd > requested.End {
			spanEnd = requested.End
		}
		spans = append(spans, middleBlockSpan{
			record:    record,
			byteRange: model.ByteRange{Start: expectedStart, End: spanEnd},
		})
		if spanEnd >= requested.End {
			return spans, true
		}
		expectedStart = spanEnd + 1
	}
	return nil, false
}

func (c *Cache) streamSpan(out chan<- []byte, span middleBlockSpan, buf []byte) bool {
	path, _ := c.recordPaths(span.record)
	handle, err := os.Open(path)
	if err != nil {
		_ = c.RemoveBlock(span.record)
		return false
	}
	defer handle.Close()
	if _, err := handle.Seek(span.byteRange.Start-span.record.Start, io.SeekStart); err != nil {
		_ = c.RemoveBlock(span.record)
		return false
	}
	remaining := span.byteRange.Length()
	for remaining > 0 {
		n := int64(len(buf))
		if n > remaining {
			n = remaining
		}
		readBuf := buf[:n]
		if _, err := io.ReadFull(handle, readBuf); err != nil {
			_ = c.RemoveBlock(span.record)
			return false
		}
		chunk := make([]byte, n)
		copy(chunk, readBuf)
		out <- chunk
		remaining -= n
	}
	return true
}

func (c *Cache) EvictExpired(now float64) (int, error) {
	records, err := c.Store.ExpiredMiddleBlocks(now)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, record := range records {
		if record.ExpiresAt <= now {
			if err := c.RemoveBlock(record); err != nil {
				return removed, err
			}
			removed++
		}
	}
	return removed, nil
}

func (c *Cache) EvictLRUIfNeeded() (int, error) {
	removed := 0
	for {
		total, err := c.Store.MiddleCacheBytes()
		if err != nil {
			return removed, err
		}
		if total <= c.MaxBytes {
			return removed, nil
		}
		records, err := c.Store.LeastRecentMiddleBlocks()
		if err != nil {
			return removed, err
		}
		if len(records) == 0 {
			return removed, nil
		}
		if err := c.RemoveBlock(records[0]); err != nil {
			return removed, err
		}
		removed++
	}
}

func (c *Cache) RemoveBlock(record state.MiddleBlockRecord) error {
	if c.validRecord(record) {
		path, sidecar := c.recordPaths(record)
		_ = os.Remove(path)
		_ = os.Remove(sidecar)
	}
	return c.Store.DeleteMiddleBlockRecord(record.CacheKey, record.Start, record.End)
}

func (c *Cache) validate(key string, byteRange model.ByteRange) error {
	if !keyRE.MatchString(key) {
		return fmt.Errorf("cache key must be 64 lowercase hex characters")
	}
	if byteRange.Start < 0 || byteRange.End < byteRange.Start {
		return fmt.Errorf("invalid byte range")
	}
	return nil
}

func (c *Cache) ensureFree(writeBytes int64) error {
	if c.MinFreeBytes <= 0 {
		return nil
	}
	free := diskfree.FreeBytes(c.Root)
	if free >= 0 && free-writeBytes < c.MinFreeBytes {
		return fmt.Errorf("insufficient disk free space")
	}
	return nil
}

func (c *Cache) record(key string, byteRange model.ByteRange, now float64, size int64) state.MiddleBlockRecord {
	return state.MiddleBlockRecord{
		CacheKey:     key,
		Start:        byteRange.Start,
		End:          byteRange.End,
		Path:         relativePath(key, byteRange),
		Size:         size,
		CreatedAt:    now,
		LastAccessAt: now,
		ExpiresAt:    now + float64(c.TTLSeconds),
	}
}

func (c *Cache) paths(key string, byteRange model.ByteRange) (string, string) {
	path := filepath.Join(c.Root, relativePath(key, byteRange))
	return path, strings.TrimSuffix(path, ".bin") + ".range"
}

func (c *Cache) recordPaths(record state.MiddleBlockRecord) (string, string) {
	path := filepath.Join(c.Root, record.Path)
	return path, strings.TrimSuffix(path, ".bin") + ".range"
}

func (c *Cache) validRecord(record state.MiddleBlockRecord) bool {
	if !keyRE.MatchString(record.CacheKey) || record.Start < 0 || record.End < record.Start {
		return false
	}
	if record.Size != record.End-record.Start+1 {
		return false
	}
	return record.Path == relativePath(record.CacheKey, model.ByteRange{Start: record.Start, End: record.End})
}

func relativePath(key string, byteRange model.ByteRange) string {
	return filepath.ToSlash(filepath.Join(key, "mid", fmt.Sprintf("%d-%d.bin", byteRange.Start, byteRange.End)))
}

func recordCovers(record state.MiddleBlockRecord, requested model.ByteRange) bool {
	return record.Start <= requested.Start && record.End >= requested.End
}

func validFiles(record state.MiddleBlockRecord, path, sidecar string) bool {
	stat, err := os.Stat(path)
	if err != nil || stat.Size() != record.Size {
		return false
	}
	start, end, err := readSidecar(sidecar)
	return err == nil && start == record.Start && end == record.End
}

func readSidecar(path string) (int64, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	parts := strings.Split(strings.TrimSpace(string(data)), "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range sidecar")
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}
