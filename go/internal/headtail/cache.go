package headtail

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

var keyRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Cache struct {
	Root     string
	MaxBytes int64
}

func NewCache(root string, maxBytes int64) *Cache {
	_ = os.MkdirAll(root, 0o755)
	return &Cache{Root: root, MaxBytes: maxBytes}
}

func (c *Cache) StoreBlock(key, blockName string, byteRange model.ByteRange, data []byte) error {
	if int64(len(data)) != byteRange.Length() {
		return fmt.Errorf("data length must match byte range")
	}
	writer, err := c.StageBlock(key, blockName, byteRange)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		writer.Abort()
		return err
	}
	return writer.Commit()
}

func (c *Cache) StageBlock(key, blockName string, byteRange model.ByteRange) (*BlockWriter, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := validateBlockName(blockName); err != nil {
		return nil, err
	}
	if byteRange.Start < 0 || byteRange.End < byteRange.Start {
		return nil, fmt.Errorf("invalid byte range")
	}
	dir := filepath.Join(c.Root, key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, blockName+".*.tmp")
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		cache:     c,
		key:       key,
		blockName: blockName,
		byteRange: byteRange,
		tmpPath:   tmp.Name(),
		handle:    tmp,
	}, nil
}

func (c *Cache) IterBlock(key, blockName string, requested model.ByteRange, chunkBytes int64) (<-chan []byte, error) {
	if chunkBytes <= 0 {
		return nil, fmt.Errorf("chunkBytes must be positive")
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := validateBlockName(blockName); err != nil {
		return nil, err
	}
	path := c.blockPath(key, blockName)
	meta := c.metaPath(key, blockName)
	stored, err := readRange(meta)
	if err != nil {
		c.removeEntry(path, meta)
		return nil, nil
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Size() != stored.Length() {
		c.removeEntry(path, meta)
		return nil, nil
	}
	if requested.Start < stored.Start || requested.End > stored.End {
		return nil, nil
	}
	handle, err := os.Open(path)
	if err != nil {
		c.removeEntry(path, meta)
		return nil, nil
	}
	if _, err := handle.Seek(requested.Start-stored.Start, io.SeekStart); err != nil {
		handle.Close()
		c.removeEntry(path, meta)
		return nil, nil
	}
	_ = touch(path)
	out := make(chan []byte)
	go func() {
		defer close(out)
		defer handle.Close()
		remaining := requested.Length()
		buf := make([]byte, int(chunkBytes))
		for remaining > 0 {
			n := int64(len(buf))
			if n > remaining {
				n = remaining
			}
			readBuf := buf[:n]
			if _, err := io.ReadFull(handle, readBuf); err != nil {
				c.removeEntry(path, meta)
				return
			}
			chunk := make([]byte, n)
			copy(chunk, readBuf)
			out <- chunk
			remaining -= n
		}
	}()
	return out, nil
}

func (c *Cache) HasBlockRange(key, blockName string, requested model.ByteRange) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	if err := validateBlockName(blockName); err != nil {
		return false, err
	}
	path := c.blockPath(key, blockName)
	meta := c.metaPath(key, blockName)
	stored, err := readRange(meta)
	if err != nil {
		c.removeEntry(path, meta)
		return false, nil
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Size() != stored.Length() {
		c.removeEntry(path, meta)
		return false, nil
	}
	return requested.Start >= stored.Start && requested.End <= stored.End, nil
}

func (c *Cache) EvictIfNeeded() error {
	if c.MaxBytes <= 0 {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(c.Root, "*", "*.bin"))
	if err != nil {
		return err
	}
	type fileInfo struct {
		path  string
		size  int64
		mtime int64
	}
	infos := make([]fileInfo, 0, len(files))
	var total int64
	for _, path := range files {
		stat, err := os.Stat(path)
		if err != nil || stat.IsDir() {
			continue
		}
		total += stat.Size()
		infos = append(infos, fileInfo{path: path, size: stat.Size(), mtime: stat.ModTime().UnixNano()})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].mtime < infos[j].mtime })
	for _, info := range infos {
		if total <= c.MaxBytes {
			break
		}
		_ = os.Remove(info.path)
		_ = os.Remove(strings.TrimSuffix(info.path, ".bin") + ".range")
		total -= info.size
	}
	return nil
}

func (c *Cache) blockPath(key, blockName string) string {
	return filepath.Join(c.Root, key, blockName+".bin")
}

func (c *Cache) metaPath(key, blockName string) string {
	return filepath.Join(c.Root, key, blockName+".range")
}

func (c *Cache) removeEntry(path, meta string) {
	_ = os.Remove(path)
	_ = os.Remove(meta)
}

type BlockWriter struct {
	cache        *Cache
	key          string
	blockName    string
	byteRange    model.ByteRange
	tmpPath      string
	handle       *os.File
	bytesWritten int64
	closed       bool
}

func (w *BlockWriter) Write(data []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("writer is closed")
	}
	n, err := w.handle.Write(data)
	w.bytesWritten += int64(n)
	return n, err
}

func (w *BlockWriter) Commit() error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}
	if err := w.handle.Close(); err != nil {
		w.closed = true
		_ = os.Remove(w.tmpPath)
		return err
	}
	w.closed = true
	if w.bytesWritten != w.byteRange.Length() {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("staged data length must match byte range")
	}
	path := w.cache.blockPath(w.key, w.blockName)
	meta := w.cache.metaPath(w.key, w.blockName)
	metaTmp := meta + "." + filepath.Base(w.tmpPath) + ".tmp"
	if err := os.WriteFile(metaTmp, []byte(fmt.Sprintf("%d-%d\n", w.byteRange.Start, w.byteRange.End)), 0o644); err != nil {
		_ = os.Remove(w.tmpPath)
		return err
	}
	if err := os.Rename(w.tmpPath, path); err != nil {
		_ = os.Remove(w.tmpPath)
		_ = os.Remove(metaTmp)
		return err
	}
	if err := os.Rename(metaTmp, meta); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(metaTmp)
		return err
	}
	return touch(path)
}

func (w *BlockWriter) Abort() {
	if !w.closed {
		_ = w.handle.Close()
		w.closed = true
	}
	_ = os.Remove(w.tmpPath)
}

func BlockForRequest(byteRange model.ByteRange, size, headSize, tailSize int64) (string, model.ByteRange) {
	headEnd := headSize
	if headEnd > size {
		headEnd = size
	}
	head := model.ByteRange{Start: 0, End: headEnd - 1}
	tailStart := size - tailSize
	if tailStart < 0 {
		tailStart = 0
	}
	tail := model.ByteRange{Start: tailStart, End: size - 1}
	if contains(head, byteRange) {
		return "head", head
	}
	if contains(tail, byteRange) {
		return "tail", tail
	}
	return "", model.ByteRange{}
}

func contains(container, requested model.ByteRange) bool {
	return requested.Start >= container.Start && requested.End <= container.End
}

func validateKey(key string) error {
	if !keyRE.MatchString(key) {
		return fmt.Errorf("cache key must be 64 lowercase hex characters")
	}
	return nil
}

func validateBlockName(blockName string) error {
	if blockName != "head" && blockName != "tail" {
		return fmt.Errorf("block name must be head or tail")
	}
	return nil
}

func readRange(path string) (model.ByteRange, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.ByteRange{}, err
	}
	parts := strings.Split(strings.TrimSpace(string(data)), "-")
	if len(parts) != 2 {
		return model.ByteRange{}, fmt.Errorf("invalid range sidecar")
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return model.ByteRange{}, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start {
		return model.ByteRange{}, fmt.Errorf("invalid range sidecar")
	}
	return model.ByteRange{Start: start, End: end}, nil
}

func touch(path string) error {
	now := time.Now()
	return os.Chtimes(path, now, now)
}
