package source

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

const strmReadLimitBytes = 16 * 1024

func ResolveMediaSource(source model.MediaSource, mappings []config.PathMapping, urlPrefixAllowlist []string) model.MediaSource {
	if isHTTP(source.Path) {
		return source
	}
	if !strings.HasSuffix(strings.ToLower(source.Path), ".strm") {
		return source
	}
	mapped := mapSourcePath(source.Path, mappings)
	if mapped == "" {
		return source
	}
	info, err := os.Stat(mapped)
	if err != nil || info.IsDir() {
		return source
	}
	url, err := readSTRMURL(mapped)
	if err != nil || !isHTTP(url) || !urlPrefixAllowed(url, urlPrefixAllowlist) {
		return source
	}
	source.Path = url
	source.Protocol = "Http"
	return source
}

func mapSourcePath(sourcePath string, mappings []config.PathMapping) string {
	for _, mapping := range mappings {
		if !strings.HasPrefix(sourcePath, mapping.SourcePrefix) {
			continue
		}
		relative := strings.TrimLeft(sourcePath[len(mapping.SourcePrefix):], "/")
		if relative == "" {
			return ""
		}
		cleanRelative := filepath.Clean(relative)
		if cleanRelative == "." || strings.HasPrefix(cleanRelative, "..") || filepath.IsAbs(cleanRelative) {
			return ""
		}
		root, err := filepath.Abs(mapping.TargetPrefix)
		if err != nil {
			return ""
		}
		candidate, err := filepath.Abs(filepath.Join(root, cleanRelative))
		if err != nil {
			return ""
		}
		rel, err := filepath.Rel(root, candidate)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			return ""
		}
		return candidate
	}
	return ""
}

func readSTRMURL(path string) (string, error) {
	handle, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer handle.Close()
	buf := make([]byte, strmReadLimitBytes)
	n, err := handle.Read(buf)
	if err != nil && n == 0 {
		return "", err
	}
	for _, line := range bytes.Split(buf[:n], []byte{'\n'}) {
		value := strings.TrimSpace(string(line))
		if value != "" && !strings.HasPrefix(value, "#") {
			return value, nil
		}
	}
	return "", nil
}

func isHTTP(value string) bool {
	lower := strings.ToLower(value)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

func urlPrefixAllowed(value string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return false
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
