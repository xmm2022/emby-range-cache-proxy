package origin

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

type Client struct {
	HTTP       *http.Client
	ChunkBytes int64
}

func NewClient(chunkBytes int64) *Client {
	return &Client{
		HTTP: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ResponseHeaderTimeout: 30 * time.Second,
			},
		},
		ChunkBytes: chunkBytes,
	}
}

func (c *Client) Head(url string) (model.SourceMetadata, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return model.SourceMetadata{}, err
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return model.SourceMetadata{}, fmt.Errorf("origin HEAD failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return model.SourceMetadata{}, fmt.Errorf("origin HEAD failed: status=%d", resp.StatusCode)
	}
	size, err := parseHeadSize(resp)
	if err != nil {
		return model.SourceMetadata{}, err
	}
	return model.SourceMetadata{
		URL:          resp.Request.URL.String(),
		Size:         size,
		ContentType:  resp.Header.Get("Content-Type"),
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
	}, nil
}

func (c *Client) OpenRange(url string, byteRange model.ByteRange, size int64) (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", byteRange.Start, byteRange.End))
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("origin range GET failed: %w", err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("origin range GET failed: status=%d", resp.StatusCode)
	}
	if !contentRangeMatches(resp.Header.Get("Content-Range"), byteRange, size) {
		resp.Body.Close()
		return nil, fmt.Errorf("origin range GET failed: invalid Content-Range")
	}
	if length := resp.Header.Get("Content-Length"); length != "" {
		parsed, err := strconv.ParseInt(length, 10, 64)
		if err != nil || parsed != byteRange.Length() {
			resp.Body.Close()
			return nil, fmt.Errorf("origin range GET failed: invalid Content-Length")
		}
	}
	return resp.Body, nil
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func parseHeadSize(resp *http.Response) (int64, error) {
	if resp.StatusCode == http.StatusPartialContent {
		_, _, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok {
			return 0, fmt.Errorf("origin HEAD failed: invalid Content-Range")
		}
		return total, nil
	}
	length := resp.Header.Get("Content-Length")
	if length == "" {
		return 0, fmt.Errorf("origin did not provide Content-Length")
	}
	parsed, err := strconv.ParseInt(length, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("origin provided invalid Content-Length")
	}
	return parsed, nil
}

var contentRangeRE = regexp.MustCompile(`^bytes (\d+)-(\d+)/(\d+)$`)

func parseContentRange(value string) (int64, int64, int64, bool) {
	match := contentRangeRE.FindStringSubmatch(value)
	if match == nil {
		return 0, 0, 0, false
	}
	start, err1 := strconv.ParseInt(match[1], 10, 64)
	end, err2 := strconv.ParseInt(match[2], 10, 64)
	total, err3 := strconv.ParseInt(match[3], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || total <= 0 || start > end || end >= total {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

func contentRangeMatches(value string, byteRange model.ByteRange, size int64) bool {
	start, end, total, ok := parseContentRange(value)
	return ok && start == byteRange.Start && end == byteRange.End && total == size
}
