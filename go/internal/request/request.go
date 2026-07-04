package request

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

var originalPathRE = regexp.MustCompile(`^/emby/videos/([A-Za-z0-9][A-Za-z0-9_-]*)/original\.([A-Za-z0-9]+)$`)

var safetyQueryKeys = []string{"MediaSourceId", "api_key", "PlaySessionId", "DeviceId"}

func ParseOriginal(method, rawPath string, headers map[string][]string) (model.RequestContext, bool) {
	method = strings.ToUpper(method)
	if method != "GET" && method != "HEAD" {
		return model.RequestContext{}, false
	}
	parsed, err := url.ParseRequestURI(rawPath)
	if err != nil {
		return model.RequestContext{}, false
	}
	match := originalPathRE.FindStringSubmatch(parsed.Path)
	if match == nil {
		return model.RequestContext{}, false
	}
	query := parsed.Query()
	if hasDuplicateSafetyParam(query) {
		return model.RequestContext{}, false
	}
	mediaSourceID := first(query, "MediaSourceId")
	token := first(query, "api_key")
	if token == "" {
		token = headerValue(headers, "X-Emby-Token")
	}
	if mediaSourceID == "" || token == "" {
		return model.RequestContext{}, false
	}
	return model.RequestContext{
		Method:        method,
		RawPath:       rawPath,
		ItemID:        match[1],
		MediaSourceID: mediaSourceID,
		Token:         token,
		Extension:     strings.ToLower(match[2]),
		PlaySessionID: first(query, "PlaySessionId"),
		DeviceID:      first(query, "DeviceId"),
	}, true
}

func first(query url.Values, name string) string {
	values := query[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func hasDuplicateSafetyParam(query url.Values) bool {
	for _, key := range safetyQueryKeys {
		if len(query[key]) > 1 {
			return true
		}
	}
	return false
}

func headerValue(headers map[string][]string, name string) string {
	target := strings.ToLower(name)
	for key, values := range headers {
		if strings.ToLower(key) == target && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}
