package emby

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

type AuthorizationError struct {
	Reason string
}

func (e AuthorizationError) Error() string {
	return e.Reason
}

type AuthUnavailable struct {
	Reason string
}

func (e AuthUnavailable) Error() string {
	return e.Reason
}

type AuthClient struct {
	BaseURL string
	HTTP    *http.Client
}

func NewAuthClient(baseURL string) *AuthClient {
	return &AuthClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *AuthClient) Authorize(ctx model.RequestContext) (model.MediaSource, error) {
	endpoint := c.BaseURL + "/Items/" + url.PathEscape(ctx.ItemID) + "/PlaybackInfo"
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return model.MediaSource{}, err
	}
	query := req.URL.Query()
	query.Set("MediaSourceId", ctx.MediaSourceID)
	query.Set("api_key", ctx.Token)
	req.URL.RawQuery = query.Encode()
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return model.MediaSource{}, AuthUnavailable{Reason: "Emby authorization unavailable"}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
		return model.MediaSource{}, AuthorizationError{Reason: "Emby authorization failed"}
	}
	if resp.StatusCode != http.StatusOK {
		return model.MediaSource{}, AuthUnavailable{Reason: fmt.Sprintf("Emby authorization unavailable: status=%d", resp.StatusCode)}
	}
	var payload struct {
		MediaSources []struct {
			ID        string `json:"Id"`
			Path      any    `json:"Path"`
			Protocol  string `json:"Protocol"`
			Size      any    `json:"Size"`
			Container string `json:"Container"`
			Bitrate   any    `json:"Bitrate"`
		} `json:"MediaSources"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return model.MediaSource{}, AuthorizationError{Reason: "invalid PlaybackInfo response"}
	}
	for _, raw := range payload.MediaSources {
		if raw.ID != ctx.MediaSourceID {
			continue
		}
		path, ok := raw.Path.(string)
		if !ok || path == "" {
			return model.MediaSource{}, AuthorizationError{Reason: "media source path is invalid"}
		}
		size, err := optionalInt64(raw.Size)
		if err != nil {
			return model.MediaSource{}, AuthorizationError{Reason: "invalid media source Size"}
		}
		bitrate, err := optionalInt64(raw.Bitrate)
		if err != nil {
			return model.MediaSource{}, AuthorizationError{Reason: "invalid media source Bitrate"}
		}
		return model.MediaSource{
			ItemID:        ctx.ItemID,
			MediaSourceID: ctx.MediaSourceID,
			Path:          path,
			Protocol:      raw.Protocol,
			Size:          size,
			Container:     raw.Container,
			Bitrate:       bitrate,
		}, nil
	}
	return model.MediaSource{}, AuthorizationError{Reason: "media source not allowed"}
}

func optionalInt64(value any) (*int64, error) {
	if value == nil {
		return nil, nil
	}
	var parsed int64
	switch typed := value.(type) {
	case float64:
		parsed = int64(typed)
		if float64(parsed) != typed {
			return nil, fmt.Errorf("not integer")
		}
	case string:
		if typed == "" {
			return nil, nil
		}
		value, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			return nil, err
		}
		parsed = value
	default:
		return nil, fmt.Errorf("not integer")
	}
	return &parsed, nil
}
