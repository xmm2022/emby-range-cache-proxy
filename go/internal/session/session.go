package session

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/state"
)

func Hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func OriginSignature(metadata model.SourceMetadata) string {
	material := strings.Join([]string{
		metadata.URL,
		strconv.FormatInt(metadata.Size, 10),
		metadata.ETag,
		metadata.LastModified,
	}, "\n")
	return Hash(material)
}

func BuildUpdate(ctx model.RequestContext, cacheKey string, metadata model.SourceMetadata, byteRange model.ByteRange, observedAt float64) state.PlaybackSessionUpdate {
	deviceHash := ""
	if ctx.DeviceID != "" {
		deviceHash = Hash(ctx.DeviceID)
	}
	sessionHash := ""
	if ctx.PlaySessionID != "" {
		sessionHash = Hash(ctx.PlaySessionID)
	} else {
		bucket := int64(observedAt / 900)
		identifierHash := deviceHash
		if identifierHash == "" && ctx.Token != "" {
			identifierHash = Hash(ctx.Token)
		}
		if identifierHash == "" {
			identifierHash = "anonymous"
		}
		sessionHash = Hash(fmt.Sprintf("synthetic:%s:%s:%s:%d", ctx.ItemID, ctx.MediaSourceID, identifierHash, bucket))
	}
	return state.PlaybackSessionUpdate{
		SessionHash:     sessionHash,
		DeviceHash:      deviceHash,
		ItemID:          ctx.ItemID,
		MediaSourceID:   ctx.MediaSourceID,
		CacheKey:        cacheKey,
		OriginSignature: OriginSignature(metadata),
		MediaSize:       metadata.Size,
		ByteRange:       byteRange,
		ObservedAt:      observedAt,
	}
}

func IsTailMetadataRange(size int64, byteRange model.ByteRange, headSize, tailSize int64) bool {
	headEnd := headSize
	if headEnd > size {
		headEnd = size
	}
	tailStart := size - tailSize
	if tailStart < headEnd {
		tailStart = headEnd
	}
	return byteRange.Start >= tailStart
}
