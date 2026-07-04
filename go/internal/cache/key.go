package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func Key(source model.MediaSource, metadata model.SourceMetadata) string {
	material := strings.Join([]string{
		source.MediaSourceID,
		metadata.URL,
		strconv.FormatInt(metadata.Size, 10),
		metadata.ETag,
		metadata.LastModified,
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}
