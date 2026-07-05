package ranges

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

const (
	mib int64 = 1024 * 1024
)

var rangeRE = regexp.MustCompile(`^bytes=(\d*)-(\d*)$`)

func AdaptiveHeadTail(size int64) (int64, int64) {
	return 8 * mib, 8 * mib
}

func ParseRangeHeader(value string, size int64) (model.ByteRange, error) {
	if size <= 0 {
		return model.ByteRange{}, fmt.Errorf("size must be positive")
	}
	if value == "" {
		return model.ByteRange{Start: 0, End: size - 1}, nil
	}
	if strings.Contains(value, ",") {
		return model.ByteRange{}, fmt.Errorf("multiple ranges are not supported")
	}
	match := rangeRE.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return model.ByteRange{}, fmt.Errorf("invalid range header")
	}
	left, right := match[1], match[2]
	if left == "" && right == "" {
		return model.ByteRange{}, fmt.Errorf("empty range")
	}
	if left == "" {
		length, err := strconv.ParseInt(right, 10, 64)
		if err != nil || length <= 0 {
			return model.ByteRange{}, fmt.Errorf("invalid suffix range")
		}
		start := size - length
		if start < 0 {
			start = 0
		}
		return model.ByteRange{Start: start, End: size - 1}, nil
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil {
		return model.ByteRange{}, fmt.Errorf("invalid range start")
	}
	if start >= size {
		return model.ByteRange{}, fmt.Errorf("range start beyond size")
	}
	end := size - 1
	if right != "" {
		parsedEnd, err := strconv.ParseInt(right, 10, 64)
		if err != nil {
			return model.ByteRange{}, fmt.Errorf("invalid range end")
		}
		end = parsedEnd
	}
	if end < start {
		return model.ByteRange{}, fmt.Errorf("range end before start")
	}
	if end >= size {
		end = size - 1
	}
	return model.ByteRange{Start: start, End: end}, nil
}

func PlanPlaybackRange(value string, size, headBytes, tailBytes, defaultOpenRangeBytes int64, openHeadResponseBytes *int64) (model.ByteRange, error) {
	byteRange, err := ParseRangeHeader(value, size)
	if err != nil {
		return model.ByteRange{}, err
	}
	if value == "" {
		return byteRange, nil
	}
	match := rangeRE.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return byteRange, nil
	}
	left, right := match[1], match[2]
	if left == "" || right != "" {
		return byteRange, nil
	}

	start := byteRange.Start
	tailStart := size - tailBytes
	if tailStart < 0 {
		tailStart = 0
	}
	headEnd := headBytes
	if headEnd > size {
		headEnd = size
	}
	if start < headEnd {
		end := headEnd - 1
		if openHeadResponseBytes != nil && *openHeadResponseBytes > 0 {
			capped := start + *openHeadResponseBytes - 1
			if capped < end {
				end = capped
			}
		}
		return model.ByteRange{Start: start, End: end}, nil
	}
	if start >= tailStart {
		return model.ByteRange{Start: start, End: size - 1}, nil
	}
	end := start + defaultOpenRangeBytes - 1
	if end >= size {
		end = size - 1
	}
	return model.ByteRange{Start: start, End: end}, nil
}

func ContentRangeHeader(byteRange model.ByteRange, size int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, size)
}
