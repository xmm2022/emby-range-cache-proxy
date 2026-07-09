package ranges

import (
	"testing"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
)

func TestAdaptiveHeadTail(t *testing.T) {
	gib := int64(1024 * 1024 * 1024)
	mib := int64(1024 * 1024)
	cases := []struct {
		size int64
		head int64
		tail int64
	}{
		{2*gib - 1, 8 * mib, 8 * mib},
		{2 * gib, 8 * mib, 8 * mib},
		{8*gib - 1, 8 * mib, 8 * mib},
		{8 * gib, 8 * mib, 8 * mib},
		{30 * gib, 8 * mib, 8 * mib},
		{30*gib + 1, 8 * mib, 8 * mib},
	}
	for _, tc := range cases {
		head, tail := AdaptiveHeadTail(tc.size)
		if head != tc.head || tail != tc.tail {
			t.Fatalf("size %d => %d/%d, want %d/%d", tc.size, head, tail, tc.head, tc.tail)
		}
	}
}

func TestConfiguredHeadTail(t *testing.T) {
	mib := int64(1024 * 1024)
	head, tail := ConfiguredHeadTail(100*mib, 32*mib, 16*mib)
	if head != 32*mib || tail != 16*mib {
		t.Fatalf("head/tail = %d/%d", head, tail)
	}
	head, tail = ConfiguredHeadTail(100*mib, 0, 0)
	if head != 8*mib || tail != 8*mib {
		t.Fatalf("fallback head/tail = %d/%d", head, tail)
	}
}

func TestPlanPlaybackRange(t *testing.T) {
	openHead := int64(32)
	cases := []struct {
		name       string
		header     string
		size       int64
		head       int64
		tail       int64
		defaultLen int64
		openHead   *int64
		want       model.ByteRange
	}{
		{"no range defaults to head", "", 100, 16, 8, 20, nil, model.ByteRange{Start: 0, End: 15}},
		{"open head clamps to head", "bytes=0-", 100, 16, 8, 20, nil, model.ByteRange{Start: 0, End: 15}},
		{"open head response cap", "bytes=4-", 100, 64, 8, 20, &openHead, model.ByteRange{Start: 4, End: 35}},
		{"open tail to eof", "bytes=95-", 100, 16, 8, 20, nil, model.ByteRange{Start: 95, End: 99}},
		{"open middle streams to eof", "bytes=40-", 100, 16, 8, 20, nil, model.ByteRange{Start: 40, End: 99}},
		{"closed range unchanged", "bytes=40-44", 100, 16, 8, 20, nil, model.ByteRange{Start: 40, End: 44}},
		{"suffix range unchanged", "bytes=-5", 100, 16, 8, 20, nil, model.ByteRange{Start: 95, End: 99}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PlanPlaybackRange(tc.header, tc.size, tc.head, tc.tail, tc.defaultLen, tc.openHead)
			if err != nil {
				t.Fatalf("PlanPlaybackRange error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("range = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseRangeRejectsInvalid(t *testing.T) {
	for _, header := range []string{"bytes=0-1,2-3", "bytes=-0", "bytes=200-", "items=0-1", "bytes=10-9"} {
		if _, err := ParseRangeHeader(header, 100); err == nil {
			t.Fatalf("expected %q to fail", header)
		}
	}
}
