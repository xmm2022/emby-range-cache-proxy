package request

import "testing"

func TestParseOriginalRequestAcceptsNumericUUIDAndHashItemIDs(t *testing.T) {
	cases := []string{
		"/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user",
		"/emby/videos/1fd715bd-1152-b0cc-f924-106ec6635169/original.mp4?MediaSourceId=ms1&api_key=user",
		"/emby/videos/a8ef94421fd1346675ad9da1855bb8ce/original.ts?MediaSourceId=ms1&api_key=user",
	}
	for _, rawPath := range cases {
		ctx, ok := ParseOriginal("GET", rawPath, nil)
		if !ok {
			t.Fatalf("expected %q to parse", rawPath)
		}
		if ctx.MediaSourceID != "ms1" || ctx.Token != "user" {
			t.Fatalf("ctx = %+v", ctx)
		}
	}
}

func TestParseOriginalRequestUsesHeaderTokenAndOptionalSessionFields(t *testing.T) {
	ctx, ok := ParseOriginal(
		"HEAD",
		"/emby/videos/10535/original.mkv?MediaSourceId=ms1&PlaySessionId=ps&DeviceId=dev",
		map[string][]string{"X-Emby-Token": {"header-token"}},
	)
	if !ok {
		t.Fatalf("expected request to parse")
	}
	if ctx.Method != "HEAD" || ctx.Token != "header-token" || ctx.PlaySessionID != "ps" || ctx.DeviceID != "dev" {
		t.Fatalf("ctx = %+v", ctx)
	}
}

func TestParseOriginalRequestRejectsUnsafeShapes(t *testing.T) {
	cases := []string{
		"/emby/videos/10535/master.m3u8?MediaSourceId=ms1&api_key=user",
		"/emby/videos/../original.mkv?MediaSourceId=ms1&api_key=user",
		"/emby/videos/10535/original.mkv?api_key=user",
		"/emby/videos/10535/original.mkv?MediaSourceId=ms1",
		"/emby/videos/10535/original.mkv?MediaSourceId=ms1&MediaSourceId=ms2&api_key=user",
		"/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user&api_key=other",
		"/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user&PlaySessionId=1&PlaySessionId=2",
	}
	for _, rawPath := range cases {
		if _, ok := ParseOriginal("GET", rawPath, nil); ok {
			t.Fatalf("expected %q to be rejected", rawPath)
		}
	}
	if _, ok := ParseOriginal("POST", "/emby/videos/10535/original.mkv?MediaSourceId=ms1&api_key=user", nil); ok {
		t.Fatalf("expected POST to be rejected")
	}
}
