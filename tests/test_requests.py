from emby_range_cache_proxy.requests import parse_original_request


def test_parse_original_request_with_query_token():
    ctx = parse_original_request(
        method="GET",
        raw_path="/emby/videos/151357/original.mkv?MediaSourceId=mediasource_151357&api_key=abc123",
        headers={},
    )

    assert ctx is not None
    assert ctx.item_id == "151357"
    assert ctx.media_source_id == "mediasource_151357"
    assert ctx.token == "abc123"
    assert ctx.extension == "mkv"


def test_parse_original_request_with_header_token():
    ctx = parse_original_request(
        method="HEAD",
        raw_path="/emby/videos/151357/original.mkv?MediaSourceId=mediasource_151357",
        headers={"X-Emby-Token": "header-token"},
    )

    assert ctx is not None
    assert ctx.token == "header-token"


def test_parse_original_request_with_case_insensitive_header_token():
    ctx = parse_original_request(
        method="GET",
        raw_path="/emby/videos/151357/original.mkv?MediaSourceId=mediasource_151357",
        headers={"x-EMBY-token": "mixed-case-header-token"},
    )

    assert ctx is not None
    assert ctx.token == "mixed-case-header-token"


def test_reject_non_original_path():
    assert parse_original_request("GET", "/web/index.html", {}) is None


def test_reject_missing_media_source_or_token():
    assert parse_original_request("GET", "/emby/videos/1/original.mkv?api_key=t", {}) is None
    assert parse_original_request("GET", "/emby/videos/1/original.mkv?MediaSourceId=m", {}) is None


def test_reject_duplicate_media_source_id():
    assert (
        parse_original_request(
            "GET",
            "/emby/videos/1/original.mkv?MediaSourceId=m1&MediaSourceId=m2&api_key=t",
            {},
        )
        is None
    )


def test_reject_duplicate_api_key():
    assert (
        parse_original_request(
            "GET",
            "/emby/videos/1/original.mkv?MediaSourceId=m&api_key=t1&api_key=t2",
            {},
        )
        is None
    )


def test_reject_duplicate_optional_safety_query_params():
    assert (
        parse_original_request(
            "GET",
            "/emby/videos/1/original.mkv?MediaSourceId=m&api_key=t&PlaySessionId=p1&PlaySessionId=p2",
            {},
        )
        is None
    )
    assert (
        parse_original_request(
            "GET",
            "/emby/videos/1/original.mkv?MediaSourceId=m&api_key=t&DeviceId=d1&DeviceId=d2",
            {},
        )
        is None
    )
