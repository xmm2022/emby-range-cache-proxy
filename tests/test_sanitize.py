from emby_range_cache_proxy.sanitize import redact_url, stable_token_hash


def test_redact_url_query_secrets():
    url = "https://a.inemby.pp.ua/emby/videos/1/original.mkv?api_key=secret&PlaySessionId=play&DeviceId=dev&MediaSourceId=ms1"

    redacted = redact_url(url)

    assert "secret" not in redacted
    assert "play" not in redacted
    assert "dev" not in redacted
    assert "MediaSourceId=ms1" in redacted


def test_stable_token_hash_is_not_plaintext():
    digest = stable_token_hash("secret-token")

    assert digest == stable_token_hash("secret-token")
    assert digest != "secret-token"
    assert len(digest) == 64
