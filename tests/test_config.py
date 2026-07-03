import json

from emby_range_cache_proxy.config import Config, load_config


def test_load_config_with_defaults(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "listen_host": "127.0.0.1",
                "listen_port": 18180,
                "cache_dir": str(tmp_path / "cache"),
                "fallback_base_url": "http://127.0.0.1:8096",
                "prewarm_api_key": "secret-prewarm-key",
                "rollout": {"enabled": True, "item_allowlist": ["151357"]},
            }
        )
    )

    config = load_config(path)

    assert config.emby_base_url == "http://127.0.0.1:8096"
    assert config.listen_host == "127.0.0.1"
    assert config.listen_port == 18180
    assert config.cache.max_bytes == 512 * 1024**3
    assert config.prewarm.enabled is False
    assert config.rollout.enabled is True
    assert config.rollout.item_allowed("151357") is True
    assert config.rollout.item_allowed("999999") is False


def test_empty_allowlists_mean_allowed_when_rollout_enabled():
    config = Config(
        emby_base_url="http://127.0.0.1:8096",
        fallback_base_url="http://127.0.0.1:8096",
        cache_dir="/tmp/cache",
    )

    assert config.rollout.enabled is False
    assert config.rollout.in_scope(item_id="1", media_source_id="ms1") is False

    config.rollout.enabled = True

    assert config.rollout.in_scope(item_id="1", media_source_id="ms1") is True
