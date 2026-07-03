import json

import pytest

from emby_range_cache_proxy.config import Config, PrewarmConfig, load_config


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
    assert config.cache_dir == str(tmp_path / "cache")
    assert config.fallback_base_url == "http://127.0.0.1:8096"
    assert config.prewarm_api_key == "secret-prewarm-key"
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


@pytest.mark.parametrize("interval_seconds", [0, -1, 59])
def test_prewarm_config_rejects_short_interval(interval_seconds):
    with pytest.raises(ValueError, match="prewarm\\.interval_seconds"):
        PrewarmConfig(enabled=True, interval_seconds=interval_seconds)


@pytest.mark.parametrize("interval_seconds", [0, -1, 59])
def test_load_config_rejects_short_prewarm_interval(tmp_path, interval_seconds):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "prewarm": {"enabled": True, "interval_seconds": interval_seconds},
            }
        )
    )

    with pytest.raises(ValueError, match="prewarm\\.interval_seconds"):
        load_config(path)


def test_prewarm_interval_allows_sixty_seconds(tmp_path):
    assert PrewarmConfig(enabled=True, interval_seconds=60).interval_seconds == 60

    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "prewarm": {"enabled": True, "interval_seconds": 60},
            }
        )
    )

    assert load_config(path).prewarm.interval_seconds == 60
