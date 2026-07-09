import json

import pytest

from emby_range_cache_proxy.config import (
    Config,
    MiddleCacheConfig,
    OpenListConfig,
    PrefetchConfig,
    PrewarmConfig,
    SessionConfig,
    load_config,
)


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
    assert config.playback_info_timeout_seconds == 15.0
    assert config.openlist.enabled is False
    assert config.cache.max_bytes == 512 * 1024**3
    assert config.cache.open_head_response_bytes is None
    assert config.prewarm.enabled is False
    assert config.prewarm.playback_info_timeout_seconds == 15.0
    assert config.path_mappings == ()
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


def test_load_config_reads_path_mappings(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "path_mappings": [{"from": "/strm/", "to": "/home/nax/emby/strm"}],
            }
        )
    )

    config = load_config(path)

    assert len(config.path_mappings) == 1
    assert config.path_mappings[0].source_prefix == "/strm/"
    assert config.path_mappings[0].target_prefix == "/home/nax/emby/strm"


def test_load_config_reads_openlist(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "openlist": {
                    "enabled": True,
                    "base_url": "https://openlist.example/",
                    "token": "openlist-token",
                    "password": "path-password",
                    "timeout_seconds": 3,
                },
            }
        )
    )

    config = load_config(path)

    assert config.openlist.enabled is True
    assert config.openlist.base_url == "https://openlist.example"
    assert config.openlist.token == "openlist-token"
    assert config.openlist.password == "path-password"
    assert config.openlist.timeout_seconds == 3


def test_load_config_reads_playback_info_timeouts(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "playback_info_timeout_seconds": 31,
                "prewarm": {"playback_info_timeout_seconds": 17},
            }
        )
    )

    config = load_config(path)

    assert config.playback_info_timeout_seconds == 31.0
    assert config.prewarm.playback_info_timeout_seconds == 17.0


def test_load_config_normalizes_path_mapping_source_prefix(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "path_mappings": [{"from": "/strm", "to": "/home/nax/emby/strm"}],
            }
        )
    )

    config = load_config(path)

    assert config.path_mappings[0].source_prefix == "/strm/"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("item_allowlist", "1"),
        ("media_source_allowlist", "ms1"),
        ("path_prefix_allowlist", "http://origin/"),
    ],
)
def test_load_config_rejects_string_allowlists(tmp_path, field, value):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "rollout": {"enabled": True, field: value},
            }
        )
    )

    with pytest.raises(ValueError, match=field):
        load_config(path)


@pytest.mark.parametrize("interval_seconds", [0, -1, 59])
def test_prewarm_config_rejects_short_interval(interval_seconds):
    with pytest.raises(ValueError, match="prewarm\\.interval_seconds"):
        PrewarmConfig(enabled=True, interval_seconds=interval_seconds)


@pytest.mark.parametrize("concurrency", [0, -1])
def test_prewarm_config_rejects_non_positive_concurrency(concurrency):
    with pytest.raises(ValueError, match="prewarm\\.concurrency"):
        PrewarmConfig(concurrency=concurrency)


@pytest.mark.parametrize("timeout_seconds", [0, -1])
def test_prewarm_config_rejects_non_positive_playback_info_timeout(timeout_seconds):
    with pytest.raises(ValueError, match="prewarm\\.playback_info_timeout_seconds"):
        PrewarmConfig(playback_info_timeout_seconds=timeout_seconds)


@pytest.mark.parametrize("timeout_seconds", [0, -1])
def test_config_rejects_non_positive_playback_info_timeout(tmp_path, timeout_seconds):
    with pytest.raises(ValueError, match="playback_info_timeout_seconds"):
        Config(
            emby_base_url="http://127.0.0.1:8096",
            fallback_base_url="http://127.0.0.1:8096",
            cache_dir=str(tmp_path),
            playback_info_timeout_seconds=timeout_seconds,
        )


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


def test_openlist_config_requires_base_url_when_enabled():
    with pytest.raises(ValueError, match="openlist\\.base_url"):
        OpenListConfig(enabled=True)


def test_openlist_config_rejects_non_positive_timeout():
    with pytest.raises(ValueError, match="openlist\\.timeout_seconds"):
        OpenListConfig(enabled=False, timeout_seconds=0)


@pytest.mark.parametrize("concurrency", [0, -1])
def test_load_config_rejects_non_positive_prewarm_concurrency(tmp_path, concurrency):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "prewarm": {"concurrency": concurrency},
            }
        )
    )

    with pytest.raises(ValueError, match="prewarm\\.concurrency"):
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


def test_load_config_reads_open_head_response_bytes(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "cache": {"open_head_response_bytes": 32 * 1024**2},
            }
        )
    )

    config = load_config(path)

    assert config.cache.open_head_response_bytes == 32 * 1024**2


def test_phase2_config_defaults_are_disabled(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
            }
        )
    )

    config = load_config(path)

    assert config.session.enabled is False
    assert config.session.state_db is None
    assert config.session.observer_enabled is False
    assert config.session.observer_interval_seconds == 30
    assert config.session.idle_seconds == 180
    assert config.session.stop_grace_seconds == 60
    assert config.session.expire_seconds == 86400
    assert config.middle_cache.enabled is False
    assert config.middle_cache.max_bytes == 128 * 1024**3
    assert config.middle_cache.ttl_seconds == 7 * 24 * 60 * 60
    assert config.middle_cache.segment_bytes == 64 * 1024**2
    assert config.middle_cache.min_free_bytes == 50 * 1024**3
    assert config.prefetch.enabled is False
    assert config.prefetch.window_bytes == 2 * 1024**3
    assert config.prefetch.resume_overlap_bytes == 128 * 1024**2
    assert config.prefetch.max_session_bytes == 4 * 1024**3
    assert config.prefetch.max_queue_depth == 200
    assert config.prefetch.concurrency == 1
    assert config.prefetch.per_origin_concurrency == 1
    assert config.prefetch.bandwidth_bytes_per_second == 30 * 1024**2
    assert config.prefetch.pause_when_rollout_session_active is True
    assert config.prefetch.poll_interval_seconds == 5
    assert config.prefetch.error_backoff_seconds == 300


def test_phase2_config_reads_explicit_values(tmp_path):
    path = tmp_path / "config.json"
    db_path = tmp_path / "phase2.sqlite3"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "session": {
                    "enabled": True,
                    "state_db": str(db_path),
                    "observer_enabled": True,
                    "observer_interval_seconds": 45,
                    "idle_seconds": 240,
                    "stop_grace_seconds": 90,
                    "expire_seconds": 7200,
                },
                "middle_cache": {
                    "enabled": True,
                    "max_bytes": 123,
                    "ttl_seconds": 456,
                    "segment_bytes": 789,
                    "min_free_bytes": 321,
                },
                "prefetch": {
                    "enabled": True,
                    "window_bytes": 111,
                    "resume_overlap_bytes": 222,
                    "max_session_bytes": 333,
                    "max_queue_depth": 44,
                    "concurrency": 2,
                    "per_origin_concurrency": 1,
                    "bandwidth_bytes_per_second": 555,
                    "pause_when_rollout_session_active": False,
                    "poll_interval_seconds": 7,
                    "error_backoff_seconds": 66,
                },
            }
        )
    )

    config = load_config(path)

    assert config.session.enabled is True
    assert config.session.state_db == str(db_path)
    assert config.session.observer_enabled is True
    assert config.session.observer_interval_seconds == 45
    assert config.session.idle_seconds == 240
    assert config.session.stop_grace_seconds == 90
    assert config.session.expire_seconds == 7200
    assert config.middle_cache.enabled is True
    assert config.middle_cache.max_bytes == 123
    assert config.middle_cache.ttl_seconds == 456
    assert config.middle_cache.segment_bytes == 789
    assert config.middle_cache.min_free_bytes == 321
    assert config.prefetch.enabled is True
    assert config.prefetch.window_bytes == 111
    assert config.prefetch.resume_overlap_bytes == 222
    assert config.prefetch.max_session_bytes == 333
    assert config.prefetch.max_queue_depth == 44
    assert config.prefetch.concurrency == 2
    assert config.prefetch.per_origin_concurrency == 1
    assert config.prefetch.bandwidth_bytes_per_second == 555
    assert config.prefetch.pause_when_rollout_session_active is False
    assert config.prefetch.poll_interval_seconds == 7
    assert config.prefetch.error_backoff_seconds == 66


@pytest.mark.parametrize(
    ("factory", "kwargs", "match"),
    [
        (SessionConfig, {"observer_interval_seconds": 0}, "observer_interval_seconds"),
        (SessionConfig, {"idle_seconds": 0}, "idle_seconds"),
        (SessionConfig, {"stop_grace_seconds": 0}, "stop_grace_seconds"),
        (SessionConfig, {"expire_seconds": 0}, "expire_seconds"),
        (MiddleCacheConfig, {"max_bytes": 0}, "middle_cache.max_bytes"),
        (MiddleCacheConfig, {"ttl_seconds": 0}, "middle_cache.ttl_seconds"),
        (MiddleCacheConfig, {"segment_bytes": 0}, "middle_cache.segment_bytes"),
        (MiddleCacheConfig, {"min_free_bytes": -1}, "middle_cache.min_free_bytes"),
        (PrefetchConfig, {"window_bytes": 0}, "prefetch.window_bytes"),
        (PrefetchConfig, {"resume_overlap_bytes": -1}, "prefetch.resume_overlap_bytes"),
        (PrefetchConfig, {"max_session_bytes": 0}, "prefetch.max_session_bytes"),
        (PrefetchConfig, {"max_queue_depth": 0}, "prefetch.max_queue_depth"),
        (PrefetchConfig, {"concurrency": 0}, "prefetch.concurrency"),
        (PrefetchConfig, {"per_origin_concurrency": 0}, "prefetch.per_origin_concurrency"),
        (PrefetchConfig, {"bandwidth_bytes_per_second": 0}, "prefetch.bandwidth_bytes_per_second"),
        (PrefetchConfig, {"poll_interval_seconds": 0}, "prefetch.poll_interval_seconds"),
        (PrefetchConfig, {"error_backoff_seconds": 0}, "prefetch.error_backoff_seconds"),
    ],
)
def test_phase2_config_rejects_invalid_values(factory, kwargs, match):
    with pytest.raises(ValueError, match=match):
        factory(**kwargs)


def test_phase2_config_rejects_string_session_enabled(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "session": {"enabled": "false"},
            }
        )
    )

    with pytest.raises(ValueError, match="session.enabled"):
        load_config(path)


def test_phase2_config_rejects_string_prefetch_pause_when_rollout_session_active(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "prefetch": {"pause_when_rollout_session_active": "false"},
            }
        )
    )

    with pytest.raises(ValueError, match="prefetch.pause_when_rollout_session_active"):
        load_config(path)


@pytest.mark.parametrize("window_bytes", [None, "abc", 1.5])
def test_phase2_config_rejects_invalid_prefetch_window_bytes(tmp_path, window_bytes):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "prefetch": {"window_bytes": window_bytes},
            }
        )
    )

    with pytest.raises(ValueError, match="prefetch.window_bytes"):
        load_config(path)


def test_phase2_config_parses_numeric_string_integer_fields(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "emby_base_url": "http://127.0.0.1:8096",
                "cache_dir": str(tmp_path / "cache"),
                "prefetch": {"window_bytes": "12345"},
            }
        )
    )

    assert load_config(path).prefetch.window_bytes == 12345
