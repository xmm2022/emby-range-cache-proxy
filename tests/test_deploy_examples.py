import json
from pathlib import Path


def test_caddy_example_is_self_contained_site_block():
    caddyfile = Path("deploy/Caddyfile.range-cache.example").read_text()
    lines = [" ".join(line.split()) for line in caddyfile.splitlines() if line.strip()]

    assert lines[0] == "a.inemby.pp.ua {"
    assert "@emby_original {" in lines
    assert "handle @emby_original {" in lines
    assert "reverse_proxy 127.0.0.1:18180 127.0.0.1:8096 {" in lines
    assert "lb_policy first" in lines
    assert "lb_try_duration 2s" in lines
    assert "lb_try_interval 100ms" in lines
    assert "fail_duration 10s" in lines
    assert "flush_interval -1" in lines
    assert "handle {" in lines
    assert "reverse_proxy 127.0.0.1:8096" in lines


def test_readme_documents_phase2_disabled_defaults():
    readme = Path("README.md").read_text()

    assert "Phase 2" in readme
    assert "session.enabled=false" in readme
    assert "middle_cache.enabled=false" in readme
    assert "prefetch.enabled=false" in readme
    assert "prefetch.poll_interval_seconds=5" in readme
    assert "prefetch.error_backoff_seconds=300" in readme
    assert "internal API key is not used for user playback authorization" in readme
    assert "Deploy code with Phase 2 disabled" in readme
    assert "Enable `session.enabled=true`" in readme
    assert "Enable `session.observer_enabled=true`" in readme
    assert "Enable `middle_cache.enabled=true` with `prefetch.enabled=false`" in readme
    assert "Enable `prefetch.enabled=true` for one or two allowlisted items" in readme


def test_docs_explain_strm_origin_allowlist_boundary():
    readme = Path("README.md").read_text()
    deploy_doc = Path("docs/deploy-test-server.md").read_text()

    assert "not tied to a hard-coded port" in readme
    assert "http://127.0.0.1:18096/" in readme
    assert "fall back to Emby" in readme
    assert "caches only `.strm` entries" in deploy_doc
    assert "Port `18096` is a test-server origin convention" in deploy_doc


def test_docs_explain_internal_prewarm_endpoint():
    readme = Path("README.md").read_text()
    deploy_doc = Path("docs/deploy-test-server.md").read_text()

    assert "POST /internal/prewarm" in readme
    assert "X-Range-Cache-Prewarm-Key" in readme
    assert "`prewarm.enabled` only controls the periodic recent-item scanner" in readme
    assert "MediaInfoKeeper" in deploy_doc
    assert "POST /internal/prewarm" in deploy_doc
    assert "X-Range-Cache-Prewarm-Key" in deploy_doc


def test_config_example_documents_prefetch_polling_defaults():
    config = json.loads(Path("config.example.json").read_text())

    assert config["prefetch"]["poll_interval_seconds"] == 5
    assert config["prefetch"]["error_backoff_seconds"] == 300
