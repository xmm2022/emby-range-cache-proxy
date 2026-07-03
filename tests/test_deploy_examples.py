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
    assert "internal API key is not used for user playback authorization" in readme
    assert "Deploy code with Phase 2 disabled" in readme
    assert "Enable `session.enabled=true`" in readme
    assert "Enable `session.observer_enabled=true`" in readme
    assert "Enable `middle_cache.enabled=true` with `prefetch.enabled=false`" in readme
    assert "Enable `prefetch.enabled=true` for one or two allowlisted items" in readme
