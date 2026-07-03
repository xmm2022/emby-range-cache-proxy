from pathlib import Path


def test_caddy_example_is_self_contained_site_block():
    caddyfile = Path("deploy/Caddyfile.range-cache.example").read_text()

    assert caddyfile.startswith("a.inemby.pp.ua {\n")
    assert "@emby_original {" in caddyfile
    assert "handle @emby_original {" in caddyfile
    assert "reverse_proxy 127.0.0.1:18180 127.0.0.1:8096" in caddyfile
    assert "lb_policy first" in caddyfile
    assert "lb_try_duration 2s" in caddyfile
    assert "lb_try_interval 100ms" in caddyfile
    assert "fail_duration 10s" in caddyfile
    assert "flush_interval -1" in caddyfile
    assert "handle {\n        reverse_proxy 127.0.0.1:8096\n    }" in caddyfile
