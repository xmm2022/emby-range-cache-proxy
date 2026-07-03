import subprocess
import sys
from types import SimpleNamespace

import pytest

from emby_range_cache_proxy import cli


def test_arg_parser_requires_config():
    parser = cli.build_arg_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_arg_parser_reads_config_path():
    parser = cli.build_arg_parser()

    args = parser.parse_args(["--config", "/etc/emby-range-cache-proxy/config.json"])

    assert args.config == "/etc/emby-range-cache-proxy/config.json"


def test_main_runs_app_from_config(monkeypatch):
    calls = {}
    config = SimpleNamespace(listen_host="127.0.0.1", listen_port=18180)
    app = object()

    def fake_load_config(path):
        calls["config_path"] = path
        return config

    def fake_create_app(loaded_config):
        calls["app_config"] = loaded_config
        return app

    monkeypatch.setattr(cli, "load_config", fake_load_config)
    monkeypatch.setattr(cli, "create_app", fake_create_app)
    monkeypatch.setattr(
        cli.web,
        "run_app",
        lambda built_app, *, host, port: calls.update(app=built_app, host=host, port=port),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["emby-range-cache-proxy", "--config", "/etc/emby-range-cache-proxy/config.json"],
    )

    cli.main()

    assert calls == {
        "config_path": "/etc/emby-range-cache-proxy/config.json",
        "app_config": config,
        "app": app,
        "host": "127.0.0.1",
        "port": 18180,
    }


def test_module_execution_shows_argparse_help():
    result = subprocess.run(
        [sys.executable, "-m", "emby_range_cache_proxy.cli", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout
    assert "--config" in result.stdout
