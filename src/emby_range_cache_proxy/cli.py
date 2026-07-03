from __future__ import annotations

import argparse
import logging

from aiohttp import web

from .app import create_app
from .config import load_config


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emby Range Cache Proxy")
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config(args.config)
    web.run_app(create_app(config), host=config.listen_host, port=config.listen_port, access_log=None)


if __name__ == "__main__":
    main()
