"""Service entry point: python -m citefuse.app."""
from __future__ import annotations

import os

import uvicorn

from .api import create_app

HOST = "127.0.0.1"
DEFAULT_PORT = 8321


def _port() -> int:
    raw = os.environ.get("CITEFUSE_PORT", str(DEFAULT_PORT))
    try:
        port = int(raw)
    except ValueError as exc:
        raise SystemExit(f"CITEFUSE_PORT must be an integer, got {raw!r}") from exc
    if not 1 <= port <= 65535:
        raise SystemExit("CITEFUSE_PORT must be between 1 and 65535")
    return port


app = create_app()


def main() -> None:
    uvicorn.run(app, host=HOST, port=_port(), log_level="info")


if __name__ == "__main__":
    main()
