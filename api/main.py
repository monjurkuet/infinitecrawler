#!/usr/bin/env python3
"""Entry point for the InfiniteCrawler API server.

Usage:
    uv run python -m api.main
    # or:
    uvicorn api.server:app --host 0.0.0.0 --port 8015
"""

import logging
import os
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

try:
    _rot = RotatingFileHandler(
        "/var/log/infinitecrawler/infinitecrawler-api.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
    )
    _rot.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(_rot)
except OSError:
    pass  # log dir may be missing in dev; stderr handler still works

HOST = os.environ.get("INFINITECRAWLER_API_HOST", "0.0.0.0")
PORT = int(os.environ.get("INFINITECRAWLER_API_PORT", "8015"))
TOKEN = os.environ.get("INFINITECRAWLER_API_TOKEN", "changeme")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.server:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
    )
