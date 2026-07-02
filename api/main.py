#!/usr/bin/env python3
"""Entry point for the InfiniteCrawler API server.

Usage:
    uv run python -m api.main
    # or:
    uvicorn api.server:app --host 0.0.0.0 --port 8015
"""

import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

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
