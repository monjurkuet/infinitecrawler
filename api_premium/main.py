#!/usr/bin/env python3
"""Premium API entry (uvicorn :8016).

Usage:
    JWT_SECRET=$(cat /run/media/growloop/codebase/infinitecrawler/.jwt_secret 2>/dev/null || head -c64 /dev/urandom | xxd -p -c64) \
    /home/growloop/.venvs/ic/bin/python3 -m api_premium.main
"""
import os

HOST = os.environ.get("PREMIUM_API_HOST", "127.0.0.1")
PORT = int(os.environ.get("PREMIUM_API_PORT", "8016"))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_premium.server:app", host=HOST, port=PORT, reload=False, log_level="info")
