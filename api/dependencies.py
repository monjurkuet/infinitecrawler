"""FastAPI dependencies — injected services."""

from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from psycopg_pool import AsyncConnectionPool

from api.services import pg_service, redis_service

_bearer_scheme = HTTPBearer(auto_error=False)
# Simple bearer token auth — set INFINITECRAWLER_API_TOKEN env var
import os
_API_TOKEN = os.environ.get("INFINITECRAWLER_API_TOKEN", "changeme")


async def verify_token(credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme)) -> str:
    if _API_TOKEN == "changeme":
        # Auth not configured — allow all
        return "anonymous"
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    if credentials.credentials != _API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return "authenticated"


async def get_pg_pool() -> AsyncConnectionPool:
    try:
        return await pg_service.get_pool()
    except RuntimeError:
        raise HTTPException(status_code=503, detail="PostgreSQL pool not initialized")


async def get_redis_client():
    try:
        return redis_service.get_client()
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Redis not initialized")