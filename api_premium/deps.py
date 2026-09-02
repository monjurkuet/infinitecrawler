"""FastAPI dependency — extract + validate current premium user from JWT."""

import logging
from typing import Any

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.dependencies import get_pg_pool
from api_premium.auth.service import decode_token

log = logging.getLogger(__name__)
_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    pool=Depends(get_pg_pool),
) -> dict[str, Any]:
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    try:
        payload = decode_token(credentials.credentials)
    except Exception as e:
        log.warning("JWT decode failed: %s", e)
        raise HTTPException(status_code=401, detail="Invalid or expired token") from e

    user_id = int(payload["sub"])
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, email, entitlement, created_at, searches_run, rows_exported "
                "FROM scraper.app_users WHERE id=%s",
                (user_id,),
            )
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="User not found")
    uid, email, entitlement, created_at, searches_run, rows_exported = row
    return {
        "id": uid,
        "email": email,
        "entitlement": entitlement,
        "created_at": created_at,
        "searches_run": searches_run,
        "rows_exported": rows_exported,
    }
