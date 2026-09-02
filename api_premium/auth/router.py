"""Auth router — register / login / me. No internal-API token; JWT only."""

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from psycopg.types.json import Jsonb

from api.dependencies import get_pg_pool
from api_premium.auth.schemas import ChangePasswordIn, LoginIn, RegisterIn, TokenOut, MeOut
from api_premium.auth.service import hash_password, sign_token, verify_password
from api_premium.deps import get_current_user

log = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])

# Auto-upgrade default: every new signup gets "pro" tier, unlimited rows for now.
DEFAULT_ENTITLEMENT = {"tier": "pro", "rows_limit": None}
# Policy knobs — safe defaults, tuned without breaking tests:
LOGIN_WINDOW = "10 minutes"
LOGIN_MAX_FAILURES = 5


async def _check_rate_limit(conn, email: str) -> None:
    """HTTP 429 if too many failed attempts in the window."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT count(*) FROM scraper.auth_attempts
            WHERE email=%s AND success=false
              AND attempted_at > now() - make_interval(mins => %s)
            """,
            (email, int(LOGIN_WINDOW.split()[0])),
        )
        hits = (await cur.fetchone())[0]
    if hits >= LOGIN_MAX_FAILURES:
        raise HTTPException(status_code=429, detail=f"Too many attempts — try again in {LOGIN_WINDOW}")


async def _record_attempt(conn, email: str, ip: str | None, ok: bool) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO scraper.auth_attempts (email, ip, success) VALUES (%s, %s::inet, %s)",
            (email, ip, ok),
        )


@router.post("/register", response_model=TokenOut, status_code=201)
async def register(body: RegisterIn, pool=Depends(get_pg_pool)):
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id FROM scraper.app_users WHERE email=%s", (body.email,))
            if await cur.fetchone():
                raise HTTPException(status_code=409, detail="Email already registered")
            await cur.execute(
                "INSERT INTO scraper.app_users (email, password_hash, entitlement) "
                "VALUES (%s, %s, %s) RETURNING id, email, entitlement, created_at, searches_run, rows_exported",
                (body.email, hash_password(body.password), Jsonb(DEFAULT_ENTITLEMENT)),
            )
            uid, email, ent, created, srch, exp = await cur.fetchone()
            await conn.commit()
    log.info("New premium user registered: %s", email)
    return TokenOut(
        token=sign_token(uid, email),
        user={"id": uid, "email": email, "entitlement": ent, "created_at": created, "searches_run": srch, "rows_exported": exp},
    )


@router.post("/login", response_model=TokenOut)
async def login(body: LoginIn, request: Request, pool=Depends(get_pg_pool)):
    client_ip = request.client.host if request.client else None
    async with pool.connection() as conn:
        await _check_rate_limit(conn, body.email)
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, email, password_hash, entitlement, created_at, searches_run, rows_exported "
                "FROM scraper.app_users WHERE email=%s",
                (body.email,),
            )
            row = await cur.fetchone()
            ok = bool(row and verify_password(body.password, row[2]))
            await cur.execute(
                "INSERT INTO scraper.auth_attempts (email, ip, success) VALUES (%s, %s::inet, %s)",
                (body.email, client_ip, ok),
            )
            if not ok:
                await conn.commit()
                raise HTTPException(status_code=401, detail="Invalid email or password")
            uid, email, _, ent, created, srch, exp = row
            await cur.execute(
                "UPDATE scraper.app_users SET last_login_at=%s WHERE id=%s",
                (datetime.now(timezone.utc), uid),
            )
            await conn.commit()
    log.info("Premium user login: %s", email)
    return TokenOut(
        token=sign_token(uid, email),
        user={"id": uid, "email": email, "entitlement": ent, "created_at": created, "searches_run": srch, "rows_exported": exp},
    )


@router.post("/change-password")
async def change_password(
    body: ChangePasswordIn,
    pool=Depends(get_pg_pool),
    user: dict[str, Any] = Depends(get_current_user),
):
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT password_hash FROM scraper.app_users WHERE id=%s", (user["id"],))
            row = await cur.fetchone()
            if not row or not verify_password(body.current_password, row[0]):
                raise HTTPException(status_code=403, detail="Current password incorrect")
            await cur.execute(
                "UPDATE scraper.app_users SET password_hash=%s WHERE id=%s",
                (hash_password(body.new_password), user["id"]),
            )
            await conn.commit()
    log.info("Password changed: %s", user["email"])
    return {"ok": True}


@router.post("/refresh", response_model=TokenOut)
async def refresh(user: dict[str, Any] = Depends(get_current_user)):
    return TokenOut(
        token=sign_token(user["id"], user["email"]),
        user={
            "id": user["id"], "email": user["email"], "entitlement": user["entitlement"],
            "created_at": user["created_at"], "searches_run": user["searches_run"],
            "rows_exported": user["rows_exported"],
        },
    )


@router.get("/me", response_model=MeOut)
async def me(user: dict[str, Any] = Depends(get_current_user)):
    return MeOut(**user)
