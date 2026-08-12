"""api/services/pg_pool.py — Connection pool lifecycle and health probe."""

from __future__ import annotations

import logging
from typing import Optional

from psycopg_pool import AsyncConnectionPool
from utils.pg import build_async_dsn, get_pg_config

log = logging.getLogger("api.pg_pool")

_pool: Optional[AsyncConnectionPool] = None


async def create_pool() -> AsyncConnectionPool:
    global _pool
    _pg = get_pg_config()
    host = _pg["host"]
    port = _pg["port"]
    dbname = _pg["dbname"]

    # DSN: when host is a unix socket path (contains '/'), omit port to
    # prevent psycopg3 from parsing "port=5432" as a hostname.  Always
    # include the three session timeouts (T3) so a single runaway query or
    # stuck `idle in transaction` backend can't lock up the whole pool.
    dsn = build_async_dsn()

    pool = AsyncConnectionPool(
        dsn,
        min_size=1,
        max_size=5,
        open=True,
        kwargs={"connect_timeout": 10},
    )
    await pool.open()
    log.info(f"PG pool created: {dbname} @ {host}:{port}")
    _pool = pool
    return pool


async def get_pool() -> AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("PG pool not initialized")
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def check_health() -> str:
    try:
        pool = await get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                return "ok"
    except Exception as e:
        log.warning(f"PG health check failed: {e}")
        return "error"
