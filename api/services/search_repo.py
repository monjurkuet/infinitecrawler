"""api/services/search_repo.py — Search-result and uncrawled-URL queries."""

from __future__ import annotations

from typing import Any, Optional

from api.services.pg_pool import get_pool
from utils.pg import get_uncrawled_count_sql


async def query_search_results(limit: int = 50, offset: int = 0, source_type: Optional[str] = None) -> tuple[list[dict], int]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            where = "WHERE source_type = %s" if source_type else ""
            params: list[Any] = [source_type] if source_type else []

            await cur.execute(
                f"SELECT count(*) FROM scraper.gmaps_search_results {where}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT id, key_value, source_type, payload, created_at, updated_at "
                f"FROM scraper.gmaps_search_results {where} "
                f"ORDER BY created_at DESC LIMIT %s OFFSET %s",
                [*params, limit, offset],
            )
            rows = await cur.fetchall()
            results = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    if isinstance(val, dict):
                        pass  # jsonb already dict
                    d[col.name] = val
                results.append(d)
            return results, total


async def get_search_result_by_id(result_id: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM scraper.gmaps_search_results WHERE id = %s",
                (result_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            d = {}
            for i, col in enumerate(cur.description):
                val = row[i]
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                d[col.name] = val
            return d


async def get_search_result_stats() -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM scraper.gmaps_search_results")
            total = (await cur.fetchone())[0]

            await cur.execute("""
                SELECT source_type, count(*) as cnt
                FROM scraper.gmaps_search_results
                WHERE source_type IS NOT NULL
                GROUP BY source_type
            """)
            by_source = {r[0]: r[1] for r in await cur.fetchall()}

            await cur.execute("""
                SELECT count(*) FROM scraper.gmaps_search_results
                WHERE created_at >= NOW() - INTERVAL '24 hours'
            """)
            recent = (await cur.fetchone())[0]

            return {"total": total, "by_source_type": by_source, "recent_24h": recent}


async def get_uncrawled_count() -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(get_uncrawled_count_sql())
            return (await cur.fetchone())[0] or 0
