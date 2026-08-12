"""api/services/luxury_repo.py — Luxury-targets & luxury-contacts queries."""

from __future__ import annotations

from typing import Any, Optional

from api.services.pg_pool import get_pool


async def query_luxury_targets(
    target_type: Optional[str] = None,
    tier: Optional[str] = None,
    city: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    pool = await get_pool()
    where = []
    params: list[Any] = []
    if target_type:
        where.append("lt.target_type = %s")
        params.append(target_type)
    if tier:
        where.append("lt.tier = %s")
        params.append(tier)
    if city:
        where.append("lt.city = %s")
        params.append(city)
    where_clause = " AND ".join(where) if where else "TRUE"

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM scraper.luxury_targets lt WHERE {where_clause}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"""
                SELECT lt.*, COUNT(lc.id) as contact_count
                FROM scraper.luxury_targets lt
                LEFT JOIN scraper.luxury_contacts lc ON lc.target_id = lt.id
                WHERE {where_clause}
                GROUP BY lt.id
                ORDER BY lt.id
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            rows = await cur.fetchall()
            targets = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                targets.append(d)
            return targets, total


async def query_luxury_contacts(
    target_id: Optional[int] = None,
    platform: Optional[str] = None,
    min_confidence: Optional[float] = None,
    is_employee: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    pool = await get_pool()
    where = []
    params: list[Any] = []
    if target_id:
        where.append("lc.target_id = %s")
        params.append(target_id)
    if platform:
        where.append("lc.platform = %s")
        params.append(platform)
    if min_confidence is not None:
        where.append("lc.confidence >= %s")
        params.append(min_confidence)
    if is_employee is not None:
        where.append("lc.is_employee = %s")
        params.append(is_employee)
    where_clause = " AND ".join(where) if where else "TRUE"

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM scraper.luxury_contacts lc WHERE {where_clause}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"""
                SELECT lc.*, lt.name as target_name
                FROM scraper.luxury_contacts lc
                LEFT JOIN scraper.luxury_targets lt ON lt.id = lc.target_id
                WHERE {where_clause}
                ORDER BY lc.confidence DESC, lc.discovered_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            rows = await cur.fetchall()
            contacts = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                contacts.append(d)
            return contacts, total


async def get_luxury_stats() -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM scraper.luxury_targets")
            total_targets = (await cur.fetchone())[0]

            await cur.execute("SELECT COUNT(*) FROM scraper.luxury_contacts")
            total_contacts = (await cur.fetchone())[0]

            await cur.execute("SELECT COUNT(DISTINCT target_id) FROM scraper.luxury_contacts")
            targets_with = (await cur.fetchone())[0]

            await cur.execute("""
                SELECT lt.target_type, lt.tier, COUNT(*) as cnt
                FROM scraper.luxury_targets lt
                GROUP BY lt.target_type, lt.tier
                ORDER BY cnt DESC
            """)
            by_type = [{"target_type": r[0], "tier": r[1], "count": r[2]} for r in await cur.fetchall()]

            await cur.execute("""
                SELECT platform, COUNT(*) as cnt
                FROM scraper.luxury_contacts
                GROUP BY platform
                ORDER BY cnt DESC
            """)
            by_platform = [{"platform": r[0], "count": r[1]} for r in await cur.fetchall()]

            await cur.execute("SELECT ROUND(AVG(confidence)::numeric, 3) FROM scraper.luxury_contacts WHERE confidence > 0")
            avg_conf = float((await cur.fetchone())[0] or 0)

            await cur.execute("SELECT COUNT(*) FROM scraper.luxury_targets WHERE linkedin_searched = FALSE OR facebook_searched = FALSE")
            pending = (await cur.fetchone())[0]

            return {
                "total_targets": total_targets,
                "total_contacts": total_contacts,
                "targets_with_contacts": targets_with,
                "by_type": by_type,
                "by_platform": by_platform,
                "avg_confidence": avg_conf,
                "pending_targets": pending,
            }
