"""api/services/dashboard_repo.py — Dashboard overview / throughput / coverage."""

from __future__ import annotations

import asyncio
import time

from api.services.pg_pool import get_pool
from utils.pg import get_uncrawled_count_sql


async def _one(sql: str):
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            return (await cur.fetchone())[0]


_OVERVIEW_CACHE: dict = {}
_OVERVIEW_TTL_S = 60


async def get_dashboard_overview() -> dict:
    # Dashboard numbers don't need to be realtime; cache for 60s so
    # concurrent panel loads don't stampede the 1M-row counts.
    now = time.monotonic()
    hit = _OVERVIEW_CACHE.get("data")
    if hit and now - hit[0] < _OVERVIEW_TTL_S:
        return hit[1]
    # The 8 counts each scan large tables — run them concurrently on
    # separate pool connections instead of sequentially on one.
    (total_listings, total_search_results, uncrawled, emails, linkedin,
     classified, last_listing, last_search) = await asyncio.gather(
        _one("SELECT count(*) FROM scraper.gmaps_listings"),
        _one("SELECT count(*) FROM scraper.gmaps_search_results"),
        _one(get_uncrawled_count_sql()),
        _one("SELECT count(*) FROM scraper.emails"),
        _one("SELECT count(*) FROM scraper.linkedin_profiles"),
        _one("SELECT count(*) FROM scraper.gmaps_listings WHERE sector_id IS NOT NULL"),
        _one("SELECT max(created_at) FROM scraper.gmaps_listings"),
        _one("SELECT max(created_at) FROM scraper.gmaps_search_results"),
    )
    uncrawled = uncrawled or 0
    classified_pct = round(classified / total_listings * 100, 1) if total_listings > 0 else 0.0

    data = {
        "total_listings": total_listings,
        "total_search_results": total_search_results,
        "uncrawled_urls": uncrawled,
        "emails_extracted": emails,
        "linkedin_profiles": linkedin,
        "classified_pct": classified_pct,
        "last_listing_activity": last_listing.isoformat() if hasattr(last_listing, "isoformat") else (str(last_listing) if last_listing else None),
        "last_search_activity": last_search.isoformat() if hasattr(last_search, "isoformat") else (str(last_search) if last_search else None),
    }
    _OVERVIEW_CACHE["data"] = (time.monotonic(), data)
    return data


async def get_throughput(period_hours: int = 24) -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            interval = f"{period_hours} hours"

            await cur.execute(
                "SELECT date_trunc('hour', created_at) as bucket, count(*) as cnt "
                "FROM scraper.gmaps_search_results "
                f"WHERE created_at >= NOW() - INTERVAL '{interval}' "
                "GROUP BY bucket ORDER BY bucket",
            )
            search_buckets = {str(r[0]): r[1] for r in await cur.fetchall()}

            await cur.execute(
                "SELECT date_trunc('hour', created_at) as bucket, count(*) as cnt "
                "FROM scraper.gmaps_listings "
                f"WHERE created_at >= NOW() - INTERVAL '{interval}' "
                "GROUP BY bucket ORDER BY bucket",
            )
            listing_buckets = {str(r[0]): r[1] for r in await cur.fetchall()}

            await cur.execute(
                "SELECT date_trunc('hour', classified_at) as bucket, count(*) as cnt "
                "FROM scraper.gmaps_listings "
                f"WHERE classified_at >= NOW() - INTERVAL '{interval}' "
                "GROUP BY bucket ORDER BY bucket",
            )
            classified_buckets = {str(r[0]): r[1] for r in await cur.fetchall()}

            import datetime as _dt
            now = _dt.datetime.now(_dt.timezone.utc)
            buckets = []
            cursor_time = now - _dt.timedelta(hours=period_hours)
            while cursor_time <= now:
                key = cursor_time.replace(minute=0, second=0, microsecond=0).isoformat()
                buckets.append({
                    "time": key,
                    "search_results": search_buckets.get(key, 0),
                    "listings_extracted": listing_buckets.get(key, 0),
                    "listings_classified": classified_buckets.get(key, 0),
                })
                cursor_time += _dt.timedelta(hours=1)

            return {"period": f"{period_hours}h", "buckets": buckets}


async def get_recent_activity(limit: int = 50) -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, name, category, phone, website, social_links, created_at "
                "FROM scraper.gmaps_listings "
                f"ORDER BY created_at DESC NULLS LAST LIMIT {limit}"
            )
            recent_listings = []
            for r in await cur.fetchall():
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                recent_listings.append(d)

            await cur.execute(
                "SELECT id, key_value, source_type, payload, created_at "
                "FROM scraper.gmaps_search_results "
                f"ORDER BY created_at DESC NULLS LAST LIMIT {limit}"
            )
            recent_search = []
            for r in await cur.fetchall():
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                recent_search.append(d)

            await cur.execute(
                "SELECT id, listing_id, email, email_type, extraction_method, discovered_at "
                "FROM scraper.emails "
                f"ORDER BY discovered_at DESC NULLS LAST LIMIT {limit}"
            )
            recent_emails = []
            for r in await cur.fetchall():
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                recent_emails.append(d)

            return {
                "recent_listings": recent_listings,
                "recent_search_results": recent_search,
                "recent_emails": recent_emails,
            }


async def get_coverage(dimension: str = "sector") -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            if dimension == "sector":
                await cur.execute("""
                    SELECT
                        COALESCE(g.sector_id, 'unclassified') as sector,
                        count(*) as total,
                        count(CASE WHEN g.sector_id IS NOT NULL THEN 1 END) as classified,
                        count(CASE WHEN g.phone IS NOT NULL THEN 1 END) as with_phone,
                        count(CASE WHEN e.email IS NOT NULL THEN 1 END) as with_email
                    FROM scraper.gmaps_listings g
                    LEFT JOIN (
                        SELECT listing_id, string_agg(email, ',') as email
                        FROM scraper.emails WHERE is_active = true
                        GROUP BY listing_id
                    ) e ON e.listing_id = g.id
                    GROUP BY g.sector_id
                    ORDER BY total DESC
                """)
            else:
                await cur.execute("""
                    SELECT
                        split_part(g.address, ',', array_length(string_to_array(g.address, ','), 1)) as city,
                        count(*) as total,
                        count(CASE WHEN g.sector_id IS NOT NULL THEN 1 END) as classified,
                        count(CASE WHEN g.phone IS NOT NULL THEN 1 END) as with_phone,
                        count(CASE WHEN e.email IS NOT NULL THEN 1 END) as with_email
                    FROM scraper.gmaps_listings g
                    LEFT JOIN scraper.emails e ON e.listing_id = g.id AND e.is_active = true
                    WHERE g.address IS NOT NULL
                    GROUP BY city
                    ORDER BY total DESC
                """)

            rows = await cur.fetchall()
            result = []
            for r in rows:
                result.append({
                    "key": r[0] or "unknown",
                    "total": r[1],
                    "classified": r[2],
                    "with_phone": r[3],
                    "with_email": r[4],
                })
            return {"dimension": dimension, "rows": result}


async def get_total_throughput_24h() -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    (SELECT count(*) FROM scraper.gmaps_search_results WHERE created_at >= NOW() - INTERVAL '24 hours') +
                    (SELECT count(*) FROM scraper.gmaps_listings WHERE created_at >= NOW() - INTERVAL '24 hours')
                    as total
            """)
            return (await cur.fetchone())[0] or 0
