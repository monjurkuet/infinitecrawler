"""Async PostgreSQL service for the API."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from psycopg_pool import AsyncConnectionPool

log = logging.getLogger("api.pg_service")

# ─── Connection ─────────────────────────────────────────────────────────────

_pool: Optional[AsyncConnectionPool] = None


async def create_pool() -> AsyncConnectionPool:
    global _pool
    host = os.environ.get("POSTGRESQL_HOST", "100.92.181.21")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ.get("POSTGRES_USERNAME", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "changeme")
    dbname = os.environ.get("POSTGRES_DB", "infinitecrawler")

    pool = AsyncConnectionPool(
        f"host={host} port={port} user={user} password={password} dbname={dbname}",
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


# ─── Health ─────────────────────────────────────────────────────────────────

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


# ─── Task Store ─────────────────────────────────────────────────────────────

TASKS_TABLE = "api_tasks"
TASKS_SCHEMA = "scraper"


async def ensure_tasks_table():
    """Create the tasks table if it doesn't exist."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {TASKS_SCHEMA}
            """)
            await cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TASKS_SCHEMA}.{TASKS_TABLE} (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    config_path TEXT,
                    query TEXT,
                    instance_count INTEGER DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    logs_tail TEXT DEFAULT '',
                    metadata JSONB DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                )
            """)


async def save_task(task: dict) -> dict:
    """Insert or update a task in PG."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            meta_json = json.dumps(task.get("metadata", {}))
            await cur.execute(f"""
                INSERT INTO {TASKS_SCHEMA}.{TASKS_TABLE}
                    (id, type, status, config_path, query, instance_count,
                     pid, exit_code, logs_tail, metadata, created_at,
                     started_at, completed_at)
                VALUES (%(id)s, %(type)s, %(status)s, %(config_path)s, %(query)s,
                        %(instance_count)s, %(pid)s, %(exit_code)s, %(logs_tail)s,
                        %(metadata)s::jsonb, %(created_at)s::timestamptz,
                        %(started_at)s::timestamptz, %(completed_at)s::timestamptz)
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    pid = EXCLUDED.pid,
                    exit_code = EXCLUDED.exit_code,
                    logs_tail = EXCLUDED.logs_tail,
                    metadata = EXCLUDED.metadata,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at
            """, {
                "id": task["id"],
                "type": task["type"],
                "status": task["status"],
                "config_path": task.get("config_path"),
                "query": task.get("query"),
                "instance_count": task.get("instance_count", 1),
                "pid": task.get("pid"),
                "exit_code": task.get("exit_code"),
                "logs_tail": task.get("logs_tail", ""),
                "metadata": meta_json,
                "created_at": task.get("created_at"),
                "started_at": task.get("started_at"),
                "completed_at": task.get("completed_at"),
            })
    return task


async def get_task(task_id: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT * FROM {TASKS_SCHEMA}.{TASKS_TABLE} WHERE id = %s",
                (task_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return _row_to_task(row, cur)


async def list_tasks(status: Optional[str] = None, limit: int = 20, offset: int = 0) -> tuple[list[dict], int]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            where = "WHERE status = %s" if status else ""
            params: list[Any] = [status] if status else []

            await cur.execute(
                f"SELECT count(*) FROM {TASKS_SCHEMA}.{TASKS_TABLE} {where}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT * FROM {TASKS_SCHEMA}.{TASKS_TABLE} {where} "
                f"ORDER BY created_at DESC LIMIT %s OFFSET %s",
                [*params, limit, offset],
            )
            rows = await cur.fetchall()
            tasks = [_row_to_task(r, cur) for r in rows]
            return tasks, total


def _row_to_task(row, cur) -> dict:
    desc = [d[0] for d in cur.description]
    d = dict(zip(desc, row))
    d["metadata"] = json.loads(d.get("metadata") or "{}")
    for ts_field in ("created_at", "started_at", "completed_at"):
        if d.get(ts_field) and hasattr(d[ts_field], "isoformat"):
            d[ts_field] = d[ts_field].isoformat()
    return d


# ─── Leads Queries ──────────────────────────────────────────────────────────

def _build_leads_where(filters: dict) -> tuple[str, list]:
    where_parts = []
    params: list[Any] = []

    if filters.get("category"):
        where_parts.append("category ILIKE %s")
        params.append(f"%{filters['category']}%")
    if filters.get("city"):
        where_parts.append("address ILIKE %s")
        params.append(f"%{filters['city']}%")
    if filters.get("has_phone") is True:
        where_parts.append("phone IS NOT NULL")
    elif filters.get("has_phone") is False:
        where_parts.append("phone IS NULL")
    if filters.get("has_website") is True:
        where_parts.append("website IS NOT NULL")
    elif filters.get("has_website") is False:
        where_parts.append("website IS NULL")
    if filters.get("min_rating") is not None:
        where_parts.append("rating >= %s")
        params.append(filters["min_rating"])
    if filters.get("min_reviews") is not None:
        where_parts.append("review_count >= %s")
        params.append(filters["min_reviews"])

    where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
    return where_sql, params


async def query_leads(
    filters: dict,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    pool = await get_pool()
    where_sql, where_params = _build_leads_where(filters)

    sort_col = filters.get("sort_by", "review_count")
    if sort_col not in ("review_count", "rating", "name", "created_at", "updated_at"):
        sort_col = "review_count"
    sort_dir = "DESC" if filters.get("sort_dir", "desc") == "desc" else "ASC"

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM scraper.gmaps_listings WHERE {where_sql}",
                where_params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT id, place_id, source_url, name, category, rating, "
                f"review_count, address, phone, website, "
                f"latitude, longitude, "
                f"created_at, updated_at "
                f"FROM scraper.gmaps_listings WHERE {where_sql} "
                f"ORDER BY {sort_col} {sort_dir} NULLS LAST "
                f"LIMIT %s OFFSET %s",
                [*where_params, limit, offset],
            )
            rows = await cur.fetchall()
            leads = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                leads.append(d)
            return leads, total


async def get_lead_by_id(lead_id: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM scraper.gmaps_listings WHERE id = %s",
                (lead_id,),
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


async def get_lead_stats() -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM scraper.gmaps_listings")
            total = (await cur.fetchone())[0]

            await cur.execute(
                "SELECT count(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL"
            )
            with_phone = (await cur.fetchone())[0]

            await cur.execute(
                "SELECT count(*) FROM scraper.gmaps_listings WHERE website IS NOT NULL"
            )
            with_website = (await cur.fetchone())[0]

            await cur.execute(
                "SELECT count(*) FROM scraper.gmaps_listings "
                "WHERE phone IS NOT NULL AND website IS NOT NULL"
            )
            with_both = (await cur.fetchone())[0]

            await cur.execute("SELECT avg(rating) FROM scraper.gmaps_listings WHERE rating IS NOT NULL")
            avg_rating = (await cur.fetchone())[0]

            await cur.execute("SELECT count(DISTINCT category) FROM scraper.gmaps_listings WHERE category IS NOT NULL")
            total_categories = (await cur.fetchone())[0]

            # Top categories
            await cur.execute("""
                SELECT category, count(*) as cnt
                FROM scraper.gmaps_listings
                WHERE category IS NOT NULL
                GROUP BY category
                ORDER BY cnt DESC
                LIMIT 20
            """)
            top_categories = [{"category": r[0], "count": r[1]} for r in await cur.fetchall()]

            # City detection from address (rough)
            await cur.execute("""
                SELECT
                    split_part(address, ',', array_length(string_to_array(address, ','), 1)) as city,
                    count(*) as cnt
                FROM scraper.gmaps_listings
                WHERE address IS NOT NULL
                GROUP BY city
                ORDER BY cnt DESC
                LIMIT 20
            """)
            top_cities = [{"city": r[0], "count": r[1]} for r in await cur.fetchall()]

            return {
                "total": total,
                "with_phone": with_phone,
                "with_website": with_website,
                "with_both": with_both,
                "avg_rating": float(avg_rating) if avg_rating else None,
                "total_cities": len(top_cities),
                "total_categories": total_categories,
                "top_categories": top_categories,
                "top_cities": top_cities,
            }


async def get_leads_by_city() -> list[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    split_part(address, ',', array_length(string_to_array(address, ','), 1)) as city,
                    count(*) as cnt
                FROM scraper.gmaps_listings
                WHERE address IS NOT NULL
                GROUP BY city
                ORDER BY cnt DESC
            """)
            return [{"city": r[0], "count": r[1]} for r in await cur.fetchall()]


async def get_leads_by_sector() -> list[dict]:
    """Group leads by BPT sector using keyword matching (mirrors generate_leads.py logic)."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # Get all leads with phone
            await cur.execute("""
                SELECT id, name, category, phone, website, address,
                       rating, review_count, latitude, longitude, place_id, source_url
                FROM scraper.gmaps_listings
                WHERE phone IS NOT NULL
                ORDER BY review_count DESC NULLS LAST
            """)
            rows = await cur.fetchall()
            cols = [d.name for d in cur.description]
            leads = [dict(zip(cols, r)) for r in rows]

    # Simple sector mapping based on category keywords
    sector_map = {
        "healthcare": ["hospital", "clinic", "doctor", "diagnostic", "dental", "pharmacy", "physiotherapy", "medical"],
        "automotive": ["car", "auto", "garage", "workshop", "tire", "spare part", "service center", "motor"],
        "education": ["school", "college", "university", "academy", "training", "tutorial", "coaching"],
        "hospitality": ["hotel", "restaurant", "cafe", "resort", "guest house", "motel", "lodge"],
        "retail": ["store", "shop", "mart", "mall", "outlet", "boutique", "supermarket"],
        "technology": ["computer", "software", "it ", "tech", "electronics", "mobile", "gadget"],
        "real-estate": ["real estate", "property", "apartment", "flat", "land", "developer", "construction"],
        "food-beverage": ["restaurant", "cafe", "bakery", "confectionery", "fast food", "pizza", "chicken"],
        "fashion": ["cloth", "fashion", "tailor", "dress", "garment", "fabric"],
        "finance": ["bank", "insurance", "finance", "loan", "investment", "accounting"],
        "logistics": ["transport", "delivery", "logistics", "courier", "shipping", "mover"],
        "beauty": ["salon", "spa", "beauty", "parlor", "barber", "grooming"],
    }

    sector_leads: dict[str, list] = {}
    for sid in sector_map:
        sector_leads[sid] = []

    for lead in leads:
        cat = (lead.get("category") or "").lower()
        name = (lead.get("name") or "").lower()
        matched = False
        for sid, keywords in sector_map.items():
            if any(kw in cat or kw in name for kw in keywords):
                sector_leads[sid].append(lead)
                matched = True
                break
        if not matched:
            sector_leads.setdefault("other", []).append(lead)

    result = []
    for sid, sl in sector_leads.items():
        if sl:
            result.append({
                "sector": sid,
                "display_name": sid.replace("-", " ").title(),
                "count": len(sl),
                "leads": [{
                    "id": l.get("id"),
                    "name": l.get("name"),
                    "category": l.get("category"),
                    "phone": l.get("phone"),
                    "website": l.get("website"),
                    "address": l.get("address"),
                    "rating": l.get("rating"),
                    "review_count": l.get("review_count"),
                } for l in sl[:50]],  # cap per sector
            })
    return result


# ─── Search Results ─────────────────────────────────────────────────────────

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


# ─── Uncrawled URLs ─────────────────────────────────────────────────────────

async def get_uncrawled_count() -> int:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT COUNT(DISTINCT sr.payload->>'url')
                FROM scraper.gmaps_search_results sr
                LEFT JOIN scraper.gmaps_listings gl
                  ON gl.source_url = sr.payload->>'url'
                WHERE sr.payload->>'url' IS NOT NULL
                  AND gl.source_url IS NULL
            """)
            return (await cur.fetchone())[0] or 0


# ─── Export ─────────────────────────────────────────────────────────────────

async def export_leads_csv(filters: dict, limit: int = 0) -> str:
    """Return CSV content for matching leads. Optional limit for quick preview."""
    import io
    import csv

    where_sql, where_params = _build_leads_where(filters)
    pool = await get_pool()

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            sql = (
                f"SELECT name, category, phone, website, address, rating, "
                f"review_count, latitude, longitude, place_id, source_url "
                f"FROM scraper.gmaps_listings WHERE {where_sql} "
                f"ORDER BY review_count DESC NULLS LAST"
            )
            if limit > 0:
                sql += f" LIMIT {limit}"
            await cur.execute(sql, where_params)
            rows = await cur.fetchall()
            cols = ["Name", "Category", "Phone", "Website", "Address",
                    "Rating", "Reviews", "Lat", "Lng", "Place ID", "Source URL"]

            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(cols)
            for r in rows:
                writer.writerow(r)
            return buf.getvalue()
