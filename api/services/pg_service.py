"""Async PostgreSQL service for the API."""

from __future__ import annotations

import json
import logging
import io
import csv
from typing import Any, Optional

from psycopg_pool import AsyncConnectionPool
from utils.pg import build_async_dsn, get_pg_config, get_uncrawled_count_sql

log = logging.getLogger("api.pg_service")

# ─── Connection ─────────────────────────────────────────────────────────────

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
                f"review_count, address, phone, website, social_links, "
                f"latitude, longitude, "
                f"sector_id, classification_confidence, classification_method, classified_at, "
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
    """Return sector breakdown with unified query + in-stream keyword fallback for unclassified.

    Fetches all qualified leads (phone IS NOT NULL) in a single query,
    applies keyword heuristic in-memory for sector_id IS NULL rows,
    and groups results by sector. Total cost: 2 queries (counts + leads)
    instead of N+1.
    """
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

    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # Single query: get all qualified leads with their sector_id (if any)
            await cur.execute("""
                SELECT id, place_id, source_url, name, category, rating,
                       review_count, address, phone, website,
                       latitude, longitude, sector_id,
                       classification_confidence, classification_method, classified_at,
                       created_at, updated_at
                FROM scraper.gmaps_listings
                WHERE phone IS NOT NULL
                ORDER BY review_count DESC NULLS LAST
            """)
            rows = await cur.fetchall()
            cols = [d.name for d in cur.description]
            leads = []
            for r in rows:
                d = dict(zip(cols, r))
                for k, v in d.items():
                    if hasattr(v, "isoformat"):
                        d[k] = v.isoformat()
                leads.append(d)

    # Group in-memory: classified leads keep their sector, unclassified get keyword fallback
    sector_leads: dict[str, list] = {sid: [] for sid in sector_map}
    sector_leads["other"] = []
    sector_leads["high-roi-niches"] = []

    for lead in leads:
        if lead.get("sector_id"):
            sid = lead["sector_id"]
            sector_leads.setdefault(sid, []).append(lead)
        else:
            cat = (lead.get("category") or "").lower()
            name = (lead.get("name") or "").lower()
            matched = False
            for sid, keywords in sector_map.items():
                if any(kw in cat or kw in name for kw in keywords):
                    sector_leads[sid].append(lead)
                    matched = True
                    break
            if not matched:
                sector_leads["other"].append(lead)

    # Build result with counts and lead samples (up to 50 per sector)
    result = []
    for sid, sl in sector_leads.items():
        if sl:
            result.append({
                "sector": sid,
                "display_name": sid.replace("-", " ").title(),
                "count": len(sl),
                "leads": [{
                    "id": lead.get("id"),
                    "place_id": lead.get("place_id"),
                    "source_url": lead.get("source_url"),
                    "name": lead.get("name"),
                    "category": lead.get("category"),
                    "rating": lead.get("rating"),
                    "review_count": lead.get("review_count"),
                    "address": lead.get("address"),
                    "phone": lead.get("phone"),
                    "website": lead.get("website"),
                    "latitude": lead.get("latitude"),
                    "longitude": lead.get("longitude"),
                    "sector_id": lead.get("sector_id"),
                    "classification_confidence": lead.get("classification_confidence"),
                    "classification_method": lead.get("classification_method"),
                    "classified_at": lead.get("classified_at"),
                } for lead in sl[:50]],
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
            await cur.execute(get_uncrawled_count_sql())
            return (await cur.fetchone())[0] or 0


# ─── Export ─────────────────────────────────────────────────────────────────

async def export_leads_csv(filters: dict, limit: int = 0) -> str:
    """Return CSV content for matching leads. Optional limit for quick preview."""

    where_sql, where_params = _build_leads_where(filters)
    pool = await get_pool()

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            sql = (
                f"SELECT name, category, phone, website, social_links, address, rating, "
                f"review_count, latitude, longitude, place_id, source_url, sector_id "
                f"FROM scraper.gmaps_listings WHERE {where_sql} "
                f"ORDER BY review_count DESC NULLS LAST"
            )
            if limit > 0:
                sql += f" LIMIT {limit}"
            await cur.execute(sql, where_params)
            rows = await cur.fetchall()
            cols = ["Name", "Category", "Phone", "Website", "Social", "Address",
                    "Rating", "Reviews", "Lat", "Lng", "Place ID", "Source URL", "Sector"]

            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(cols)
            for r in rows:
                writer.writerow(r)
            return buf.getvalue()


# ─── Emails ──────────────────────────────────────────────────────────────────

async def query_emails(
    listing_id: Optional[int] = None,
    source_type: Optional[str] = None,
    is_obfuscated: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    pool = await get_pool()
    where_parts = []
    params: list[Any] = []

    if listing_id is not None:
        where_parts.append("e.listing_id = %s")
        params.append(listing_id)
    if source_type is not None:
        where_parts.append("e.extraction_method = %s")
        params.append(source_type)
    if is_obfuscated is not None:
        where_parts.append("e.is_obfuscated = %s")
        params.append(is_obfuscated)

    where_sql = " AND ".join(where_parts) if where_parts else "TRUE"

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM scraper.emails e WHERE {where_sql}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT e.id, e.listing_id, e.website_url, e.email, e.email_type, "
                f"e.extraction_method, e.is_obfuscated, e.context_snippet, "
                f"e.discovered_at, e.last_verified, e.is_active "
                f"FROM scraper.emails e WHERE {where_sql} "
                f"ORDER BY e.discovered_at DESC NULLS LAST "
                f"LIMIT %s OFFSET %s",
                [*params, limit, offset],
            )
            rows = await cur.fetchall()
            emails = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                emails.append(d)
            return emails, total


async def get_email_stats() -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM scraper.emails")
            total_emails = (await cur.fetchone())[0]

            await cur.execute("SELECT count(DISTINCT listing_id) FROM scraper.emails WHERE listing_id IS NOT NULL")
            unique_listings = (await cur.fetchone())[0]

            await cur.execute("""
                SELECT extraction_method, count(*) as cnt
                FROM scraper.emails
                WHERE extraction_method IS NOT NULL
                GROUP BY extraction_method
            """)
            by_extraction = {r[0]: r[1] for r in await cur.fetchall()}

            await cur.execute("""
                SELECT email_type, count(*) as cnt
                FROM scraper.emails
                WHERE email_type IS NOT NULL
                GROUP BY email_type
            """)
            by_source_type = {r[0]: r[1] for r in await cur.fetchall()}

            await cur.execute("SELECT count(*) FROM scraper.emails WHERE is_obfuscated = true")
            obfuscated = (await cur.fetchone())[0]

            obf_rate = round(obfuscated / total_emails, 4) if total_emails > 0 else 0.0

            return {
                "total_emails": total_emails,
                "unique_listings": unique_listings,
                "by_source_type": by_source_type,
                "by_extraction_method": by_extraction,
                "obfuscated_count": obfuscated,
                "obfuscation_rate": obf_rate,
            }


async def get_emails_by_listing(listing_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, listing_id, website_url, email, email_type, "
                "extraction_method, is_obfuscated, context_snippet, "
                "discovered_at, last_verified, is_active "
                "FROM scraper.emails WHERE listing_id = %s "
                "ORDER BY discovered_at DESC",
                (listing_id,),
            )
            rows = await cur.fetchall()
            emails = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                emails.append(d)
            return emails


# ─── LinkedIn Profiles ───────────────────────────────────────────────────────

async def query_linkedin_profiles(
    sector_id: Optional[str] = None,
    min_confidence: Optional[float] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    pool = await get_pool()
    joins = []
    where_parts = []
    params: list[Any] = []

    if sector_id is not None:
        joins.append("LEFT JOIN scraper.gmaps_listings g ON lp.listing_id = g.id")
        where_parts.append("g.sector_id = %s")
        params.append(sector_id)
    if min_confidence is not None:
        where_parts.append("lp.confidence >= %s")
        params.append(min_confidence)

    join_sql = " ".join(joins)
    where_sql = " AND ".join(where_parts) if where_parts else "TRUE"

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM scraper.linkedin_profiles lp {join_sql} WHERE {where_sql}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT lp.id, lp.listing_id, lp.full_name, lp.profile_url, "
                f"lp.profile_title, lp.company_name, lp.search_query, lp.confidence, "
                f"lp.snippet, lp.checked_at, lp.last_updated, lp.notes "
                f"FROM scraper.linkedin_profiles lp {join_sql} WHERE {where_sql} "
                f"ORDER BY lp.checked_at DESC NULLS LAST "
                f"LIMIT %s OFFSET %s",
                [*params, limit, offset],
            )
            rows = await cur.fetchall()
            profiles = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                profiles.append(d)
            return profiles, total


async def get_linkedin_stats() -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM scraper.linkedin_profiles")
            total_profiles = (await cur.fetchone())[0]

            await cur.execute("SELECT count(DISTINCT listing_id) FROM scraper.linkedin_profiles WHERE listing_id IS NOT NULL")
            unique_listings = (await cur.fetchone())[0]

            await cur.execute("SELECT avg(confidence) FROM scraper.linkedin_profiles WHERE confidence IS NOT NULL")
            avg_conf = (await cur.fetchone())[0]

            await cur.execute("""
                SELECT company_name, count(*) as cnt
                FROM scraper.linkedin_profiles
                WHERE company_name IS NOT NULL
                GROUP BY company_name
                ORDER BY cnt DESC
                LIMIT 20
            """)
            by_company = [{"company": r[0], "count": r[1]} for r in await cur.fetchall()]

            await cur.execute("SELECT count(*) FROM scraper.linkedin_profiles WHERE full_name IS NOT NULL")
            matched = (await cur.fetchone())[0]
            unmatched = total_profiles - matched

            return {
                "total_profiles": total_profiles,
                "unique_listings": unique_listings,
                "by_company": by_company,
                "avg_confidence": float(avg_conf) if avg_conf else None,
                "matched_count": matched,
                "unmatched_count": unmatched,
            }


async def get_linkedin_by_listing(listing_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, listing_id, full_name, profile_url, profile_title, "
                "company_name, search_query, confidence, snippet, "
                "checked_at, last_updated, notes "
                "FROM scraper.linkedin_profiles WHERE listing_id = %s "
                "ORDER BY checked_at DESC",
                (listing_id,),
            )
            rows = await cur.fetchall()
            profiles = []
            for r in rows:
                d = {}
                for i, col in enumerate(cur.description):
                    val = r[i]
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    d[col.name] = val
                profiles.append(d)
            return profiles


# ─── Classification Stats ────────────────────────────────────────────────────

async def get_classification_stats() -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM scraper.gmaps_listings")
            total = (await cur.fetchone())[0]

            await cur.execute("SELECT count(*) FROM scraper.gmaps_listings WHERE sector_id IS NOT NULL")
            classified = (await cur.fetchone())[0]
            unclassified = total - classified

            await cur.execute("""
                SELECT classification_method, count(*) as cnt
                FROM scraper.gmaps_listings
                WHERE classification_method IS NOT NULL
                GROUP BY classification_method
            """)
            by_method = {r[0]: r[1] for r in await cur.fetchall()}

            await cur.execute("""
                SELECT CASE
                    WHEN classification_confidence >= 0.9 THEN '0.9-1.0'
                    WHEN classification_confidence >= 0.7 THEN '0.7-0.9'
                    WHEN classification_confidence >= 0.5 THEN '0.5-0.7'
                    WHEN classification_confidence >= 0.3 THEN '0.3-0.5'
                    ELSE '0.0-0.3'
                END as bucket, count(*) as cnt
                FROM scraper.gmaps_listings
                WHERE classification_confidence IS NOT NULL
                GROUP BY bucket
                ORDER BY bucket DESC
            """)
            conf_dist = [{"bucket": r[0], "count": r[1]} for r in await cur.fetchall()]

            await cur.execute("""
                SELECT DATE(classified_at) as date, count(*) as cnt
                FROM scraper.gmaps_listings
                WHERE classified_at IS NOT NULL
                GROUP BY DATE(classified_at)
                ORDER BY date DESC
                LIMIT 30
            """)
            classified_by_date = [{"date": str(r[0]), "count": r[1]} for r in await cur.fetchall()]

            return {
                "total_listings": total,
                "classified_count": classified,
                "unclassified_count": unclassified,
                "by_method": by_method,
                "confidence_distribution": conf_dist,
                "classified_by_date": classified_by_date,
            }


async def query_unclassified(
    min_reviews: int = 0,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    pool = await get_pool()
    extra = "AND review_count >= %s" if min_reviews > 0 else ""
    extra_params: list[Any] = [min_reviews] if min_reviews > 0 else []

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM scraper.gmaps_listings "
                f"WHERE sector_id IS NULL AND classification_method IS NULL {extra}",
                extra_params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT id, name, category, rating, review_count, phone, website, "
                f"social_links, address, classification_method, classified_at "
                f"FROM scraper.gmaps_listings "
                f"WHERE sector_id IS NULL AND classification_method IS NULL {extra} "
                f"ORDER BY review_count DESC NULLS LAST "
                f"LIMIT %s OFFSET %s",
                [*extra_params, limit, offset],
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


# ─── Dashboard Queries ───────────────────────────────────────────────────────

async def get_dashboard_overview() -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM scraper.gmaps_listings")
            total_listings = (await cur.fetchone())[0]

            await cur.execute("SELECT count(*) FROM scraper.gmaps_search_results")
            total_search_results = (await cur.fetchone())[0]

            from utils.pg import get_uncrawled_count_sql  # noqa: F811
            await cur.execute(get_uncrawled_count_sql())
            uncrawled = (await cur.fetchone())[0] or 0

            await cur.execute("SELECT count(*) FROM scraper.emails")
            emails = (await cur.fetchone())[0]

            await cur.execute("SELECT count(*) FROM scraper.linkedin_profiles")
            linkedin = (await cur.fetchone())[0]

            await cur.execute("SELECT count(*) FROM scraper.gmaps_listings WHERE sector_id IS NOT NULL")
            classified = (await cur.fetchone())[0]
            classified_pct = round(classified / total_listings * 100, 1) if total_listings > 0 else 0.0

            await cur.execute("SELECT max(created_at) FROM scraper.gmaps_listings")
            last_listing = (await cur.fetchone())[0]
            await cur.execute("SELECT max(created_at) FROM scraper.gmaps_search_results")
            last_search = (await cur.fetchone())[0]

            return {
                "total_listings": total_listings,
                "total_search_results": total_search_results,
                "uncrawled_urls": uncrawled,
                "emails_extracted": emails,
                "linkedin_profiles": linkedin,
                "classified_pct": classified_pct,
                "last_listing_activity": last_listing.isoformat() if hasattr(last_listing, "isoformat") else (str(last_listing) if last_listing else None),
                "last_search_activity": last_search.isoformat() if hasattr(last_search, "isoformat") else (str(last_search) if last_search else None),
            }


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


# ─── Pipeline Tasks ──────────────────────────────────────────────────────────

async def create_pipeline_task(task: dict) -> dict:
    return await save_task(task)


async def update_task_status(task_id: str, status: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE {TASKS_SCHEMA}.{TASKS_TABLE} SET status = %s, "
                "completed_at = CASE WHEN %s IN ('completed','failed','cancelled') THEN NOW() ELSE completed_at END "
                "WHERE id = %s",
                (status, status, task_id),
            )
    return await get_task(task_id)


async def list_pipeline_tasks(
    task_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    where_parts = []
    params: list[Any] = []
    if task_type:
        where_parts.append("type = %s")
        params.append(task_type)
    if status:
        where_parts.append("status = %s")
        params.append(status)
    where_sql = " AND ".join(where_parts) if where_parts else "TRUE"

    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM scraper.api_tasks WHERE {where_sql}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT * FROM scraper.api_tasks WHERE {where_sql} "
                f"ORDER BY created_at DESC LIMIT %s OFFSET %s",
                [*params, limit, offset],
            )
            rows = await cur.fetchall()
            tasks = [_row_to_task(r, cur) for r in rows]
            return tasks, total


async def get_active_pipeline_tasks() -> list[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM scraper.api_tasks WHERE status IN ('pending', 'running') "
                "ORDER BY created_at DESC"
            )
            rows = await cur.fetchall()
            return [_row_to_task(r, cur) for r in rows]


async def cancel_pipeline_task(task_id: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT status FROM scraper.api_tasks WHERE id = %s",
                (task_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            if row[0] not in ("pending",):
                return None
            await cur.execute(
                "UPDATE scraper.api_tasks SET status = 'cancelled', completed_at = NOW() "
                "WHERE id = %s",
                (task_id,),
            )
    return await get_task(task_id)


# ─── Luxury Leads ─────────────────────────────────────────────────────────────

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
