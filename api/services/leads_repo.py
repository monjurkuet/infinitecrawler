"""api/services/leads_repo.py — Lead queries against scraper.gmaps_listings."""

from __future__ import annotations

from typing import Any, Optional

from api.services.pg_pool import get_pool


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
