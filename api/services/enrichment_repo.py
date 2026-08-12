"""api/services/enrichment_repo.py — Email, LinkedIn, classification queries."""

from __future__ import annotations

from typing import Any, Optional

from api.services.pg_pool import get_pool


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
