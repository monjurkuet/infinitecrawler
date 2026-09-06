"""Paywalled premium endpoints — JWT required, unlimited rows for 'pro' tier."""
import csv
import io
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from api.dependencies import get_pg_pool
from api_premium.deps import get_current_user
from api_premium.premium.repo import (
    BBB_CSV_COLUMNS,
    BBB_LIST_SELECT,
    CSV_COLUMNS,
    DETAIL_SELECT,
    LIST_SELECT,
    build_bbb_where,
    build_where,
)
from api_premium.premium.schemas import (
    BbbLeadListResponse,
    LeadDetailResponse,
    LeadListItem,
    LeadListResponse,
    StatsOut,
)

_BBB_COLS = [
    "id", "business_id", "business_name", "address", "city", "state",
    "zip", "phone", "rating", "accredited", "profile_url", "email",
    "website", "years_in_business", "social_links", "source_query",
    "created_at", "updated_at",
]

log = logging.getLogger(__name__)
router = APIRouter(prefix="/premium", tags=["premium"])

# Base column list matching the repo LIST_SELECT output (order matters)
_LIST_COLS = [
    "id", "place_id", "source_url", "source_type", "name", "category",
    "rating", "review_count", "address", "phone", "website", "booking_url",
    "plus_code", "is_claimed", "latitude", "longitude", "sector_id",
    "classification_confidence", "classification_method", "created_at",
    "updated_at", "email_scanned_at", "emails", "linkedin_url", "linkedin_title",
]


def _row_to_item(row: tuple) -> dict:
    return dict(zip(_LIST_COLS, row))


@router.get("/filters")
async def filters(pool=Depends(get_pg_pool), user: dict = Depends(get_current_user)):
    """Distinct categories + canonical city/country lists for the dropdowns."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT DISTINCT category FROM scraper.gmaps_listings "
                "WHERE source_type='gmaps_listing' AND category IS NOT NULL AND category<>'' "
                "ORDER BY category LIMIT 500"
            )
            cats = [r[0] for r in await cur.fetchall()]
    from api_premium.premium.repo import BD_CITY_ALIASES, COUNTRY_PATTERNS
    return {
        "categories": cats,
        "countries": sorted(COUNTRY_PATTERNS.keys()),
        "cities_by_country": {
            "Bangladesh": sorted(BD_CITY_ALIASES.keys()),
        },
    }


@router.get("/leads", response_model=LeadListResponse)
async def list_leads(
    pool=Depends(get_pg_pool),
    user: dict = Depends(get_current_user),
    city: str | None = Query(None),
    category: str | None = Query(None),
    min_rating: float | None = Query(None, ge=0, le=5),
    has_email: bool | None = Query(None),
    q: str | None = Query(None),
    country: str | None = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
):
    where_sql, params = build_where(city, category, min_rating, has_email, q, country=country)
    base = LIST_SELECT

    total_sql = f"SELECT count(*) FROM scraper.gmaps_listings l WHERE {where_sql}"
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(total_sql, params)
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"""
                {base} WHERE {where_sql}
                ORDER BY l.created_at DESC
                OFFSET %s LIMIT %s
                """,
                params + [(page - 1) * size, size],
            )
            rows = await cur.fetchall()

        # bump usage counters
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE scraper.app_users SET searches_run = searches_run + 1 WHERE id=%s",
                (user["id"],),
            )
            await conn.commit()

    items = [_row_to_item(r) for r in rows]
    return LeadListResponse(total=total, page=page, size=size, items=items)


@router.get("/leads/{lead_id}", response_model=LeadDetailResponse)
async def get_lead(
    lead_id: int,
    pool=Depends(get_pg_pool),
    user: dict = Depends(get_current_user),
):
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(DETAIL_SELECT, (lead_id,))
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    # Build dict keyed off the gmaps_listings * column order (+payload, emails_full, linkedin_profiles)
    # Get exact column order via information_schema would be a runtime query; easier: do a second select that returns only the needed columns.
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"{LIST_SELECT} WHERE l.id = %s",
                (lead_id,),
            )
            base = await cur.fetchone()
    if not base:
        raise HTTPException(status_code=404, detail="Lead not found")
    base_dict = _row_to_item(base)

    # Full row + payload — simpler: re-fetch l.* via named columns we care about + extras
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT l.payload, l.social_links,
                    COALESCE(
                        (SELECT array_agg(row_to_json(e)) FROM (
                            SELECT email, is_obfuscated, extraction_method, discovered_at
                            FROM scraper.emails WHERE listing_id = l.id ORDER BY discovered_at DESC
                        ) e), '{}'
                    ) AS emails_full,
                    COALESCE(
                        (SELECT array_agg(row_to_json(p)) FROM (
                            SELECT profile_url, full_name, profile_title, company_name,
                                profile_location, profile_country, connections_count,
                                headline, checked_at
                            FROM scraper.linkedin_profiles WHERE listing_id = l.id
                            ORDER BY checked_at DESC
                        ) p), '{}'
                    ) AS linkedin_profiles
                FROM scraper.gmaps_listings l WHERE l.id=%s
                """,
                (lead_id,),
            )
            extras = await cur.fetchone()

    return LeadDetailResponse(
        **base_dict,
        payload=extras[0],
        social_links=extras[1],
        emails_full=extras[2] or [],
        linkedin_profiles=extras[3] or [],
    )


@router.get("/export.csv")
async def export_csv(
    pool=Depends(get_pg_pool),
    user: dict = Depends(get_current_user),
    city: str | None = Query(None),
    category: str | None = Query(None),
    min_rating: float | None = Query(None, ge=0, le=5),
    has_email: bool | None = Query(None),
    q: str | None = Query(None),
    country: str | None = Query(None),
):
    """Full-row CSV export. No row cap (unlimited pro tier)."""
    where_sql, params = build_where(city, category, min_rating, has_email, q, country=country)
    sql = f"{LIST_SELECT} WHERE {where_sql} ORDER BY l.created_at DESC"

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE scraper.app_users SET rows_exported = rows_exported + %s WHERE id=%s",
                (len(rows), user["id"]),
            )
            await conn.commit()

    def _generate():
        out = io.StringIO()
        writer = csv.writer(out)
        header = CSV_COLUMNS
        writer.writerow(header)
        yield out.getvalue()
        for r in rows:
            item = _row_to_item(r)
            out = io.StringIO()
            w = csv.writer(out)
            w.writerow([
                item.get("id"),
                item.get("name"),
                item.get("category"),
                item.get("rating"),
                item.get("review_count"),
                item.get("address"),
                item.get("phone"),
                item.get("website"),
                ",".join(item.get("emails") or []),
                item.get("linkedin_url"),
                item.get("latitude"),
                item.get("longitude"),
                item.get("plus_code"),
                item.get("is_claimed"),
                item.get("sector_id"),
                item.get("created_at"),
                item.get("updated_at"),
            ])
            yield out.getvalue()

    log.info("CSV export user=%s rows=%d filters=%s", user["email"], len(rows),
             {"city": city, "category": category, "min_rating": min_rating, "has_email": has_email, "q": q})
    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="infinitecrawler-leads.csv"'},
    )


@router.get("/stats", response_model=StatsOut)
async def stats(
    pool=Depends(get_pg_pool),
    user: dict = Depends(get_current_user),
):
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing'"
            )
            total_listings = (await cur.fetchone())[0]
            await cur.execute("SELECT count(*) FROM scraper.emails")
            total_emails = (await cur.fetchone())[0]
            await cur.execute("SELECT count(*) FROM scraper.linkedin_profiles")
            total_linkedin = (await cur.fetchone())[0]

    return StatsOut(
        user={
            "id": user["id"],
            "email": user["email"],
            "entitlement": user["entitlement"],
        },
        total_listings=total_listings,
        total_emails=total_emails,
        total_linkedin=total_linkedin,
        rows_exported=user["rows_exported"],
        searches_run=user["searches_run"],
        rows_limit=None,
    )


@router.get("/bbb-leads", response_model=BbbLeadListResponse)
async def list_bbb_leads(
    pool=Depends(get_pg_pool),
    user: dict = Depends(get_current_user),
    state: str | None = Query(None),
    q: str | None = Query(None),
    accredited: bool | None = Query(None),
    has_website: bool | None = Query(None),
    has_email: bool | None = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
):
    """BBB pipeline leads (US trades), same auth + usage accounting as /leads."""
    where_sql, params = build_bbb_where(state, q, accredited, has_website, has_email)
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM scraper.bbb_listings l WHERE {where_sql}", params
            )
            total = (await cur.fetchone())[0]
            await cur.execute(
                f"{BBB_LIST_SELECT} WHERE {where_sql} "
                "ORDER BY l.updated_at DESC OFFSET %s LIMIT %s",
                params + [(page - 1) * size, size],
            )
            rows = await cur.fetchall()
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE scraper.app_users SET searches_run = searches_run + 1 WHERE id=%s",
                (user["id"],),
            )
            await conn.commit()
    return BbbLeadListResponse(
        total=total, page=page, size=size,
        items=[dict(zip(_BBB_COLS, r)) for r in rows],
    )


@router.get("/bbb-leads/export.csv")
async def export_bbb_csv(
    pool=Depends(get_pg_pool),
    user: dict = Depends(get_current_user),
    state: str | None = Query(None),
    q: str | None = Query(None),
    accredited: bool | None = Query(None),
    has_website: bool | None = Query(None),
    has_email: bool | None = Query(None),
):
    """Full-row BBB CSV export (unlimited pro tier)."""
    where_sql, params = build_bbb_where(state, q, accredited, has_website, has_email)
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"{BBB_LIST_SELECT} WHERE {where_sql} ORDER BY l.updated_at DESC", params
            )
            rows = await cur.fetchall()
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE scraper.app_users SET rows_exported = rows_exported + %s WHERE id=%s",
                (len(rows), user["id"]),
            )
            await conn.commit()

    def _generate():
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(BBB_CSV_COLUMNS)
        yield out.getvalue()
        for r in rows:
            item = dict(zip(_BBB_COLS, r))
            out = io.StringIO()
            w = csv.writer(out)
            w.writerow([item.get(c) for c in BBB_CSV_COLUMNS])
            yield out.getvalue()

    log.info("BBB CSV export user=%s rows=%d", user["email"], len(rows))
    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="ic-bbb-leads.csv"'},
    )
