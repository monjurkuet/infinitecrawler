"""Leads router — query, export, stats."""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse

from api.dependencies import verify_token
from api.models.models import (
    CityBreakdown,
    Lead,
    LeadStats,
    PaginatedLeads,
    SectorBreakdown,
)
from api.services import pg_service

router = APIRouter(prefix="/api/leads", tags=["leads"])


@router.get("", response_model=PaginatedLeads)
async def list_leads(
    category: str | None = Query(None),
    city: str | None = Query(None),
    has_phone: bool | None = Query(None),
    has_website: bool | None = Query(None),
    min_rating: float | None = Query(None),
    min_reviews: int | None = Query(None),
    sort_by: str = Query("review_count"),
    sort_dir: str = Query("desc"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    filters = {
        "category": category,
        "city": city,
        "has_phone": has_phone,
        "has_website": has_website,
        "min_rating": min_rating,
        "min_reviews": min_reviews,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
    }
    leads, total = await pg_service.query_leads(filters, limit, offset)
    return PaginatedLeads(
        leads=[Lead(**lead) for lead in leads],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/stats", response_model=LeadStats)
async def get_stats(_user: str = Depends(verify_token)):
    stats = await pg_service.get_lead_stats()
    return LeadStats(**stats)


@router.get("/export")
async def export_leads(
    format: str = Query("csv"),
    limit: int = Query(0, ge=0, le=50000),
    category: str | None = Query(None),
    city: str | None = Query(None),
    has_phone: bool | None = Query(None),
    has_website: bool | None = Query(None),
    min_rating: float | None = Query(None),
    min_reviews: int | None = Query(None),
    _user: str = Depends(verify_token),
):
    filters = {
        "category": category,
        "city": city,
        "has_phone": has_phone,
        "has_website": has_website,
        "min_rating": min_rating,
        "min_reviews": min_reviews,
    }
    csv_content = await pg_service.export_leads_csv(filters, limit)
    return PlainTextResponse(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=infinitecrawler_leads.csv"},
    )


@router.get("/{lead_id}", response_model=Lead)
async def get_lead(
    lead_id: int,
    _user: str = Depends(verify_token),
):
    lead = await pg_service.get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return Lead(**lead)


@router.get("/breakdown/cities", response_model=list[CityBreakdown])
async def cities_breakdown(_user: str = Depends(verify_token)):
    data = await pg_service.get_leads_by_city()
    return [CityBreakdown(**d) for d in data]


@router.get("/breakdown/sectors", response_model=list[SectorBreakdown])
async def sectors_breakdown(_user: str = Depends(verify_token)):
    data = await pg_service.get_leads_by_sector()
    return [SectorBreakdown(**d) for d in data]