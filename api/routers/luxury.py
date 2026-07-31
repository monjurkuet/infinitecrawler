"""Luxury leads router — targets, contacts, stats, export."""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse

from api.dependencies import verify_token
from api.models.models import (
    LuxuryContact,
    LuxuryStats,
    LuxuryTarget,
    PaginatedLuxuryContacts,
    PaginatedLuxuryTargets,
)
from api.services import pg_service

router = APIRouter(prefix="/api/luxury", tags=["luxury"])


@router.get("/targets", response_model=PaginatedLuxuryTargets)
async def list_targets(
    target_type: str | None = Query(None, description="hotel, bar, social_club, golf_club, event_venue, fine_dining, business_link"),
    tier: str | None = Query(None, description="luxury, premium, elite"),
    city: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    targets, total = await pg_service.query_luxury_targets(
        target_type=target_type, tier=tier, city=city, limit=limit, offset=offset,
    )
    return PaginatedLuxuryTargets(
        targets=[LuxuryTarget(**t) for t in targets],
        total=total, limit=limit, offset=offset,
    )


@router.get("/contacts", response_model=PaginatedLuxuryContacts)
async def list_contacts(
    target_id: int | None = Query(None),
    platform: str | None = Query(None, description="linkedin, facebook, instagram"),
    min_confidence: float | None = Query(None, ge=0, le=1),
    is_employee: bool | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    contacts, total = await pg_service.query_luxury_contacts(
        target_id=target_id, platform=platform,
        min_confidence=min_confidence, is_employee=is_employee,
        limit=limit, offset=offset,
    )
    return PaginatedLuxuryContacts(
        contacts=[LuxuryContact(**c) for c in contacts],
        total=total, limit=limit, offset=offset,
    )


@router.get("/stats", response_model=LuxuryStats)
async def luxury_stats(_user: str = Depends(verify_token)):
    return LuxuryStats(**await pg_service.get_luxury_stats())


@router.get("/export")
async def export_contacts(
    target_type: str | None = Query(None),
    platform: str | None = Query(None),
    _user: str = Depends(verify_token),
):
    contacts, _ = await pg_service.query_luxury_contacts(
        platform=platform, limit=10000, offset=0,
    )
    import csv
    import io

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "full_name", "platform", "profile_url", "profile_title",
        "company_name", "confidence", "target_name", "is_employee", "is_guest",
    ], extrasaction="ignore")
    writer.writeheader()
    for c in contacts:
        writer.writerow(c)
    return PlainTextResponse(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=luxury_contacts.csv"},
    )