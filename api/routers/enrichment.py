"""Enrichment router — emails, LinkedIn profiles, classification data."""

from fastapi import APIRouter, Depends, Query

from api.dependencies import verify_token
from api.models.models import (
    ClassificationStats,
    EmailRecord,
    EmailStats,
    LinkedInProfile,
    LinkedInStats,
    PaginatedEmails,
    PaginatedLinkedIn,
    PaginatedUnclassified,
    UnclassifiedLead,
)
from api.services import pg_service

router = APIRouter(prefix="/api/enrichment", tags=["enrichment"])

# ─── Emails ──────────────────────────────────────────────────────────────────

@router.get("/emails", response_model=PaginatedEmails)
async def list_emails(
    listing_id: int | None = Query(None),
    source_type: str | None = Query(None),
    is_obfuscated: bool | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    emails, total = await pg_service.query_emails(
        listing_id=listing_id,
        source_type=source_type,
        is_obfuscated=is_obfuscated,
        limit=limit,
        offset=offset,
    )
    return PaginatedEmails(emails=[EmailRecord(**e) for e in emails], total=total, limit=limit, offset=offset)


@router.get("/emails/stats", response_model=EmailStats)
async def email_stats(_user: str = Depends(verify_token)):
    stats = await pg_service.get_email_stats()
    return EmailStats(**stats)


@router.get("/emails/listing/{listing_id}")
async def emails_by_listing(
    listing_id: int,
    _user: str = Depends(verify_token),
):
    emails = await pg_service.get_emails_by_listing(listing_id)
    return emails


# ─── LinkedIn ────────────────────────────────────────────────────────────────

@router.get("/linkedin", response_model=PaginatedLinkedIn)
async def list_linkedin(
    sector_id: str | None = Query(None),
    min_confidence: float | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    profiles, total = await pg_service.query_linkedin_profiles(
        sector_id=sector_id,
        min_confidence=min_confidence,
        limit=limit,
        offset=offset,
    )
    return PaginatedLinkedIn(profiles=[LinkedInProfile(**p) for p in profiles], total=total, limit=limit, offset=offset)


@router.get("/linkedin/stats", response_model=LinkedInStats)
async def linkedin_stats(_user: str = Depends(verify_token)):
    stats = await pg_service.get_linkedin_stats()
    return LinkedInStats(**stats)


@router.get("/linkedin/listing/{listing_id}")
async def linkedin_by_listing(
    listing_id: int,
    _user: str = Depends(verify_token),
):
    profiles = await pg_service.get_linkedin_by_listing(listing_id)
    return profiles


# ─── Classification ──────────────────────────────────────────────────────────

@router.get("/classification/stats", response_model=ClassificationStats)
async def classification_stats(_user: str = Depends(verify_token)):
    stats = await pg_service.get_classification_stats()
    return ClassificationStats(**stats)


@router.get("/classification/unclassified", response_model=PaginatedUnclassified)
async def unclassified_leads(
    min_reviews: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    leads, total = await pg_service.query_unclassified(
        min_reviews=min_reviews,
        limit=limit,
        offset=offset,
    )
    return PaginatedUnclassified(leads=[UnclassifiedLead(**lead) for lead in leads], total=total, limit=limit, offset=offset)