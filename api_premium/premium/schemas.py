"""Pydantic models for premium dashboard responses."""
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class LeadListItem(BaseModel):
    # Core identifiers
    id: int
    place_id: str | None
    source_url: str | None
    source_type: str
    # Contact-rich fields
    name: str | None
    category: str | None
    rating: float | None
    review_count: int | None
    address: str | None
    phone: str | None
    website: str | None
    booking_url: str | None
    plus_code: str | None
    is_claimed: bool | None
    latitude: float | None
    longitude: float | None
    sector_id: str | None
    classification_confidence: float | None
    classification_method: str | None
    created_at: datetime | None
    updated_at: datetime | None
    email_scanned_at: datetime | None
    # Contact enrichment (aggregated — comma-joined for list view)
    emails: list[str] = []
    linkedin_url: str | None = None
    linkedin_title: str | None = None


class LeadListResponse(BaseModel):
    total: int
    page: int
    size: int
    items: list[LeadListItem]


class LeadDetailResponse(LeadListItem):
    # Full row passthrough + extra payload
    payload: dict[str, Any] | None = None
    social_links: dict[str, Any] | None = None
    emails_full: list[dict[str, Any]] = []
    linkedin_profiles: list[dict[str, Any]] = []


class StatsOut(BaseModel):
    user: dict[str, Any]
    total_listings: int
    total_emails: int
    total_linkedin: int
    rows_exported: int
    searches_run: int
    rows_limit: int | None = None


class BbbLeadItem(BaseModel):
    id: int
    business_id: str | None = None
    business_name: str | None = None
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    phone: str | None = None
    rating: str | None = None
    accredited: bool | None = None
    profile_url: str | None = None
    email: str | None = None
    website: str | None = None
    years_in_business: str | None = None
    social_links: Any | None = None
    source_query: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class BbbLeadListResponse(BaseModel):
    total: int
    page: int
    size: int
    items: list[BbbLeadItem]
