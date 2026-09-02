"""Pydantic models for the infinitecrawler API."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


# ─── Auth ───────────────────────────────────────────────────────────────────

class TokenPayload(BaseModel):
    """Decoded token payload."""
    sub: str = "api"


# ─── Tasks ──────────────────────────────────────────────────────────────────

class TaskType(str, Enum):
    search = "search"
    crawl = "crawl"
    pipeline = "pipeline"


class TaskStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class ScraperTask(BaseModel):
    """Persistent scraper task record (stored in PG)."""
    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    type: TaskType
    status: TaskStatus = TaskStatus.pending
    config_path: Optional[str] = None
    query: Optional[str] = None
    instance_count: int = 1
    pid: Optional[int] = None
    exit_code: Optional[int] = None
    logs_tail: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class TaskListResponse(BaseModel):
    tasks: list[ScraperTask]
    total: int


# ─── Leads ──────────────────────────────────────────────────────────────────

class Lead(BaseModel):
    id: int
    place_id: Optional[str] = None
    source_url: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    social_links: Optional[Dict[str, Any]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    sector_id: Optional[str] = None
    classification_confidence: Optional[float] = None
    classification_method: Optional[str] = None
    classified_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class LeadStats(BaseModel):
    total: int
    with_phone: int
    with_website: int
    with_both: int
    avg_rating: Optional[float] = None
    total_cities: int
    total_categories: int
    top_categories: list[dict[str, Any]] = Field(default_factory=list)
    top_cities: list[dict[str, Any]] = Field(default_factory=list)


class LeadFilter(BaseModel):
    category: Optional[str] = None
    city: Optional[str] = None
    has_phone: Optional[bool] = None
    has_website: Optional[bool] = None
    min_rating: Optional[float] = None
    min_reviews: Optional[int] = None
    sort_by: str = "review_count"
    sort_dir: str = "desc"
    limit: int = 50
    offset: int = 0


class LeadExportFilter(LeadFilter):
    format: str = "csv"  # csv or json


class PaginatedLeads(BaseModel):
    leads: list[Lead]
    total: int
    limit: int
    offset: int


class CityBreakdown(BaseModel):
    city: str
    count: int


class SectorBreakdown(BaseModel):
    sector: str
    display_name: str
    count: int
    leads: list[Lead] = Field(default_factory=list)


# ─── Search Results ─────────────────────────────────────────────────────────

class SearchResult(BaseModel):
    id: int
    key_value: Optional[str] = None
    source_type: Optional[str] = None
    payload: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PaginatedSearchResults(BaseModel):
    results: list[SearchResult]
    total: int
    limit: int
    offset: int


class SearchResultStats(BaseModel):
    total: int
    by_source_type: dict[str, int] = Field(default_factory=dict)
    recent_24h: int = 0


# ─── Queue / Monitoring ─────────────────────────────────────────────────────

class QueueStats(BaseModel):
    key: str
    pending: int
    processing: int
    completed: int
    failed: int


class FailedItem(BaseModel):
    url: str
    error: str
    retries: int
    failed_at: str


class CrawlerProcess(BaseModel):
    pid: int
    command: str
    start_time: str
    memory_mb: float
    instance_label: str = "unknown"
    uptime_seconds: float


class SystemStatus(BaseModel):
    crawlers_running: int
    crawler_pids: list[int]
    queues: list[QueueStats]
    database: dict[str, Any]
    last_pipeline_run: Optional[str] = None
    tasks_running: Optional[int] = None
    uptime_seconds: float
    healthy: bool
    issues: list[str]


# ─── Health ─────────────────────────────────────────────────────────────────

class HealthCheck(BaseModel):
    status: str  # ok | degraded | down
    postgres: str
    redis: str
    disk_free_gb: float
    disk_total_gb: float
    uptime_seconds: float


# ─── Pipeline ───────────────────────────────────────────────────────────────

class PipelineRun(BaseModel):
    id: str
    phase: str
    status: str
    started_at: str
    completed_at: Optional[str] = None
    output: str = ""


# ─── Enrichment — Emails ─────────────────────────────────────────────────────

class EmailRecord(BaseModel):
    id: int
    listing_id: Optional[int] = None
    website_url: Optional[str] = None
    email: str
    email_type: Optional[str] = None
    extraction_method: Optional[str] = None
    is_obfuscated: bool = False
    context_snippet: Optional[str] = None
    discovered_at: Optional[str] = None
    last_verified: Optional[str] = None
    is_active: bool = True


class PaginatedEmails(BaseModel):
    emails: list[EmailRecord]
    total: int
    limit: int
    offset: int


class EmailStats(BaseModel):
    total_emails: int
    unique_listings: int
    by_source_type: dict[str, int] = Field(default_factory=dict)
    by_extraction_method: dict[str, int] = Field(default_factory=dict)
    obfuscated_count: int = 0
    obfuscation_rate: float = 0.0


# ─── Enrichment — LinkedIn ──────────────────────────────────────────────────

class LinkedInProfile(BaseModel):
    id: int
    listing_id: Optional[int] = None
    full_name: Optional[str] = None
    profile_url: Optional[str] = None
    profile_title: Optional[str] = None
    company_name: Optional[str] = None
    search_query: Optional[str] = None
    confidence: Optional[float] = None
    snippet: Optional[str] = None
    checked_at: Optional[str] = None
    last_updated: Optional[str] = None
    notes: Optional[str] = None


class PaginatedLinkedIn(BaseModel):
    profiles: list[LinkedInProfile]
    total: int
    limit: int
    offset: int


class LinkedInStats(BaseModel):
    total_profiles: int
    unique_listings: int
    by_company: list[dict[str, Any]] = Field(default_factory=list)
    avg_confidence: Optional[float] = None
    matched_count: int = 0
    unmatched_count: int = 0


# ─── Enrichment — Classification ─────────────────────────────────────────────

class UnclassifiedLead(BaseModel):
    id: int
    name: Optional[str] = None
    category: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    classification_method: Optional[str] = None
    classified_at: Optional[str] = None


class ClassificationStats(BaseModel):
    total_listings: int
    classified_count: int
    unclassified_count: int
    by_method: dict[str, int] = Field(default_factory=dict)
    confidence_distribution: list[dict[str, Any]] = Field(default_factory=list)
    classified_by_date: list[dict[str, Any]] = Field(default_factory=list)


class PaginatedUnclassified(BaseModel):
    leads: list[UnclassifiedLead]
    total: int
    limit: int
    offset: int


# ─── Daemon ─────────────────────────────────────────────────────────────────

class DaemonUnit(BaseModel):
    unit: str
    active: str  # active | inactive | failed
    sub: str
    pid: Optional[int] = None
    uptime_seconds: Optional[float] = None
    memory_mb: Optional[float] = None

    # Admin SPA fields (same data, friendlier names)
    active_state: Optional[str] = None
    sub_state: Optional[str] = None
    description: Optional[str] = None
    n_restarts: Optional[int] = None
    memory_current: Optional[int] = None
    main_pid: Optional[int] = None
    last_state_change: Optional[str] = None


# ─── Dashboard ───────────────────────────────────────────────────────────────

class Overview(BaseModel):
    total_listings: int
    total_search_results: int
    uncrawled_urls: int
    emails_extracted: int
    linkedin_profiles: int
    classified_pct: float
    last_listing_activity: Optional[str] = None
    last_search_activity: Optional[str] = None


class ActivityBucket(BaseModel):
    time: str
    search_results: int = 0
    listings_extracted: int = 0
    listings_classified: int = 0


class ThroughputResponse(BaseModel):
    period: str
    buckets: list[ActivityBucket]


class RecentActivity(BaseModel):
    recent_listings: list[dict[str, Any]]
    recent_search_results: list[dict[str, Any]]
    recent_emails: list[dict[str, Any]]


class CoverageRow(BaseModel):
    key: str
    total: int
    classified: int
    with_phone: int
    with_email: int


class CoverageResponse(BaseModel):
    dimension: str
    rows: list[CoverageRow]


class DashboardHealth(BaseModel):
    api_uptime_seconds: float
    postgres: str
    redis: str
    disk_free_gb: float
    disk_total_gb: float
    daemon_statuses: list[DaemonUnit]
    queue_sizes: list[dict[str, int]]
    failure_rate_pct: float
    throughput_24h: int


# ─── Pipeline Orchestration ──────────────────────────────────────────────────

class PipelineTaskType(str, Enum):
    search = "search"
    crawl = "crawl"
    email_extract = "email_extract"
    linkedin_search = "linkedin_search"
    classification = "classification"


class PipelineTaskBase(BaseModel):
    type: PipelineTaskType
    status: TaskStatus = TaskStatus.pending
    query: Optional[str] = None
    config_path: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PipelineTaskCreate(PipelineTaskBase):
    pass


class PipelineTaskResponse(BaseModel):
    id: str
    type: str
    status: str
    query: Optional[str] = None
    config_path: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class PaginatedPipelineTasks(BaseModel):
    tasks: list[PipelineTaskResponse]
    total: int
    limit: int
    offset: int


# ─── Luxury Leads ────────────────────────────────────────────────────────────

class LuxuryTarget(BaseModel):
    id: int
    name: str
    alternative_names: Optional[list[str]] = None
    address: Optional[str] = None
    city: Optional[str] = None
    target_type: str
    tier: str
    linkedin_searched: bool = False
    facebook_searched: bool = False
    contact_count: int = 0
    created_at: Optional[str] = None

class LuxuryContact(BaseModel):
    id: int
    target_id: int
    target_name: Optional[str] = None
    full_name: Optional[str] = None
    platform: str
    profile_url: str
    profile_title: Optional[str] = None
    company_name: Optional[str] = None
    confidence: float = 0.3
    snippet: Optional[str] = None
    is_employee: bool = False
    is_guest: bool = True
    discovered_at: Optional[str] = None

class PaginatedLuxuryTargets(BaseModel):
    targets: list[LuxuryTarget]
    total: int
    limit: int
    offset: int

class PaginatedLuxuryContacts(BaseModel):
    contacts: list[LuxuryContact]
    total: int
    limit: int
    offset: int

class LuxuryStats(BaseModel):
    total_targets: int
    total_contacts: int
    targets_with_contacts: int
    by_type: list[dict[str, Any]]
    by_platform: list[dict[str, Any]]
    avg_confidence: float
    pending_targets: int
