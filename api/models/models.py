"""Pydantic models for the infinitecrawler API."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

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


class TaskCreateSearch(BaseModel):
    query: Optional[str] = None
    config: str = "config/gmaps_bd_business_search.yaml"
    headless: bool = True


class TaskCreateCrawl(BaseModel):
    urls: Optional[list[str]] = None
    from_uncrawled: bool = True
    instances: int = 3
    config: str = "config/gmaps_listings_file_input.yaml"


class TaskCreatePipeline(BaseModel):
    generate_only: bool = False
    crawl_only: bool = False
    instances: int = 3


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
    latitude: Optional[float] = None
    longitude: Optional[float] = None
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
    tasks_running: int
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
