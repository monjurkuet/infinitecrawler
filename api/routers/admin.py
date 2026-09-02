"""Admin router — alias routes for the admin SPA under /admin/*.

These simply re-use the service functions of the existing routers so we can
proxy the admin SPA to /admin/* without collision with the public /api/* routes.
Auth is still Bearer with INFINITECRAWLER_API_TOKEN."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Query

from api.dependencies import verify_token
from api.routers import monitor, dashboard

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])


# Mirror /api/monitor & /api/dashboard routes under /admin/* for the SPA.
# Auth strategy stays the same (Bearer INFINITECRAWLER_API_TOKEN) — admins
# can use the same credentials they use for CLI tooling.

router.add_api_route(
    "/overview",
    endpoint=dashboard.overview,
    methods=["GET"],
)
router.add_api_route(
    "/throughput",
    endpoint=dashboard.throughput,
    methods=["GET"],
)
router.add_api_route(
    "/recent-activity",
    endpoint=dashboard.recent_activity,
    methods=["GET"],
)
router.add_api_route(
    "/coverage",
    endpoint=dashboard.coverage,
    methods=["GET"],
)
router.add_api_route(
    "/status",
    endpoint=monitor.system_status,
    methods=["GET"],
)
router.add_api_route(
    "/crawlers",
    endpoint=monitor.crawler_processes,
    methods=["GET"],
)
router.add_api_route(
    "/queue",
    endpoint=monitor.queue_status,
    methods=["GET"],
)
router.add_api_route(
    "/queue/{prefix}/failed",
    endpoint=monitor.failed_items,
    methods=["GET"],
)
router.add_api_route(
    "/daemons",
    endpoint=monitor.list_daemons,
    methods=["GET"],
)
router.add_api_route(
    "/daemons/{unit}/logs",
    endpoint=monitor.daemon_logs,
    methods=["GET"],
)


@router.get("/health")
async def admin_health(_user: str = Depends(verify_token)):
    return {"status": "ok", "scope": "admin"}
