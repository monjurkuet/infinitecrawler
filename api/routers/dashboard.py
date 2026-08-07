"""Dashboard router — overview, throughput, activity, coverage, health."""

import time

from fastapi import APIRouter, Depends, Query

from api.dependencies import verify_token
from api.models.models import (
    CoverageResponse,
    DaemonUnit,
    Overview,
    RecentActivity,
    ThroughputResponse,
)
from api.services import pg_service, redis_service

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

_start_time = time.time()

_DAEMON_ALLOWLIST = {
    "infinitecrawler-listing.service",
    "infinitecrawler-search.service",
}


@router.get("/overview", response_model=Overview)
async def overview(_user: str = Depends(verify_token)):
    data = await pg_service.get_dashboard_overview()
    return Overview(**data)


@router.get("/throughput", response_model=ThroughputResponse)
async def throughput(
    period: str = Query("24h", pattern=r"^\d+h$"),
    _user: str = Depends(verify_token),
):
    hours = int(period.rstrip("h"))
    data = await pg_service.get_throughput(period_hours=hours)
    return ThroughputResponse(**data)


@router.get("/recent-activity", response_model=RecentActivity)
async def recent_activity(_user: str = Depends(verify_token)):
    data = await pg_service.get_recent_activity()
    return RecentActivity(**data)


@router.get("/coverage", response_model=CoverageResponse)
async def coverage(
    dimension: str = Query("sector", pattern="^(sector|city)$"),
    _user: str = Depends(verify_token),
):
    data = await pg_service.get_coverage(dimension=dimension)
    return CoverageResponse(**data)


@router.get("/health")
async def health(_user: str = Depends(verify_token)):
    pg_status = await pg_service.check_health()
    redis_status = await redis_service.check_health()

    import shutil
    usage = shutil.disk_usage("/")

    daemon_statuses = []
    import subprocess
    for unit in sorted(_DAEMON_ALLOWLIST):
        try:
            r = subprocess.run(
                ["systemctl", "--user", "show", unit],
                capture_output=True, text=True, timeout=5,
            )
            # Parse key=value output — no --value because systemctl sorts props alphabetically
            props = {}
            for line in r.stdout.strip().split("\n"):
                if "=" in line:
                    k, v = line.split("=", 1)
                    props[k] = v
            active = props.get("ActiveState", "unknown")
            sub_state = props.get("SubState", "unknown")
            pid = None
            try:
                v = props.get("MainPID", "0")
                pid = int(v) if v and v != "0" else None
            except ValueError:
                pid = None

            import psutil
            mem = None
            uptime_s = None
            if pid:
                try:
                    proc = psutil.Process(pid)
                    mem = round(proc.memory_info().rss / (1024 ** 2), 2)
                    uptime_s = time.time() - proc.create_time()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            daemon_statuses.append(DaemonUnit(
                unit=unit.removesuffix(".service"),
                active=active,
                sub=sub_state,
                pid=pid,
                uptime_seconds=uptime_s,
                memory_mb=mem,
            ))
        except Exception:
            daemon_statuses.append(DaemonUnit(
                unit=unit.removesuffix(".service"),
                active="unknown",
                sub="unknown",
            ))

    queues = await redis_service.get_all_queue_stats()
    queue_sizes = []
    fail_count = 0
    for q in queues:
        queue_sizes.append({"key": q["key"], "pending": q["pending"]})
        fail_count += q["failed"]

    total_pending = sum(q["pending"] for q in queues)
    failure_rate = round(fail_count / (total_pending + fail_count) * 100, 1) if (total_pending + fail_count) > 0 else 0.0
    tput = await pg_service.get_total_throughput_24h()

    return {
        "api_uptime_seconds": time.time() - _start_time,
        "postgres": pg_status,
        "redis": redis_status,
        "disk_free_gb": round(usage.free / (1024 ** 3), 2),
        "disk_total_gb": round(usage.total / (1024 ** 3), 2),
        "daemon_statuses": daemon_statuses,
        "queue_sizes": queue_sizes,
        "failure_rate_pct": failure_rate,
        "throughput_24h": tput,
    }