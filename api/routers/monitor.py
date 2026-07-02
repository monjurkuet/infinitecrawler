"""Monitoring router — crawler process status, queue health, system snapshot."""

import subprocess
import time
from datetime import datetime, timezone

import psutil
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import verify_token
from api.models.models import (
    CrawlerProcess,
    FailedItem,
    QueueStats,
    SystemStatus,
)
from api.services import pg_service, redis_service, task_runner

router = APIRouter(prefix="/api", tags=["monitor"])
_start_time = time.time()


@router.get("/health")
async def health():
    pg_status = await pg_service.check_health()
    redis_status = await redis_service.check_health()
    import shutil
    usage = shutil.disk_usage("/")
    
    return {
        "status": "ok" if pg_status == "ok" and redis_status == "ok" else "degraded",
        "postgres": pg_status,
        "redis": redis_status,
        "disk_free_gb": round(usage.free / (1024**3), 2),
        "disk_total_gb": round(usage.total / (1024**3), 2),
        "uptime_seconds": time.time() - _start_time,
    }


@router.get("/status", response_model=SystemStatus)
async def system_status(_user: str = Depends(verify_token)):
    # Crawler processes
    try:
        result = subprocess.run(
            ["pgrep", "-cf", r"main\.py.*listing"],
            capture_output=True, text=True, timeout=5
        )
        crawlers_running = int(result.stdout.strip() or 0)
    except Exception:
        crawlers_running = 0

    try:
        pid_result = subprocess.run(
            ["pgrep", "-f", r"main\.py.*listing"],
            capture_output=True, text=True, timeout=5
        )
        pids = [int(p) for p in pid_result.stdout.strip().split("\n") if p]
    except Exception:
        pids = []

    # Queue stats
    queues = await redis_service.get_all_queue_stats()
    queue_models = [QueueStats(**q) for q in queues]

    # DB snapshot
    stats = await pg_service.get_lead_stats()
    uncrawled = await pg_service.get_uncrawled_count()
    db_snapshot = {
        "total_listings": stats["total"],
        "total_search_results": (await pg_service.get_search_result_stats())["total"],
        "listings_with_phone": stats["with_phone"],
        "uncrawled_urls": uncrawled,
    }

    # Tasks running
    running_tasks = len(task_runner.get_all_tasks(status="running"))

    # Issues
    issues = []
    if crawlers_running == 0 and uncrawled > 0:
        issues.append("No crawlers running but uncrawled URLs exist")
    for q in queues:
        if q["failed"] > 10:
            issues.append(f"High failure count in {q['key']}: {q['failed']}")

    return SystemStatus(
        crawlers_running=crawlers_running,
        crawler_pids=pids,
        queues=queue_models,
        database=db_snapshot,
        last_pipeline_run=None,
        tasks_running=running_tasks,
        uptime_seconds=time.time() - _start_time,
        healthy=len(issues) == 0,
        issues=issues,
    )


@router.get("/crawlers", response_model=list[CrawlerProcess])
async def crawler_processes(_user: str = Depends(verify_token)):
    try:
        pid_result = subprocess.run(
            ["pgrep", "-f", r"main\.py.*listing"],
            capture_output=True, text=True, timeout=5
        )
        pids = [int(p) for p in pid_result.stdout.strip().split("\n") if p]
    except Exception:
        pids = []

    processes = []
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            cmdline = " ".join(proc.cmdline())
            label = "unknown"
            for part in proc.cmdline():
                if "instance-label" in part or "listing" in part:
                    label = part

            # Extract instance-label value
            cmd_parts = proc.cmdline()
            for i, part in enumerate(cmd_parts):
                if part == "--instance-label" and i + 1 < len(cmd_parts):
                    label = cmd_parts[i + 1]
                    break

            processes.append(CrawlerProcess(
                pid=pid,
                command=" ".join(cmd_parts[-3:]) if len(cmd_parts) > 3 else " ".join(cmd_parts),
                start_time=datetime.fromtimestamp(proc.create_time(), tz=timezone.utc).isoformat(),
                memory_mb=round(proc.memory_info().rss / (1024**2), 2),
                instance_label=label,
                uptime_seconds=time.time() - proc.create_time(),
            ))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return processes


@router.get("/queue", response_model=list[QueueStats])
async def queue_status(
    prefix: str | None = Query(None),
    _user: str = Depends(verify_token),
):
    if prefix:
        stats = [await redis_service.get_queue_stats(prefix)]
    else:
        stats = await redis_service.get_all_queue_stats()
    return [QueueStats(**s) for s in stats]


@router.get("/queue/{prefix}/failed", response_model=list[FailedItem])
async def failed_items(
    prefix: str,
    _user: str = Depends(verify_token),
):
    items = await redis_service.get_failed_items(prefix)
    return [FailedItem(**i) for i in items]


@router.post("/queue/{prefix}/requeue-stalled")
async def requeue_stalled(
    prefix: str,
    _user: str = Depends(verify_token),
):
    count = await redis_service.requeue_stalled(prefix)
    return {"prefix": prefix, "requeued": count}


@router.delete("/queue/{prefix}/failed")
async def clear_failed(
    prefix: str,
    _user: str = Depends(verify_token),
):
    await redis_service.clear_queue(prefix, "failed")
    return {"prefix": prefix, "status": "cleared"}