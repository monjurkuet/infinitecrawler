"""Monitoring router — crawler process status, queue health, system snapshot."""

import shutil
import subprocess
import time
from datetime import datetime, timezone

import psutil
from fastapi import APIRouter, Depends, Query

from api.dependencies import verify_token
from api.models.models import (
    CrawlerProcess,
    DaemonUnit,
    FailedItem,
    QueueStats,
    SystemStatus,
)
from api.services import pg_service, redis_service

router = APIRouter(prefix="/api", tags=["monitor"])
_start_time = time.time()

# ----- systemd daemon detection (replaces legacy main.py pgrep) -----
_LISTING_UNIT = "infinitecrawler-listing"
_DAEMON_ALLOWLIST = {
    "infinitecrawler-listing",
    "infinitecrawler-search",
}


def _systemd_active(unit: str) -> bool:
    try:
        r = subprocess.run(
            ["systemctl", "--user", "is-active", unit],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() == "active"
    except Exception:
        return False


def _listing_pids() -> list[int]:
    """Return listing daemon PIDs. systemd MainPID when active, else vendor pgrep."""
    if _systemd_active(_LISTING_UNIT):
        try:
            r = subprocess.run(
                ["systemctl", "--user", "show", "-p", "MainPID", "--value", _LISTING_UNIT],
                capture_output=True, text=True, timeout=5,
            )
            pid = r.stdout.strip()
            return [int(pid)] if pid and pid != "0" else []
        except Exception:
            return []
    try:
        r = subprocess.run(
            ["pgrep", "-f", r"daemons\.listing_daemon"],
            capture_output=True, text=True, timeout=5,
        )
        return [int(p) for p in r.stdout.strip().split("\n") if p.strip()]
    except Exception:
        return []


@router.get("/health")
async def health():
    pg_status = await pg_service.check_health()
    redis_status = await redis_service.check_health()
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
    # Crawler processes (systemd daemon) — migrate from legacy pgrep to systemd
    pids = _listing_pids()
    crawlers_running = len(pids)

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
        tasks_running=None,
        uptime_seconds=time.time() - _start_time,
        healthy=len(issues) == 0,
        issues=issues,
    )


@router.get("/crawlers", response_model=list[CrawlerProcess])
async def crawler_processes(_user: str = Depends(verify_token)):
    pids = _listing_pids()

    processes = []
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            cmdline = " ".join(proc.cmdline())
            label = "infinitecrawler-listing"

            processes.append(CrawlerProcess(
                pid=pid,
                command=cmdline[-80:] if len(cmdline) > 80 else cmdline,
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


# ─── Daemon Control ──────────────────────────────────────────────────────────

def _daemon_info(unit: str) -> dict:
    try:
        r = subprocess.run(
            ["systemctl", "--user", "show", unit],
            capture_output=True, text=True, timeout=5,
        )
        # Parse key=value output — systemctl sorts props alphabetically,
        # so positional --value parsing is unreliable
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

        mem = None
        uptime_s = None
        if pid:
            try:
                proc = psutil.Process(pid)
                mem = round(proc.memory_info().rss / (1024 ** 2), 2)
                uptime_s = time.time() - proc.create_time()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        return {
            "unit": unit,
            "active": active,
            "sub": sub_state,
            "pid": pid,
            "uptime_seconds": uptime_s,
            "memory_mb": mem,
        }
    except Exception:
        return {
            "unit": unit,
            "active": "unknown",
            "sub": "unknown",
            "pid": None,
            "uptime_seconds": None,
            "memory_mb": None,
        }


@router.get("/daemons", response_model=list[DaemonUnit])
async def list_daemons(_user: str = Depends(verify_token)):
    result = []
    for unit in sorted(_DAEMON_ALLOWLIST):
        info = _daemon_info(unit)
        result.append(DaemonUnit(**info))
    return result


@router.get("/daemons/{unit}/logs")
async def daemon_logs(
    unit: str,
    tail: int = Query(100, ge=1, le=1000),
    filter: str | None = Query(None),
    _user: str = Depends(verify_token),
):
    if unit not in _DAEMON_ALLOWLIST:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=f"Unknown daemon unit: {unit}")
    cmd = ["journalctl", "--user", "-u", unit, "--no-pager", "-n", str(tail)]
    if filter:
        cmd.extend(["--grep", filter])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except subprocess.TimeoutExpired:
        return {"logs": "", "error": "journalctl timed out"}
    return {"unit": unit, "lines": tail, "logs": r.stdout.strip()}


def _systemctl_action(unit: str, action: str) -> dict:
    try:
        r = subprocess.run(
            ["systemctl", "--user", action, unit],
            capture_output=True, text=True, timeout=10,
        )
        success = r.returncode == 0
        return {"status": "ok" if success else "error", "message": r.stdout.strip() or r.stderr.strip(), "pid": None}
    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "systemctl timed out", "pid": None}
    except Exception as e:
        return {"status": "error", "message": str(e), "pid": None}


@router.post("/daemons/{unit}/restart")
async def restart_daemon(unit: str, _user: str = Depends(verify_token)):
    if unit not in _DAEMON_ALLOWLIST:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=f"Unknown daemon unit: {unit}")
    result = _systemctl_action(unit, "restart")
    info = _daemon_info(unit)
    result["pid"] = info["pid"]
    return result


@router.post("/daemons/{unit}/stop")
async def stop_daemon(unit: str, _user: str = Depends(verify_token)):
    if unit not in _DAEMON_ALLOWLIST:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=f"Unknown daemon unit: {unit}")
    result = _systemctl_action(unit, "stop")
    return result


@router.post("/daemons/{unit}/start")
async def start_daemon(unit: str, _user: str = Depends(verify_token)):
    if unit not in _DAEMON_ALLOWLIST:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=f"Unknown daemon unit: {unit}")
    result = _systemctl_action(unit, "start")
    info = _daemon_info(unit)
    result["pid"] = info["pid"]
    return result