"""Monitoring router — crawler process status, queue health, system snapshot."""

import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

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

# ----- daemon detection (hand-launched processes + log files) -----
# Canonical names are full-prefixed (`infinitecrawler-<short>`); the same
# prefix is used for log files in /var/log/infinitecrawler.
_DAEMON_ALLOWLIST = {
    "infinitecrawler-listing",
    "infinitecrawler-search",
    "infinitecrawler-email-extract",
    "infinitecrawler-linkedin-firehose",
    "infinitecrawler-linkedin-search",
    "infinitecrawler-classify",
    "infinitecrawler-linkedin-match",
}

_DAEMON_PGREP = {
    "infinitecrawler-listing": r"daemons\.listing_daemon",
    "infinitecrawler-search": r"daemons\.search_daemon",
    "infinitecrawler-email-extract": r"db_email_extract\.py",
    "infinitecrawler-linkedin-firehose": r"db_linkedin_firehose\.py",
    "infinitecrawler-linkedin-search": r"db_linkedin_search\.py",
    "infinitecrawler-classify": r"db_classify\.py",
    "infinitecrawler-linkedin-match": r"match_linkedin_to_gmaps\.py",
}

# Same commands as scripts/launch_daemons.sh (keep in sync).
_DAEMON_LAUNCH_CMD = {
    "infinitecrawler-listing": "uv run python -m daemons.listing_daemon",
    "infinitecrawler-search": "uv run python -m daemons.search_daemon",
    "infinitecrawler-email-extract": "uv run python scripts/db_email_extract.py --loop --loop-gap 30 --max 2000 --concurrency 25 --burst 3",
    "infinitecrawler-linkedin-firehose": "uv run python scripts/db_linkedin_firehose.py --max-queries 8000 --concurrency 8 --loop --loop-gap 60",
    "infinitecrawler-linkedin-search": "uv run python scripts/db_linkedin_search.py --loop --loop-gap 600 --max 2000",
    "infinitecrawler-classify": "uv run python scripts/db_classify.py --loop --loop-gap 300 --max 2000",
    "infinitecrawler-linkedin-match": "uv run python scripts/match_linkedin_to_gmaps.py --loop --loop-gap 900",
}

_PROJECT_DIR = "/root/codebase/vhd/infinitecrawler"
_DAEMON_LOGFILE = {
    unit: f"/var/log/infinitecrawler/{unit}.log" for unit in _DAEMON_ALLOWLIST
}


def _listing_pids() -> list[int]:
    """Return listing daemon PIDs via pgrep."""
    try:
        r = subprocess.run(
            ["pgrep", "-f", _DAEMON_PGREP["infinitecrawler-listing"]],
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
            ["pgrep", "-af", _DAEMON_PGREP[unit]],
            capture_output=True, text=True, timeout=5,
        )
        pid = None
        for line in r.stdout.strip().split("\n"):
            first = line.split(None, 1)
            if first and first[0].isdigit():
                pid = int(first[0])
                break

        mem = None
        uptime_s = None
        if pid:
            try:
                proc = psutil.Process(pid)
                mem = round(proc.memory_info().rss / (1024 ** 2), 2)
                uptime_s = time.time() - proc.create_time()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pid = None

        return {
            "unit": unit,
            "active": "active" if pid else "inactive",
            "sub": "running" if pid else "dead",
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
    try:
        lines = Path(_DAEMON_LOGFILE[unit]).read_text(
            errors="replace"
        ).splitlines()
    except OSError:
        return {"unit": unit, "lines": tail, "logs": "", "error": "log file not found"}
    if filter:
        lines = [ln for ln in lines if filter in ln]
    return {"unit": unit, "lines": tail, "logs": "\n".join(lines[-tail:])}


def _system_action(unit: str, action: str) -> dict:
    try:
        pattern = _DAEMON_PGREP[unit]
        if action == "stop":
            r = subprocess.run(
                ["pkill", "-TERM", "-f", pattern],
                capture_output=True, text=True, timeout=10,
            )
            code = r.returncode
            message = (r.stdout.strip() or r.stderr.strip() or
                       ("signal sent" if code == 0 else "no matching process"))
            return {"status": "ok" if code == 0 else "error",
                    "message": message, "pid": None}
        # start / restart: kill first (restart), then spawn via launcher command
        if action == "restart":
            subprocess.run(["pkill", "-TERM", "-f", pattern],
                           capture_output=True, text=True, timeout=10)
        cmd = _DAEMON_LAUNCH_CMD[unit]
        import os
        with open(os.devnull, "w") as devnull:
            subprocess.Popen(
                ["nohup", "bash", "-c", f"cd {_PROJECT_DIR} && exec {cmd}"],
                stdout=devnull, stderr=devnull,
                start_new_session=True, cwd=_PROJECT_DIR,
            )
        return {"status": "ok", "message": f"{action} initiated", "pid": None}
    except KeyError:
        return {"status": "error", "message": f"unknown daemon unit: {unit}", "pid": None}
    except Exception as e:
        return {"status": "error", "message": str(e), "pid": None}


@router.post("/daemons/{unit}/restart")
async def restart_daemon(unit: str, _user: str = Depends(verify_token)):
    if unit not in _DAEMON_ALLOWLIST:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=f"Unknown daemon unit: {unit}")
    result = _system_action(unit, "restart")
    info = _daemon_info(unit)
    result["pid"] = info["pid"]
    return result


@router.post("/daemons/{unit}/stop")
async def stop_daemon(unit: str, _user: str = Depends(verify_token)):
    if unit not in _DAEMON_ALLOWLIST:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=f"Unknown daemon unit: {unit}")
    result = _system_action(unit, "stop")
    return result


@router.post("/daemons/{unit}/start")
async def start_daemon(unit: str, _user: str = Depends(verify_token)):
    if unit not in _DAEMON_ALLOWLIST:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail=f"Unknown daemon unit: {unit}")
    result = _system_action(unit, "start")
    info = _daemon_info(unit)
    result["pid"] = info["pid"]
    return result