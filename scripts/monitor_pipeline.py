#!/usr/bin/env python3
"""monitor_pipeline.py — Auto-monitor + self-heal the BD lead pipeline.

Checks:
  1. Listing crawler process health (are workers running?)
  2. Redis queue health (pending/processing/completed)
  3. Database growth rate (are new listings being added?)
  4. Stale processing items (items stuck in "processing")
  5. Uncrawled URL count

Actions:
  - Reports status (stdout JSON for cron no_agent mode)
  - Auto-restarts crawlers if dead and work remaining
  - Clears stuck processing items

Usage:
    uv run python scripts/monitor_pipeline.py
    uv run python scripts/monitor_pipeline.py --restart    # auto-restart dead crawlers
    uv run python scripts/monitor_pipeline.py --json       # machine-readable
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import time

from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("monitor_pipeline")

REPO_ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_uncrawled_count_sql, PG_DEFAULT_HOST, PG_DEFAULT_PASSWORD, PG_DEFAULT_DB


def redis_cmd(cmd: str) -> str:
    """Run a redis-cli command, return stripped output."""
    try:
        result = subprocess.run(
            ["redis-cli"] + cmd.split(),
            capture_output=True, text=True, timeout=10
        )
        return result.stdout.strip()
    except Exception as e:
        log.warning(f"Redis command failed: {cmd} — {e}")
        return "0"


def pg_query(sql: str) -> str:
    """Run a PostgreSQL query, return stripped output."""
    try:
        env = os.environ.copy()
        env["PGPASSWORD"] = PG_DEFAULT_PASSWORD
        env["PGCONNECT_TIMEOUT"] = "10"
        result = subprocess.run(
            ["psql", "-h", PG_DEFAULT_HOST, "-U", "postgres", "-d", PG_DEFAULT_DB,
             "-t", "-A", "-c", sql],
            capture_output=True, text=True, timeout=60, env=env
        )
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log.warning(f"PG query timed out after 60s: {sql}")
        return "error"
    except Exception as e:
        log.warning(f"PG query failed: {sql} — {e}")
        return "error"


def _systemd_daemon_active(unit: str) -> bool:
    """Check if a systemd user unit is active."""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", unit],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() == "active"
    except Exception:
        return False


def count_listing_processes() -> int:
    """Count running listing daemon processes (systemd first, pgrep fallback)."""
    pids = get_crawler_pids()
    if pids:
        return len(pids)
    # Fallback: pgrep for daemons.listing_daemon when not running under systemd
    try:
        result = subprocess.run(
            ["pgrep", "-c", "-f", r"daemons\.listing_daemon"],
            capture_output=True, text=True, timeout=5,
        )
        return int(result.stdout.strip() or 0)
    except Exception:
        return 0


def get_crawler_pids() -> list[str]:
    """Return the listing daemon PID(s) via systemd, or legacy pgrep fallback.

    The 24/7 daemons run as ``uv run python -m daemons.listing_daemon`` under
    systemd, so there is exactly one process when active. We surface its MainPID
    when available.
    """
    if not _systemd_daemon_active("infinitecrawler-listing"):
        return []
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", "-p", "MainPID", "--value",
             "infinitecrawler-listing"],
            capture_output=True, text=True, timeout=5,
        )
        pid = result.stdout.strip()
        return [pid] if pid and pid != "0" else []
    except Exception:
        return []


def clear_stale_processing():
    """Move items stuck in processing back to pending."""
    processing = redis_cmd("LLEN gmaps:processing")
    # Items in processing for >30 min are stuck (queue TTL is usually minutes)
    if int(processing or 0) > 0:
        # Check if any crawlers are actually running
        procs = count_listing_processes()
        if procs == 0:
            # No crawlers running — all processing items are stale
            count = redis_cmd("RPOPLPUSH gmaps:processing gmaps:pending")
            moved = 0
            while count:
                moved += 1
                count = redis_cmd("RPOPLPUSH gmaps:processing gmaps:pending")
                if moved > 50:
                    break
            log.info(f"Moved {moved} stale processing items back to pending")
            return moved
    return 0


def kill_orphan_chrome():
    """Kill orphaned Chrome processes (no matching Python crawler parent)."""
    try:
        # Get Chrome PIDs that have a matching user-data-dir pattern
        chrome_procs = subprocess.run(
            ["pgrep", "-f", r"chrome.*remote-debugging-port"],
            capture_output=True, text=True, timeout=5,
        )
        chrome_pids = [p.strip() for p in chrome_procs.stdout.strip().split("\n") if p.strip()]

        # Get Python daemon PIDs (systemd MainPID) — falls back to pgrep for non-systemd runs
        py_pids = set(get_crawler_pids())
        if not py_pids:
            legacy = subprocess.run(
                ["pgrep", "-f", r"daemons\.listing_daemon"],
                capture_output=True, text=True, timeout=5,
            )
            py_pids = set(p.strip() for p in legacy.stdout.strip().split("\n") if p.strip())

        if not chrome_pids:
            return

        # Kill Chrome processes that are NOT parents of running crawlers
        killed = 0
        for pid in chrome_pids:
            # Check if this Chrome has a Python parent (PPID chain)
            try:
                ppid = int(subprocess.run(
                    ["ps", "-o", "ppid=", "-p", pid],
                    capture_output=True, text=True, timeout=3,
                ).stdout.strip())
                # Walk up the PPID chain looking for a known crawler PID
                is_crawler_child = False
                visited = set()
                while ppid > 1 and ppid not in visited:
                    visited.add(ppid)
                    if str(ppid) in py_pids:
                        is_crawler_child = True
                        break
                    ppid = int(subprocess.run(
                        ["ps", "-o", "ppid=", "-p", str(ppid)],
                        capture_output=True, text=True, timeout=3,
                    ).stdout.strip())
                if not is_crawler_child:
                    subprocess.run(["kill", str(pid)], capture_output=True, timeout=3)
                    killed += 1
            except (ValueError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
                subprocess.run(["kill", str(pid)], capture_output=True, timeout=3)
                killed += 1

        if killed > 0:
            log.info(f"Killed {killed} orphaned Chrome processes")
            # Clean up orphan temp dirs
            for d in Path("/tmp").glob("uc_*"):
                try:
                    shutil.rmtree(d)
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"Orphan cleanup failed: {e}")


def restart_crawlers():
    """Restart listing daemon via systemd (replaces legacy launch script).

    The 24/7 daemons are managed by systemd; launching the old
    ``launch_listing_crawlers.sh`` script would race the systemd daemons on the
    Redis queues and PG writes. ``systemctl --user restart`` is the only safe
    way.
    """
    log.info("Restarting listing daemon via systemd...")
    try:
        lock_dir = "/tmp/listing-crawler.lock"
        subprocess.run(["rm", "-rf", lock_dir], capture_output=True, timeout=5)
        kill_orphan_chrome()

        unit = "infinitecrawler-listing"
        # Clear any failed state — restart won't start a failed unit
        subprocess.run(
            ["systemctl", "--user", "reset-failed", unit],
            capture_output=True, text=True, timeout=10,
        )

        result = subprocess.run(
            ["systemctl", "--user", "restart", unit],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            log.error("Listing daemon restart failed: %s", result.stderr[-500:])
            return False

        # Post-restart health check — wait 3s then verify MainPID
        time.sleep(3)
        health = subprocess.run(
            ["systemctl", "--user", "show", "-p", "MainPID", "--value", unit],
            capture_output=True, text=True, timeout=10,
        )
        pid = health.stdout.strip()
        if pid and pid != "0":
            log.info("Listing daemon restarted successfully (PID %s)", pid)
            return True
        log.error("Listing daemon not running after restart — MainPID=%s", pid or "missing")
        return False
    except Exception as e:
        log.error("Restart failed: %s", e)
        return False


def run_checks(restart: bool = False) -> dict:
    """Run all health checks. Returns status dict."""
    now = datetime.now(timezone.utc).isoformat()

    # 1. Process health
    procs = count_listing_processes()
    pids = get_crawler_pids()

    # 2. Redis queue
    # Listing queue (gmaps:*)
    pending = int(redis_cmd("LLEN gmaps:pending") or 0)
    processing = int(redis_cmd("LLEN gmaps:processing") or 0)
    completed = int(redis_cmd("SCARD gmaps:completed") or 0)
    # failed is a HASH (HSET per-url), use HLEN not LLEN
    failed = int(redis_cmd("HLEN gmaps:failed") or 0)

    # Search queue (gmaps_bd_business:*)
    search_pending = int(redis_cmd("LLEN gmaps_bd_business:pending") or 0)
    search_processing = int(redis_cmd("LLEN gmaps_bd_business:processing") or 0)
    search_completed = int(redis_cmd("SCARD gmaps_bd_business:completed") or 0)
    # failed is a HASH, use HLEN
    search_failed = int(redis_cmd("HLEN gmaps_bd_business:failed") or 0)

    # 3. DB counts
    total_listings = pg_query("SELECT COUNT(*) FROM scraper.gmaps_listings")
    total_search = pg_query("SELECT COUNT(*) FROM scraper.gmaps_search_results")
    listings_with_phone = pg_query(
        "SELECT COUNT(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL"
    )

    # 4. Uncrawled count
    uncrawled = pg_query(get_uncrawled_count_sql())

    # 5. Lead quality
    leads_with_website = pg_query(
        "SELECT COUNT(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL AND website IS NOT NULL"
    )

    # Determine pipeline status
    is_healthy = True
    issues = []

    if procs == 0 and pending > 0:
        is_healthy = False
        issues.append("No crawlers running but pending URLs exist")
    if processing > 3 and procs == 0:
        is_healthy = False
        issues.append(f"{processing} items stuck in processing with no crawlers")
    if failed > 10:
        issues.append(f"High failure count: {failed}")

    # Auto-heal
    healed = []
    if processing > 0 and procs == 0:
        moved = clear_stale_processing()
        if moved > 0:
            healed.append(f"Moved {moved} stale processing items to pending")

    if restart and procs == 0 and int(uncrawled or 0) > 0:
        success = restart_crawlers()
        if success:
            healed.append("Restarted crawlers for uncrawled URLs")
        else:
            issues.append("Crawler restart failed")

    status = {
        "timestamp": now,
        "healthy": is_healthy,
        "issues": issues,
        "healed": healed,
        "crawlers": {
            "running": procs,
            "pids": pids[:10],
        },
        "redis": {
            "listing": {
                "pending": pending,
                "processing": processing,
                "completed": completed,
                "failed": failed,
            },
            "search": {
                "pending": search_pending,
                "processing": search_processing,
                "completed": search_completed,
                "failed": search_failed,
            },
        },
        "database": {
            "total_listings": int(total_listings) if total_listings != "error" else None,
            "total_search_results": int(total_search) if total_search != "error" else None,
            "listings_with_phone": int(listings_with_phone) if listings_with_phone != "error" else None,
            "leads_with_website": int(leads_with_website) if leads_with_website != "error" else None,
            "uncrawled_urls": int(uncrawled) if uncrawled != "error" else None,
        },
    }

    return status


def main():
    parser = argparse.ArgumentParser(description="Monitor BD lead pipeline")
    parser.add_argument("--restart", action="store_true",
                        help="Auto-restart crawlers if dead and work remaining")
    parser.add_argument("--json", action="store_true",
                        help="Output JSON (for cron no_agent mode)")
    parser.add_argument("--quiet", action="store_true",
                        help="Only output if issues found")
    args = parser.parse_args()

    status = run_checks(restart=args.restart)

    if args.json:
        print(json.dumps(status, indent=2, ensure_ascii=False))
    elif args.quiet and status["healthy"] and not status["healed"]:
        pass  # silent when healthy
    else:
        # Human-readable report
        icon = "✅" if status["healthy"] else "❌"
        print(f"\n{icon} Pipeline Health: {'HEALTHY' if status['healthy'] else 'UNHEALTHY'}")
        print(f"   Crawler processes: {status['crawlers']['running']}")
        r = status["redis"]
        print(f"   Redis Listing: pending={r['listing']['pending']} processing={r['listing']['processing']} completed={r['listing']['completed']} failed={r['listing']['failed']}")
        print(f"   Redis Search:  pending={r['search']['pending']} processing={r['search']['processing']} completed={r['search']['completed']} failed={r['search']['failed']}")
        print(f"   DB: {status['database']['total_listings']} listings, {status['database']['uncrawled_urls']} uncrawled URLs")
        if status["issues"]:
            print(f"   Issues: {'; '.join(status['issues'])}")
        if status["healed"]:
            print(f"   Healed: {'; '.join(status['healed'])}")


if __name__ == "__main__":
    main()
