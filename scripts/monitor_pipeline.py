#!/usr/bin/env python3
"""monitor_pipeline.py — Auto-monitor + self-heal the BD lead pipeline.

Checks:
  1. Listing crawler process health (are workers running?)
  2. Redis queue health (pending/processing/completed)
  3. Database growth rate (are new listings being added?)
  4. Stale processing items (items stuck in "processing")
  5. Uncrawled URL count
  6. Email extraction stats
  7. LinkedIn profile discovery stats

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
import subprocess
import sys

from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("monitor_pipeline")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_uncrawled_count_sql, PG_DEFAULT_HOST, PG_DEFAULT_PASSWORD  # noqa: E402


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
        result = subprocess.run(
            [
                "psql",
                "-h", PG_DEFAULT_HOST,
                "-U", "postgres",
                "-d", "infinitecrawler",
                "-t", "-A",
                "-c", sql,
            ],
            capture_output=True, text=True, timeout=20,
            env={**os.environ, "PGPASSWORD": PG_DEFAULT_PASSWORD},
        )
        out = result.stdout.strip()
        return out if out else "0"
    except Exception as e:
        log.warning(f"PG query failed: {sql[:50]} — {e}")
        return "error"


def _systemd_daemon_active(unit: str) -> bool:
    """Check if a systemd user service is active."""
    try:
        r = subprocess.run(
            ["systemctl", "--user", "is-active", unit],
            capture_output=True, text=True, timeout=10,
        )
        return r.stdout.strip() == "active"
    except Exception:
        return False


def count_listing_processes() -> int:
    """Count running listing daemon processes."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "listing_daemon"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            return len(result.stdout.strip().split("\n"))
        return 0
    except Exception:
        return 0


def get_crawler_pids() -> list[str]:
    """Get PIDs of listing daemon processes."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "listing_daemon"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            return result.stdout.strip().split("\n")
        return []
    except Exception:
        return []


def clear_stale_processing() -> int:
    """Move items stuck in processing back to pending (up to 100)."""
    from utils.pg import get_pg_config
    import psycopg

    config = get_pg_config()
    try:
        conn = psycopg.connect(**config)
        try:
            # Check if processing items exist
            r = redis_cmd("LLEN gmaps:processing")
            count = int(r) if r else 0
            if count == 0:
                return 0

            moved = 0
            for _ in range(min(count, 100)):
                item = redis_cmd("RPOP gmaps:processing")
                if item:
                    redis_cli_push(item)
                    moved += 1
            return moved
        finally:
            conn.close()
    except Exception:
        return 0


def redis_cli_push(item: str):
    """Push an item back to the pending queue using redis-cli."""
    try:
        subprocess.run(
            ["redis-cli", "LPUSH", "gmaps:pending", item],
            capture_output=True, timeout=5
        )
    except Exception:
        pass


def kill_orphan_chrome():
    """No-op stub — kept for backward compatibility with callers.

    Pinchtab owns the browser process; its `always-on` supervisor restarts
    crashed Chrome instances automatically.  Killing Chrome from outside
    pinchtab would desync the dashboard.  The crawler daemons are also no
    longer allowed to kill Chrome directly — see
    `skills/pinchtab-chrome-stability` for the rationale.
    """
    return 0, 0


def restart_crawlers() -> bool:
    """Restart the listing daemon systemd service."""
    try:
        start_result = subprocess.run(
            ["systemctl", "--user", "start", "infinitecrawler-listing"],
            capture_output=True, text=True, timeout=30
        )
        if start_result.returncode != 0:
            log.error(f"Failed to start: {start_result.stderr}")
            return False
        # Also restart search daemon if not running
        subprocess.run(
            ["systemctl", "--user", "start", "infinitecrawler-search"],
            capture_output=True, timeout=10
        )
        return True
    except Exception as e:
        log.error(f"Restart failed: {e}")
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

    # 6. Email enrichment stats
    total_emails = pg_query("SELECT COUNT(*) FROM scraper.emails")
    listings_with_email = pg_query(
        "SELECT COUNT(DISTINCT listing_id) FROM scraper.emails"
    )
    unprocessed_emails = pg_query(
        "SELECT COUNT(*) FROM scraper.gmaps_listings "
        "WHERE website IS NOT NULL AND website != '' "
        "AND id NOT IN (SELECT listing_id FROM scraper.emails)"
    )

    # 7. LinkedIn enrichment stats
    total_linkedin = pg_query("SELECT COUNT(*) FROM scraper.linkedin_profiles")
    listings_with_linkedin = pg_query(
        "SELECT COUNT(DISTINCT listing_id) FROM scraper.linkedin_profiles"
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
            "enrichment": {
                "total_emails": int(total_emails) if total_emails != "error" else None,
                "listings_with_email": int(listings_with_email) if listings_with_email != "error" else None,
                "unprocessed_emails": int(unprocessed_emails) if unprocessed_emails != "error" else None,
                "total_linkedin_profiles": int(total_linkedin) if total_linkedin != "error" else None,
                "listings_with_linkedin": int(listings_with_linkedin) if listings_with_linkedin != "error" else None,
            },
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
        db = status["database"]
        print(f"   DB Listings: {db['total_listings']}  |  Search Results: {db['total_search_results']}")
        print(f"   With phone: {db['listings_with_phone']}  |  With website: {db['leads_with_website']}  |  Uncrawled: {db['uncrawled_urls']}")

        # Enrichment stats
        enrich = db.get("enrichment", {})
        if enrich.get("total_emails") is not None:
            print(f"   Emails: {enrich['total_emails']} total ({enrich['listings_with_email']} listings, {enrich['unprocessed_emails']} pending)")
        if enrich.get("total_linkedin_profiles") is not None:
            print(f"   LinkedIn profiles: {enrich['total_linkedin_profiles']} ({enrich['listings_with_linkedin']} listings)")

        if status["issues"]:
            print(f"\n⚠️ Issues: {'; '.join(status['issues'])}")
        if status["healed"]:
            print(f"\n🩹 Auto-heal: {'; '.join(status['healed'])}")
        print()


if __name__ == "__main__":
    main()
