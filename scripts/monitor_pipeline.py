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
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import redis as redis_lib

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("monitor_pipeline")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_pg_config, get_uncrawled_count_sql, get_uncrawled_urls_sql  # noqa: E402

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_DECODE = os.environ.get("REDIS_DECODE_RESPONSES", "1") == "1"
REDIS_SOCKET_TIMEOUT_SEC = int(os.environ.get("REDIS_SOCKET_TIMEOUT_SEC", "2"))

# Per-daemon PG-progress staleness thresholds (seconds). When the primary
# table's newest row is older than this, the daemon is considered stuck even
# if systemctl says "active". Env-overridable.
DAEMON_HEALTH_CHECKS = [
    {
        "unit": "infinitecrawler-listing.service",
        "sql": "SELECT EXTRACT(EPOCH FROM NOW() - max(created_at))::int FROM scraper.gmaps_listings",
        "max_age_s": int(os.environ.get("WATCHDOG_LISTING_MAX_AGE_S", "600")),
    },
    {
        "unit": "infinitecrawler-search.service",
        "sql": "SELECT EXTRACT(EPOCH FROM NOW() - max(created_at))::int FROM scraper.gmaps_search_results",
        "max_age_s": int(os.environ.get("WATCHDOG_SEARCH_MAX_AGE_S", "600")),
    },
    {
        "unit": "infinitecrawler-linkedin-firehose-loop.service",
        "sql": "SELECT EXTRACT(EPOCH FROM NOW() - max(checked_at))::int FROM scraper.linkedin_profiles WHERE source = 'firehose'",
        "max_age_s": int(os.environ.get("WATCHDOG_FIREHOSE_MAX_AGE_S", "900")),
    },
    {
        "unit": "infinitecrawler-email-extract-loop.service",
        "sql": "SELECT EXTRACT(EPOCH FROM NOW() - max(discovered_at))::int FROM scraper.emails",
        "max_age_s": int(os.environ.get("WATCHDOG_EMAIL_MAX_AGE_S", "1800")),
    },
]

_redis: redis_lib.Redis | None = None


def _redis_raw() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = redis_lib.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
            decode_responses=REDIS_DECODE, socket_timeout=REDIS_SOCKET_TIMEOUT_SEC,
        )
    return _redis


def redis_cmd(cmd: str) -> str:
    args = cmd.split()
    try:
        r = _redis_raw()
        result = r.execute_command(*args)
        return str(result) if result is not None else "0"
    except Exception as e:
        log.warning("Redis command failed: %s — %s", cmd, e)
        return "0"


def pg_query(sql: str) -> str:
    try:
        cfg = dict(get_pg_config())
        cfg["connect_timeout"] = 10
        with psycopg.connect(**cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return str(row[0]) if row and row[0] is not None else "0"
    except Exception as e:
        log.warning("PG query failed: %s — %s", sql[:50], e)
        return "error"


def pg_query_col(sql: str) -> list:
    """Return the first column of every row as a list (for URL backfills)."""
    try:
        cfg = dict(get_pg_config())
        cfg["connect_timeout"] = 10
        with psycopg.connect(**cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception as e:
        log.warning("PG col query failed: %s — %s", sql[:50], e)
        return []


def _systemd_daemon_active(unit: str) -> bool:
    """Check if a systemd user service is active."""
    try:
        r = subprocess.run(
            ["systemctl", "--user", "is-active", unit],
            capture_output=True, text=True, timeout=10,
        )
        return r.stdout.strip() == "active"
    except Exception:
        log.debug("monitor_pipeline: systemctl check failed for %s", unit, exc_info=True)
        return False


def _last_heartbeat_age_sec(log_path: Path) -> float | None:
    """Seconds since the most recent `heartbeat ` log line, or None."""
    try:
        if not log_path.exists():
            return None
        mtime = log_path.stat().st_mtime
        # We can't cheaply grep-tail a huge rotated log, so the file's mtime
        # is the most-recent-activity proxy. The heartbeat line lands within
        # HEARTBEAT_SEC of an active daemon's last write, so this approximates
        # heartbeat age to within a few seconds.
        return max(0.0, datetime.now(tz=timezone.utc).timestamp() - mtime)
    except Exception:
        log.debug("monitor_pipeline: heartbeat age lookup failed for %s", log_path, exc_info=True)
        return None


def _http_ok(url: str, timeout: int = 5) -> bool:
    """Check an HTTP endpoint responds (used for API health)."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status < 500
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
    r = redis_cmd("LLEN gmaps:processing")
    count = int(r) if r else 0
    if count == 0:
        return 0
    moved = 0
    for _ in range(min(count, 100)):
        item = redis_cmd("RPOP gmaps:processing")
        if item:
            redis_push(item)
            moved += 1
    return moved


def redis_push(item: str, pipe_key: str = "gmaps:pending") -> None:
    try:
        _redis_raw().lpush(pipe_key, item)
    except Exception:
        pass


def backfill_pending(batch: int = 1000, uncrawled_threshold: int = 1000, pending_threshold: int = 50) -> int:
    """Pitfall #9 safety net: when PG has a large uncrawled backlog but Redis
    is nearly empty, push the next batch of uncrawled URLs straight into
    gmaps:pending so the browser tier never sits idle.

    Mirrors daemons/listing_daemon.py's refill (utils.pg.get_uncrawled_urls_sql):
      SELECT sr.payload->>'url' FROM scraper.gmaps_search_results sr
      LEFT JOIN scraper.gmaps_listings gl ON gl.source_url = sr.payload->>'url'
      WHERE sr.payload->>'url' IS NOT NULL AND gl.source_url IS NULL
      ORDER BY sr.updated_at DESC LIMIT <batch>;
    """
    try:
        pending = int(redis_cmd("LLEN gmaps:pending") or 0)
        uncrawled = int(pg_query(get_uncrawled_count_sql()) or 0)
    except Exception as e:
        log.warning("backfill_pending: probe failed: %s", e)
        return 0
    if uncrawled < uncrawled_threshold or pending >= pending_threshold:
        return 0
    try:
        urls = pg_query_col(get_uncrawled_urls_sql(limit=batch))
    except Exception as e:
        log.warning("backfill_pending: fetch failed: %s", e)
        return 0
    if not urls:
        return 0
    moved = 0
    for u in urls:
        redis_push(u)
        moved += 1
    log.info("backfill_pending: enqueued %d uncrawled URLs (pending was %d, uncrawled %d)", moved, pending, uncrawled)
    return moved


def backfill_phantom(batch: int = 200, phantom_threshold: int = 20) -> int:
    """Phantom-row backfill: requeues phantom rows (name-only, no phone/website/
    rating/address) that landed in gmaps_listings via `_mark_phantom_url`.
    Called by the watchdog so the sweeper has fresh phantom URLs to retry.

    Why: phantom URLs are queued in gmaps:phantom atomically. Every picket
    should eventually become a full listing — we just don't block the live
    queue with URLs that kept rendering as bare shells.

    ALSO sweeps persisted phantom rows from PG (rows with a name but no
    phone/website/rating/address, created >2h ago) — this catches the
    pre-fix legacy phantoms.
    """
    try:
        phantom_len = int(redis_cmd("LLEN gmaps:phantom") or 0)
    except Exception as e:
        log.warning("backfill_phantom: probe failed: %s", e)
        return 0
    if phantom_len < phantom_threshold:
        return 0

    # Pull the next batch of phantom URLs from Redis.
    try:
        import redis as _r
        client = _r.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
        urls = client.lrange("gmaps:phantom", 0, batch - 1)
        if urls:
            client.ltrim("gmaps:phantom", batch, -1)  # drop processed batch
    except Exception as e:
        log.warning("backfill_phantom: redis lpop failed: %s", e)
        return 0

    # Also sweep persisted phantom rows from PG (created >2h ago so we don't
    # re-fight URLs currently being retried by the daemon itself).
    sql = """
        SELECT DISTINCT source_url FROM scraper.gmaps_listings
        WHERE source_type='gmaps_listing'
          AND name IS NOT NULL
          AND phone IS NULL AND website IS NULL AND rating IS NULL AND address IS NULL
          AND created_at < now() - interval '2 hours'
        ORDER BY created_at DESC
        LIMIT %d
    """ % batch
    try:
        pg_urls = pg_query_col(sql)
        urls = urls + [u for u in pg_urls if u not in urls]
    except Exception as e:
        log.warning("backfill_phantom: PG sweep failed: %s", e)
        pg_urls = []

    moved = 0
    for u in urls:
        redis_push(u)
        moved += 1
    log.info("backfill_phantom: requeued %d phantom URLs (phantom_queue=%d, pg_sweep=%d)", moved, phantom_len, len(pg_urls or []))
    return moved


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


def restart_unit(unit: str) -> bool:
    """Restart a systemd user unit (oneshot or simple)."""
    try:
        r = subprocess.run(
            ["systemctl", "--user", "restart", unit],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            log.error("Failed to restart %s: %s", unit, r.stderr)
            return False
        return True
    except Exception as e:
        log.error("Restart %s failed: %s", unit, e)
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
        "AND NOT EXISTS (SELECT 1 FROM scraper.emails e WHERE e.listing_id = scraper.gmaps_listings.id)"
    )

    # 7. LinkedIn enrichment stats
    total_linkedin = pg_query("SELECT COUNT(*) FROM scraper.linkedin_profiles")
    listings_with_linkedin = pg_query(
        "SELECT COUNT(DISTINCT listing_id) FROM scraper.linkedin_profiles"
    )
    emails_1h = pg_query("SELECT COUNT(*) FROM scraper.emails WHERE discovered_at > NOW() - INTERVAL '1 hour'")
    # T1.x — LinkedIn company enrichment (added 2026-08-09)
    companies_enriched = pg_query("SELECT COUNT(*) FROM scraper.linkedin_companies")
    companies_with_emp = pg_query(
        "SELECT COUNT(*) FROM scraper.linkedin_companies WHERE employee_count IS NOT NULL"
    )
    companies_with_industry = pg_query(
        "SELECT COUNT(*) FROM scraper.linkedin_companies WHERE industry IS NOT NULL"
    )
    companies_with_hq = pg_query(
        "SELECT COUNT(*) FROM scraper.linkedin_companies WHERE headquarters IS NOT NULL"
    )
    profiles_with_location = pg_query(
        "SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE profile_location IS NOT NULL"
    )
    profiles_with_country = pg_query(
        "SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE profile_country IS NOT NULL"
    )
    profiles_with_connections = pg_query(
        "SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE connections_count IS NOT NULL"
    )
    profiles_with_headline = pg_query(
        "SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE headline IS NOT NULL"
    )

    # 8. Data velocity (last 1h and 6h window)
    velocity_search_1h = pg_query(
        "SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour'"
    )
    velocity_listings_1h = pg_query(
        "SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour'"
    )
    velocity_search_6h = pg_query(
        "SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '6 hours'"
    )
    velocity_listings_6h = pg_query(
        "SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '6 hours'"
    )

    # 9. Service-level checks (email-extract freshness, firehose, API)
    email_stale_min = pg_query(
        "SELECT CASE WHEN COUNT(*) = 0 THEN 9999 "
        "  ELSE EXTRACT(EPOCH FROM (NOW() - MAX(discovered_at)))::int / 60 END "
        "FROM scraper.emails"
    )
    email_service_active = _systemd_daemon_active("infinitecrawler-email-extract.service")
    firehose_active = _systemd_daemon_active("infinitecrawler-linkedin-firehose-loop.service")
    listing_daemon_active = _systemd_daemon_active("infinitecrawler-listing.service")
    search_daemon_active = _systemd_daemon_active("infinitecrawler-search.service")
    api_ok = _http_ok("http://127.0.0.1:8015/")

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

    # Stalled search output (velocity check)
    vs1h = int(velocity_search_1h) if velocity_search_1h != "error" else 0
    vl1h = int(velocity_listings_1h) if velocity_listings_1h != "error" else 0
    if vl1h > 0 and vs1h == 0:
        issues.append("Search daemon producing zero new DB rows — likely query exhaustion (all queries produce identical GMaps results)")

    # Service freshness checks
    if email_stale_min != "error":
        stale = int(email_stale_min)
        if stale > 45:
            is_healthy = False
            state = "active" if email_service_active else "inactive"
            issues.append(f"Email extraction stale: no emails written for {stale} min (service {state})")
        if emails_1h != "error" and int(emails_1h) < 5:
            is_healthy = False
            issues.append(f"Email extraction low-throughput: only {emails_1h} emails written in last 1h")
    else:
        is_healthy = False
        issues.append("PG unreachable — cannot verify pipeline state")

    if not firehose_active:
        is_healthy = False
        issues.append("LinkedIn firehose-loop service not active")
    if not api_ok:
        is_healthy = False
        issues.append("API not responding on port 8015")

    # Auto-heal
    healed = []
    if processing > 0 and procs == 0:
        moved = clear_stale_processing()
        if moved > 0:
            healed.append(f"Moved {moved} stale processing items to pending")

    # Structured daemon health: restart dead-perpetual or PG-stale services.
    if restart:
        for check in DAEMON_HEALTH_CHECKS:
            unit = check["unit"]
            is_active = _systemd_daemon_active(unit)
            age_raw = pg_query(check["sql"])
            if not is_active:
                log.warning("watchdog: %s is dead — restarting", unit)
                if restart_unit(unit):
                    healed.append(f"Restarted {unit} (dead)")
                else:
                    issues.append(f"Restart {unit} failed (dead)")
            elif age_raw not in ("error", ""):
                age = int(age_raw)
                if age > check["max_age_s"]:
                    log.warning("watchdog: %s alive but PG progress age=%ds > %ds — restarting",
                                unit, age, check["max_age_s"])
                    if restart_unit(unit):
                        healed.append(f"Restarted {unit} (stale {age}s)")
                    else:
                        issues.append(f"Restart {unit} failed (stale {age}s)")

    if restart and procs == 0 and int(uncrawled or 0) > 0:
        # Fall back to the legacy crawler restart for the listing/search
        # queue path (covers cases the PG-progress check doesn't catch).
        success = restart_crawlers()
        if success:
            healed.append("Restarted crawlers for uncrawled URLs")
        else:
            issues.append("Crawler restart failed")

    # Pitfall #9 — keep Redis deep: backfill uncrawled PG URLs into gmaps:pending
    # whenever the queue is nearly empty but a large backlog remains.
    if restart:
        moved = backfill_pending()
        if moved > 0:
            healed.append(f"Backfilled {moved} uncrawled URLs into Redis")

        # Phantom-row sweeper: requeue name-only rows (bare-shell render)
        # that came back to the top of the backlog.
        phantom_moved = backfill_phantom()
        if phantom_moved > 0:
            healed.append(f"Phantom backfill: requeued {phantom_moved} bare-shell URLs")

    status = {
        "timestamp": now,
        "healthy": is_healthy,
        "issues": issues,
        "healed": healed,
        "crawlers": {
            "running": procs,
            "pids": pids[:10],
        },
        "services": {
            "email_extract": {
                "active": email_service_active,
                "stale_minutes": int(email_stale_min) if email_stale_min not in ("error", "") else None,
            },
            "linkedin_firehose": {
                "active": firehose_active,
            },
            "listing_daemon": {
                "active": listing_daemon_active,
                "last_heartbeat_age_sec": _last_heartbeat_age_sec(
                    Path("/var/log/infinitecrawler/infinitecrawler-listing.log")
                ),
            },
            "search_daemon": {
                "active": search_daemon_active,
                "last_heartbeat_age_sec": _last_heartbeat_age_sec(
                    Path("/var/log/infinitecrawler/infinitecrawler-search.log")
                ),
            },
            "api": {
                "port": 8015,
                "responding": api_ok,
            },
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
            "velocity": {
                "search_1h": int(velocity_search_1h) if velocity_search_1h != "error" else None,
                "listings_1h": int(velocity_listings_1h) if velocity_listings_1h != "error" else None,
                "search_6h": int(velocity_search_6h) if velocity_search_6h != "error" else None,
                "listings_6h": int(velocity_listings_6h) if velocity_listings_6h != "error" else None,
            },
            "enrichment": {
                "total_emails": int(total_emails) if total_emails != "error" else None,
                "listings_with_email": int(listings_with_email) if listings_with_email != "error" else None,
                "unprocessed_emails": int(unprocessed_emails) if unprocessed_emails != "error" else None,
                "emails_1h": int(emails_1h) if emails_1h != "error" else None,
                "total_linkedin_profiles": int(total_linkedin) if total_linkedin != "error" else None,
                "listings_with_linkedin": int(listings_with_linkedin) if listings_with_linkedin != "error" else None,
                "companies_enriched": int(companies_enriched) if companies_enriched != "error" else None,
                "companies_with_emp": int(companies_with_emp) if companies_with_emp != "error" else None,
                "companies_with_industry": int(companies_with_industry) if companies_with_industry != "error" else None,
                "companies_with_hq": int(companies_with_hq) if companies_with_hq != "error" else None,
                "profiles_with_location": int(profiles_with_location) if profiles_with_location != "error" else None,
                "profiles_with_country": int(profiles_with_country) if profiles_with_country != "error" else None,
                "profiles_with_connections": int(profiles_with_connections) if profiles_with_connections != "error" else None,
                "profiles_with_headline": int(profiles_with_headline) if profiles_with_headline != "error" else None,
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
        vel = db.get("velocity", {})
        if vel:
            print(f"   Velocity (1h/6h): search {vel.get('search_1h', '?')}/{vel.get('search_6h', '?')}  |  listings {vel.get('listings_1h', '?')}/{vel.get('listings_6h', '?')}")
        print(f"   With phone: {db['listings_with_phone']}  |  With website: {db['leads_with_website']}  |  Uncrawled: {db['uncrawled_urls']}")

        # Enrichment stats
        enrich = db.get("enrichment", {})
        if enrich.get("total_emails") is not None:
            print(f"   Emails: {enrich['total_emails']} total ({enrich['listings_with_email']} listings, {enrich['unprocessed_emails']} pending, {enrich.get('emails_1h', '?')} last 1h)")
        if enrich.get("total_linkedin_profiles") is not None:
            print(f"   LinkedIn profiles: {enrich['total_linkedin_profiles']} ({enrich['listings_with_linkedin']} listings)")
        # T1.x — LinkedIn enrichment depth (2026-08-09)
        if enrich.get("companies_enriched") is not None:
            ce = enrich
            print(
                f"   Companies enriched: {ce['companies_enriched']} ({ce['companies_with_emp']} emp, {ce['companies_with_industry']} industry, {ce['companies_with_hq']} HQ)"
            )
        if enrich.get("profiles_with_location") is not None:
            pe = enrich
            print(
                f"   Profile backfill:   {pe['profiles_with_location']} location, {pe['profiles_with_country']} country, {pe['profiles_with_connections']} connections, {pe['profiles_with_headline']} headline"
            )

        svc = status.get("services", {})
        if svc:
            ee = svc.get("email_extract", {})
            fh = svc.get("linkedin_firehose", {})
            api = svc.get("api", {})
            print(f"   Services: email-stale={ee.get('stale_minutes', '?')}min/{'active' if ee.get('active') else 'inactive'}  firehose={'up' if fh.get('active') else 'DOWN'}  api={'up' if api.get('responding') else 'DOWN'}")

        if status["issues"]:
            print(f"\n⚠️ Issues: {'; '.join(status['issues'])}")
        if status["healed"]:
            print(f"\n🩹 Auto-heal: {'; '.join(status['healed'])}")
        print()


if __name__ == "__main__":
    main()
