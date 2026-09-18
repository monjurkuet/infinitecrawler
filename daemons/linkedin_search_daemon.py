#!/usr/bin/env python3
"""daemons/linkedin_search_daemon.py — Search daemon for LinkedIn jobs.

Walks the (keyword, location) query matrix from sectors.yaml linkedin_jobs block,
hits the guest search API (seeMoreJobPostings/search), extracts job_ids,
and pushes them to Redis queue `linkedin:jobs:pending`.

Usage:
    uv run python -m daemons.linkedin_search_daemon           # single pass
    uv run python -m daemons.linkedin_search_daemon --loop    # perpetual
    uv run python -m daemons.linkedin_search_daemon --dry-run # preview
"""

import argparse
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Optional

import redis
import curl_cffi.requests as curl_cffi

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.linkedin_jobs_config import build_keyword_location_pairs  # noqa: E402
from utils.pg import get_pg_config, get_query_state, bump_query_state, mark_query_exhausted  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("linkedin_search")

# ── Config ────────────────────────────────────────────────────────────────────

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)

SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

# Redis keys
REDIS_JOBS_PENDING = "linkedin:jobs:pending"
REDIS_JOBS_SEEN = "linkedin:jobs:seen"

# Pacing
MIN_DELAY = 0.6
MAX_DELAY = 1.2
BATCH_SIZE = 50
QUEUE_WATERMARK = 50000  # sleep when pending queue exceeds this


def get_proxy() -> Optional[str]:
    """Get proxy from env; returns None if not set."""
    return os.environ.get("DATASOLVED_PROXY")


def make_session() -> curl_cffi.Session:
    """Create a curl_cffi session with proper headers and proxy."""
    session = curl_cffi.Session(impersonate="chrome120")
    proxy = get_proxy()
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
    session.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    })
    return session


def fetch_search_page(session: curl_cffi.Session, keyword: str, location: str, start: int) -> Optional[str]:
    """Fetch one search page, return HTML text or None on error."""
    params = {
        "keywords": keyword,
        "location": location,
        "start": start,
    }
    try:
        resp = session.get(SEARCH_URL, params=params, timeout=25)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 400:
            # start > 975 — exhausted
            log.info("Query exhausted (400): kw=%r loc=%r start=%d", keyword, location, start)
            return "EXHAUSTED"
        elif resp.status_code in (429, 999):
            log.warning("Rate limited (%d): kw=%r loc=%r", resp.status_code, keyword, location)
            return "RATE_LIMITED"
        else:
            log.warning("HTTP %d for kw=%r loc=%r start=%d", resp.status_code, keyword, location, start)
            return None
    except Exception as e:
        log.warning("Request failed for kw=%r loc=%r start=%d: %s", keyword, location, start, e)
        return None


def process_query(session: curl_cffi.Session, redis_client: redis.Redis, pg_conn,
                  keyword: str, location: str, sector: Optional[str], dry_run: bool) -> tuple[int, bool]:
    """Process a single (keyword, location) query across all pages.

    Returns: (jobs_queued, is_exhausted)
    """
    # Reset the per-cycle rate-limit counter at the start of every query.
    process_query._rl_strikes = 0  # type: ignore[attr-defined]

    # Check query state
    last_start, is_exhausted = get_query_state(pg_conn, keyword, location)
    if is_exhausted:
        log.debug("Skipping exhausted query: %r @ %r", keyword, location)
        return (0, True)

    jobs_queued = 0
    start = last_start or 0

    while start <= 970:  # 0..970 inclusive, step 10
        # Respect queue watermark
        while True:
            pending_count = redis_client.scard(REDIS_JOBS_PENDING)
            if pending_count < QUEUE_WATERMARK:
                break
            log.info("Queue watermark reached (%d), sleeping 60s...", pending_count)
            time.sleep(60)

        if dry_run:
            log.info("[DRY-RUN] Would fetch: kw=%r loc=%r start=%d", keyword, location, start)
            start += 10
            continue

        html = fetch_search_page(session, keyword, location, start)
        if html == "EXHAUSTED":
            mark_query_exhausted(pg_conn, keyword, location)
            return (jobs_queued, True)
        elif html == "RATE_LIMITED":
            # Rotate proxy or back off. Exponential backoff, capped at 3 min,
            # plus session refresh every other strike to swap cookies.
            rl_strikes = getattr(process_query, "_rl_strikes", 0) + 1
            process_query._rl_strikes = rl_strikes  # type: ignore[attr-defined]
            backoff = min(30 * (2 ** (rl_strikes - 1)), 180)
            log.warning("Rate-limited (#%d). Backing off %ds, refreshing session.", rl_strikes, backoff)
            if rl_strikes % 2 == 0:
                session.close()
                session.headers.clear()
                session = make_session()
                log.info("Session refreshed after rate-limit strike #%d", rl_strikes)
            time.sleep(backoff)
            continue
        elif html is None:
            # Transient error, retry same page after delay
            time.sleep(5)
            continue

        # Extract job IDs
        from utils.linkedin_jobs_parser import extract_job_ids_from_search
        job_ids = extract_job_ids_from_search(html)

        if not job_ids:
            log.info("No job IDs on page (exhausted): kw=%r loc=%r start=%d", keyword, location, start)
            mark_query_exhausted(pg_conn, keyword, location)
            return (jobs_queued, True)

        # Dedupe via Redis set and push new ones
        new_ids = []
        for jid in job_ids:
            if redis_client.sadd(REDIS_JOBS_SEEN, jid):
                redis_client.sadd(REDIS_JOBS_PENDING, jid)
                new_ids.append(jid)

        jobs_queued += len(new_ids)
        log.info("kw=%r loc=%r start=%d -> %d new job_ids (total queued: %d)",
                 keyword, location, start, len(new_ids), jobs_queued)

        # Update PG state
        bump_query_state(pg_conn, keyword, location, start)

        # If we got fewer than 10, we've hit the end
        if len(job_ids) < 10:
            log.info("Page has <10 results, marking exhausted: kw=%r loc=%r", keyword, location)
            mark_query_exhausted(pg_conn, keyword, location)
            return (jobs_queued, True)

        start += 10
        # Pace
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    return (jobs_queued, False)


def main() -> int:
    parser = argparse.ArgumentParser(description="LinkedIn jobs search daemon")
    parser.add_argument("--loop", action="store_true", help="Run perpetually")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--max-queries", type=int, default=None, help="Limit queries per cycle")
    parser.add_argument("--gap", type=float, default=300.0, help="Sleep between cycles (seconds)")
    args = parser.parse_args()

    # Validate env
    if not os.environ.get("PG_HOST"):
        log.error("PG_HOST env var required")
        return 1

    redis_client = redis.Redis(decode_responses=True)
    pg_conn = psycopg.connect(**get_pg_config())

    try:
        import signal
        stop = {"v": False}
        def _stop(*_):
            stop["v"] = True
        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        while True:
            try:
                if pg_conn.closed:
                    log.warning("PG connection closed, reconnecting...")
                    pg_conn = psycopg.connect(**get_pg_config())

                pairs = build_keyword_location_pairs()

                if args.max_queries:
                    pairs = pairs[:args.max_queries]

                log.info("Starting cycle: %d (keyword, location) pairs", len(pairs))

                total_queued = 0
                session = make_session()

                for i, (kw, loc, sector) in enumerate(pairs):
                    if stop["v"]:
                        break

                    if i % 20 == 0:
                        # Refresh session periodically to avoid cookie/session issues
                        session.close()
                        session = make_session()

                    queued, exhausted = process_query(session, redis_client, pg_conn, kw, loc, sector, args.dry_run)
                    total_queued += queued

                log.info("Cycle complete: %d job_ids queued", total_queued)

                if not args.loop or stop["v"]:
                    break
                time.sleep(args.gap)

            except Exception as e:
                log.error("Cycle error: %s", e, exc_info=True)
                if not args.loop or stop["v"]:
                    break
                time.sleep(args.gap)
    finally:
        pg_conn.close()

    return 0


if __name__ == "__main__":
    import psycopg
    sys.exit(main())