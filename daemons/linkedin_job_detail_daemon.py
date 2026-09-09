#!/usr/bin/env python3
"""daemons/linkedin_job_detail_daemon.py — Job detail fetcher.

Pops job_ids from Redis `linkedin:jobs:pending`, fetches the jobPosting/{id} page,
parses all fields, upserts into scraper.linkedin_jobs, and pushes company slugs
to `linkedin:companies:pending`.

Usage:
    uv run python -m daemons.linkedin_job_detail_daemon           # single pass
    uv run python -m daemons.linkedin_job_detail_daemon --loop    # perpetual
    uv run python -m daemons.linkedin_job_detail_daemon --dry-run # preview
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
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_pg_config, upsert_linkedin_job, get_sector_id  # noqa: E402
from utils.linkedin_jobs_parser import parse_job_detail  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("linkedin_job_detail")

# ── Config ────────────────────────────────────────────────────────────────────

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)

JOB_URL_BASE = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/"

# Redis keys
REDIS_JOBS_PENDING = "linkedin:jobs:pending"
REDIS_COMPANIES_PENDING = "linkedin:companies:pending"
REDIS_JOBS_FAILED = "linkedin:jobs:failed"

# Pacing
MIN_DELAY = 0.5
MAX_DELAY = 1.0
BATCH_SIZE = 50
MAX_RETRIES = 3


def get_proxy() -> Optional[str]:
    return os.environ.get("DATASOLVED_PROXY")


def make_session() -> curl_cffi.Session:
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


def fetch_job_page(session: curl_cffi.Session, job_id: int) -> Optional[str]:
    """Fetch a single job detail page."""
    url = f"{JOB_URL_BASE}{job_id}"
    try:
        resp = session.get(url, timeout=25)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 404:
            log.info("Job %d not found (404)", job_id)
            return "NOT_FOUND"
        elif resp.status_code in (429, 999):
            log.warning("Rate limited (%d) for job %d", resp.status_code, job_id)
            return "RATE_LIMITED"
        else:
            log.warning("HTTP %d for job %d", resp.status_code, job_id)
            return None
    except Exception as e:
        log.warning("Request failed for job %d: %s", job_id, e)
        return None


def process_job(session: curl_cffi.Session, redis_client: redis.Redis, pg_conn,
                job_id: int, dry_run: bool) -> bool:
    """Process a single job ID. Returns True if successful."""
    # Fetch
    html = fetch_job_page(session, job_id)
    if html == "NOT_FOUND":
        # Mark as processed but don't re-queue
        return True
    elif html == "RATE_LIMITED":
        return False
    elif html is None:
        return False

    # Parse
    try:
        # Need keyword and sector for this job — we don't have them in Redis
        # We'll store empty and rely on the query_state table if needed
        parsed = parse_job_detail(html, job_id, "", "", "")
    except Exception as e:
        log.warning("Parse failed for job %d: %s", job_id, e)
        return False

    if dry_run:
        log.info("[DRY-RUN] Job %d: %s @ %s", job_id, parsed.title, parsed.company_name)
        return True

    # Get sector_id if we have source_sector
    sector_id = None
    if parsed.source_sector:
        sector_id = get_sector_id(pg_conn, parsed.source_sector)

    # Build upsert dict
    job_dict = {
        "job_id": parsed.job_id,
        "source_url": parsed.source_url,
        "title": parsed.title,
        "company_name": parsed.company_name,
        "company_linkedin_slug": parsed.company_linkedin_slug,
        "company_linkedin_url": parsed.company_linkedin_url,
        "location": parsed.location,
        "location_city": parsed.location_city,
        "listed_at": parsed.listed_at,
        "seniority_level": parsed.seniority_level,
        "employment_type": parsed.employment_type,
        "job_function": parsed.job_function,
        "industries": parsed.industries,
        "description_html": parsed.description_html,
        "description_text": parsed.description_text,
        "applicants_count": parsed.applicants_count,
        "promoted": parsed.promoted,
        "easy_apply": parsed.easy_apply,
        "apply_url": parsed.apply_url,
        "source_sector": parsed.source_sector,
        "source_keyword": parsed.source_keyword,
        "sector_id": sector_id,
    }

    try:
        upsert_linkedin_job(pg_conn, job_dict)
    except Exception as e:
        log.error("Upsert failed for job %d: %s", job_id, e)
        return False

    # If we have a company slug, push to company queue
    if parsed.company_linkedin_slug:
        redis_client.sadd(REDIS_COMPANIES_PENDING, parsed.company_linkedin_slug)

    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="LinkedIn job detail daemon")
    parser.add_argument("--loop", action="store_true", help="Run perpetually")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE, help="Jobs per batch")
    parser.add_argument("--gap", type=float, default=60.0, help="Sleep between cycles (seconds)")
    args = parser.parse_args()

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

                # Check queue size
                pending = redis_client.scard(REDIS_JOBS_PENDING)
                if pending == 0:
                    log.info("No pending jobs, sleeping %ds...", args.gap)
                    if not args.loop or stop["v"]:
                        break
                    time.sleep(args.gap)
                    continue

                batch_size = min(args.batch, pending)
                log.info("Processing batch of %d jobs (queue: %d)", batch_size, pending)

                session = make_session()
                processed = 0
                succeeded = 0

                for _ in range(batch_size):
                    if stop["v"]:
                        break

                    # SPOP gets and removes a random member
                    job_id_str = redis_client.spop(REDIS_JOBS_PENDING)
                    if not job_id_str:
                        break

                    try:
                        job_id = int(job_id_str)  # type: ignore  # decode_responses=True → str
                    except (ValueError, TypeError):
                        log.warning("Invalid job_id in queue: %s", job_id_str)
                        continue

                    ok = process_job(session, redis_client, pg_conn, job_id, args.dry_run)
                    processed += 1
                    if ok:
                        succeeded += 1
                    else:
                        # Re-queue for retry (with a delay mechanism)
                        redis_client.sadd(REDIS_JOBS_FAILED, job_id_str)  # type: ignore

                    # Pace
                    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

                log.info("Batch done: %d processed, %d succeeded", processed, succeeded)

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
    sys.exit(main())