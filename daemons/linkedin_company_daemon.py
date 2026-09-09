#!/usr/bin/env python3
"""daemons/linkedin_company_daemon.py — Company enrichment.

Pops company slugs from Redis `linkedin:companies:pending`, fetches the
company/{slug} page, parses the Organization JSON-LD (or HTML fallback),
upserts into scraper.linkedin_companies (slug-first, then name-normalized).

Usage:
    uv run python -m daemons.linkedin_company_daemon           # single pass
    uv run python -m daemons.linkedin_company_daemon --loop    # perpetual
    uv run python -m daemons.linkedin_company_daemon --dry-run # preview
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

from utils.pg import get_pg_config  # noqa: E402
from utils.linkedin_jobs_parser import parse_company_page  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("linkedin_company")

# ── Config ────────────────────────────────────────────────────────────────────

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)

COMPANY_URL_BASE = "https://www.linkedin.com/company/"

# Redis keys
REDIS_COMPANIES_PENDING = "linkedin:companies:pending"
REDIS_COMPANIES_SEEN = "linkedin:companies:seen"
REDIS_COMPANIES_FAILED = "linkedin:companies:failed"

# Pacing (company pages need ~2s between fresh connections; 0.5s triggers 999)
MIN_DELAY = 1.8
MAX_DELAY = 2.5
BATCH_SIZE = 20


def get_proxy() -> Optional[str]:
    """Prefer the -cf (Cloudflare-trusted) exit for company pages."""
    return (
        os.environ.get("DATASOLVED_PROXY_CF")
        or os.environ.get("DATASOLVED_PROXY")
    )


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


UPSERT_COMPANY_SQL = """
    INSERT INTO scraper.linkedin_companies
        (company_name_norm, company_name, slug, linkedin_slug, linkedin_url, industry,
         company_size, employee_count, followers, headquarters, website,
         founded, specialties, description, logo_url,
         last_checked_at, attempts, notes)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 1, %s)
    ON CONFLICT (company_name_norm) DO UPDATE SET
        slug              = COALESCE(EXCLUDED.slug,              scraper.linkedin_companies.slug),
        linkedin_slug     = COALESCE(EXCLUDED.linkedin_slug,     scraper.linkedin_companies.linkedin_slug),
        linkedin_url      = COALESCE(EXCLUDED.linkedin_url,      scraper.linkedin_companies.linkedin_url),
        industry          = COALESCE(EXCLUDED.industry,          scraper.linkedin_companies.industry),
        company_size      = COALESCE(EXCLUDED.company_size,      scraper.linkedin_companies.company_size),
        employee_count    = COALESCE(EXCLUDED.employee_count,    scraper.linkedin_companies.employee_count),
        followers         = COALESCE(EXCLUDED.followers,         scraper.linkedin_companies.followers),
        headquarters      = COALESCE(EXCLUDED.headquarters,      scraper.linkedin_companies.headquarters),
        website           = COALESCE(EXCLUDED.website,           scraper.linkedin_companies.website),
        founded           = COALESCE(EXCLUDED.founded,           scraper.linkedin_companies.founded),
        specialties       = COALESCE(EXCLUDED.specialties,       scraper.linkedin_companies.specialties),
        description       = COALESCE(EXCLUDED.description,       scraper.linkedin_companies.description),
        logo_url          = COALESCE(EXCLUDED.logo_url,          scraper.linkedin_companies.logo_url),
        last_checked_at   = NOW(),
        attempts          = scraper.linkedin_companies.attempts + 1,
        jobs_count        = greatest(scraper.linkedin_companies.jobs_count, 0)
"""


def _norm(name: str) -> str:
    import re
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def upsert_company(conn, c: dict) -> int:
    """Upsert company by slug-first, then name-normalized."""
    company_name = c.get("company_name") or c.get("slug") or ""
    company_name_norm = _norm(company_name)

    # specialties may be a list from the parser; DB column is TEXT
    specialties = c.get("specialties")
    if isinstance(specialties, (list, tuple)):
        specialties = ", ".join(str(s) for s in specialties if s)
    elif not specialties:
        specialties = None

    cur = conn.cursor()

    # If slug present, try to match an existing row by linkedin_slug column first
    slug = c.get("slug")
    if slug:
        cur.execute(
            "SELECT company_name_norm FROM scraper.linkedin_companies WHERE linkedin_slug = %s",
            (slug,),
        )
        row = cur.fetchone()
        if row:
            # Existing row found by slug — enrich it in place and return
            existing_norm = row[0]
            cur.execute("""
                UPDATE scraper.linkedin_companies SET
                    company_name      = COALESCE(%s, company_name),
                    linkedin_url      = COALESCE(%s, linkedin_url),
                    industry          = COALESCE(%s, industry),
                    company_size      = COALESCE(%s, company_size),
                    employee_count    = COALESCE(%s, employee_count),
                    headquarters      = COALESCE(%s, headquarters),
                    website           = COALESCE(%s, website),
                    description       = COALESCE(%s, description),
                    logo_url          = COALESCE(%s, logo_url),
                    last_checked_at   = NOW(),
                    attempts          = attempts + 1
                WHERE company_name_norm = %s
            """, (
                company_name, c.get("linkedin_url"), c.get("industry"),
                c.get("company_size"), c.get("employee_count"),
                c.get("headquarters"), c.get("website"),
                c.get("description"), c.get("logo_url"),
                existing_norm,
            ))
            conn.commit()
            return 1

    # No existing slug match — upsert by name_norm
    try:
        cur.execute(UPSERT_COMPANY_SQL, (
            company_name_norm,
            company_name,
            slug,
            slug,
            c.get("linkedin_url"),
            c.get("industry"),
            c.get("company_size"),
            c.get("employee_count"),
            c.get("followers"),
            c.get("headquarters"),
            c.get("website"),
            c.get("founded"),
            specialties,
            c.get("description"),
            c.get("logo_url"),
            c.get("notes"),
        ))
    except psycopg.errors.UniqueViolation:
        conn.rollback()
        log.debug("Unique violation for slug %s / name %s", slug, company_name)
        return 0
    written = cur.rowcount or 1
    conn.commit()
    return written


def fetch_company_page(slug: str) -> Optional[str]:
    """Fetch a company page using a FRESH connection per request.

    Company pages are aggressively rate-limited (HTTP 999) against pooled
    sessions — a fresh curl_cffi session per request avoids this, same
    technique as utils/linkedin_enrich.py (fresh requests connection beats
    pooling ~50% of the time).
    """
    url = f"{COMPANY_URL_BASE}{slug}"
    session = make_session()
    try:
        resp = session.get(url, timeout=25)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 404:
            log.info("Company %s not found (404)", slug)
            return "NOT_FOUND"
        elif resp.status_code in (429, 999):
            log.warning("Rate limited (%d) for company %s", resp.status_code, slug)
            return "RATE_LIMITED"
        else:
            log.warning("HTTP %d for company %s", resp.status_code, slug)
            return None
    except Exception as e:
        log.warning("Request failed for company %s: %s", slug, e)
        return None
    finally:
        session.close()


def process_company(redis_client: redis.Redis, pg_conn, slug: str, dry_run: bool) -> bool:
    html = fetch_company_page(slug)
    if html == "NOT_FOUND":
        return True
    elif html == "RATE_LIMITED" or html is None:
        return False

    try:
        parsed = parse_company_page(html, slug)
    except Exception as e:
        log.warning("Parse failed for company %s: %s", slug, e)
        return False

    if dry_run:
        log.info("[DRY-RUN] Company %s: %s | %s", slug, parsed.get("company_name"), parsed.get("website"))
        return True

    try:
        upsert_company(pg_conn, parsed)
        log.info("Company %s: %s (%s emp) %s",
                 slug, parsed.get("company_name"), parsed.get("employee_count"),
                 parsed.get("website") or "")
        return True
    except Exception as e:
        log.error("Upsert failed for company %s: %s", slug, e)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="LinkedIn company daemon")
    parser.add_argument("--loop", action="store_true", help="Run perpetually")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE, help="Companies per batch")
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
                    pg_conn = psycopg.connect(**get_pg_config())

                pending = redis_client.scard(REDIS_COMPANIES_PENDING)
                if pending == 0:
                    log.info("No pending companies, sleeping %ds...", args.gap)
                    if not args.loop or stop["v"]:
                        break
                    time.sleep(args.gap)
                    continue

                batch_size = min(args.batch, pending)
                log.info("Processing batch of %d companies (queue: %d)", batch_size, pending)

                processed = 0
                succeeded = 0

                for _ in range(batch_size):
                    if stop["v"]:
                        break

                    slug = redis_client.spop(REDIS_COMPANIES_PENDING)
                    if not slug:
                        break

                    ok = process_company(redis_client, pg_conn, slug, args.dry_run)  # type: ignore
                    processed += 1
                    if ok:
                        succeeded += 1
                    else:
                        redis_client.sadd(REDIS_COMPANIES_FAILED, slug)  # type: ignore

                    # Fresh connection already created per request; pace to avoid 999
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