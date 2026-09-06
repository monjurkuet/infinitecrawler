#!/usr/bin/env python3
"""
BBB.org scraper daemon — searches via BBB /api/search JSON (httpx + proxy),
enriches profiles (httpx + proxy), filters, upserts to PG, loops forever.

Key facts:
- BBB API: 15 results per page, hard cap at page 15 (225 max per query)
- Geographic fan-out needed for coverage (city-level queries)
- Profiles enriched via HTTP proxy (no browser needed)
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx
import psycopg
import redis
from dotenv import load_dotenv

# ── Redis queue (same pattern as gmaps/linkedin daemons) ──────────────────────
# BBB search queries are enqueued for the daemon to pull + process. This makes
# the pipeline restart-safe and distributed (multiple bbb_scraper instances can
# share the queue) instead of a linear python loop.
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
QUEUE_PROCESSING = "bbb:processing"
QUEUE_DONE = "bbb:completed"

# Redis client (lazy — no socket ops until first use)
_redis = redis.Redis(host=os.environ.get("REDIS_HOST", "localhost"), port=6379, db=0)


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("bbb_scraper")

# ── Config ────────────────────────────────────────────────────────────────────

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)
PER_PAGE_DELAY = 0.5       # seconds between API pages
PER_PROFILE_DELAY = 1.0    # seconds between profile fetches
BATCH_SIZE = 15             # BBB fixed page size
MAX_PAGES_PER_QUERY = 15    # BBB hard cap
SWEEP_SLEEP = 300           # seconds between sweeps (5 min)

# HTTP proxy for BBB (optional, from env)
BBB_PROXY = os.environ.get("BBB_PROFILE_PROXY", "")


def get_proxy():
    """Return proxy config or empty dict."""
    if BBB_PROXY:
        return {"proxy": BBB_PROXY}
    return {}


# ── Nebraska cities and niches ──────────────────────────────────────────────

NE_CITIES = [
    "Lincoln, NE",
    "Omaha, NE",
    "Fairbury, NE",
    "Beatrice, NE",
    "Fremont, NE",
    "Grand Island, NE",
    "Kearney, NE",
    "Hastings, NE",
    "North Platte, NE",
    "Columbus, NE",
    "Bellevue, NE",
    "Scottsbluff, NE",
    "Norfolk, NE",
    "Papillion, NE",
    "Gretna, NE",
]

NE_NICHES = [
    "Handyman Services",
    "Handyman",
    "Property Preservation",
    "General Contractor",
    "Home Improvement",
    "Remodeling Contractors",
    "Carpenters",
    "Painters",
    "Property Management",
    "Real Estate Services",
    "Building Maintenance",
    "Roofing Contractors",
    "Siding Contractors",
    "Construction Services",
    "Landscape Contractors",
]

# ── Filter thresholds ───────────────────────────────────────────────────────

_RATING_TIER = {"A+": 6, "A": 5, "A-": 4, "B+": 3, "B": 2, "B-": 1}
MIN_RATING = "B"


def filter_result(r: "BBBSearchResult") -> bool:
    """Keep only quality leads."""
    if not r.name or not r.business_id:
        return False
    if not r.phone:
        return False
    if not (r.city and r.state):
        return False
    if _RATING_TIER.get(r.rating, -1) < _RATING_TIER.get(MIN_RATING, 0):
        return False
    return True


# ── Data model ──────────────────────────────────────────────────────────────

@dataclass
class BBBSearchResult:
    name: str
    address: str
    city: str
    state: str
    zip: str
    phone: list
    rating: str
    accredited: bool
    profile_url: str
    business_id: str
    source_query: str
    fetched_at: str


# ── BBB API ─────────────────────────────────────────────────────────────────

def bbb_api_search(keyword: str, location: str, page: int = 1) -> dict:
    """Search BBB API for businesses matching keyword in location."""
    params = {
        "find_country": "USA",
        "find_text": keyword,
        "find_type": "Category",
        "page": str(page),
        "find_loc": location,
    }
    headers = {
        "Accept": "application/json",
        "Referer": "https://www.bbb.org/search",
        "User-Agent": USER_AGENT,
    }
    try:
        with httpx.Client(proxy=BBB_PROXY, timeout=30, follow_redirects=True) as c:
            r = c.get("https://www.bbb.org/api/search", params=params, headers=headers)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        log.error(f"BBB search failed for '{keyword}' @ {location}: {e}")
        return {}


def parse_search_results(response: dict) -> list:
    """Parse BBB search JSON into BBBSearchResult list."""
    out = []
    for item in response.get("results", []):
        try:
            phone = item.get("phone") or []
            if isinstance(phone, str):
                phone = [phone]
            elif not isinstance(phone, list):
                phone = []

            out.append(BBBSearchResult(
                name=item.get("businessName", ""),
                address=item.get("address") or "",
                city=item.get("city") or "",
                state=item.get("state") or "",
                zip=item.get("postalcode") or "",
                phone=phone,
                rating=item.get("rating", ""),
                accredited=item.get("bbbMember", False),
                profile_url=item.get("reportUrl", ""),
                business_id=item.get("businessId", ""),
                source_query=f"{item.get('tobText', '')}|{item.get('bbbName', '')}",
                fetched_at=str(int(time.time())),
            ))
        except Exception as e:
            log.warning(f"Skipped malformed record: {e}")
    return out


# ── Profile enrichment (via proxy) ──────────────────────────────────────────

def enrich_profile(profile_url: str) -> dict:
    """Fetch BBB profile page via proxy and extract extra data."""
    if not profile_url or not BBB_PROXY:
        return {}
    try:
        with httpx.Client(proxy=BBB_PROXY, timeout=30, follow_redirects=True) as c:
            r = c.get(f"https://www.bbb.org{profile_url}", headers={"User-Agent": USER_AGENT})
            if r.status_code != 200 or "Just a moment" in r.text:
                return {}
            text = r.text
            result = {}
            # Phone
            m = re.search(r'\(?(\d{3}\.?\)?[\s.-]*\d{3}[\s.-]*\d{4})', text)
            if m:
                result["phone"] = m.group(1)
            # Email
            m = re.search(r'mailto:([\w.+-]+@[\w-]+\.[\w.]+)', text)
            if m:
                result["email"] = m.group(1)
            # Website
            m = re.search(r'https?://(?:www\.)?([a-z0-9-]+\.(?:com|org|net|info))', text, re.I)
            if m:
                result["website"] = m.group(1)
            # Years in business
            m = re.search(r'(\d+)\+?\s*years?\s*in\s*business', text, re.I)
            if m:
                result["years_in_business"] = m.group(1)
            return result
    except Exception as e:
        log.debug(f"Profile enrich failed for {profile_url}: {e}")
        return {}


# ── Database ops ─────────────────────────────────────────────────────────────

def ensure_schema(conn):
    """Create necessary tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scraper.bbb_listings (
                id BIGSERIAL PRIMARY KEY,
                business_id TEXT NOT NULL,
                business_name TEXT NOT NULL,
                address TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                phone TEXT,
                rating TEXT,
                accredited BOOLEAN DEFAULT FALSE,
                profile_url TEXT,
                source_query TEXT NOT NULL,
                email TEXT,
                website TEXT,
                years_in_business TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (business_id, source_query)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scraper.scrape_jobs (
                id BIGSERIAL PRIMARY KEY,
                job_type TEXT NOT NULL,
                source TEXT NOT NULL,
                keyword TEXT NOT NULL,
                location TEXT,
                status TEXT DEFAULT 'pending',
                rows_written INTEGER DEFAULT 0,
                error_msg TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );
        """)
        conn.commit()


def store_listing(conn, r: BBBSearchResult):
    """Store a BBB listing in the database."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scraper.bbb_listings
                (business_id, business_name, address, city, state, zip, phone,
                 rating, accredited, profile_url, source_query, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (business_id, source_query)
            DO UPDATE SET
                business_name = EXCLUDED.business_name,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip = EXCLUDED.zip,
                phone = EXCLUDED.phone,
                rating = EXCLUDED.rating,
                accredited = EXCLUDED.accredited,
                updated_at = NOW()
        """, (
            r.business_id, r.name, r.address, r.city, r.state, r.zip,
            json.dumps(r.phone), r.rating, r.accredited, r.profile_url, r.source_query,
        ))
    conn.commit()


def mark_enriched(conn, business_id: str, fields: dict):
    """Update an existing listing with enriched profile data."""
    if not fields:
        return
    with conn.cursor() as cur:
        if fields.get("email"):
            cur.execute(
                "UPDATE scraper.bbb_listings SET email=%s, updated_at=NOW() WHERE business_id=%s",
                (fields["email"], business_id),
            )
        if fields.get("website"):
            cur.execute(
                "UPDATE scraper.bbb_listings SET website=%s, updated_at=NOW() WHERE business_id=%s",
                (fields["website"], business_id),
            )
        if fields.get("years_in_business"):
            cur.execute(
                "UPDATE scraper.bbb_listings SET years_in_business=%s, updated_at=NOW() WHERE business_id=%s",
                (fields["years_in_business"], business_id),
            )
    conn.commit()


def record_job(conn, keyword: str, location: str) -> int:
    """Record a scrape job in the ledger."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scraper.scrape_jobs (job_type, source, keyword, location, status, started_at)
            VALUES ('search', 'bbb', %s, %s, 'running', NOW())
            RETURNING id
        """, (keyword, location))
        conn.commit()
        return cur.fetchone()[0]


def finish_job(conn, job_id: int, rows: int, status: str, err: str = ""):
    """Mark a scrape job as complete."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE scraper.scrape_jobs
            SET status=%s, rows_written=%s, error_msg=%s, completed_at=NOW()
            WHERE id=%s
        """, (status, rows, err, job_id))
        conn.commit()


# ── Main scrape ────────────────────────────────────────────────────────────

def scrape_query(conn, keyword: str, location: str, page_limit: int = 15):
    """Scrape all pages for a keyword/location combo."""
    job_id = record_job(conn, keyword, location)
    total = 0
    try:
        for page in range(1, page_limit + 1):
            resp = bbb_api_search(keyword, location, page=page)
            if not resp or not resp.get("results"):
                break
            results = parse_search_results(resp)
            filtered = [r for r in results if filter_result(r)]
            if not filtered:
                break

            # Store all filtered results
            for r in filtered:
                store_listing(conn, r)
            total += len(filtered)
            log.info(f"  page {page}: +{len(filtered)} (total {total})")

            # Enrich first few profiles
            for r in filtered[:3]:
                fields = enrich_profile(r.profile_url)
                if fields:
                    mark_enriched(conn, r.business_id, fields)
                time.sleep(PER_PROFILE_DELAY)

            if len(results) < BATCH_SIZE:
                break
            time.sleep(PER_PAGE_DELAY)

        finish_job(conn, job_id, total, "done")
        log.info(f"✓ {keyword} @ {location}: {total} stored")
        return total
    except Exception as e:
        finish_job(conn, job_id, total, "failed", str(e))
        log.error(f"✗ {keyword} @ {location}: {e}")
        return 0


# ── Daemon loop ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="BBB scraper for Nebraska")
    parser.add_argument("--keyword", default=None, help="Search keyword")
    parser.add_argument("--location", default=None, help="Location (city, state)")
    parser.add_argument("--page-limit", type=int, default=15, help="Max pages per query")
    parser.add_argument("--fast", action="store_true", help="Skip delay between pages")
    args = parser.parse_args()

    # Connect to database
    conn = psycopg.connect(
        host=os.environ.get("PG_HOST", "/var/run/postgresql"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", ""),
        dbname=os.environ.get("PG_DB", "infinitecrawler"),
    )
    ensure_schema(conn)
    log.info(f"BBB scraper started (proxy={'set' if BBB_PROXY else 'none'})")

    total = 0
    keyword = args.keyword or "Handyman Services"
    location = args.location or "Lincoln, NE"

    try:
        # Single query mode
        if args.keyword and args.location:
            count = scrape_query(conn, keyword, location, args.page_limit)
            total += count
            log.info(f"Single query complete: {total} results")

        # Full sweep mode (default)
        else:
            sweep = 0
            while True:
                sweep += 1
                log.info(f"=== Sweep {sweep} starting ===")
                for niche in NE_NICHES:
                    for loc in NE_CITIES:
                        count = scrape_query(conn, niche, loc, args.page_limit)
                        total += count
                        if not args.fast:
                            time.sleep(2)  # Throttle between queries
                    time.sleep(SWEEP_SLEEP)
                log.info(f"=== Sweep {sweep} complete ({total} total listings) ===")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
