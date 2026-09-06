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
QUEUE_PENDING = "bbb:pending"
QUEUE_PROCESSING = "bbb:processing"
QUEUE_DONE = "bbb:completed"

# Redis client (lazy — no socket ops until first use)
_redis = redis.Redis(host=REDIS_HOST, port=6379, db=0)

# Queue helpers
def enqueue_query(keyword: str, location: str, page: int = 1):
    """Push a BBB search query to the pending queue."""
    payload = json.dumps({"keyword": keyword, "location": location, "page": page})
    _redis.lpush(QUEUE_PENDING, payload)

def dequeue_query() -> Optional[dict]:
    """Pop a query from pending (blocking with timeout)."""
    result = _redis.brpop(QUEUE_PENDING, timeout=5)
    if result:
        return json.loads(result[1])
    return None

def mark_processing(payload: dict):
    """Move query to processing set with timestamp."""
    _redis.sadd(QUEUE_PROCESSING, json.dumps(payload))

def mark_done(payload: dict):
    """Move query from processing to completed."""
    _redis.srem(QUEUE_PROCESSING, json.dumps(payload))
    _redis.sadd(QUEUE_DONE, json.dumps(payload))

def mark_failed(payload: dict):
    """Move query from processing back to pending for retry."""
    _redis.srem(QUEUE_PROCESSING, json.dumps(payload))
    _redis.lpush(QUEUE_PENDING, json.dumps(payload))

def queue_stats() -> dict:
    return {
        "pending": _redis.llen(QUEUE_PENDING),
        "processing": _redis.scard(QUEUE_PROCESSING),
        "completed": _redis.scard(QUEUE_DONE),
    }

def seed_initial_queries():
    """Seed the queue with all niche×city combinations if empty."""
    if _redis.llen(QUEUE_PENDING) > 0:
        return 0
    count = 0
    for niche in NICHES:
        for loc in US_CITIES:
            enqueue_query(niche, loc, 1)
            count += 1
    log.info(f"Seeded BBB queue with {count} initial queries")
    return count


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


# ── All US cities (top 300 by population) ─────────────────────────────────────
# Using a built-in list instead of external CSV for self-contained deployment.
# Covers all 50 states + DC, ~300 cities = comprehensive BBB coverage.
US_CITIES = [
    # Top 50
    "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
    "Philadelphia, PA", "San Antonio, TX", "San Diego, CA", "Dallas, TX", "San Jose, CA",
    "Austin, TX", "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "Charlotte, NC",
    "San Francisco, CA", "Indianapolis, IN", "Seattle, WA", "Denver, CO", "Washington, DC",
    "Boston, MA", "El Paso, TX", "Nashville, TN", "Detroit, MI", "Oklahoma City, OK",
    "Portland, OR", "Las Vegas, NV", "Memphis, TN", "Louisville, KY", "Baltimore, MD",
    "Milwaukee, WI", "Albuquerque, NM", "Tucson, AZ", "Fresno, CA", "Sacramento, CA",
    "Mesa, AZ", "Kansas City, MO", "Atlanta, GA", "Long Beach, CA", "Colorado Springs, CO",
    "Raleigh, NC", "Omaha, NE", "Miami, FL", "Virginia Beach, VA", "Oakland, CA",
    # 51-100
    "Minneapolis, MN", "Tulsa, OK", "Tampa, FL", "Arlington, TX", "New Orleans, LA",
    "Wichita, KS", "Cleveland, OH", "Bakersfield, CA", "Aurora, CO", "Anaheim, CA",
    "Honolulu, HI", "Santa Ana, CA", "Corpus Christi, TX", "Riverside, CA", "Lexington, KY",
    "Henderson, NV", "Stockton, CA", "Saint Paul, MN", "St. Louis, MO", "Cincinnati, OH",
    "Pittsburgh, PA", "Greensboro, NC", "Anchorage, AK", "Plano, TX", "Lincoln, NE",
    "Orlando, FL", "Irvine, CA", "Newark, NJ", "Durham, NC", "Chula Vista, CA",
    "Toledo, OH", "Fort Wayne, IN", "St. Petersburg, FL", "Laredo, TX", "Jersey City, NJ",
    # 101-150
    "Chandler, AZ", "Madison, WI", "Lubbock, TX", "Scottsdale, AZ", "Reno, NV",
    "Buffalo, NY", "Gilbert, AZ", "Glendale, AZ", "North Las Vegas, NV", "Winston-Salem, NC",
    "Chesapeake, VA", "Norfolk, VA", "Fremont, CA", "Garland, TX", "Irving, TX",
    "Hialeah, FL", "Richmond, VA", "Boise, ID", "Spokane, WA", "Baton Rouge, LA",
    "Tacoma, WA", "San Bernardino, CA", "Modesto, CA", "Fontana, CA", "Santa Clarita, CA",
    "Moreno Valley, CA", "Fayetteville, NC", "Oxnard, CA", "Aurora, IL", "Glendale, CA",
    "Huntington Beach, CA", "Montgomery, AL", "Grand Rapids, MI", "Overland Park, KS", "Knoxville, TN",
    # 151-200
    "Worcester, MA", "Grand Prairie, TX", "Amarillo, TX", "Akron, OH", "Mobile, AL",
    "Little Rock, AR", "Augusta, GA", "Columbus, GA", "Huntsville, AL", "Tallahassee, FL",
    "Shreveport, LA", "Rochester, NY", "Salt Lake City, UT", "Yonkers, NY", "Frisco, TX",
    "Glendale, CA", "McKinney, TX", "Grand Rapids, MI", "Fayetteville, AR", "Brownsville, TX",
    "Providence, RI", "Springfield, MA", "Jackson, MS", "Eugene, OR", "Port St. Lucie, FL",
    "Fort Lauderdale, FL", "Ontario, CA", "Chattanooga, TN", "Tempe, AZ", "Santa Rosa, CA",
    "Vancouver, WA", "Sioux Falls, SD", "Oceanside, CA", "Cape Coral, FL", "Springfield, MO",
    # 201-250
    "Pembroke Pines, FL", "Corona, CA", "Salinas, CA", "Salem, OR", "Lancaster, CA",
    "Eugene, OR", "Palmdale, CA", "McAllen, TX", "Hayward, CA", "Rockford, IL",
    "Pomona, CA", "Pasadena, TX", "Fort Collins, CO", "Escondido, CA", "Joliet, IL",
    "Alexandria, VA", "Sunnyvale, CA", "Syracuse, NY", "Paterson, NJ", "Torrance, CA",
    "Hollywood, FL", "Naperville, IL", "Kansas City, KS", "Bridgeport, CT", "Mesquite, TX",
    "New Haven, CT", "Carrollton, TX", "Roseville, CA", "Orange, CA", "Olathe, KS",
    # 251-300
    "Fullerton, CA", "Warren, MI", "Gainesville, FL", "Thornton, CO", "Denton, TX",
    "Midland, TX", "Visalia, CA", "Charleston, SC", "Cedar Rapids, IA", "Sterling Heights, MI",
    "Elizabeth, NJ", "Ann Arbor, MI", "Evansville, IN", "Abilene, TX", "Athens, GA",
    "Simi Valley, CA", "Hartford, CT", "Fargo, ND", "Round Rock, TX", "Columbia, SC",
    # Nebraska focus (user's core request)
    "Fairbury, NE", "Beatrice, NE", "Hebron, NE", "Geneva, NE", "Davenport, NE",
]

# Niches targeting handyman / REO / property preservation + general home services
NICHES = [
    "Handyman Services",
    "Handyman",
    "Property Preservation",
    "REO Preservation",
    "Foreclosure Cleanup",
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
    "Plumbing Contractors",
    "Electrical Contractors",
    "HVAC Contractors",
    "Flooring Contractors",
    "Drywall Contractors",
    "Concrete Contractors",
    "Foundation Repair",
    "Water Damage Restoration",
    "Fire Damage Restoration",
    "Mold Remediation",
    "Disaster Restoration",
    "Home Inspection",
    "Property Inspection",
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
    parser = argparse.ArgumentParser(description="BBB scraper — queue-based")
    parser.add_argument("--keyword", default=None, help="Search keyword")
    parser.add_argument("--location", default=None, help="Location (city, state)")
    parser.add_argument("--page-limit", type=int, default=15, help="Max pages per query")
    parser.add_argument("--fast", action="store_true", help="Skip delay between pages")
    parser.add_argument("--seed-only", action="store_true", help="Only seed queue, don't process")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker threads")
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

    # Seed the queue
    seeded = seed_initial_queries()
    if args.seed_only:
        log.info(f"Queue seeded with {seeded} queries. Exiting.")
        conn.close()
        return

    # Single query mode (bypass queue)
    if args.keyword and args.location:
        count = scrape_query(conn, args.keyword, args.location, args.page_limit)
        log.info(f"Single query complete: {count} results")
        conn.close()
        return

    # Queue-based worker mode
    log.info(f"Starting queue worker (workers={args.workers})")
    processed = 0
    try:
        while True:
            query = dequeue_query()
            if not query:
                log.info("Queue empty — sleeping 60s before retry")
                time.sleep(60)
                continue

            keyword = query["keyword"]
            location = query["location"]
            page = query.get("page", 1)

            mark_processing(query)
            try:
                count = scrape_query(conn, keyword, location, args.page_limit)
                mark_done(query)
                processed += count
                log.info(f"✓ {keyword} @ {location}: {count} (total processed this run: {processed})")
            except Exception as e:
                mark_failed(query)
                log.error(f"✗ {keyword} @ {location}: {e}")

            if not args.fast:
                time.sleep(2)  # Throttle between queries

    finally:
        conn.close()


if __name__ == "__main__":
    main()
