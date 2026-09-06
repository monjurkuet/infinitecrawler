#!/usr/bin/env python3
"""BBB.org scraper daemon — searches via BBB /api/search JSON (httpx + proxy),
enriches profiles (httpx + CF proxy), filters, upserts to PG, loops forever."""

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
from dotenv import load_dotenv

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
PER_PAGE_DELAY = 1.0       # between API pages
PER_PROFILE_DELAY = 2.0    # between profile fetches
BATCH_SIZE = 15             # BBB fixed page size
MAX_PAGES_PER_QUERY = 15    # BBB hard cap (pages cap at 15 regardless)
SWEEP_SLEEP = 300           # between full niche×locale sweeps (5 min)

# CF-exit proxy — verified working for BBB profile HTML (2026-09-06)
BBB_PROXY = os.environ.get(
    "BBB_PROFILE_PROXY",
    "http://datasolved-cf:pBNV6W3G2uW8kJiM@gw.proxy.datasolved.org:10044",
)

NE_LOCALES = [
    "Lincoln, NE",
    "Omaha, NE",
    "Fairbury, NE",
    "Beatrice, NE",
    "Fremont, NE",
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

# ── Filter ────────────────────────────────────────────────────────────────────

_RATING_TIER = {"A+": 6, "A": 5, "A-": 4, "B+": 3, "B": 2, "B-": 1}
MIN_RATING = "B"


def filter_result(r: "BBBSearchResult") -> bool:
    if not r.name or not r.business_id:
        return False
    if not r.phone:
        return False
    if not (r.city and r.state):
        return False
    if _RATING_TIER.get(r.rating, -1) < _RATING_TIER.get(MIN_RATING, 0):
        return False
    return True


# ── Model ─────────────────────────────────────────────────────────────────────

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


# ── API + Profile fetch ───────────────────────────────────────────────────────

def bbb_api_search(keyword: str, location: str, page: int = 1) -> dict:
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
        log.error(f"search fail keyword='{keyword}' loc='{location}' p={page}: {e}")
        return {}


def parse_search_results(response: dict) -> list[BBBSearchResult]:
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
            log.warning(f"parse row skipped: {e}")
    return out


def enrich_profile(profile_url: str) -> dict:
    """HTTP fetch via CF proxy. Returns dict of extra fields (email/website/years)."""
    if not profile_url:
        return {}
    try:
        with httpx.Client(proxy=BBB_PROXY, timeout=30, follow_redirects=True) as c:
            r = c.get(f"https://www.bbb.org{profile_url}",
                      headers={"User-Agent": USER_AGENT})
            if r.status_code != 200 or "Just a moment" in r.text:
                return {}
            text = r.text
            out = {}
            m = re.search(r'mailto:([\w.+-]+@[\w-]+\.[\w.]+)', text)
            if m:
                out["email"] = m.group(1)
            # Website link on profile (first non-bbb external link near business info)
            for cand in re.findall(r'https?://[\w.-]+\.[a-z]{2,}', text):
                if "bbb.org" not in cand and "adobedtm" not in cand and "googletagmanager" not in cand:
                    out["website"] = cand
                    break
            m = re.search(r"Years in Business[:\s]+(\d+)", text, re.I)
            if m:
                out["years_in_business"] = m.group(1)
            return out
    except Exception as e:
        log.debug(f"profile enrich fail {profile_url}: {e}")
        return {}


# ── Storage ───────────────────────────────────────────────────────────────────

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS scraper.bbb_listings (
            id BIGSERIAL PRIMARY KEY,
            business_id TEXT NOT NULL,
            business_name TEXT NOT NULL,
            address TEXT, city TEXT, state TEXT, zip TEXT,
            phone TEXT, rating TEXT, accredited BOOLEAN DEFAULT FALSE,
            profile_url TEXT, source_query TEXT NOT NULL,
            email TEXT, website TEXT, years_in_business TEXT,
            niche_tags TEXT[] DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (business_id, source_query)
        );
        """)
        for col in ("email TEXT", "website TEXT", "years_in_business TEXT"):
            try:
                cur.execute(f"ALTER TABLE scraper.bbb_listings ADD COLUMN IF NOT EXISTS {col}")
            except Exception:
                pass
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


def store_result(conn, r: BBBSearchResult):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO scraper.bbb_listings
            (business_id, business_name, address, city, state, zip, phone, rating,
             accredited, profile_url, source_query, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (business_id, source_query) DO UPDATE SET
            business_name = EXCLUDED.business_name,
            address = EXCLUDED.address, city = EXCLUDED.city, state = EXCLUDED.state,
            zip = EXCLUDED.zip, phone = EXCLUDED.phone, rating = EXCLUDED.rating,
            accredited = EXCLUDED.accredited, updated_at = NOW()
        """, (
            r.business_id, r.name, r.address, r.city, r.state, r.zip,
            json.dumps(r.phone), r.rating, r.accredited, r.profile_url, r.source_query,
        ))


def enrich_and_store(conn, r: BBBSearchResult):
    fields = enrich_profile(r.profile_url)
    if not fields:
        return
    with conn.cursor() as cur:
        if fields.get("email"):
            cur.execute("UPDATE scraper.bbb_listings SET email=%s, updated_at=NOW() WHERE business_id=%s",
                        (fields["email"], r.business_id))
        if fields.get("website"):
            cur.execute("UPDATE scraper.bbb_listings SET website=%s, updated_at=NOW() WHERE business_id=%s",
                        (fields["website"], r.business_id))
        if fields.get("years_in_business"):
            cur.execute("UPDATE scraper.bbb_listings SET years_in_business=%s, updated_at=NOW() WHERE business_id=%s",
                        (fields["years_in_business"], r.business_id))
    conn.commit()


# ── Job ledger ────────────────────────────────────────────────────────────────

def _record_job(conn, keyword: str, location: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO scraper.scrape_jobs (job_type, source, keyword, location, status, started_at)
        VALUES ('search', 'bbb', %s, %s, 'running', NOW()) RETURNING id
        """, (keyword, location))
        conn.commit()
        return cur.fetchone()[0]


def _finish_job(conn, job_id: int, rows: int, status: str, err: str = ""):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE scraper.scrape_jobs
        SET status=%s, rows_written=%s, completed_at=NOW(), error_msg=%s WHERE id=%s
        """, (status, rows, err, job_id))
        conn.commit()


# ── Main loop ─────────────────────────────────────────────────────────────────

def scrape_query(conn, keyword: str, location: str, page_limit: int) -> int:
    job_id = _record_job(conn, keyword, location)
    total = 0
    try:
        for page in range(1, page_limit + 1):
            resp = bbb_api_search(keyword, location, page=page)
            if not resp or not resp.get("results"):
                break
            items = parse_search_results(resp)
            filtered = [r for r in items if filter_result(r)]
            for r in filtered:
                store_result(conn, r)
                total += 1
            conn.commit()
            # Profile enrichment for filtered results
            for r in filtered[:3]:
                enrich_and_store(conn, r)
                time.sleep(PER_PROFILE_DELAY)
            if len(items) < BATCH_SIZE:
                break
            time.sleep(PER_PAGE_DELAY)
        _finish_job(conn, job_id, total, "done")
        log.info(f"done {keyword} @ {location}: {total} rows")
        return total
    except Exception as e:
        _finish_job(conn, job_id, total, "failed", str(e))
        log.error(f"fail {keyword} @ {location}: {e}")
        return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword")
    parser.add_argument("--location")
    parser.add_argument("--page-limit", type=int, default=15)
    args = parser.parse_args()

    conn = psycopg.connect(
        host=os.environ.get("PG_HOST", "/var/run/postgresql"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD"),
        dbname=os.environ.get("PG_DB", "infinitecrawler"),
        autocommit=False,
    )
    ensure_schema(conn)
    log.info(f"BBB scraper started (proxy={'set' if BBB_PROXY else 'none'})")

    try:
        sweep = 0
        while True:
            sweep += 1
            log.info(f"=== sweep {sweep} starting ({len(NE_NICHES)} niches x {len(NE_LOCALES)} locales) ===")
            for niche in NE_NICHES:
                for loc in NE_LOCALES:
                    scrape_query(conn, niche, loc, args.page_limit)
                    time.sleep(2)
            log.info(f"=== sweep {sweep} complete; sleeping {SWEEP_SLEEP}s ===")
            time.sleep(SWEEP_SLEEP)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
