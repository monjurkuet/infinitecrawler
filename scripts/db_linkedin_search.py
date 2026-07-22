#!/usr/bin/env python3
"""db_linkedin_search.py — Discover LinkedIn employee profiles via DDGS search.

Reads listings from scraper.gmaps_listings that haven't been searched for
LinkedIn profiles recently. For each, queries the DDGS search API with
site:linkedin.com/in/ queries, parses results, scores confidence, and writes
to scraper.linkedin_profiles.

Uses sector-aware role keywords when the listing has a sector_id.

Usage:
    uv run python scripts/db_linkedin_search.py                    # up to 100
    uv run python scripts/db_linkedin_search.py --max 200          # limit
    uv run python scripts/db_linkedin_search.py --dry-run          # preview
    uv run python scripts/db_linkedin_search.py --stats            # stats only
    uv run python scripts/db_linkedin_search.py --sector Software  # filter by sector
"""

import argparse
import asyncio
import logging
import re
import sys
from pathlib import Path
from typing import Optional

import httpx
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_pg_config, get_unprocessed_linkedin, upsert_linkedin_profiles  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("db_linkedin_search")

DEFAULT_MAX_LISTINGS = 100
DDGS_BASE_URL = "https://search.datasolved.org/search/text"
REQUEST_DELAY = 2.0  # seconds between API calls (rate limiting)
MAX_RESULTS_PER_QUERY = 5

# ── Sector → role keywords ────────────────────────────────────────────────────
# When a listing has a sector_id, we append targeted role keywords to the query.

SECTOR_ROLE_KEYWORDS = {
    "Software": ['"software engineer"', '"developer"', '"tech lead"', '"CTO"'],
    "BIM": ['"BIM"', '"CAD"', '"architect"', '"MEP"'],
    "Construction-Real-Estate": ['"architect"', '"engineer"', '"project manager"', '"real estate"'],
    "Healthcare-Pharma": ['"doctor"', '"pharmacist"', '"medical"', '"healthcare"'],
    "Media-Marketing-Digital": ['"marketing"', '"digital"', '"media"', '"content"'],
    "Clothing-Fashion": ['"designer"', '"fashion"', '"retail"', '"merchandiser"'],
    "Electronics-Gadgets": ['"engineer"', '"electronics"', '"technician"', '"IT"'],
    "Food-Beverage": ['"manager"', '"chef"', '"restaurant"', '"food"'],
    "Education-Training": ['"teacher"', '"trainer"', '"instructor"', '"education"'],
    "Logistics-Transport": ['"logistics"', '"transport"', '"supply chain"', '"driver"'],
    "Agriculture-Agro": ['"agriculture"', '"farmer"', '"agro"', '"food"'],
    "Travel-Tourism": ['"travel"', '"tour"', '"hospitality"', '"hotel"'],
    "Service-Agents-Distribution": ['"sales"', '"agent"', '"distributor"', '"service"'],
    "Business-Support": ['"administrator"', '"executive"', '"manager"', '"director"'],
}

# Default keywords used when sector is unknown or not in the map
DEFAULT_ROLE_KEYWORDS = [
    '"owner"', '"director"', '"manager"', '"executive"',
]


def confidence_from_result(company_name: str, result: dict) -> float:
    """Score how likely this LinkedIn profile belongs to the target company.

    Returns a float 0.0 – 1.0.
    """
    score = 0.0
    title = result.get("title", "")
    href = result.get("href", "")
    body = result.get("body", "")

    company_lower = company_name.lower()

    # Signal 1: Company name in snippet body (+0.4)
    if company_lower in body.lower():
        score += 0.4

    # Signal 2: URL path contains company name substring (+0.3)
    # e.g. /in/acme-software-123
    url_path = href.lower().split("/in/")[-1] if "/in/" in href else ""
    # Check if significant part of company name appears in URL
    company_words = [w for w in re.split(r"[\s\-_]+", company_lower) if len(w) > 2]
    for word in company_words:
        if word in url_path and len(word) > 3:
            score += 0.15
            break

    # Signal 3: Title contains role keywords (+0.2)
    role_words = [
        "engineer", "developer", "manager", "director", "founder", "owner",
        "CTO", "CEO", "president", "lead", "head", "architect", "consultant",
        "specialist", "coordinator", "executive", "partner",
    ]
    title_lower = title.lower() + body.lower()
    for role in role_words:
        if role in title_lower:
            score += 0.2
            break

    # Signal 4: Body mentions connections / experience (+0.1)
    if "connection" in body.lower() or "experience" in body.lower():
        score += 0.1

    return min(score, 1.0)


def build_queries(company_name: str, sector: Optional[str] = None) -> list[str]:
    """Build search queries for a company.

    Returns ordered list of queries — primary first, then role-targeted.
    Uses `AND` between words rather than a quoted phrase for better DDGS
    matching against LinkedIn profile snippet text.
    """
    # Split company name into significant words (>2 chars) and join with AND
    words = [w for w in company_name.split() if len(w) > 2]
    if not words:
        words = [company_name]

    # Primary: "Word1" AND "Word2" AND ... with site:linkedin.com/in/
    and_chain = ' AND '.join(f'"{w}"' for w in words)
    primary = f"site:linkedin.com/in/ {and_chain}"

    # Also try a shorter version with just the first 2 words (some profiles
    # truncate company names)
    short_and = ' AND '.join(f'"{w}"' for w in words[:2])
    queries = [primary]
    if short_and != primary:
        queries.append(f"site:linkedin.com/in/ {short_and}")

    # Role-targeted queries (if sector known)
    keywords = SECTOR_ROLE_KEYWORDS.get(sector, DEFAULT_ROLE_KEYWORDS)
    for kw in keywords[:2]:  # max 2 role queries
        queries.append(f"site:linkedin.com/in/ {and_chain} {kw}")

    return queries


def extract_name_from_title(title: str) -> Optional[str]:
    """Extract person name from a LinkedIn search result title.

    Format is typically: "John Doe - Title at Company | LinkedIn"
    """
    # Take everything before the first " - "
    parts = title.split(" - ")
    if parts:
        name = parts[0].strip()
        # Basic validation: 2+ chars, not URL-like
        if len(name) > 2 and not name.startswith("http"):
            return name
    return None


def extract_title_from_title(title: str) -> Optional[str]:
    """Extract job title from search result title."""
    parts = title.split(" - ")
    if len(parts) >= 2:
        title_part = parts[1].strip()
        # Remove trailing " | LinkedIn"
        title_part = re.sub(r"\s*\|.*", "", title_part)
        return title_part
    return None


async def search_linkedin(
    client: httpx.AsyncClient, company: dict, sector: Optional[str]
) -> list[dict]:
    """Run DDGS searches for a company and return found LinkedIn profiles.

    Returns list of profile dicts ready for upsert_linkedin_profiles().
    """
    listing_id = company["id"]
    company_name = company["name"]
    all_results: list[dict] = []
    seen_urls: set[str] = set()

    queries = build_queries(company_name, sector)

    for query in queries:
        try:
            resp = await client.get(
                DDGS_BASE_URL,
                params={
                    "query": query,
                    "max_results": MAX_RESULTS_PER_QUERY,
                    "region": "bd-bn",  # Bangladesh + international
                },
            )
            if resp.status_code != 200:
                log.debug("DDGS returned %d for query: %s", resp.status_code, query[:50])
                await asyncio.sleep(REQUEST_DELAY)
                continue

            data = resp.json()
            results = data.get("results", [])

            for r in results:
                href = r.get("href", "")
                # Only consider LinkedIn profile URLs (not company pages, search pages)
                if "linkedin.com/in/" not in href:
                    continue
                if href in seen_urls:
                    continue
                seen_urls.add(href)

                confidence = confidence_from_result(company_name, r)
                profile = {
                    "listing_id": listing_id,
                    "company_name": company_name,
                    "profile_url": href,
                    "full_name": extract_name_from_title(r.get("title", "")),
                    "profile_title": extract_title_from_title(r.get("title", "")),
                    "search_query": query,
                    "confidence": round(confidence, 2),
                    "snippet": r.get("body", "")[:300],
                }
                all_results.append(profile)

            # Rate limit between queries
            await asyncio.sleep(REQUEST_DELAY)

        except Exception as e:
            log.debug("DDGS search failed for '%s': %s", query[:50], e)
            await asyncio.sleep(REQUEST_DELAY)

    log.debug("Found %d unique LinkedIn profiles for '%s'", len(all_results), company_name[:40])
    return all_results


async def process_batch(
    conn, listings: list[dict], sector_filter: Optional[str], dry_run: bool
) -> tuple[int, int]:
    """Process a batch of listings.

    Returns (listings_processed, profiles_found).
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as client:
        processed = 0
        total_profiles = 0

        for listing in listings:
            company_sector = None
            if sector_filter:
                company_sector = sector_filter

            if dry_run:
                log.info("  [DRY-RUN] Would search '%s' (id=%d)",
                         listing["name"], listing["id"])
                processed += 1
                continue

            profiles = await search_linkedin(client, listing, company_sector)
            processed += 1

            if profiles:
                written = upsert_linkedin_profiles(conn, profiles)
                total_profiles += written
                log.info("  [%d] '%s' → %d profiles (written: %d)",
                         listing["id"], listing["name"][:35], len(profiles), written)
            else:
                log.debug("  [%d] '%s' → no profiles found",
                          listing["id"], listing["name"][:35])

    return processed, total_profiles


def show_stats(conn):
    """Print LinkedIn profile discovery statistics."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM scraper.linkedin_profiles")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT listing_id) FROM scraper.linkedin_profiles")
        listings_with = cur.fetchone()[0]

        cur.execute("""
            SELECT ROUND(AVG(confidence)::numeric, 2)
            FROM scraper.linkedin_profiles
            WHERE checked_at > NOW() - INTERVAL '30 days'
        """)
        avg_conf = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT confidence, COUNT(*)
            FROM scraper.linkedin_profiles
            GROUP BY confidence
            ORDER BY confidence
        """)
        conf_dist = cur.fetchall()

    print("\n" + "=" * 55)
    print("  LinkedIn Profile Discovery Stats")
    print("=" * 55)
    print(f"  Total profiles found:              {total:>6}")
    print(f"  Unique listings with profiles:     {listings_with:>6}")
    print(f"  Avg confidence (30d):              {avg_conf:>6}")
    print("\n  Confidence distribution:")
    for conf, count in conf_dist:
        bar = "█" * min(count, 20)
        print(f"    {conf:.2f} [{bar:<20}] {count}")
    print(f"{'=' * 55}")


def main():
    parser = argparse.ArgumentParser(description="LinkedIn employee discovery via DDGS")
    parser.add_argument("--max", type=int, default=DEFAULT_MAX_LISTINGS,
                        help=f"Max listings to process (default: {DEFAULT_MAX_LISTINGS})")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    parser.add_argument("--sector", type=str, default=None,
                        help="Filter by sector name (e.g. 'Software')")
    args = parser.parse_args()

    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    conn.autocommit = False

    try:
        if args.stats:
            show_stats(conn)
            return

        # Fetch unprocessed listings (optionally filtered by sector)
        listings = get_unprocessed_linkedin(conn, limit=args.max)

        if args.sector:
            sector_filter = args.sector
            # Fetch listings with that sector name from PG
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name FROM scraper.gmaps_listings "
                    "WHERE name IS NOT NULL AND name != '' "
                    "AND sector_id = (SELECT id FROM scraper.sectors WHERE name = %s) "
                    "AND id NOT IN (SELECT listing_id FROM scraper.linkedin_profiles "
                    "  WHERE checked_at > NOW() - INTERVAL '7 days') "
                    "ORDER BY updated_at DESC LIMIT %s",
                    (sector_filter, args.max),
                )
                rows = cur.fetchall()
                listings = [{"id": r[0], "name": r[1]} for r in rows]

        if not listings:
            log.info("No unprocessed listings found.")
            if args.sector:
                log.info("  (filtered by sector: %s)", args.sector)
            return

        log.info("Processing %d listing(s) for LinkedIn profiles", len(listings))

        if args.dry_run:
            log.info("=== DRY RUN === (no writes)")
            for lead in listings[:5]:
                log.info("  Would search: '%s' (id=%d)", lead["name"], lead["id"])
            if len(listings) > 5:
                log.info("  ... and %d more", len(listings) - 5)
            return

        processed, found = asyncio.run(
            process_batch(conn, listings, args.sector, dry_run=False)
        )

        log.info("Done: searched %d listings, found %d LinkedIn profiles",
                 processed, found)

    finally:
        conn.close()


if __name__ == "__main__":
    main()