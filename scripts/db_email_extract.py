#!/usr/bin/env python3
"""db_email_extract.py — Offline HTTP-based email extraction backfill.

Reads listings from scraper.gmaps_listings WHERE website is present but no
emails have been extracted yet. Fetches each website via httpx, scans for
email addresses (standard + obfuscated + mailto), upserts to scraper.emails.

Designed for cron — runs periodically to backfill and catch new listings
that were missed by the inline extraction in listing_daemon.py.

Usage:
    uv run python scripts/db_email_extract.py                    # up to 500
    uv run python scripts/db_email_extract.py --max 200          # limit
    uv run python scripts/db_email_extract.py --dry-run          # preview only
    uv run python scripts/db_email_extract.py --stats            # stats only
"""

import argparse
import asyncio
import logging
import sys

from pathlib import Path

import httpx
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.email_extractor import (  # noqa: E402
    scan_text_for_emails,
    extract_mailto_links,
    filter_noise,
    deduplicate_emails,
)
from utils.pg import get_pg_config, get_unprocessed_emails, upsert_emails  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("db_email_extract")

DEFAULT_MAX_LISTINGS = 500
DEFAULT_CONCURRENCY = 10  # parallel httpx fetches
FETCH_TIMEOUT = 12  # seconds per website fetch


async def extract_listing(client: httpx.AsyncClient, listing: dict) -> list[dict]:
    """Fetch one website and return email upsert dicts."""
    listing_id = listing["id"]
    website = listing["website"]

    if not website.startswith(("http://", "https://")):
        website = f"https://{website}"

    results: list[dict] = []

    try:
        resp = await client.get(website)
        if resp.status_code != 200:
            log.debug("HTTP %d for listing %d: %s", resp.status_code, listing_id, website[:50])
            return results

        html = resp.text

        # 1. Standard + obfuscated
        found = scan_text_for_emails(html)

        # 2. mailto: links
        mailto_emails = extract_mailto_links(html)
        for email in mailto_emails:
            if not any(e["email"] == email for e in found):
                found.append({
                    "email": email,
                    "is_obfuscated": False,
                    "context_snippet": f"mailto:{email}",
                })

        # 3. Filter + dedup
        found = filter_noise(found)
        found = deduplicate_emails(found)

        for e in found:
            results.append({
                "listing_id": listing_id,
                "website_url": website,
                "email": e["email"],
                "email_type": "general",
                "extraction_method": "http",
                "is_obfuscated": e["is_obfuscated"],
                "context_snippet": e.get("context_snippet", "")[:200],
            })

        if results:
            log.debug("Found %d email(s) for listing %d", len(results), listing_id)

    except httpx.TimeoutException:
        log.debug("Timeout listing %d: %s", listing_id, website[:50])
    except Exception as e:
        log.debug("Error listing %d: %s — %s", listing_id, website[:50], e)

    return results


async def process_batch(
    conn, listings: list[dict], concurrency: int, dry_run: bool
) -> tuple[int, int]:
    """Process a batch of listings concurrently.

    Returns (listings_processed, emails_written).
    """
    semaphore = asyncio.Semaphore(concurrency)
    listings_processed = 0
    emails_written = 0

    async def process_one(listing: dict):
        nonlocal listings_processed, emails_written
        async with semaphore:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(FETCH_TIMEOUT),
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/125.0.0.0 Safari/537.36"
                    ),
                },
            ) as client:
                results = await extract_listing(client, listing)
                listings_processed += 1
                if results and not dry_run:
                    written = upsert_emails(conn, results)
                    emails_written += written

    tasks = [process_one(lead) for lead in listings]
    await asyncio.gather(*tasks, return_exceptions=True)
    return listings_processed, emails_written


def show_stats(conn):
    """Print email extraction statistics."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM scraper.emails")
        total_emails = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT listing_id) FROM scraper.emails")
        listings_with_email = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM scraper.gmaps_listings
            WHERE website IS NOT NULL AND website != ''
        """)
        listings_with_website = cur.fetchone()[0]

        cur.execute("""
            SELECT extraction_method, COUNT(*)
            FROM scraper.emails GROUP BY extraction_method ORDER BY 2 DESC
        """)
        methods = cur.fetchall()

    print("\n" + "=" * 55)
    print("  Email Extraction Stats")
    print("=" * 55)
    print(f"  Total emails extracted:        {total_emails:>6}")
    print(f"  Listings with emails:          {listings_with_email:>6}")
    print(f"  Listings with website (total): {listings_with_website:>6}")
    print(f"  Coverage: {listings_with_email / max(listings_with_website, 1) * 100:.1f}%")
    print("\n  By extraction method:")
    for method, count in methods:
        print(f"    {method:<25} {count:>6}")
    print(f"{'=' * 55}")


def main():
    parser = argparse.ArgumentParser(description="Offline email extraction backfill")
    parser.add_argument("--max", type=int, default=DEFAULT_MAX_LISTINGS,
                        help=f"Max listings to process (default: {DEFAULT_MAX_LISTINGS})")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        help=f"Parallel fetches (default: {DEFAULT_CONCURRENCY})")
    args = parser.parse_args()

    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    conn.autocommit = False

    try:
        if args.stats:
            show_stats(conn)
            return

        listings = get_unprocessed_emails(conn, limit=args.max)
        if not listings:
            log.info("No listings with unprocessed emails found.")
            return

        log.info("Found %d listings needing email extraction (limit: %d)",
                 len(listings), args.max)

        if args.dry_run:
            log.info("=== DRY RUN === (no writes)")
            for lead in listings[:5]:
                log.info("  [%d] %s", lead["id"], lead["website"][:60])
            if len(listings) > 5:
                log.info("  ... and %d more", len(listings) - 5)
            return

        processed, written = asyncio.run(
            process_batch(conn, listings, args.concurrency, dry_run=False)
        )

        log.info("Done: processed %d / %d listings, wrote %d emails",
                 processed, len(listings), written)

    finally:
        conn.close()


if __name__ == "__main__":
    main()