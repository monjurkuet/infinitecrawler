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
import os
import sys
import time

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
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("db_email_extract")
log.setLevel(logging.INFO)
_h = logging.StreamHandler()
_h.setLevel(logging.INFO)
_h.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
log.handlers.clear()
log.addHandler(_h)
log.propagate = False

DEFAULT_MAX_LISTINGS = 500
DEFAULT_CONCURRENCY = 25  # parallel httpx fetches
FETCH_TIMEOUT = 8  # seconds per website fetch
MAX_HTML_BYTES = 2 * 1024 * 1024  # cap page size for regex safety
HEARTBEAT_INTERVAL = 30  # seconds between progress logs
PIDFILE = REPO_ROOT / "_system" / "email_extract.pid"
PATH_CANDIDATES = ["contact", "contact-us", "about", "about-us", "team"]
MAX_REDIRECTS = 3


def acquire_lock() -> bool:
    """Prevent overlapping runs: exit early if a previous instance is alive."""
    if not PIDFILE.exists():
        return True
    try:
        pid = int(PIDFILE.read_text().strip())
        os.kill(pid, 0)  # signal 0 = existence check only
        log.warning("Another email-extract instance is running (pid %d) — exiting.", pid)
        return False
    except (ValueError, ProcessLookupError, PermissionError):
        # Stale pidfile (dead process or unreadable) — safe to proceed
        return True


def release_lock() -> None:
    try:
        PIDFILE.unlink(missing_ok=True)
    except OSError:
        pass


async def extract_listing(client: httpx.AsyncClient, listing: dict) -> list[dict]:
    """Fetch one website (homepage + contact-page candidates) and return email dicts."""
    listing_id = listing["id"]
    website = listing["website"]

    if not website.startswith(("http://", "https://")):
        website = f"https://{website}"

    base = _base_url(website)
    urls = [website] + [f"{base.rstrip('/')}/{p}" for p in PATH_CANDIDATES]

    found: list[dict] = []
    deadline = asyncio.get_running_loop().time() + FETCH_TIMEOUT * 2

    for url in urls:
        if asyncio.get_running_loop().time() > deadline:
            break
        try:
            async with asyncio.timeout(FETCH_TIMEOUT + 5):
                resp = await client.get(url)
        except (httpx.TimeoutException, TimeoutError):
            continue
        except Exception:
            continue
        if resp.status_code != 200:
            continue

        html = resp.text[:MAX_HTML_BYTES]

        # 1. Standard + obfuscated
        page_emails = scan_text_for_emails(html)

        # 2. mailto: links
        mailto_emails = extract_mailto_links(html)
        for email in mailto_emails:
            if not any(e["email"] == email for e in page_emails):
                page_emails.append({
                    "email": email,
                    "is_obfuscated": False,
                    "context_snippet": f"mailto:{email}",
                })

        found.extend(page_emails)

    # 3. Filter + dedup across all fetched pages
    found = filter_noise(found)
    found = deduplicate_emails(found)

    results: list[dict] = []
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

    return results


def _base_url(website: str) -> str:
    """Strip path/query from website URL, return scheme://host[:port]"""
    from urllib.parse import urlparse

    p = urlparse(website if website.startswith(("http://", "https://")) else f"https://{website}")
    return f"{p.scheme}://{p.netloc}"


async def process_batch(
    conn, listings: list[dict], concurrency: int, dry_run: bool
) -> tuple[int, int]:
    """Process a batch of listings concurrently with one shared httpx client.

    Returns (listings_processed, emails_written).
    """
    semaphore = asyncio.Semaphore(concurrency)
    counter_lock = asyncio.Lock()
    listings_processed = 0
    emails_written = 0
    start = time.monotonic()

    async def process_one(listing: dict):
        nonlocal listings_processed, emails_written
        async with semaphore:
            results = await extract_listing(client, listing)
            async with counter_lock:
                listings_processed += 1
            if results and not dry_run:
                written = upsert_emails(conn, results)
                async with counter_lock:
                    emails_written += written

    client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10, read=FETCH_TIMEOUT, write=10, pool=10),
        follow_redirects=True,
        max_redirects=MAX_REDIRECTS,
        limits=httpx.Limits(
            max_connections=concurrency,
            max_keepalive_connections=concurrency,
        ),
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
        },
    )

    tasks = [asyncio.create_task(process_one(lead)) for lead in listings]

    async def heartbeat():
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            async with counter_lock:
                done, found = listings_processed, emails_written
            elapsed = time.monotonic() - start
            rate = done / max(elapsed / 60, 0.01)
            log.info(
                "heartbeat: processed %d/%d, %d emails, %.0fs elapsed, %.1f listings/min",
                done, len(listings), found, elapsed, rate,
            )

    hb = asyncio.create_task(heartbeat())
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        hb.cancel()
        await client.aclose()
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

    if not args.stats and not acquire_lock():
        sys.exit(0)

    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    conn.autocommit = False

    try:
        if args.stats:
            show_stats(conn)
            return

        try:
            PIDFILE.parent.mkdir(parents=True, exist_ok=True)
            PIDFILE.write_text(str(os.getpid()))
        except OSError as e:
            log.warning("Could not write pidfile %s: %s", PIDFILE, e)

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
        release_lock()
        conn.close()


if __name__ == "__main__":
    main()
