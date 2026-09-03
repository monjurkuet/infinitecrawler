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
from utils.pg import (  # noqa: E402
    get_all_listings_with_website,
    get_pg_config,
    get_unprocessed_emails,
    mark_listings_email_scanned,
    upsert_emails,
)

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

DEFAULT_MAX_LISTINGS = 2000
DEFAULT_CONCURRENCY = 50  # parallel httpx fetches
FETCH_TIMEOUT = 8  # seconds per website fetch
MAX_HTML_BYTES = 2 * 1024 * 1024  # cap page size for regex safety
HEARTBEAT_INTERVAL = 30  # seconds between progress logs
PIDFILE = REPO_ROOT / "_system" / "email_extract.pid"
PATH_CANDIDATES = [
    # High-hit contact surfaces (ordered — first page that yields email wins,
    # so front-load the pages that actually carry one).
    "contact", "contact-us", "contact_us", "contactus", "contacts",
    "about", "about-us", "about_us", "aboutus",
    "imprint", "impressum",
    "team", "our-team", "staff",
    "support",
]
MAX_REDIRECTS = 3

# T2 — browser-fallback knobs.  Concurrency is intentionally low (3) to avoid
# the Facebook/Instagram CLOSE-WAIT stall documented in project memory; 0.2s
# between navigations keeps the pinchtab request queue from starving.
BROWSER_CONCURRENCY = int(os.environ.get("BROWSER_CONCURRENCY", "3"))
BROWSER_NAV_DELAY = float(os.environ.get("BROWSER_NAV_DELAY", "0.2"))
BROWSER_PAGE_TIMEOUT = int(os.environ.get("BROWSER_PAGE_TIMEOUT", "60"))


def acquire_lock() -> bool:
    """Prevent overlapping runs: exit early if a previous instance is alive.

    Belt-and-suspenders: OS may have reaped our old PID and reused the number,
    so also check /proc/<pid>/cmdline matches ``db_email_extract`` before
    trusting the existence signal.
    """
    if not PIDFILE.exists():
        return True
    try:
        pid = int(PIDFILE.read_text().strip())
        os.kill(pid, 0)  # signal 0 = existence check only
        # Verify it's actually our process — guards against PID reuse.
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                cmdline = fh.read().decode("utf-8", errors="replace")
            if "db_email_extract" in cmdline:
                log.warning("Another email-extract instance is running (pid %d) — exiting.", pid)
                return False
            log.warning("Stale pidfile (pid %d reused by non-email-extract process) — proceeding", pid)
            return True
        except OSError:
            return True
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
        # Early-exit: once a page yields emails, stop probing candidates.
        # Fetches remain best-effort: 404/redirect/timeout `continue` above
        # never reaches here, so only a real hit truncates the path list.
        if found:
            break

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


async def extract_listing_browser(client, listing: dict) -> list[dict]:
    """Browser-rendered email extraction via pinchtab (T2 fallback path).

    The browser has already executed the page's JS, so emails hidden behind
    obfuscation scripts (Cloudflare email-decoder, [at]/[dot] rewrites,
    mailto: anchors built at runtime) are visible.  Returns the same dict
    shape as `extract_listing` so downstream `upsert_emails` is unchanged.

    Mirrors the HTTP path's PATH_CANDIDATES: walks the homepage then the top
    contact/about URLs until one yields an email.  Homepages alone surface a
    contact email only a sliver of the time — /contact* carries ~80% of the
    yield for BD SMBs, which is why the previous homepage-only pass returned 0.
    """
    import re

    listing_id = listing["id"]
    website = listing["website"]
    if not website.startswith(("http://", "https://")):
        website = f"https://{website}"

    base = website.split("/")[0] + "//" + website.split("/")[2] if "://" in website else website
    urls_to_try = [website] + [
        f"{base.rstrip('/')}/{p}" for p in PATH_CANDIDATES[:6]  # contact*, about*, imprint — top-6 only (browser is slow)
    ]

    found: list[dict] = []
    tab = None
    for url in urls_to_try:
        if found:  # one page yielded — short-circuit
            break
        try:
            # First nav allocates a fresh tab; subsequent ones reuse it via tab_id.
            tab = await asyncio.wait_for(
                client.navigate(url, tab_id=(tab._tab_id if tab else None)),
                timeout=BROWSER_PAGE_TIMEOUT,
            )
        except Exception as exc:
            log.debug("browser nav failed for %s: %s", url[:60], exc)
            continue

        if BROWSER_NAV_DELAY:
            await asyncio.sleep(BROWSER_NAV_DELAY)

        try:
            page = await asyncio.wait_for(
                tab.extract_emails_from_page(), timeout=BROWSER_PAGE_TIMEOUT,
            )
        except Exception as exc:
            log.debug("browser extract failed for %s: %s", url[:60], exc)
            continue

        text = (page.get("text") or "")[:MAX_HTML_BYTES]
        mailto = page.get("mailto_hrefs") or []
        page_found = scan_text_for_emails(text)
        for href in mailto:
            m = re.search(r"mailto:([^?\"'>]+)", href, re.I)
            if not m:
                continue
            addr = m.group(1)
            if not any(e["email"] == addr for e in page_found):
                page_found.append({
                    "email": addr,
                    "is_obfuscated": False,
                    "context_snippet": f"mailto:{addr}",
                })

        page_found = filter_noise(page_found)
        page_found = deduplicate_emails(page_found)
        if page_found:
            found = page_found

    if tab is not None:
        try:
            await client.close_tab(tab=tab)
        except Exception:
            log.debug("browser tab close failed", exc_info=True)

    return [
        {
            "listing_id": listing_id,
            "website_url": website,
            "email": e["email"],
            "email_type": "general",
            "extraction_method": "browser",
            "is_obfuscated": e["is_obfuscated"],
            "context_snippet": e.get("context_snippet", "")[:200],
        }
        for e in found
    ]


async def process_batch_http(
    conn, listings: list[dict], concurrency: int, dry_run: bool
) -> tuple[int, int]:
    """HTTP-only batch (legacy)."""
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


async def process_batch_both(
    conn, listings: list[dict], concurrency: int, dry_run: bool
) -> tuple[int, int, int]:
    """HTTP pass first → browser fallback for zero-result listings.

    Returns (listings_processed, emails_written, browser_queue_size).  The
    browser queue is logged for observability so the cron watchdog can
    detect connection stalls (see project memory `email.facebook_connection_stall`).
    """
    http_done, http_emails = await process_batch_http(conn, listings, concurrency, dry_run)

    if dry_run:
        return http_done, http_emails, 0

    # Identify which listings still need a browser pass.  We can't trust the
    # in-memory `results` list (it was scoped to each call), so re-query.
    website_by_id = {lst["id"]: lst["website"] for lst in listings}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT listing_id, email FROM scraper.emails
            WHERE listing_id = ANY(%s)
            """,
            (list(website_by_id),),
        )
        have_email = {row[0] for row in cur.fetchall()}
    todo_browser = [
        {"id": lid, "website": site}
        for lid, site in website_by_id.items()
        if lid not in have_email and site
    ]
    if not todo_browser:
        return http_done, http_emails, 0

    log.info("browser pass: %d listings without HTTP emails", len(todo_browser))

    from base.pinchtab_client import PinchtabConfig, PinchtabClient
    pt_cfg = PinchtabConfig.from_env_and_config({})
    pt = PinchtabClient(pt_cfg)
    try:
        await pt.start()
    except Exception as exc:
        log.warning("browser pass skipped: pinchtab unreachable: %s", exc)
        return http_done, http_emails, 0

    sem = asyncio.Semaphore(BROWSER_CONCURRENCY)
    counter_lock = asyncio.Lock()
    browser_processed = 0
    browser_emails = 0

    async def one(listing: dict):
        nonlocal browser_processed, browser_emails
        async with sem:
            results = await extract_listing_browser(pt, listing)
            async with counter_lock:
                browser_processed += 1
            if results:
                written = upsert_emails(conn, results)
                async with counter_lock:
                    browser_emails += written

    tasks = [asyncio.create_task(one(lst)) for lst in todo_browser]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await pt.cleanup()

    log.info("browser pass: processed=%d emails=%d", browser_processed, browser_emails)
    return http_done, http_emails + browser_emails, len(todo_browser)


async def process_batch(
    conn, listings: list[dict], concurrency: int, dry_run: bool, mode: str = "http",
) -> tuple[int, int] | tuple[int, int, int]:
    """Back-compat shim: routes to http or both.  Mode='http' returns
    (processed, written).  Mode='both' returns (processed, written, browser_q)."""
    if mode == "both":
        return await process_batch_both(conn, listings, concurrency, dry_run)
    return await process_batch_http(conn, listings, concurrency, dry_run)


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
    parser.add_argument("--loop", action="store_true",
                        help="Run continuously: after each batch, claim new unscanned listings and repeat forever")
    parser.add_argument("--loop-gap", type=float, default=30.0,
                        help="Pause seconds between loop cycles (default 30)")
    parser.add_argument("--burst", type=int, default=1,
                        help="Run N consecutive cycles with a short 5s gap before honoring --loop-gap (default 1)")
    parser.add_argument("--mode", choices=["http", "browser", "both"], default="http",
                        help="Extraction mode (T2: 'both' runs http first then browser fallback for zero-result listings)")
    parser.add_argument("--force-rescan", action="store_true",
                        help="Re-scan listings even if they already have an email row (T6: backlog drain)")
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
            # The /proc/<pid>/cmdline check in _another_instance_running() above
            # already guards against PID reuse; a numerical threshold here would
            # over-reject on hosts with kernel.pid_max raised above the default
            # 4194304 (AWS/tuned profiles sometimes do this). Trust os.getpid().
            PIDFILE.write_text(str(os.getpid()))
        except OSError as e:
            log.warning("Could not write pidfile %s: %s", PIDFILE, e)

        cycle = 0
        while True:
            try:
                cycle += 1

                # Ensure PG connection is alive (idle-in-transaction timeout
                # fires after long sleeps; recover by reconnecting).
                if conn.closed:
                    log.warning("email_extract.reconnect reason=connection_closed")
                    conn = psycopg.connect(**pg_config)
                    conn.autocommit = False

                if args.force_rescan:
                    listings = get_all_listings_with_website(conn, limit=args.max)
                    log.info("[cycle %d] --force-rescan: %d listings with website",
                             cycle, len(listings))
                else:
                    listings = get_unprocessed_emails(conn, limit=args.max)
                if not listings:
                    log.info("[cycle %d] No listings with unprocessed emails found.", cycle)
                    if not args.loop:
                        return
                    log.info("loop: sleeping %.0fs before next cycle", args.loop_gap)
                    time.sleep(args.loop_gap)
                    continue

                log.info("[cycle %d] Found %d listings needing email extraction (limit: %d, mode=%s)",
                         cycle, len(listings), args.max, args.mode)

                if args.dry_run:
                    log.info("=== DRY RUN === (no writes)")
                    for lead in listings[:5]:
                        log.info("  [%d] %s", lead["id"], lead["website"][:60])
                    if len(listings) > 5:
                        log.info("  ... and %d more", len(listings) - 5)
                    return

                result = asyncio.run(
                    process_batch(conn, listings, args.concurrency, dry_run=False, mode=args.mode)
                )
                if args.mode == "both":
                    processed, written, browser_q = result  # type: ignore[misc]
                    log.info("[cycle %d] Done: processed %d / %d listings, wrote %d emails (browser_queue=%d)",
                             cycle, processed, len(listings), written, browser_q)
                else:
                    processed, written = result  # type: ignore[misc]
                    log.info("[cycle %d] Done: processed %d / %d listings, wrote %d emails",
                             cycle, processed, len(listings), written)

                # Stamp `email_scanned_at` on every listing we touched this batch
                # (including zero-result ones) so the perpetual loop does not
                # re-fetch the same sites on the next 30s cycle. Listings older
                # than the staleness window (FETCH_UNPROCESSED_EMAILS_SQL) will be
                # re-scanned for newly published contact pages.
                ids = [item["id"] for item in listings]
                if ids and not args.dry_run:
                    marked = mark_listings_email_scanned(conn, ids)
                    log.info("[cycle %d] Marked %d listings email_scanned_at=NOW()", cycle, marked)

                if not args.loop:
                    return

                # Burst mode: run N consecutive cycles with a short 5s gap before
                # honoring --loop-gap. Drains backlogs (e.g. 4,446 listings) faster.
                if cycle < args.burst:
                    log.info("burst: cycle %d/%d complete — pausing 5s", cycle, args.burst)
                    time.sleep(5)
                    continue

                log.info("loop: cycle %d complete — pausing %.0fs before next batch",
                         cycle, args.loop_gap)
                time.sleep(args.loop_gap)
            except Exception as cycle_err:
                log.error(f"email_extract.cycle_error err={cycle_err}", exc_info=True)
                if not args.loop:
                    raise
                # Close the broken connection so the top-of-loop reconnect kicks in.
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(args.loop_gap)

    finally:
        release_lock()
        conn.close()


if __name__ == "__main__":
    main()
