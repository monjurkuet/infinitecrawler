#!/usr/bin/env python3
"""
listing_daemon.py — Eternal Google Maps listing deep-extraction daemon.

Runs 24/7: pulls uncrawled listing URLs from PostgreSQL (search results not yet
extracted), deep-extracts phone/website/category/rating via multi-step scraping,
upserts to scraper.gmaps_listings.

Reuses existing ListingCrawler strategies (extraction, output, queue, navigation).
Adds: live PG feed (no file export step), wall-clock browser restart (1h).

systemd unit: ~/.config/systemd/user/infinitecrawler-listing.service
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg
from dotenv import load_dotenv

# ── Project imports ─────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory
from utils.helpers import DelayManager
from utils.pg import get_pg_config, get_uncrawled_urls_sql
from daemons.common import (
    BROWSER_RESTART_INTERVAL_SEC,
    BROWSER_RESTART_PAGES,
    QUEUE_LOW_THRESHOLD,
    install_signal_handlers,
    shutdown_strategies,
)
from scripts.llm_classifier import _single_fallback, load_sectors, METHOD_FALLBACK_RULE
from utils.email_extractor import (
    scan_text_for_emails,
    extract_mailto_links,
    filter_noise,
    deduplicate_emails,
)
from utils.pg import upsert_emails, get_pg_config as pg_cfg

# ── Config ──────────────────────────────────────────────────────────────────

load_dotenv(REPO_ROOT / ".env")

CONFIG_PATH = REPO_ROOT / "config" / "gmaps_listings_working.yaml"
URL_FETCH_BATCH = 100  # How many uncrawled URLs to pull from PG per refill
URL_MAX_RETRIES = 3  # Per-URL retry attempts
URL_RETRY_DELAY = 5  # Seconds between per-URL retries
URL_EXTRACTION_TIMEOUT = 35  # Seconds before extraction attempt is aborted (page_wait already gives time to render)
URL_NAV_TIMEOUT = 120  # Seconds for initial URL navigation (includes page_wait_seconds=45 plus page load)
BROWSER_START_TIMEOUT = 30  # Seconds for browser launch
STALLED_REQUEUE_INTERVAL = 60  # Check for stalled processing items every N sec

# PG connection (separate from output strategy — used for live URL feed)
_pg = get_pg_config()
PG_HOST, PG_PORT = _pg["host"], _pg["port"]
PG_USER, PG_PASSWORD, PG_DB = _pg["user"], _pg["password"], _pg["dbname"]

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - listing-daemon - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("listing_daemon")


# ── State ───────────────────────────────────────────────────────────────────

class DaemonState:
    """Mutable state tracked across the eternal loop."""

    def __init__(self):
        self.browser_manager: Optional[BrowserManager] = None
        self.output_strategy: Optional[Any] = None
        self.extraction_strategy: Optional[Any] = None
        self.queue_strategy: Optional[Any] = None
        self.delay_manager: Optional[DelayManager] = None
        self.pg_conn: Optional[psycopg.Connection] = None
        self.config: dict = {}
        self.sectors: dict = {}  # BPT sectors for in-stream fallback classification

        # Restart tracking
        self.pages_since_restart: int = 0
        self.last_restart_time: float = 0.0
        self.total_pages_processed: int = 0

        # Error tracking
        self.consecutive_errors: int = 0
        self.max_consecutive_errors: int = 10

        # Retry tracking
        self.retry_counts: dict[str, int] = {}  # url → attempts

        # Shutdown flag
        self.shutdown_requested: bool = False


# ── Browser lifecycle ───────────────────────────────────────────────────────

async def start_browser(state: DaemonState):
    """Attach to the running pinchtab server (port 9868 by default).

    Pinchtab's `always-on` policy auto-restarts crashed Chrome instances, so we
    don't have to launch Chrome ourselves — we just connect to the existing
    server.  See `base/pinchtab_client.py` for the HTTP client + Tab adapter.
    """
    browser_config = state.config.get("browser", {})
    headless = browser_config.get("headless", True)
    page_wait = browser_config.get("page_wait_seconds", 1.0)
    pinchtab_cfg = state.config.get("pinchtab", {})

    state.browser_manager = BrowserManager(
        engine="pinchtab",
        headless=headless,
        page_wait_seconds=page_wait,
        pinchtab_config=pinchtab_cfg,
    )
    await state.browser_manager.start()
    state.last_restart_time = time.time()
    state.pages_since_restart = 0
    state.retry_counts.clear()
    log.info("Browser started (engine=pinchtab, headless=%s)", headless)


async def restart_browser(state: DaemonState):
    """Clean reconnect to pinchtab + refresh bound strategies.

    With pinchtab there are no orphaned Chrome processes to kill — the
    `always-on` supervisor manages Chrome itself.  We just release the HTTP
    session, acquire a fresh tab, and re-bind the extraction strategy.
    """
    log.info("Reconnecting to pinchtab (pages=%d, uptime=%ds)...",
             state.pages_since_restart, int(time.time() - state.last_restart_time))
    if state.browser_manager:
        await state.browser_manager.cleanup()
    state.browser_manager = None
    # Unbind browser-bound strategies
    state.extraction_strategy = None
    await asyncio.sleep(1)
    try:
        await asyncio.wait_for(start_browser(state), timeout=BROWSER_START_TIMEOUT)
        await asyncio.wait_for(
            _refresh_browser_bound_strategies(state), timeout=BROWSER_START_TIMEOUT)
    except asyncio.TimeoutError:
        log.error("Pinchtab reconnect timed out after %ds", BROWSER_START_TIMEOUT)
        state.browser_manager = None
        raise


async def _refresh_browser_bound_strategies(state: DaemonState):
    """Rebuild extraction strategy (requires fresh browser reference)."""
    if not state.browser_manager:
        return
    ext_section = state.config.get("extraction", {})
    ext_name = ext_section.get("strategy", "multi_step")
    state.extraction_strategy = ScraperFactory.create_strategy(
        "extraction", ext_name, state.browser_manager, state.config,
    )
    log.info("Extraction strategy reinitialized: %s", ext_name)


# ── Infrastructure init ─────────────────────────────────────────────────────

async def init_infrastructure(state: DaemonState):
    """One-time init: load config, create output + queue + extraction strategies,
    connect PG (for live URL feed), start browser."""
    config = ScraperFactory.load_config(str(CONFIG_PATH))
    state.config = config

    # Output strategy (PG listing upsert)
    output_section = config.get("output", {})
    if output_section:
        out_name = output_section.get("strategy", "postgresql_listing_upsert")
        state.output_strategy = ScraperFactory.create_strategy(
            "output", out_name, output_section,
        )
    else:
        log.error("No output strategy configured")
        sys.exit(1)

    # Queue strategy (Redis)
    queue_section = config.get("queue", {})
    if queue_section:
        state.queue_strategy = ScraperFactory.create_strategy(
            "queue", "redis_queue", queue_section,
        )

    # PG connection (for live uncrawled URL feed)
    try:
        state.pg_conn = psycopg.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASSWORD, dbname=PG_DB,
            connect_timeout=10,
        )
        state.pg_conn.autocommit = True
        log.info("PG connected: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    except Exception as e:
        log.error("PG connection failed: %s", e)
        sys.exit(1)

    # Delay manager
    rate_limiting = config.get("rate_limiting", {})
    state.delay_manager = DelayManager(rate_limiting)

    # Worker config
    worker_cfg = config.get("workers", {})
    state.max_consecutive_errors = worker_cfg.get("max_consecutive_errors", 10)

    # Start browser
    await start_browser(state)
    await _refresh_browser_bound_strategies(state)

    log.info("Infrastructure initialized. Entering eternal loop.")


# ── Live URL feed from PG ───────────────────────────────────────────────────

def _pg_reconnect(state: DaemonState) -> None:
    """Reconnect PG if the connection is closed or broken."""
    if state.pg_conn is not None:
        try:
            with state.pg_conn.cursor() as cur:
                cur.execute("SELECT 1")
            return  # alive
        except Exception:
            log.info("PG connection stale — reconnecting…")
            try:
                state.pg_conn.close()
            except Exception:
                pass
            state.pg_conn = None
    try:
        state.pg_conn = psycopg.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASSWORD, dbname=PG_DB,
            connect_timeout=10,
        )
        state.pg_conn.autocommit = True
        log.info("PG reconnected: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    except Exception as e:
        log.error("PG reconnect failed: %s", e)
        state.pg_conn = None


def _check_staleness(state: DaemonState, last_write_time: float, label: str) -> float:
    """Log WARNING if no new data written in 1h. Returns current time."""
    now = time.monotonic()
    if now - last_write_time > 3600:  # 1 hour
        log.warning("STALENESS ALERT: no new %s data written in 1h", label)
    return now


def fetch_uncrawled_urls(state: DaemonState) -> list[str]:
    """Pull uncrawled listing URLs directly from PG (no file intermediary)."""
    _pg_reconnect(state)
    if not state.pg_conn:
        return []
    try:
        with state.pg_conn.cursor() as cur:
            sql, params = get_uncrawled_urls_sql(limit=URL_FETCH_BATCH)
            cur.execute(sql, params)
            rows = cur.fetchall()
        urls = [r[0] for r in rows if r[0]]
        return urls
    except Exception as e:
        log.error("PG URL fetch failed: %s", e)
        state.pg_conn = None  # invalidate so next call reconnects
        return []


def refill_queue(state: DaemonState):
    """Pull uncrawled URLs from PG and enqueue to Redis."""
    if not state.queue_strategy:
        return 0

    pending = state.queue_strategy.get_stats().get("pending", 0)
    if pending >= QUEUE_LOW_THRESHOLD:
        return 0

    urls = fetch_uncrawled_urls(state)
    if not urls:
        log.debug("No uncrawled URLs in PG")
        return 0

    added = state.queue_strategy.enqueue(urls)
    log.info("Refilled queue: pulled %d URLs from PG, enqueued %d (pending now ~%d)",
             len(urls), added, pending + added)
    return added


def requeue_stalled(state: DaemonState):
    """Move timed-out processing items back to pending."""
    if state.queue_strategy and hasattr(state.queue_strategy, "maybe_requeue_stalled"):
        requeued = state.queue_strategy.maybe_requeue_stalled()
        if requeued:
            log.info("Requeued %d stalled URLs", requeued)


# ── Email extraction ─────────────────────────────────────────────────────────

EMAIL_EXTRACTION_TIMEOUT = 10  # seconds per website fetch
EMAIL_BATCH_INSERT = True  # batch emails before PG commit


async def extract_emails_from_website(
    website_url: str, listing_id: int
) -> list[dict]:
    """Fetch a company website via HTTP and extract email addresses.

    Uses httpx (no browser tab) — fast, lightweight, independent of the
    browser session. Falls back silently on any error.

    Returns list of dicts ready for upsert_emails().
    """
    import httpx

    results: list[dict] = []
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(EMAIL_EXTRACTION_TIMEOUT),
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml",
            },
        ) as client:
            resp = await client.get(website_url)
            if resp.status_code != 200:
                log.debug("Website fetch returned %d for %s", resp.status_code, website_url[:60])
                return results

            text = resp.text
            page_text = resp.text

            # 1. Standard + obfuscated emails from page text
            found = scan_text_for_emails(page_text)

            # 2. mailto: links from HTML
            mailto_emails = extract_mailto_links(text)
            for email in mailto_emails:
                if not any(e["email"] == email for e in found):
                    found.append({
                        "email": email,
                        "is_obfuscated": False,
                        "context_snippet": f"mailto:{email}",
                    })

            # 3. Filter noise
            found = filter_noise(found)
            found = deduplicate_emails(found)

            # 4. Build upsert dicts
            for e in found:
                results.append({
                    "listing_id": listing_id,
                    "website_url": website_url,
                    "email": e["email"],
                    "email_type": "general",
                    "extraction_method": "http" if not e["is_obfuscated"] else "obfuscated",
                    "is_obfuscated": e["is_obfuscated"],
                    "context_snippet": e.get("context_snippet", "")[:200],
                })

            if results:
                log.info("Found %d email(s) on %s", len(results), website_url[:50])

    except httpx.TimeoutException:
        log.debug("Timeout fetching %s for email extraction", website_url[:60])
    except Exception as e:
        log.debug("Email extraction failed for %s: %s", website_url[:60], e)

    return results


# ── Listing extraction logic ────────────────────────────────────────────────


def _has_meaningful_data(item: dict) -> bool:
    """Check if extracted item has at least one non-empty key field."""
    required = ['name', 'phone', 'website', 'category', 'rating']
    return any(item.get(k) for k in required)


async def process_url(state: DaemonState, url: str) -> bool:
    """Deep-extract a single listing URL with retry logic.
    Returns True if data was extracted and written to PG, False otherwise.
    """
    # Track retries per URL
    if url not in state.retry_counts:
        state.retry_counts[url] = 0

    for attempt in range(URL_MAX_RETRIES):
        try:
            # Navigate with timeout
            try:
                await asyncio.wait_for(
                    state.browser_manager.navigate(url),
                    timeout=URL_NAV_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning("Navigation timed out for %s (attempt %d/%d)",
                            url[:60], attempt + 1, URL_MAX_RETRIES)
                if attempt < URL_MAX_RETRIES - 1:
                    await restart_browser(state)
                    continue
                return False
            if state.delay_manager:
                await state.delay_manager.apply_delay("page_load")

            # Extract with timeout — multi-step extraction can hang on slow/broken pages
            try:
                items = await asyncio.wait_for(
                    state.extraction_strategy.extract_items(),
                    timeout=URL_EXTRACTION_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning("Extraction timed out after %ds for %s",
                            URL_EXTRACTION_TIMEOUT, url[:60])
                items = []
                # Restart browser after ANY extraction timeout — wait_for cancel
                # can leave the Chrome tab in a bad state, causing subsequent
                # navigate() calls to hang indefinitely.
                await restart_browser(state)
                state.consecutive_errors = 0

            # Filter out items with no meaningful data (empty name, phone, etc.)
            if items:
                items = [item for item in items if _has_meaningful_data(item)]
                if not items:
                    log.warning("No meaningful data extracted from %s (attempt %d/%d)",
                                url[:60], attempt + 1, URL_MAX_RETRIES)

            if not items:
                log.warning("No data extracted from %s (attempt %d/%d)",
                            url[:60], attempt + 1, URL_MAX_RETRIES)
                # Extraction timeout OR empty result: increment error counter so
                # a stuck browser tab triggers a restart on threshold instead of
                # silently wasting 45s per URL.  Empty extraction on the final
                # attempt is treated as failure (was previously phantom-success,
                # which inflated the listing count with empty rows).
                state.consecutive_errors += 1
                if attempt == URL_MAX_RETRIES - 1:
                    return False
                await asyncio.sleep(URL_RETRY_DELAY)
                continue

            # Write to PG
            for item in items:
                item["_crawl_meta"] = {
                    "source_url": url,
                    "pages_processed": state.total_pages_processed,
                    "retry_count": attempt,
                }
                # In-stream rule-based fallback classification — zero-cost, pure CPU.
                # LLM cron (db_classify.py) can upgrade these later with higher confidence.
                if state.sectors:
                    fb = _single_fallback(item, 0, state.sectors)
                    item["sector_id"] = fb["sector"]
                    item["classification_confidence"] = fb["confidence"]
                    item["classification_method"] = METHOD_FALLBACK_RULE
                    item["classified_at"] = datetime.now(timezone.utc)
                await state.output_strategy.write_item(item)

            # ── Inline email extraction ─────────────────────────────────────
            if items and items[0].get("website"):
                website_url = items[0]["website"]
                # source_url is the GMaps listing URL — use it to look up listing_id
                source_url = items[0].get("_crawl_meta", {}).get("source_url") or url
                if website_url.startswith(("http://", "https://")):
                    try:
                        listing_id = None
                        if state.pg_conn:
                            with state.pg_conn.cursor() as cur:
                                cur.execute(
                                    "SELECT id FROM scraper.gmaps_listings "
                                    "WHERE source_url = %s LIMIT 1",
                                    (source_url,),
                                )
                                row = cur.fetchone()
                                if row:
                                    listing_id = row[0]

                        if not listing_id:
                            log.debug("Could not resolve listing_id for %s", source_url[:60])
                        else:
                            emails = await asyncio.wait_for(
                                extract_emails_from_website(website_url, listing_id),
                                timeout=EMAIL_EXTRACTION_TIMEOUT + 5,
                            )
                            if emails and state.pg_conn:
                                written = upsert_emails(state.pg_conn, emails)
                                if written:
                                    log.info("Upserted %d email(s) for listing %d",
                                             written, listing_id)
                    except asyncio.TimeoutError:
                        log.debug("Email extraction timed out for %s", website_url[:50])
                    except Exception:
                        log.debug("Email extraction error for %s", website_url[:50], exc_info=True)

            log.info("Extracted %d fields from %s (attempt %d/%d)",
                     len(items), url[:60], attempt + 1, URL_MAX_RETRIES)
            return True

        except Exception as e:
            log.warning("Attempt %d/%d failed for %s: %s",
                        attempt + 1, URL_MAX_RETRIES, url[:60], e)

            # Restart browser before retry if configured
            if attempt < URL_MAX_RETRIES - 1:
                await restart_browser(state)
                await asyncio.sleep(2)

    # All retries exhausted
    log.error("All %d attempts failed for %s", URL_MAX_RETRIES, url[:60])
    return False


# ── Main loop ───────────────────────────────────────────────────────────────

async def eternal_loop(state: DaemonState):
    """The forever loop: refill → dequeue → extract → repeat."""
    last_stalled_check = 0.0

    while not state.shutdown_requested:
        try:
            now = time.monotonic()

            # 1. Periodic stalled requeue
            if now - last_stalled_check > STALLED_REQUEUE_INTERVAL:
                requeue_stalled(state)
                last_stalled_check = now

            # 2. Refill queue from PG if low
            refill_queue(state)

            # 3. Check browser restart triggers
            need_restart = False
            if state.pages_since_restart >= BROWSER_RESTART_PAGES:
                need_restart = True
                log.info("Restart trigger: %d pages processed", state.pages_since_restart)
            elif (now - state.last_restart_time) >= BROWSER_RESTART_INTERVAL_SEC:
                need_restart = True
                log.info("Restart trigger: %.0f seconds uptime",
                         now - state.last_restart_time)

            if need_restart:
                await restart_browser(state)

            # 4. Too many consecutive errors?
            if state.consecutive_errors >= state.max_consecutive_errors:
                log.warning("%d consecutive errors — restarting browser",
                            state.consecutive_errors)
                await restart_browser(state)
                state.consecutive_errors = 0

            # 5. Dequeue next URL
            url = state.queue_strategy.dequeue(timeout=10)
            if not url:
                stats = state.queue_strategy.get_stats()
                log.debug("No URL available (pending=%d processing=%d)",
                          stats.get("pending", 0), stats.get("processing", 0))
                await asyncio.sleep(5)
                continue

            # 6. Process URL
            log.info("Processing: %s", url[:80])
            success = await process_url(state, url)

            if success:
                state.queue_strategy.mark_completed(url)
                state.consecutive_errors = 0
                state.pages_since_restart += 1
                state.total_pages_processed += 1
            else:
                state.consecutive_errors += 1
                state.queue_strategy.mark_failed(url, "Extraction exhausted retries",
                                                 state.consecutive_errors)
                await restart_browser(state)

            # 7. Jitter delay
            await state.delay_manager.apply_delay("between_requests")

        except Exception as e:
            log.error("Loop iteration failed: %s", e, exc_info=True)
            state.consecutive_errors += 1
            await asyncio.sleep(10)

    # Shutdown
    log.info("Shutdown requested. Cleaning up...")
    await shutdown(state)


async def shutdown(state: DaemonState):
    """Graceful cleanup — shared strategies + daemon-specific PG close."""
    await shutdown_strategies(state)
    if state.pg_conn:
        try:
            state.pg_conn.close()
        except Exception:
            pass
        state.pg_conn = None

    log.info("Listing daemon stopped. Total pages: %d.",
             state.total_pages_processed)
    log.info("Cleanup complete.")


# ── Signal handling ─────────────────────────────────────────────────────────

# ── Entry point ─────────────────────────────────────────────────────────────

async def main():
    state = DaemonState()

    # Register signal handlers
    install_signal_handlers(state)

    log.info("=" * 60)
    log.info("InfiniteCrawler Listing Daemon starting")
    log.info("Config: %s", CONFIG_PATH)
    log.info("PG: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    log.info("Browser restart: every %ds or %d pages",
             BROWSER_RESTART_INTERVAL_SEC, BROWSER_RESTART_PAGES)
    log.info("Queue low threshold: %d, PG fetch batch: %d",
             QUEUE_LOW_THRESHOLD, URL_FETCH_BATCH)
    log.info("URL retries: %d attempts, %ds delay",
             URL_MAX_RETRIES, URL_RETRY_DELAY)
    log.info("=" * 60)

    # Preload BPT sectors once for in-stream fallback classification
    try:
        state.sectors = load_sectors()
        active = sum(1 for s in state.sectors.values() if s.get("status") == "active")
        log.info("Loaded %d sectors (%d active) for in-stream fallback", len(state.sectors), active)
    except Exception as e:
        log.warning("Failed to load sectors, in-stream fallback disabled: %s", e)
        state.sectors = {}

    await init_infrastructure(state)
    await eternal_loop(state)


if __name__ == "__main__":
    asyncio.run(main())