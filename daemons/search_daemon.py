#!/usr/bin/env python3
"""
search_daemon.py — Eternal Google Maps search daemon.

Runs 24/7: generates queries from BPT sectors × BD cities × international markets,
searches Google Maps, extracts result URLs, upserts to PostgreSQL.

Reuses existing DynamicScraper strategies (pagination, extraction, output, queue).
Adds: infinite query generation, wall-clock browser restart (1h), PG connection pool.

systemd unit: ~/.config/systemd/user/infinitecrawler-search.service
"""

import asyncio
import logging
import random
import sys
import time

from pathlib import Path
from typing import Any, Dict, Optional

import nodriver as uc
from dotenv import load_dotenv

# ── Project imports ─────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory
from daemons.query_generator import InfiniteQueryGenerator
from utils.helpers import DelayManager
from utils.pg import get_pg_config
from daemons.common import (
    BROWSER_RESTART_INTERVAL_SEC,
    BROWSER_RESTART_PAGES,
    QUEUE_LOW_THRESHOLD,
    cleanup_orphaned_chrome_dirs,
    install_signal_handlers,
    shutdown_strategies,
)

# ── Config ──────────────────────────────────────────────────────────────────

load_dotenv(REPO_ROOT / ".env")

CONFIG_PATH = REPO_ROOT / "config" / "gmaps_bd_business_search.yaml"
QUERY_NAV_TIMEOUT = 30  # Seconds for GMaps search query navigation
QUERY_BATCH_SIZE = 50  # How many queries to generate per refill
STALLED_REQUEUE_INTERVAL = 60  # Check for stalled processing items every N sec

# PG connection (separate from output strategy — used for direct queries)
_pg = get_pg_config()
PG_HOST, PG_PORT = _pg["host"], _pg["port"]
PG_USER, PG_PASSWORD, PG_DB = _pg["user"], _pg["password"], _pg["dbname"]

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - search-daemon - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("search_daemon")


# ── State ───────────────────────────────────────────────────────────────────

class DaemonState:
    """Mutable state tracked across the eternal loop."""

    def __init__(self):
        self.browser_manager: Optional[BrowserManager] = None
        self.output_strategy: Optional[Any] = None
        self.extraction_strategy: Optional[Any] = None
        self.pagination_strategy: Optional[Any] = None
        self.queue_strategy: Optional[Any] = None
        self.delay_manager: Optional[DelayManager] = None
        self.query_generator: Optional[InfiniteQueryGenerator] = None
        self.config: dict = {}

        # Restart tracking
        self.pages_since_restart: int = 0
        self.last_restart_time: float = 0.0
        self.total_pages_processed: int = 0

        # Error tracking
        self.consecutive_errors: int = 0
        self.max_consecutive_errors: int = 10

        # Shutdown flag
        self.shutdown_requested: bool = False


# ── Browser lifecycle ───────────────────────────────────────────────────────

async def start_browser(state: DaemonState):
    """Launch a fresh Chrome instance via nodriver."""
    browser_config = state.config.get("browser", {})
    headless = browser_config.get("headless", True)
    page_wait = browser_config.get("page_wait_seconds", 1.0)

    state.browser_manager = BrowserManager(
        engine="nodriver",
        headless=headless,
        page_wait_seconds=page_wait,
    )
    await state.browser_manager.start()
    state.last_restart_time = time.time()
    state.pages_since_restart = 0
    log.info("Browser started (headless=%s)", headless)


async def restart_browser(state: DaemonState):
    """Clean shutdown + fresh start. Removes Chrome temp profiles."""
    log.info("Restarting browser (pages=%d, uptime=%ds)...",
             state.pages_since_restart, int(time.time() - state.last_restart_time))
    if state.browser_manager:
        await state.browser_manager.cleanup()
    state.browser_manager = None
    # Unbind strategies that hold browser references
    state.extraction_strategy = None
    state.pagination_strategy = None
    await asyncio.sleep(3)
    await start_browser(state)
    await _init_browser_bound_strategies(state)


async def _init_browser_bound_strategies(state: DaemonState):
    """Create pagination + extraction strategies (require browser)."""
    if not state.browser_manager:
        return
    # Pagination
    pag_name = state.config.get("pagination_strategy", "infinite_scroll")
    state.pagination_strategy = ScraperFactory.create_strategy(
        "pagination", pag_name, state.browser_manager, state.config,
    )
    # Extraction
    ext_name = state.config.get("extraction_strategy", "generic_selector")
    state.extraction_strategy = ScraperFactory.create_strategy(
        "extraction", ext_name, state.browser_manager, state.config,
    )


async def init_infrastructure(state: DaemonState):
    """One-time init: load config, create PG output + Redis queue strategies."""
    config = ScraperFactory.load_config(str(CONFIG_PATH))
    state.config = config

    # Output strategy (PG upsert)
    output_section = config.get("output", {})
    if output_section:
        out_name = output_section.get("strategy", "postgresql_upsert")
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

    # Query generator (infinite rotation)
    state.query_generator = InfiniteQueryGenerator()
    st = state.query_generator.stats()
    log.info("Query pools: %s (total %d unique)",
             st["pool_sizes"], st["total_unique"])

    # Delay manager — config uses int rate_limit; convert to DelayManager's dict shape
    rate_limit = config.get("rate_limit", 2)
    rate_limiting = config.get("rate_limiting") or {"between_requests": (rate_limit, rate_limit)}
    state.delay_manager = DelayManager(rate_limiting)

    # Worker config
    worker_cfg = config.get("workers", {})
    state.max_consecutive_errors = worker_cfg.get("max_consecutive_errors", 10)

    # Start browser
    await start_browser(state)
    await _init_browser_bound_strategies(state)

    log.info("Infrastructure initialized. Entering eternal loop.")


# ── Search logic ────────────────────────────────────────────────────────────

async def search_single_query(state: DaemonState, query: str) -> bool:
    """Search GMaps for one query, scroll-extract results, upsert to PG.
    Returns True on success, False on failure.
    """
    try:
        # Build search URL and navigate
        url_template = state.config.get("search_url_template",
                                        "https://www.google.com/maps/search/{query}/")
        search_url = url_template.format(query=query)
        try:
            tab = await asyncio.wait_for(
                state.browser_manager.navigate(search_url),
                timeout=QUERY_NAV_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning("Navigation timed out for query '%s'", query[:60])
            return False

        # Verify navigation actually reached Google Maps (detect stuck browsers)
        try:
            current_url = await tab.evaluate("window.location.href", timeout=5)
            if "google.com/maps" not in current_url:
                log.warning("Navigation verification failed - expected GMaps, got: %s", current_url[:60])
                return False
        except Exception as e:
            log.warning("Navigation verification error: %s", e)
            return False

        if state.delay_manager:
            await state.delay_manager.apply_delay("between_requests")

        # Scroll and extract
        seen_items: set[str] = set()
        state.extraction_strategy.seen_items = set()  # reset

        scroll_attempts = 0
        max_scroll = state.config.get("pagination", {}).get("max_scroll_attempts", 200)

        while scroll_attempts < max_scroll:
            if state.shutdown_requested:
                return False

            if state.output_strategy and state.output_strategy.has_reached_limit():
                break

            if not await state.pagination_strategy.has_more_results():
                break

            items = await state.extraction_strategy.extract_items()
            new_count = 0

            for item in items:
                item["query"] = query
                item["source"] = "google_maps_search"
                item_id = item.get("url") or item.get("href") or str(hash(str(item)))
                if item_id and item_id not in seen_items:
                    if state.output_strategy:
                        await state.output_strategy.write_item(item)
                    seen_items.add(item_id)
                    new_count += 1

            log.info("Query '%s': extracted %d items (%d new)",
                     query[:60], len(items), new_count)

            if not await state.pagination_strategy.load_more_results():
                break

            scroll_attempts += 1
            await state.delay_manager.apply_delay("between_requests")

        return True

    except Exception as e:
        log.error("Search failed for '%s': %s", query[:60], e)
        return False


# ── Queue management ────────────────────────────────────────────────────────

def refill_queue(state: DaemonState):
    """Generate fresh queries and enqueue to Redis."""
    if not state.queue_strategy or not state.query_generator:
        return 0

    pending = state.queue_strategy.get_stats().get("pending", 0)
    if pending >= QUEUE_LOW_THRESHOLD:
        return 0

    batch = state.query_generator.next_batch(QUERY_BATCH_SIZE)
    added = state.queue_strategy.enqueue(batch)
    log.info("Refilled queue: generated %d queries, enqueued %d (pending now ~%d)",
             len(batch), added, pending + added)
    return added


def requeue_stalled(state: DaemonState):
    """Move timed-out processing items back to pending."""
    if state.queue_strategy and hasattr(state.queue_strategy, "maybe_requeue_stalled"):
        requeued = state.queue_strategy.maybe_requeue_stalled()
        if requeued:
            log.info("Requeued %d stalled queries", requeued)


# ── Main loop ───────────────────────────────────────────────────────────────

async def eternal_loop(state: DaemonState):
    """The forever loop: refill → dequeue → search → repeat."""
    last_stalled_check = 0.0

    while not state.shutdown_requested:
        try:
            now = time.monotonic()

            # 1. Periodic stalled requeue
            if now - last_stalled_check > STALLED_REQUEUE_INTERVAL:
                requeue_stalled(state)
                last_stalled_check = now

            # 2. Refill queue if low
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

            # 4. Too many consecutive errors? Restart browser
            if state.consecutive_errors >= state.max_consecutive_errors:
                log.warning("%d consecutive errors — restarting browser",
                            state.consecutive_errors)
                await restart_browser(state)
                state.consecutive_errors = 0

            # 5. Dequeue next query
            query = state.queue_strategy.dequeue(timeout=10)
            if not query:
                stats = state.queue_strategy.get_stats()
                log.debug("No query available (pending=%d processing=%d)",
                          stats.get("pending", 0), stats.get("processing", 0))
                await asyncio.sleep(5)
                continue

            # 6. Search
            log.info("Processing query: %s", query[:80])
            success = await search_single_query(state, query)

            if success:
                state.queue_strategy.mark_completed(query)
                state.consecutive_errors = 0
                state.pages_since_restart += 1
                state.total_pages_processed += 1
            else:
                state.consecutive_errors += 1
                state.queue_strategy.mark_failed(query, "Search failed",
                                                 state.consecutive_errors)

            # 7. Jitter delay (anti-detection)
            jitter = random.uniform(2.0, 5.0)
            await asyncio.sleep(jitter)

        except Exception as e:
            log.error("Loop iteration failed: %s", e, exc_info=True)
            state.consecutive_errors += 1
            await asyncio.sleep(10)

    # Shutdown
    log.info("Shutdown requested. Cleaning up...")
    await shutdown(state)


async def shutdown(state: DaemonState):
    """Graceful cleanup — shared strategies + daemon-specific stats."""
    await shutdown_strategies(state)

    stats = state.query_generator.stats() if state.query_generator else {}
    log.info("Search daemon stopped. Total pages: %d. Queries generated: %s",
             state.total_pages_processed, stats.get("total_generated", "?"))
    log.info("Cleanup complete.")


# ── Signal handling ─────────────────────────────────────────────────────────

# ── Entry point ─────────────────────────────────────────────────────────────

async def main():
    state = DaemonState()

    # Register signal handlers
    install_signal_handlers(state)

    log.info("=" * 60)
    log.info("InfiniteCrawler Search Daemon starting")
    log.info("Config: %s", CONFIG_PATH)
    log.info("PG: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    log.info("Browser restart: every %ds or %d pages",
             BROWSER_RESTART_INTERVAL_SEC, BROWSER_RESTART_PAGES)
    log.info("Queue low threshold: %d, batch size: %d",
             QUEUE_LOW_THRESHOLD, QUERY_BATCH_SIZE)
    log.info("=" * 60)

    # Clean up orphaned Chrome temp dirs on startup
    cleanup_orphaned_chrome_dirs()

    await init_infrastructure(state)
    await eternal_loop(state)


if __name__ == "__main__":
    uc.loop().run_until_complete(main())