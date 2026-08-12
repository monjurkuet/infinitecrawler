#!/usr/bin/env python3
"""
search_daemon.py — Eternal Google Maps search daemon.

Runs 24/7: generates queries from BPT sectors × BD cities × international markets,
searches Google Maps, extracts result URLs, upserts to PostgreSQL.

Uses pinchtab-based strategies (pagination, extraction) + Redis queue + PG upsert.
Adds: infinite query generation, wall-clock browser restart (1h), PG connection pool.

systemd unit: ~/.config/systemd/user/infinitecrawler-search.service
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone  # noqa: E402

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from dotenv import load_dotenv

if TYPE_CHECKING:
    from base.strategies import (
        ExtractionStrategy,
        OutputStrategy,
        PaginationStrategy,
        QueueStrategy,
    )

# psutil is optional on minimal envs.  Guard the import once at module load;
# the heartbeat block skips the mem line when this flag is False instead of
# raising ImportError inside the eternal loop (which would otherwise trip
# consecutive_errors and churn browser restarts).
try:
    import psutil as _psutil  # noqa: F401
    _HAS_PSUTIL = True
except ImportError:
    _psutil = None  # type: ignore[assignment]
    _HAS_PSUTIL = False

# ── Project imports ─────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from base.browser_manager import BrowserManager  # noqa: E402
from factory.scraper_factory import ScraperFactory  # noqa: E402
from daemons.query_generator import InfiniteQueryGenerator  # noqa: E402
from utils.helpers import DelayManager  # noqa: E402
from utils.pg import get_pg_config  # noqa: E402
from daemons.common import (  # noqa: E402
    BROWSER_RESTART_INTERVAL_SEC,
    BROWSER_RESTART_PAGES,
    QUEUE_LOW_THRESHOLD,
    install_signal_handlers,
    shutdown_strategies,
)

# ── Config ──────────────────────────────────────────────────────────────────

load_dotenv(REPO_ROOT / ".env")

CONFIG_PATH = REPO_ROOT / "config" / "gmaps_bd_business_search.yaml"
QUERY_NAV_TIMEOUT = 30  # Seconds for GMaps search query navigation
EXTRACTION_TIMEOUT = 25  # Seconds for extraction (prevents browser tab hang)
SCROLL_TIMEOUT = 15  # Seconds for scroll/load-more operations
BROWSER_START_TIMEOUT = 30  # Seconds for browser launch
QUERY_BATCH_SIZE = 50  # How many queries to generate per refill
STALLED_REQUEUE_INTERVAL = 60  # Check for stalled processing items every N sec
PG_STALENESS_INTERVAL = 900  # Check PG staleness every 15 min
HEARTBEAT_SEC = int(os.environ.get("DAEMON_HEARTBEAT_SEC", "300"))  # log heartbeat every N sec

# PG connection (separate from output strategy — used for direct queries)
_pg = get_pg_config()
PG_HOST, PG_PORT = _pg["host"], _pg["port"]
PG_USER, PG_PASSWORD, PG_DB = _pg["user"], _pg["password"], _pg["dbname"]

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("search_daemon")


# ── State ───────────────────────────────────────────────────────────────────

class DaemonState:
    """Mutable state tracked across the eternal loop."""

    def __init__(self):
        self.browser_manager: Optional[BrowserManager] = None
        self.output_strategy: Optional[OutputStrategy] = None
        self.extraction_strategy: Optional[ExtractionStrategy] = None
        self.pagination_strategy: Optional[PaginationStrategy] = None
        self.queue_strategy: Optional[QueueStrategy] = None
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
        self.url_retries: dict[str, int] = {}
        self.zero_item_streak: dict[str, int] = {}  # query → consecutive zero-item runs

        # Shutdown flag
        self.shutdown_requested: bool = False

        # Heartbeat tracking
        self.last_heartbeat_time: float = 0.0
        self.last_heartbeat_pages: int = 0


# ── Browser lifecycle ───────────────────────────────────────────────────────

async def start_browser(state: DaemonState):
    """Attach to the running pinchtab server (bridge port 9868 by default).

    Pinchtab's `always-on` policy auto-restarts crashed Chrome instances, so we
    don't have to launch Chrome ourselves — we just connect to the existing
    server.  See `base/pinchtab_client.py` for the HTTP client + Tab adapter.
    """
    browser_config = state.config.get("browser", {})
    headless = browser_config.get("headless", True)
    page_wait = browser_config.get("page_wait_seconds", 1.0)
    pinchtab_cfg = state.config.get("pinchtab", {})

    state.browser_manager = BrowserManager(
        headless=headless,
        page_wait_seconds=page_wait,
        pinchtab_config=pinchtab_cfg,
    )
    await state.browser_manager.start()
    state.last_restart_time = time.time()
    state.pages_since_restart = 0
    log.info("Browser started (engine=pinchtab, headless=%s)", headless)


async def restart_browser(state: DaemonState):
    """Clean reconnect to pinchtab + re-bind strategies.

    With pinchtab there are no orphaned Chrome processes to kill — the
    `always-on` supervisor manages Chrome itself, and it auto-restarts a
    crashed instance within seconds.  We only need to release our HTTP
    session and acquire a fresh tab.
    """
    log.info("Reconnecting to pinchtab (pages=%d, uptime=%ds)...",
             state.pages_since_restart, int(time.time() - state.last_restart_time))
    if state.browser_manager:
        await state.browser_manager.cleanup()
    state.browser_manager = None
    # Unbind strategies that hold browser references
    state.extraction_strategy = None
    state.pagination_strategy = None
    await asyncio.sleep(1)
    try:
        await asyncio.wait_for(start_browser(state), timeout=BROWSER_START_TIMEOUT)
        await asyncio.wait_for(
            _init_browser_bound_strategies(state), timeout=BROWSER_START_TIMEOUT)
    except asyncio.TimeoutError:
        log.error("Pinchtab reconnect timed out after %ds", BROWSER_START_TIMEOUT)
        state.browser_manager = None
        raise


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
        # Build search URL and navigate.
        # BD-local queries carry `KEYWORD|LAT|LNG` (from query_generator).
        # We strip the coords and build a region-anchored search URL:
        #   /search/KEYWORD/@lat,lng,13z?entry=ttu
        # Verified 2026-08-01: keyword + coords yields ~5x more results than
        # the unanchored text-only form (120 vs 22 for 'manufacturing
        # company' on Rajshahi).
        search_text = query
        coord_suffix = ""
        if "|" in query:
            parts = query.rsplit("|", 2)
            if len(parts) == 3:
                try:
                    lat, lng = float(parts[1]), float(parts[2])
                    search_text = parts[0]
                    coord_suffix = f"/@{lat:.4f},{lng:.4f},13z?entry=ttu"
                except (ValueError, IndexError):
                    pass  # malformed — fall back to plain text search

        url_template = state.config.get("search_url_template",
                                        "https://www.google.com/maps/search/{query}/")
        if coord_suffix:
            import urllib.parse
            # Strip the /{query}/ placeholder from the template, append coords.
            url_base = url_template.rsplit("{query}", 1)[0].rstrip("/")
            search_url = (
                f"{url_base}/{urllib.parse.quote(search_text, safe='')}{coord_suffix}"
            )
        else:
            # National/global queries: keep the original behavior.
            search_url = url_template.format(query=search_text)
        try:
            tab = await asyncio.wait_for(
                state.browser_manager.navigate(search_url),
                timeout=QUERY_NAV_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning("Navigation timed out for query '%s'", query[:60])
            return False, 0

        # Verify navigation actually reached Google Maps (detect stuck browsers)
        try:
            current_url = await asyncio.wait_for(
                tab.evaluate("window.location.href"),
                timeout=5.0
            )
            if "google.com/maps" not in current_url:
                log.warning("Navigation verification failed - expected GMaps, got: %s", current_url[:60])
                await restart_browser(state)
                return False, 0
        except Exception as e:
            log.warning("Navigation verification error: %s", e)
            await restart_browser(state)
            return False, 0

        if state.delay_manager:
            await state.delay_manager.apply_delay("between_requests")

        # Scroll and extract
        seen_items: set[str] = set()
        state.extraction_strategy.seen_items = set()  # reset
        if state.pagination_strategy and hasattr(state.pagination_strategy, "reset"):
            state.pagination_strategy.reset()

        scroll_attempts = 0
        max_scroll = state.config.get("pagination", {}).get("max_scroll_attempts", 200)
        total_extracted = 0

        while scroll_attempts < max_scroll:
            if state.shutdown_requested:
                return False, total_extracted

            if state.output_strategy and state.output_strategy.has_reached_limit():
                break

            try:
                has_more = await asyncio.wait_for(
                    state.pagination_strategy.has_more_results(),
                    timeout=SCROLL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning("has_more_results timed out — breaking scroll loop")
                break
            if not has_more:
                break

            try:
                items = await asyncio.wait_for(
                    state.extraction_strategy.extract_items(),
                    timeout=EXTRACTION_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning("extract_items timed out after %ds — breaking scroll loop",
                            EXTRACTION_TIMEOUT)
                break
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

            total_extracted += new_count
            log.info("Query '%s': extracted %d items (%d new)",
                     query[:60], len(items), new_count)

            try:
                loaded = await asyncio.wait_for(
                    state.pagination_strategy.load_more_results(),
                    timeout=SCROLL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning("load_more_results timed out — breaking scroll loop")
                break
            if not loaded:
                break

            scroll_attempts += 1
            await state.delay_manager.apply_delay("between_requests")

        return True, total_extracted

    except Exception as e:
        log.error("Search failed for '%s': %s", query[:60], e)
        return False, 0
    finally:
        # Close the tab after each query to prevent tab buildup (maxTabs=20 eviction)
        if state.browser_manager and state.browser_manager.tab:
            try:
                await state.browser_manager.close_tab()
            except Exception:
                log.debug("search_daemon: tab close failed", exc_info=True)


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


def retry_stale_failures(state: DaemonState, max_age_hours: float = 6.0):
    """Re-enqueue failed search queries older than max_age_hours for retry."""
    if not state.queue_strategy:
        return 0
    if hasattr(state.queue_strategy, "requeue_stale_failed"):
        return state.queue_strategy.requeue_stale_failed(max_age_hours)
    return 0


# Cap retries on queries that keep producing zero items.  Without this, a
# single bad query (e.g. a Bengali keyword with no real results, or a Google
# Maps search page that never renders the expected DOM) re-enters the loop
# every Redis visibility-timeout and burns ~30s of CPU per attempt — observed
# in 2026-08-09 logs as the same query being dequeued 97× back-to-back.
# After this many consecutive zero-item runs we mark the query as completed
# (skip) so it leaves the rotation for ~1h, after which retry_stale_failures
# will bring it back if it's still worth trying.
ZERO_ITEM_RETRY_CAP = 3


# ── Main loop ───────────────────────────────────────────────────────────────

def _check_staleness(state: DaemonState, last_write_time: float, label: str) -> float:
    """Log WARNING if no new data written in 1h. Returns current time."""
    now = time.monotonic()
    if now - last_write_time > 3600:  # 1 hour
        log.warning("STALENESS ALERT: no new %s data written in 1h", label)
    return now


def _check_pg_staleness(last_pg_check: float, table: str = "scraper.gmaps_search_results") -> float:
    """Query PG for latest updated_at. Warn if stale > 1h. Returns current time."""
    now = time.monotonic()
    if now - last_pg_check < PG_STALENESS_INTERVAL:
        return last_pg_check
    try:
        from psycopg import connect
        with connect(
            f"host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASSWORD} dbname={PG_DB}",
            autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(updated_at) FROM {table}")
                row = cur.fetchone()
                if row and row[0]:
                    age_s = (datetime.now(timezone.utc) - row[0]).total_seconds()
                    if age_s > 3600:
                        log.warning(
                            "STALENESS ALERT: %s last write was %.1fh ago (%s UTC)",
                            table, age_s / 3600, row[0],
                        )
                    else:
                        log.info("%s: last PG write %.0f min ago", table, age_s / 60)
    except Exception as e:
        log.error("PG staleness check failed: %s", e)
    return now


async def eternal_loop(state: DaemonState):
    """The forever loop: refill → dequeue → search → repeat."""
    last_stalled_check = 0.0
    last_write_time = time.monotonic()  # staleness watchdog
    last_pg_staleness_check = 0.0  # PG-level staleness (catches silent upsert exhaustion)

    while not state.shutdown_requested:
        try:
            now = time.monotonic()

            # 0. Heartbeat
            if now - state.last_heartbeat_time > HEARTBEAT_SEC:
                elapsed = now - state.last_heartbeat_time
                delta = state.total_pages_processed - state.last_heartbeat_pages
                if _HAS_PSUTIL:
                    mem_mb = _psutil.Process().memory_info().rss // 1048576
                    log.info(
                        "heartbeat uptime=%.0fs processed_delta=%d velocity=%d/hr mem=%dMB",
                        elapsed, delta, int(3600 * delta / elapsed) if elapsed > 0 else 0, mem_mb,
                    )
                else:
                    log.info(
                        "heartbeat uptime=%.0fs processed_delta=%d velocity=%d/hr",
                        elapsed, delta, int(3600 * delta / elapsed) if elapsed > 0 else 0,
                    )
                state.last_heartbeat_time = now
                state.last_heartbeat_pages = state.total_pages_processed

            # 1. Periodic stalled requeue
            if now - last_stalled_check > STALLED_REQUEUE_INTERVAL:
                requeue_stalled(state)
                last_stalled_check = now

            # 2. Refill queue if low
            refill_queue(state)

            # 2b. Retry stale failures (older than 6h)
            retry_stale_failures(state, max_age_hours=6.0)

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

            # 5. Staleness watchdog
            last_write_time = _check_staleness(state, last_write_time, "search")

            # 5b. PG-level staleness — catches silent upsert exhaustion
            # where mark_completed fires but ON CONFLICT DO UPDATE is a no-op
            # because GMaps returns identical results for exhausted queries.
            last_pg_staleness_check = _check_pg_staleness(last_pg_staleness_check)

            # 6. Dequeue next query
            query = state.queue_strategy.dequeue(timeout=10)
            if not query:
                stats = state.queue_strategy.get_stats()
                log.debug("No query available (pending=%d processing=%d)",
                          stats.get("pending", 0), stats.get("processing", 0))
                await asyncio.sleep(5)
                continue

            # 7. Process the query
            success, items_extracted = await search_single_query(state, query)
            if success:
                state.queue_strategy.mark_completed(query)
                # Reset the zero-item streak on any successful extraction, so
                # queries that intermittently return items are not punished.
                if items_extracted > 0:
                    state.zero_item_streak.pop(query, None)
                last_write_time = time.monotonic()  # update staleness timer
                state.consecutive_errors = 0
                state.pages_since_restart += 1
                state.total_pages_processed += 1
            elif items_extracted > 0:
                # Reached the scroll cap with at least some items but stopped
                # cleanly. Treat as completed so we don't retry-storm the query.
                state.queue_strategy.mark_completed(query)
                state.zero_item_streak.pop(query, None)
                state.consecutive_errors = 0
            else:
                # Zero items — count a streak and skip the query once it hits
                # the cap. `retry_stale_failures` will eventually bring it
                # back if it has a real chance of working later.
                streak = state.zero_item_streak.get(query, 0) + 1
                state.zero_item_streak[query] = streak
                if streak >= ZERO_ITEM_RETRY_CAP:
                    log.info(
                        "Skipping query after %d zero-item runs: %s",
                        streak, query[:60],
                    )
                    state.queue_strategy.mark_completed(query)
                    state.zero_item_streak.pop(query, None)
                else:
                    state.url_retries[query] = state.url_retries.get(query, 0) + 1
                    state.queue_strategy.mark_failed(
                        query, "Search extraction failed",
                        state.url_retries[query],
                    )
                state.consecutive_errors += 1

            # 8. Jitter delay
            await state.delay_manager.apply_delay("between_requests")

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
    import sys as _sys
    state = DaemonState()

    # Register signal handlers
    install_signal_handlers(state)

    log.info("started version=1 args=%s", " ".join(_sys.argv[1:]))
    log.info("Config: %s", CONFIG_PATH)
    log.info("PG: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    log.info("Browser restart: every %ds or %d pages",
             BROWSER_RESTART_INTERVAL_SEC, BROWSER_RESTART_PAGES)
    log.info("Queue low threshold: %d, batch size: %d",
             QUEUE_LOW_THRESHOLD, QUERY_BATCH_SIZE)

    await init_infrastructure(state)
    try:
        await eternal_loop(state)
    finally:
        log.info("stopped reason=%s", "SIGTERM" if state.shutdown_requested else "exit")


if __name__ == "__main__":
    asyncio.run(main())