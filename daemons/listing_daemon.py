#!/usr/bin/env python3
"""
listing_daemon.py — Eternal Google Maps listing deep-extraction daemon.

Runs 24/7: pulls uncrawled listing URLs from PostgreSQL (search results not yet
extracted), deep-extracts phone/website/category/rating via multi-step scraping,
upserts to scraper.gmaps_listings.

Uses pinchtab-based strategies (extraction, output, queue, navigation).
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
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse  # noqa: E402

import psycopg
from dotenv import load_dotenv

if TYPE_CHECKING:
    from base.strategies import (
        ExtractionStrategy,
        OutputStrategy,
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
from utils.helpers import DelayManager  # noqa: E402
from utils.pg import get_pg_config, get_uncrawled_urls_sql  # noqa: E402
from daemons.common import (  # noqa: E402
    BROWSER_RESTART_INTERVAL_SEC,
    BROWSER_RESTART_PAGES,
    QUEUE_LOW_THRESHOLD,
    install_signal_handlers,
    shutdown_strategies,
)
from services.classification import _single_fallback, load_sectors, METHOD_FALLBACK_RULE  # noqa: E402
import re as _re  # noqa: E402


def to_cid_url(url: str) -> str:
    """Rewrite a Google Maps ``/place/<name>/data=!...:0xAAAA:0xBBBB`` URL into
    the headless-friendly ``?cid=<decimal>`` form.

    Headless Chrome (pinchtab) cannot render the ``/place/<name>/data=...``
    detail view — Google strips the CID payload and bounces to a bare map
    shell, so every selector misses.  Navigating via ``?cid=<decimal>`` loads
    the full detail panel reliably (name, rating, phone, address, plus code
    all populate).  Returns the original URL unchanged if no CID is found.
    """
    if not url or "?cid=" in url:
        return url
    # The place CID is the trailing 0x… feature id, e.g.
    #   /place/X/data=!…!1s0xAAAA:0xBBBB…   -> use 0xBBBB
    # Capture the LAST ":0x<hex>" (the feature CID Google keys ?cid on).
    matches = _re.findall(r":0x([0-9a-fA-F]+)", url)
    if not matches:
        return url
    try:
        cid = int(matches[-1], 16)
    except ValueError:
        return url
    return f"https://www.google.com/maps/place/?cid={cid}"


# ── Config ──────────────────────────────────────────────────────────────────

load_dotenv(REPO_ROOT / ".env")

CONFIG_PATH = REPO_ROOT / "config" / "gmaps_listings_working.yaml"
URL_FETCH_BATCH = 2000  # How many uncrawled URLs to pull from PG per refill (pitfall #9: keep Redis deep so workers never idle; raised from 1000 as backlog still grows ~800/h)
URL_MAX_RETRIES = 3  # Per-URL retry attempts
URL_RETRY_DELAY = 5  # Seconds between per-URL retries
URL_EXTRACTION_TIMEOUT = 25  # Seconds before extraction attempt is aborted (page_wait already gives time to render)
URL_NAV_TIMEOUT = 75  # Seconds for initial URL navigation (was 120; tightened so slow/blank pages fail fast and retry instead of pinning a worker)
BROWSER_START_TIMEOUT = 30  # Seconds for browser launch
STALLED_REQUEUE_INTERVAL = 60  # Check for stalled processing items every N sec
HEARTBEAT_SEC = int(os.environ.get("DAEMON_HEARTBEAT_SEC", "300"))  # log heartbeat every N sec

# Concurrency-storm guard: when two+ coroutines in the same gather() batch
# detect a fault after a browser restart (cleanup_all() invalidated all the
# pooled tabs at once), only ALLOW one to actually rebuild — the others skip
# the restart inside _restart_locked_get_tab if another restart completed
# within this window. 5s is comfortably above BROWSER_START_TIMEOUT-heavy
# rebuilds while small enough that a real second fault still re-triggers.
_RESTART_DEDUP_WINDOW_S = 5

# T4 diagnostics — sample HTML on persistent failure.  Off by default because
# page bytes bloat logs; set LOG_SAMPLE_HTML=1 to enable forensics.
LOG_SAMPLE_HTML = os.environ.get("LOG_SAMPLE_HTML", "0") == "1"
LOG_SAMPLE_BYTES = 500

# PG connection (separate from output strategy — used for live URL feed)
_pg = get_pg_config()
PG_HOST, PG_PORT = _pg["host"], _pg["port"]
PG_USER, PG_PASSWORD, PG_DB = _pg["user"], _pg["password"], _pg["dbname"]


def _connect_pg() -> psycopg.Connection:
    """Open a PG connection. Omit port for unix-socket hosts (psycopg3 parse bug)."""
    kwargs = dict(host=PG_HOST, user=PG_USER, password=PG_PASSWORD,
                  dbname=PG_DB, connect_timeout=10)
    if "/" not in str(PG_HOST):
        kwargs["port"] = PG_PORT
    return psycopg.connect(**kwargs)

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("listing_daemon")


# ── State ───────────────────────────────────────────────────────────────────

class DaemonState:
    """Mutable state tracked across the eternal loop."""

    def __init__(self):
        self.browser_manager: Optional[BrowserManager] = None
        self.output_strategy: Optional[OutputStrategy] = None
        self.extraction_strategy: Optional[ExtractionStrategy] = None
        self.queue_strategy: Optional[QueueStrategy] = None
        self.delay_manager: Optional[DelayManager] = None
        self.pg_conn: Optional[psycopg.Connection] = None
        self.config: dict = {}
        self.sectors: dict = {}  # BPT sectors for in-stream fallback classification

        # Restart tracking
        self.pages_since_restart: int = 0
        self.last_restart_time: float = 0.0
        self.total_pages_processed: int = 0
        # B14 — per-tab local page counters (avoid race under tab-pool concurrency);
        # folded into total_pages_processed after each gather() batch.
        self.tab_pages: dict[int, int] = {}
        # Serializes restart_browser() so concurrent process_url coroutines
        # in a gather batch don't race on cleanup_all() / start_browser().
        self.restart_lock = asyncio.Lock()
        # Monotonic timestamp of the most recent restart_browser() call; used
        # by _restart_locked_get_tab to dedup restarts when sibling coroutines
        # in the same gather() batch trip after the same underlying fault.
        self.last_browser_restart_monotonic: float = 0.0

        # Error tracking
        self.consecutive_errors: int = 0
        self.max_consecutive_errors: int = 10

        self.shutdown_requested: bool = False

        # Heartbeat tracking
        self.last_heartbeat_time: float = 0.0
        self.last_heartbeat_pages: int = 0

        # T4 — per-cycle field success counters for the cycle_summary log line
        self.cycle_success: dict[str, int] = {
            "phone": 0, "website": 0, "plus_code": 0, "category": 0,
        }
        self.cycle_retries: int = 0
        self.cycle_processed: int = 0
        self.cycle_last_summary: float = time.monotonic()

        # Phantom queue tracking — URLs that produced a name but NO
        # phone/website/rating/address/plus_code. These are queue-poison:
        # they come back through `backfill_pending` (uncrawled PG sweep) or
        # `retry_stale_failures` and re-render the bare shell again. Routing
        # them to `gmaps:phantom` lets the phantom-sweeper requeue them once
        # after a page-wait increase, without blocking the live queue.
        self.phantom_count: int = 0


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
    """Clean reconnect to pinchtab + refresh bound strategies.

    With pinchtab there are no orphaned Chrome processes to kill — the
    `always-on` supervisor manages Chrome itself.  We just release the HTTP
    session, acquire a fresh tab, and re-bind the extraction strategy.
    """
    log.info("Reconnecting to pinchtab (pages=%d, uptime=%ds)...",
             state.pages_since_restart, int(time.time() - state.last_restart_time))
    if state.browser_manager:
        await state.browser_manager.cleanup_all()
    state.browser_manager = None
    state.tab_pages = {}
    # Unbind browser-bound strategies
    state.extraction_strategy = None
    await asyncio.sleep(1)
    try:
        await asyncio.wait_for(start_browser(state), timeout=BROWSER_START_TIMEOUT)
        await asyncio.wait_for(
            _refresh_browser_bound_strategies(state), timeout=BROWSER_START_TIMEOUT)
        # Stamp the monotonic restart completion so concurrent callers in the
        # same gather() batch can dedup their own restart attempts (see
        # _restart_locked_get_tab).
        state.last_browser_restart_monotonic = time.monotonic()
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
        state.pg_conn = _connect_pg()
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
        except (psycopg.OperationalError, ConnectionError):
            log.info("PG connection stale — reconnecting…")
            try:
                state.pg_conn.close()
            except Exception:
                log.debug("listing_daemon: stale pg close failed", exc_info=True)
            state.pg_conn = None
    try:
        state.pg_conn = _connect_pg()
        state.pg_conn.autocommit = True
        log.info("PG reconnected: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    except (psycopg.OperationalError, ConnectionError) as e:
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
            sql = get_uncrawled_urls_sql(limit=URL_FETCH_BATCH)
            cur.execute(sql)
            rows = cur.fetchall()
        # Only Google Maps URLs can be extracted by the ?cid= /maps/place/ flow.
        # Anything else (business URLs leaked into the queue, wrong-domain URLs)
        # burns retries and never extracts, so drop it at intake.
        urls = []
        nonmaps_dropped = 0
        for r in rows:
            u = r[0]
            if not u:
                continue
            h = urlparse(u).netloc.lower()
            if "google." in h and "/maps" in u:
                urls.append(u)
            else:
                nonmaps_dropped += 1
        if nonmaps_dropped:
            log.info("prefilter_nonmaps dropped=%d", nonmaps_dropped)
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


def retry_stale_failures(state: DaemonState, max_age_hours: float = 6.0):
    """Re-enqueue failed extraction URLs that are older than max_age_hours."""
    if not state.queue_strategy:
        return 0
    if hasattr(state.queue_strategy, "requeue_stale_failed"):
        return state.queue_strategy.requeue_stale_failed(max_age_hours)
    return 0


def requeue_stalled(state: DaemonState):
    """Move timed-out processing items back to pending."""
    if state.queue_strategy and hasattr(state.queue_strategy, "maybe_requeue_stalled"):
        requeued = state.queue_strategy.maybe_requeue_stalled()
        if requeued:
            log.info("Requeued %d stalled URLs", requeued)


# ── Listing extraction logic ────────────────────────────────────────────────


def _has_meaningful_data(item: dict) -> bool:
    """Name-only rows are thin extractions (page rendered but phone/website/
    rating/address all missed). Returning False routes them to the retry loop
    instead of persisting a bare row and marking the URL complete.
    Genuinely sparse businesses will exhaust retries and move to failed —
    better than silently persisting empty rows as 'done'."""
    return bool(item.get("name")) and any(
        item.get(k) for k in ("phone", "website", "rating", "address", "plus_code", "category", "review_count")
    )


def _is_phantom_row(row: dict) -> bool:
    """True if the row has a name but NO contact/rating signals — i.e. the
    listing detail never rendered (bare shell). Phantom rows come from a
    tab-throttling render, not from a legitimately sparse business.

    Where possible we mark these *before* they land in DB (via
    `_mark_phantom_url`), but if they got persisted (partial success), the
    watchdog's `backfill_phantom()` sweeps them.

    Only name-only rows qualify; any extra field (even an empty string)
    means real data flowed.
    """
    return bool(row.get("name")) and not any(
        row.get(k) for k in ("phone", "website", "rating", "address", "plus_code")
    )


def _mark_phantom_url(state: DaemonState, url: str) -> None:
    """Flag a URL so it's retried via the phantom sweep instead of the
    live queue. Uses the queue_strategy's failover hooks so the phantom URL
    is invisible to `mark_completed` until it either renders with full data
    or ages out past `retry_stale_failures`.
    """
    if not state.queue_strategy:
        return
    try:
        # We don't reinsert into gmaps:pending because that would block the
        # cycle — the URL has already been marked completed/failed. Instead
        # we push to a dedicated phantom list that the phantom-sweeper.service
        # re-pushes to pending when the page-wait condition has stabilized.
        client = getattr(state.queue_strategy, "client", None)
        if client is None:
            return
        client.rpush("gmaps:phantom", url)
        state.phantom_count += 1
        log.info("Phantom URL queued for backfill: %s (total=%d)", url[:70], state.phantom_count)
    except Exception as e:
        log.warning("phantom requeue failed for %s: %s", url[:60], e)


async def _restart_locked_get_tab(state: DaemonState, tab_key: int):
    """Serialize browser restart across the concurrent batch, then hand back a
    fresh tab from the rebuilt pool (old tab objects die with the old session).

    Dedup: if a sibling coroutine in the same gather() batch already triggered
    a restart within the last ~5s, skip the (idempotent-but-expensive) rebuild
    and just acquire a fresh tab. Without this dedup, N concurrent coroutines
    that all detect a fault (because cleanup_all() invalidated every pooled
    tab at once) would each serialize on restart_lock and rebuild the browser
    N times for the single underlying fault.
    """
    async with state.restart_lock:
        if state.shutdown_requested:
            return None
        now = time.monotonic()
        if now - state.last_browser_restart_monotonic > _RESTART_DEDUP_WINDOW_S:
            await restart_browser(state)
        else:
            log.debug("skip restart (within %ds dedup window)", _RESTART_DEDUP_WINDOW_S)
    try:
        return await state.browser_manager.acquire_tab()
    except Exception as e:
        log.warning("tab re-acquire after restart failed: %s", e)
        return None


async def process_url(state: DaemonState, url: str, tab, tab_key: int) -> bool:
    """Deep-extract a single listing URL with retry logic.

    ``tab`` is the pre-acquired PinchtabTab from the pool. ``tab_key`` is an
    **opaque identity** (currently ``id(tab)``) used as the per-iteration key
    for ``state.tab_pages`` — it is NOT a stable pool slot index.  A fresh tab
    acquired after a restart gets a new ``id()``, so the key is unique per
    live tab object.  Per-tab local counter (`state.tab_pages[tab_key]`)
    avoids racing on `state.total_pages_processed` under concurrency; the
    aggregate is folded into the global counter after gather() returns.

    NOTE: a browser restart (nav timeout / extraction timeout / fatal
    exception) invalidates every pooled tab.  After a restart we re-acquire a
    fresh tab and keep retrying with it.
    """
    last_failure_kind: Optional[str] = None
    for attempt in range(URL_MAX_RETRIES):
        try:
            # Navigate with timeout — re-navigate the existing tab in-place
            # via the tab-scoped /tabs/{id}/navigate endpoint so the listing
            # daemon's worker pool keeps the same set of tabs (avoiding the
            # pinchtab ``maxTabs`` eviction cliff that orphans worker tabs
            # under concurrent navigation).
            try:
                nav_url = to_cid_url(url)
                tab = await asyncio.wait_for(
                    tab._client.navigate(nav_url, tab_id=tab._tab_id),
                    timeout=URL_NAV_TIMEOUT,
                )
            except asyncio.TimeoutError:
                last_failure_kind = "nav_timeout"
                log.warning("Navigation timed out for %s (attempt %d/%d)",
                            url[:60], attempt + 1, URL_MAX_RETRIES)
                if attempt < URL_MAX_RETRIES - 1:
                    tab = await _restart_locked_get_tab(state, tab_key)
                    if tab is None:
                        return False
                    continue
                return False
            if state.delay_manager:
                await state.delay_manager.apply_delay("page_load")

            # Extract with timeout — multi-step extraction can hang on slow/broken pages
            try:
                items = await asyncio.wait_for(
                    state.extraction_strategy.extract_items(tab),
                    timeout=URL_EXTRACTION_TIMEOUT,
                )
            except asyncio.TimeoutError:
                last_failure_kind = "extract_timeout"
                log.warning("Extraction timed out after %ds for %s",
                            URL_EXTRACTION_TIMEOUT, url[:60])
                items = []
                # Restart browser after ANY extraction timeout — wait_for cancel
                # can leave the Chrome tab in a bad state, causing subsequent
                # navigate() calls to hang indefinitely.
                tab = await _restart_locked_get_tab(state, tab_key)
                if tab is None:
                    return False
                state.consecutive_errors = 0

            # Filter out items with no meaningful data (empty name, phone, etc.)
            if items:
                items = [item for item in items if _has_meaningful_data(item)]
                if not items:
                    last_failure_kind = "no_meaningful_data"
                    log.warning("No meaningful data extracted from %s (attempt %d/%d)",
                                url[:60], attempt + 1, URL_MAX_RETRIES)

            if not items:
                last_failure_kind = last_failure_kind or "empty_extract"
                log.warning("No data extracted from %s (attempt %d/%d)",
                            url[:60], attempt + 1, URL_MAX_RETRIES)
                # Extraction timeout OR empty result: increment error counter so
                # a stuck browser tab triggers a restart on threshold instead of
                # silently wasting 45s per URL.  Empty extraction on the final
                # attempt is treated as failure (was previously phantom-success,
                # which inflated the listing count with empty rows).
                state.consecutive_errors += 1
                if attempt == URL_MAX_RETRIES - 1:
                    break
                state.cycle_retries += 1
                await asyncio.sleep(URL_RETRY_DELAY)
                continue

            # Write to PG
            for item in items:
                item["_crawl_meta"] = {
                    "source_url": url,
                    "pages_processed": state.tab_pages.get(tab_key, 0),
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
                # T4 — per-cycle field success counters
                for field in ("phone", "website", "plus_code", "category"):
                    if item.get(field):
                        state.cycle_success[field] += 1

            # Phantom-row detection (pre-write gate): if the page rendered but
            # contains ONLY a name (no phone/website/rating/address/plus_code),
            # Google bounced us to the bare mobile view. Don't write the shell
            # row to PG — requeue it for a later retry after the tab settles.
            if items and all(_is_phantom_row(it) for it in items):
                _mark_phantom_url(state, url)
                state.consecutive_errors += 1
                # Don't burn more retries — Google is actively bouncing this URL.
                return False

            log.info("Extracted %d fields from %s (attempt %d/%d)",
                     len(items), url[:60], attempt + 1, URL_MAX_RETRIES)
            state.tab_pages[tab_key] = state.tab_pages.get(tab_key, 0) + 1
            return True

        except psycopg.errors.UniqueViolation:
            log.debug("Already in DB (duplicate source_url): %s", url[:60])
            # Tab is reused by the pool — no close.
            return True
        except Exception as e:
            last_failure_kind = f"exception:{type(e).__name__}"
            log.warning("Attempt %d/%d failed for %s: %s",
                        attempt + 1, URL_MAX_RETRIES, url[:60], e)
            if attempt < URL_MAX_RETRIES - 1:
                tab = await _restart_locked_get_tab(state, tab_key)
                if tab is None:
                    return False
                await asyncio.sleep(2)

    # All retries exhausted — T4: emit ERROR with sample HTML for forensics
    log.error("All %d attempts failed for %s (kind=%s)",
              URL_MAX_RETRIES, url[:60], last_failure_kind)
    if LOG_SAMPLE_HTML and tab is not None:
        try:
            sample = await tab.evaluate(
                "(document.documentElement.outerHTML || '')[:%d]" % LOG_SAMPLE_BYTES
            )
            if sample:
                log.error("sample_html url=%s html=%s", url[:120], str(sample)[:LOG_SAMPLE_BYTES])
        except Exception as sample_exc:
            log.debug("sample_html capture failed: %s", sample_exc)
    return False


# ── Main loop ───────────────────────────────────────────────────────────────

async def eternal_loop(state: DaemonState):
    """The forever loop: refill → dequeue → extract → repeat."""
    last_stalled_check = 0.0
    SUMMARY_INTERVAL = 300  # T4 — emit cycle_summary every 5 min

    while not state.shutdown_requested:
        try:
            now = time.monotonic()

            # 0a. Heartbeat
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

            # T4 — per-cycle summary line (success{...} retries{...})
            if now - state.cycle_last_summary >= SUMMARY_INTERVAL:
                log.info(
                    "cycle_summary processed=%d success{phone:%d, website:%d, plus_code:%d, category:%d} retries{%d→%d}",
                    state.cycle_processed,
                    state.cycle_success["phone"],
                    state.cycle_success["website"],
                    state.cycle_success["plus_code"],
                    state.cycle_success["category"],
                    state.cycle_retries, state.cycle_retries,
                )
                state.cycle_last_summary = now

            # 0. Periodic write-batch flush (5s timer, supersedes 50-row size trigger)
            if state.output_strategy and hasattr(state.output_strategy, "flush_if_due"):
                try:
                    await state.output_strategy.flush_if_due()
                except Exception as flush_exc:
                    log.warning("Periodic flush failed: %s", flush_exc)

            # 1. Periodic stalled requeue
            if now - last_stalled_check > STALLED_REQUEUE_INTERVAL:
                requeue_stalled(state)
                last_stalled_check = now

            # 2. Refill queue from PG if low
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

            # 4. Too many consecutive errors?
            if state.consecutive_errors >= state.max_consecutive_errors:
                log.warning("%d consecutive errors — restarting browser",
                            state.consecutive_errors)
                await restart_browser(state)
                state.consecutive_errors = 0

            # 5. Dequeue a batch of URLs (one per tab in the pool).
            pool_size = state.browser_manager.tab_pool_size if state.browser_manager else 1
            batch = []
            for _ in range(pool_size):
                url = state.queue_strategy.dequeue(timeout=10)
                if not url:
                    break
                batch.append(url)

            if not batch:
                stats = state.queue_strategy.get_stats()
                log.debug("No URL available (pending=%d processing=%d)",
                          stats.get("pending", 0), stats.get("processing", 0))
                await asyncio.sleep(5)
                continue

            # 6. Acquire one tab per URL (creates tabs lazily up to pool size),
            # then process the batch in parallel.  ``key = id(tab)`` is an
            # opaque per-iteration identity for state.tab_pages — not a slot.
            tabs_with_key = []
            for url in batch:
                tab = await state.browser_manager.acquire_tab()
                tabs_with_key.append((tab, id(tab)))

            results = await asyncio.gather(
                *(process_url(state, url, tab, key) for url, (tab, key) in zip(batch, tabs_with_key)),
                return_exceptions=True,
            )

            # 7. Aggregate per-tab local counters into global.
            for url, (_tab, key), result in zip(batch, tabs_with_key, results):
                state.cycle_processed += 1
                if isinstance(result, Exception):
                    log.error("process_url raised: %s url=%s", result, url[:80])
                    state.consecutive_errors += 1
                    state.queue_strategy.mark_failed(url, f"Exception: {result}",
                                                     state.consecutive_errors)
                    continue
                if result:
                    state.queue_strategy.mark_completed(url)
                    state.consecutive_errors = 0
                    state.pages_since_restart += 1
                else:
                    state.consecutive_errors += 1
                    state.queue_strategy.mark_failed(url, "Extraction exhausted retries",
                                                     state.consecutive_errors)
                    # Browser restart handled by scheduled triggers (step 3/4)
                    # or by process_url() on timeout. Do NOT restart on every failure.

            # Fold per-tab counters into the global total (atomic write).
            tab_total = sum(state.tab_pages.values())
            state.total_pages_processed += tab_total
            state.tab_pages = {}

            # 8. Jitter delay between batches
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
            log.debug("listing_daemon: pg close failed", exc_info=True)
        state.pg_conn = None

    log.info("Listing daemon stopped. Total pages: %d.",
             state.total_pages_processed)
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
    log.info("Queue low threshold: %d, PG fetch batch: %d",
             QUEUE_LOW_THRESHOLD, URL_FETCH_BATCH)
    log.info("URL retries: %d attempts, %ds delay",
             URL_MAX_RETRIES, URL_RETRY_DELAY)

    # Preload BPT sectors once for in-stream fallback classification
    try:
        state.sectors = load_sectors()
        active = sum(1 for s in state.sectors.values() if s.get("status") == "active")
        log.info("Loaded %d sectors (%d active) for in-stream fallback", len(state.sectors), active)
    except Exception as e:
        log.warning("Failed to load sectors, in-stream fallback disabled: %s", e)
        state.sectors = {}

    await init_infrastructure(state)
    try:
        await eternal_loop(state)
    finally:
        log.info("stopped reason=%s total_pages=%d",
                 "SIGTERM" if state.shutdown_requested else "exit",
                 state.total_pages_processed)


if __name__ == "__main__":
    asyncio.run(main())