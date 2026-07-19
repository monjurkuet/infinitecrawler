"""Shared daemon boilerplate — browser restart constants, signal handling, graceful shutdown.

Both ``listing_daemon.py`` and ``search_daemon.py`` import from here to avoid ~40
lines of duplicated code per file.
"""

import asyncio
import logging
import signal

log = logging.getLogger("daemon.common")

# ── Browser lifecycle ────────────────────────────────────────────────────────

BROWSER_RESTART_INTERVAL_SEC = 3600  # 1 hour — restart browser regardless
BROWSER_RESTART_PAGES = 100         # also restart after this many pages
QUEUE_LOW_THRESHOLD = 20            # pull more items from source when pending < this

# ── Signal handling ──────────────────────────────────────────────────────────

async def shutdown_strategies(state):
    """Clean up browser, output, queue strategies (daemon-agnostic).

    Each daemon adds its own extras (PG close, stats logging, etc.) after calling
    this function.
    """
    if state.browser_manager:
        await state.browser_manager.cleanup()
        state.browser_manager = None
    if state.output_strategy and hasattr(state.output_strategy, "cleanup"):
        cleanup = state.output_strategy.cleanup()
        if asyncio.iscoroutine(cleanup):
            await cleanup
    if state.queue_strategy and hasattr(state.queue_strategy, "cleanup"):
        cleanup = state.queue_strategy.cleanup()
        if asyncio.iscoroutine(cleanup):
            await cleanup


def create_signal_handler(state):
    """Return a signal handler that sets ``state.shutdown_requested = True``."""
    def handler(sig, frame):
        log.info("Received signal %s — initiating graceful shutdown", sig)
        state.shutdown_requested = True
    return handler


def install_signal_handlers(state):
    """Register SIGTERM/SIGINT handlers on the current event loop."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, create_signal_handler(state), sig, None)


def cleanup_orphaned_chrome_dirs():
    """No-op stub — kept for backward compatibility with callers that still
    invoke it.

    With pinchtab the browser runs in an external server and manages its own
    Chrome profile directory (`/root/.pinchtab/chrome-profile`), so there are
    no per-process temp profile dirs to clean up.  Before the pinchtab migration
    this function dropped nodriver's `~/.local/share/nodriver/uc_*` and
    `/tmp/uc_*` temp dirs.
    """
    return 0