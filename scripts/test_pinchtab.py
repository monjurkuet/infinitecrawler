#!/usr/bin/env python3
"""Quick integration test: pinchtab client + strategies against live GMaps.

Verifies the pinchtab adapter works end-to-end with the daemon's actual
pagination/extraction strategies — no PG writes, just prints what it finds.
"""
import asyncio
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("test_pinchtab")

from base.pinchtab_client import PinchtabClient, PinchtabConfig
from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory


async def main():
    config = ScraperFactory.load_config(str(REPO / "config" / "gmaps_bd_business_search.yaml"))
    log.info("Config loaded: %s", config.get("name"))

    # Create BrowserManager with pinchtab engine
    pinchtab_cfg = config.get("pinchtab", {})
    bm = BrowserManager(
        engine="pinchtab",
        headless=True,
        page_wait_seconds=4.0,
        pinchtab_config=pinchtab_cfg,
    )
    await bm.start()
    log.info("BrowserManager started (pinchtab)")

    # Verify connectivity — evaluate something basic
    try:
        tab = await bm.navigate("https://www.google.com/maps/search/it+companies+in+dhaka/")
        log.info("Navigated; tab=%s", type(tab).__name__)
        await asyncio.sleep(2)
        title = await tab.evaluate("document.title")
        log.info("Page title: %s", title)
        count = await tab.evaluate("document.querySelectorAll('a.hfpxzc').length")
        log.info("Result count: %s", count)
        # Try extraction via the strategies
        pag = ScraperFactory.create_strategy(
            "pagination", "infinite_scroll", bm, config,
        )
        ext = ScraperFactory.create_strategy(
            "extraction", "generic_selector", bm, config,
        )
        items = await ext.extract_items()
        log.info("Extracted %d items:", len(items))
        for it in items[:3]:
            log.info("  %s → %s", it.get("name", "?")[:40], (it.get("url") or "")[:80])

        # Try one scroll
        loaded = await pag.load_more_results()
        log.info("load_more: %s", loaded)

        items2 = await ext.extract_items()
        log.info("After scroll: %d items", len(items2))

    except Exception as e:
        log.error("Test failed: %s", e, exc_info=True)
        return 1
    finally:
        await bm.cleanup()

    log.info("Test PASSED")
    return 0


if __name__ == "__main__":
    rc = asyncio.run(main())
    sys.exit(rc)
