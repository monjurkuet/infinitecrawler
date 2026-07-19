#!/usr/bin/env python3
"""Multi-scroll integration test: navigate once, scroll up to N times, extract each round.

This simulates the `search_single_query` loop from the daemon — real sustained
GMaps scraping.  Checks that recovery logic survives through scroll iterations.
"""
import asyncio
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("test_scrollback")

from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory


async def main():
    config = ScraperFactory.load_config(str(REPO / "config" / "gmaps_bd_business_search.yaml"))
    bm = BrowserManager(
        engine="pinchtab",
        headless=True,
        page_wait_seconds=4.0,
        pinchtab_config=config.get("pinchtab", {}),
    )
    await bm.start()

    pag = ScraperFactory.create_strategy("pagination", "infinite_scroll", bm, config)
    ext = ScraperFactory.create_strategy("extraction", "generic_selector", bm, config)
    pag.last_result_count = 0  # reset
    seen = set()

    try:
        tab = await bm.navigate("https://www.google.com/maps/search/clothing+stores+in+dhaka/")
        log.info("Navigated; tab=%s", type(tab).__name__)

        # Initial extraction
        items = await ext.extract_items()
        for i in items:
            seen.add(i.get("url") or i.get("name"))
        log.info("Initial extract: %d items (cumulative=%d)", len(items), len(seen))

        for i in range(8):  # 8 scroll rounds
            loaded = await pag.load_more_results()
            log.info("round %d: load_more=%s", i + 1, loaded)
            if not loaded:
                break
            items = await ext.extract_items()
            new = 0
            for it in items:
                k = it.get("url") or it.get("name")
                if k and k not in seen:
                    seen.add(k)
                    new += 1
            log.info("round %d: %d items, %d new (cumulative=%d)",
                     i + 1, len(items), new, len(seen))

        log.info("SUSTAINED SCROLL TEST PASSED — cumulative items: %d", len(seen))
        return 0
    except Exception as e:
        log.error("Test FAILED: %s", e, exc_info=True)
        return 1
    finally:
        await bm.cleanup()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
