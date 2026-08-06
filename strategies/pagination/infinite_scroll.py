from base.strategies import PaginationStrategy
import json
import logging



class InfiniteScrollPaginationStrategy(PaginationStrategy):
    """Handles infinite scroll pagination like Google Maps.

    GMaps virtualizes its results pane: the DOM only ever holds ~10-20 cards,
    old cards unmount as new ones mount, and the feed container itself
    disappears/re-appears during lazy-load. Neither card count nor scrollHeight
    is a reliable end-of-results signal (scrollHeight fluctuates on re-render).

    The reliable signal is *cumulative unique result URLs*: keep scrolling while
    each scroll adds at least one new href. Stop after `stall_threshold`
    consecutive scrolls that add zero new unique hrefs (measured gap between
    batches can reach 6 scrolls, so default threshold is 8).
    """

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config.get("pagination", {})
        self.logger = logging.getLogger(self.__class__.__name__)
        self.max_scroll_attempts = self.config.get("max_scroll_attempts", 500)
        self.stall_threshold = self.config.get("stall_threshold", 8)
        self.scroll_attempts = 0
        self._seen_urls: set[str] = set()
        self._no_new_streak = 0
        # After the first successful scroll, GMaps may remount the feed with a
        # different container. Probe list in preference order.
        self._probe_count = 0

    # Containers GMaps uses for its scrollable results pane, in preference order.
    _CONTAINER_SELECTORS = [
        'div[role="feed"]',
        'div[role="list"]',
        'div[role="main"]',
    ]

    async def reset(self):
        """Call once per new query — clears per-query scroll state."""
        self.scroll_attempts = 0
        self._seen_urls = set()
        self._no_new_streak = 0

    async def has_more_results(self) -> bool:
        """Check if there are more results to load"""
        if self.scroll_attempts >= self.max_scroll_attempts:
            self.logger.info(
                f"Hit max_scroll_attempts={self.max_scroll_attempts} — stopping"
            )
            return False
        return True

    async def _scroll_pane(self, tab) -> bool:
        """Scroll the results pane to the bottom.

        Returns True if a scroll was issued (even if the container was missing —
        GMaps recreates the feed during lazy-load, so a missing container is not
        a failure; we just retry next cycle).
        """
        import json as _json

        # Probe for an existing scrollable container.
        # Selectors may contain double quotes (e.g. `div[role="feed"]`) so we
        # embed them as a JSON-encoded JS array and JSON.parse it inside the
        # snippet — that avoids the unbalanced-quote SyntaxError the old
        # string-concat version produced.
        sels_json = _json.dumps(self._CONTAINER_SELECTORS)
        probe_js = (
            "(() => {"
            "const sels = JSON.parse(" + _json.dumps(sels_json) + ");"
            "for (const s of sels) {"
            "  const el = document.querySelector(s);"
            "  if (el && el.scrollHeight > el.clientHeight) return s;"
            "}"
            "return '';})()"
        )
        try:
            sel = await tab.evaluate(probe_js)
        except Exception as e:
            self.logger.debug(f"Container probe failed: {e}")
            return False
        if not sel:
            # No scrollable pane found — GMaps may be mid re-render. Try window.
            try:
                await tab.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                pass
            return False

        sel_json = _json.dumps(sel)
        # GMaps lazy-load is driven by IntersectionObserver + scroll events.
        # Programmatic scrollTop assignment alone does NOT trigger it — we
        # must also dispatch a scroll event so the observer fires and new
        # cards mount.  Verified against pinchtab 0.15 + headless Chrome 144.
        scroll_js = (
            "(() => {const s = JSON.parse(" + _json.dumps(sel_json) + ");"
            "const el = document.querySelector(s);"
            "if (!el) return false;"
            "el.scrollTop = el.scrollHeight;"
            "el.dispatchEvent(new Event('scroll', {bubbles: true}));"
            "return true;})()"
        )
        try:
            return bool(await tab.evaluate(scroll_js))
        except Exception as e:
            self.logger.debug(f"Scroll failed: {e}")
            return False

    async def _collect_urls(self, tab) -> list[str]:
        """Return current result card URLs (deduped, in DOM order)."""
        import json as _json

        items_selector = self.config.get("items_selector", "a.hfpxzc")
        js = (
            "(() => {"
            "const els = document.querySelectorAll("
            + _json.dumps(items_selector)
            + ");"
            "const out = [];"
            "for (const el of els) {"
            "  const href = el.getAttribute('href') || '';"
            "  if (href && out.indexOf(href) === -1) out.push(href);"
            "}"
            "return out;})()"
        )
        try:
            urls = await tab.evaluate(js)
        except Exception as e:
            self.logger.debug(f"URL collection failed: {e}")
            return []
        if isinstance(urls, str):
            try:
                urls = json.loads(urls)
            except Exception:
                return []
        return [u for u in (urls or []) if isinstance(u, str) and u]

    async def load_more_results(self) -> bool:
        """Scroll to the bottom and report whether results are still arriving.

        Continue signal: cumulative unique href growth. Each cycle scrolls,
        waits for lazy-load, then counts how many *new* unique URLs appeared.
        NO_FEED cycles (feed mid-re-render) return True without consuming the
        stall budget so transient re-renders never end the query early.
        """
        try:
            tab = self.browser_manager.tab
            if not tab:
                self.logger.error("No tab available for scrolling")
                return False

            scrolled = await self._scroll_pane(tab)

            # Wait for lazy-loaded content
            await tab.wait(4)

            urls = await self._collect_urls(tab)
            new_urls = [u for u in urls if u not in self._seen_urls]
            self._seen_urls.update(urls)

            self.scroll_attempts += 1

            if new_urls:
                self._no_new_streak = 0
                self.logger.info(
                    f"Scroll {self.scroll_attempts}: {len(urls)} cards, "
                    f"{len(new_urls)} new (cumulative {len(self._seen_urls)})"
                )
                return True

            if not scrolled:
                # Feed temporarily missing (re-render) — don't burn stall budget.
                self.logger.debug(
                    f"Scroll {self.scroll_attempts}: no container, retrying"
                )
                return True

            self._no_new_streak += 1
            self.logger.info(
                f"Scroll {self.scroll_attempts}: {len(urls)} cards, "
                f"no new ({self._no_new_streak}/{self.stall_threshold})"
            )

            if self._no_new_streak >= self.stall_threshold:
                self.logger.info(
                    f"No new results after {self.stall_threshold} scrolls. "
                    f"Reached the end ({len(self._seen_urls)} unique)."
                )
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error during scrolling: {e}")
            return False
