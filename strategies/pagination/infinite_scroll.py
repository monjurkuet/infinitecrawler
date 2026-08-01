from base.strategies import PaginationStrategy
import logging


# Container selectors to probe for, in order of preference.  GMaps uses
# `div[role="feed"]` on the current UI; older layouts used `role="list"`.
# `role="main"` is a fallback scrollable pane.
_CONTAINER_SELECTORS = [
    'div[role="feed"]',
    'div[role="list"]',
    'div[role="main"]',
]


class InfiniteScrollPaginationStrategy(PaginationStrategy):
    """Handles infinite scroll pagination like Google Maps"""

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config.get("pagination", {})
        self.logger = logging.getLogger(self.__class__.__name__)
        self.last_result_count = 0
        self.max_scroll_attempts = self.config.get("max_scroll_attempts", 500)
        self.scroll_attempts = 0
        # Number of consecutive scrolls that added no new results before we
        # declare the end-of-list.  GMaps sometimes re-renders the feed mid
        # scroll, so one empty round is not conclusive.
        self.stall_threshold = self.config.get("stall_threshold", 8)
        self._stall_count = 0
        self._cumulative = 0

    async def has_more_results(self) -> bool:
        """Check if there are more results to load"""
        if self.scroll_attempts >= self.max_scroll_attempts:
            return False
        return True

    def reset(self):
        """Reset per-query state. Called by the daemon before each query —
        the strategy instance lives for the daemon lifetime, so without this
        scroll_attempts/last_result_count would bleed across queries and the
        max_scroll_attempts cap would eventually kill scrolling for every
        subsequent query.
        """
        self.last_result_count = 0
        self.scroll_attempts = 0
        self._stall_count = 0
        self._cumulative = 0

    async def _scroll_pane(self, tab) -> bool:
        """Probe for a scrollable results pane and scroll it to the bottom.

        Returns True if a scroll was issued; False if no pane was found.
        """
        import json as _json

        # Probe for an existing scrollable container.  Selectors may contain
        # double quotes (e.g. `div[role="feed"]`) so we embed them as a
        # JSON-encoded JS array and JSON.parse it inside the snippet — this
        # avoids the unbalanced-quote SyntaxError that naive string
        # interpolation produces.
        sels_json = _json.dumps(_CONTAINER_SELECTORS)
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
            # No scrollable pane found — GMaps may be mid re-render.
            return False

        sel_json = _json.dumps(sel)
        # GMaps lazy-load is driven by IntersectionObserver + scroll events.
        # A programmatic scrollTop assignment alone does NOT trigger it — we
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

    async def load_more_results(self) -> bool:
        """Load more results by scrolling to bottom of container"""
        try:
            tab = self.browser_manager.tab
            if not tab:
                self.logger.error("No tab available for scrolling")
                return False

            scrolled = await self._scroll_pane(tab)
            if not scrolled:
                # No scrollable pane found — GMaps may be mid re-render or
                # the feed hasn't mounted yet.  Do not burn the stall budget
                # on this; just wait and let the caller retry.
                await tab.wait(1)
                return True

            # Wait for new content to load
            await tab.wait(3)

            # Check if new content was loaded
            items_selector = self.config.get("items_selector", "a.hfpxzc")
            feed_elements = await tab.select_all(items_selector, timeout=10)
            current_count = len(feed_elements)

            new_this_round = max(0, current_count - self._cumulative)
            self._cumulative = current_count
            self.logger.info(
                f"Scroll {self.scroll_attempts + 1}: {current_count} cards, "
                f"{new_this_round} new (cumulative {current_count})"
            )

            # Check if we've reached the end
            if current_count == self.last_result_count:
                self._stall_count += 1
                if self._stall_count >= self.stall_threshold:
                    self.logger.info(
                        f"No new results after {self.stall_threshold} scrolls. "
                        f"Reached the end ({current_count} unique)."
                    )
                    self._stall_count = 0
                    self._cumulative = 0
                    return False
            else:
                self._stall_count = 0

            self.last_result_count = current_count
            self.scroll_attempts += 1
            return True

        except Exception as e:
            self.logger.error(f"Error during scrolling: {e}")
            return False
