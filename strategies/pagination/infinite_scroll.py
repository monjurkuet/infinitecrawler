from base.strategies import PaginationStrategy
import logging
import asyncio


class InfiniteScrollPaginationStrategy(PaginationStrategy):
    """Handles infinite scroll pagination like Google Maps"""

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config.get("pagination", {})
        self.logger = logging.getLogger(self.__class__.__name__)
        self.last_result_count = 0
        self.max_scroll_attempts = self.config.get("max_scroll_attempts", 500)
        self.scroll_attempts = 0

    async def has_more_results(self) -> bool:
        """Check if there are more results to load"""
        if self.scroll_attempts >= self.max_scroll_attempts:
            return False
        return True

    async def load_more_results(self) -> bool:
        """Load more results by scrolling to bottom of container"""
        try:
            tab = self.browser_manager.tab
            if not tab:
                self.logger.error("No tab available for scrolling")
                return False

            # Get container selector from config
            container_selector = self.config.get("container", 'div[role="feed"]')

            # Find the scrollable container
            feed_container = await tab.select(container_selector, timeout=10)
            if not feed_container:
                self.logger.warning(
                    f"Could not find feed container with selector: {container_selector}"
                )
                return False

            # Get current scroll height
            current_height = await tab.evaluate(
                f"document.querySelector('{container_selector}').scrollHeight"
            )

            # Scroll to the bottom using configured script or default
            scroll_script = self.config.get("scroll_script")
            if scroll_script:
                await tab.evaluate(scroll_script)
            else:
                await tab.evaluate(
                    f"document.querySelector('{container_selector}').scrollTo(0, {current_height})"
                )

            # Wait for new content to load
            await tab.wait(3)

            # Check if new content was loaded
            items_selector = self.config.get("items_selector", "a.hfpxzc")
            restaurant_elements = await tab.select_all(items_selector)
            current_count = len(restaurant_elements)

            self.logger.info(f"Current result count: {current_count}")

            # Check if we've reached the end
            if current_count == self.last_result_count:
                # Try one more time to be sure
                await tab.wait(2)
                restaurant_elements = await tab.select_all(items_selector)
                current_count = len(restaurant_elements)
                if current_count == self.last_result_count:
                    self.logger.info("No new results loaded. Reached the end.")
                    return False

            self.last_result_count = current_count
            self.scroll_attempts += 1
            return True

        except Exception as e:
            self.logger.error(f"Error during scrolling: {e}")
            return False
