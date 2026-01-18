from base.strategies import PaginationStrategy
import logging
import asyncio


class NextButtonPaginationStrategy(PaginationStrategy):
    """Handles next button pagination"""

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config.get("pagination", {})
        self.logger = logging.getLogger(self.__class__.__name__)
        self.current_page = 1
        self.max_pages = self.config.get("max_pages", 50)

    async def has_more_results(self) -> bool:
        """Check if there are more pages to load"""
        return self.current_page <= self.max_pages

    async def load_more_results(self) -> bool:
        """Load next page by clicking next button"""
        try:
            tab = self.browser_manager.tab
            if not tab:
                self.logger.error("No tab available for navigation")
                return False

            # Get next button selector from config
            next_button_selector = self.config.get(
                "next_button_selector", '.next-page, .next-button, a[rel="next"]'
            )

            # Find next button
            next_button = await tab.select(next_button_selector, timeout=5)
            if not next_button:
                # Check if button is disabled
                disabled_selectors = [
                    f"{next_button_selector}.disabled",
                    f"{next_button_selector}[disabled]",
                    f"{next_button_selector}[aria-disabled='true']",
                ]
                for selector in disabled_selectors:
                    disabled_button = await tab.select(selector, timeout=1)
                    if disabled_button:
                        self.logger.info("Next button is disabled. Reached last page.")
                        return False
                self.logger.info("Next button not found. Assuming last page.")
                return False

            # Click next button
            await next_button.click()
            await tab.wait(3)  # Wait for page to load

            self.current_page += 1
            self.logger.info(f"Loaded page {self.current_page}")
            return True

        except Exception as e:
            self.logger.error(f"Error loading next page: {e}")
            return False
