from base.scraper import BaseScraper
from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory
import asyncio
import logging
import time
from typing import Dict, List


class DynamicScraper(BaseScraper):
    """Dynamic scraper for JavaScript-heavy websites using nodriver"""

    def __init__(self, config: Dict, **kwargs):
        super().__init__(config, **kwargs)
        self.query = kwargs.get("query", "default")

    async def scrape(self, query: str):
        """Main scraping method"""
        try:
            # Get search URL from config
            search_url = await self.get_search_url(query)

            # Initialize browser
            await self.start_browser()

            # Navigate to search URL
            await self.navigate_to_search(search_url)

            # Initialize strategies (with query in config for file path)
            await self.initialize_strategies(query)

            # Scrape all results
            await self.scrape_all_results()

        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            raise
        finally:
            await self.cleanup()

    async def get_search_url(self, query: str) -> str:
        """Generate search URL based on configuration"""
        # Get URL template from config
        url_template = self.config.get("search_url_template")
        if url_template:
            return url_template.format(query=query)
        else:
            # Default behavior - use existing URL pattern
            return f"https://www.google.com/maps/search/{query}/"

    async def start_browser(self):
        """Start browser instance"""
        engine = self.config.get("browser_automation", "nodriver")
        self.browser_manager = BrowserManager(engine=engine, headless=True)
        await self.browser_manager.start()

    async def navigate_to_search(self, url: str):
        """Navigate to search URL"""
        await self.browser_manager.navigate(url)

    async def initialize_strategies(self, query: str):
        """Initialize all strategies based on configuration"""
        # Pagination strategy
        pagination_strategy_name = self.config.get(
            "pagination_strategy", "infinite_scroll"
        )
        self.pagination_strategy = ScraperFactory.create_strategy(
            "pagination", pagination_strategy_name, self.browser_manager, self.config
        )

        # Extraction strategy
        extraction_strategy_name = self.config.get(
            "extraction_strategy", "generic_selector"
        )
        self.extraction_strategy = ScraperFactory.create_strategy(
            "extraction", extraction_strategy_name, self.browser_manager, self.config
        )

        # Output strategy - update file path with actual query
        output_config = self.config.copy()
        if "output" in output_config and "file_path" in output_config["output"]:
            original_path = output_config["output"]["file_path"]
            # Replace {query} placeholder with sanitized query
            sanitized_query = (
                query.replace(" ", "_").replace("/", "_").replace("\\", "_")
            )
            output_config["output"]["file_path"] = original_path.replace(
                "{query}", sanitized_query
            )

        output_strategy_name = output_config.get("output_strategy", "jsonl_file")
        self.output_strategy = ScraperFactory.create_strategy(
            "output", output_strategy_name, output_config
        )

    async def scrape_all_results(self):
        """Scrape all results using configured strategies"""
        while True:
            # Check if we've reached max results
            if self.output_strategy.has_reached_limit():
                self.logger.info(
                    f"Reached maximum results limit: {self.output_strategy.max_results}"
                )
                break

            # Check if there are more results to load
            if not await self.pagination_strategy.has_more_results():
                self.logger.info("No more results to load")
                break

            # Extract items
            items = await self.extraction_strategy.extract_items()

            # Write new items (avoid duplicates)
            new_items = 0
            for item in items:
                # Create unique identifier for duplicate detection
                item_id = self.get_item_id(item)
                if item_id and item_id not in self.seen_items:
                    await self.output_strategy.write_item(item)
                    self.seen_items.add(item_id)
                    new_items += 1

                    # Check limit again after each item
                    if self.output_strategy.has_reached_limit():
                        break

            self.logger.info(f"Extracted {len(items)} items, {new_items} new items")

            # Load more results
            if not await self.pagination_strategy.load_more_results():
                self.logger.info("Finished loading all results")
                break

            # Add delay to be respectful
            await asyncio.sleep(self.config.get("rate_limit", 2))

    def get_item_id(self, item: Dict) -> str:
        """Get unique identifier for an item to avoid duplicates"""
        # Try to find a unique field, default to URL or name
        for field in ["url", "href", "id", "link", "name"]:
            if field in item and item[field]:
                return str(item[field])
        return str(hash(str(item)))

    async def cleanup(self):
        """Clean up browser resources"""
        await self.browser_manager.cleanup()
