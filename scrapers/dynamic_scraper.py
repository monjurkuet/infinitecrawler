from base.scraper import BaseScraper
from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory
from utils.helpers import DelayManager
import asyncio
import logging
import time
from typing import Dict, List, Optional


class DynamicScraper(BaseScraper):
    """Dynamic scraper for JavaScript-heavy websites using nodriver.

    Supports both single query (--query CLI) and batch processing (input file + Redis queue).
    """

    def __init__(self, config: Dict, **kwargs):
        super().__init__(config, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.query = kwargs.get("query", "default")
        self.headless = kwargs.get("headless", True)

        # Initialize components
        self.input_strategy = None
        self.queue_strategy = None
        self.pagination_strategy = None
        self.extraction_strategy = None
        self.output_strategy = None
        self.delay_manager = DelayManager(config.get("rate_limiting", {}))

        # Worker settings
        self.worker_config = config.get("workers", {})
        self.max_consecutive_errors = self.worker_config.get(
            "max_consecutive_errors", 5
        )
        self.pages_per_session = self.worker_config.get("max_pages_per_session", 100)

        # State tracking
        self.pages_processed = 0
        self.consecutive_errors = 0
        self.session_start_pages = 0

    async def scrape(self, query: str = None):
        """Main scraping method.

        Args:
            query: If provided, process single query (CLI --query override).
                   Otherwise, process all queries from input file via Redis queue.
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting Dynamic Scraper")
            self.logger.info("=" * 60)

            # CLI --query override: single query mode
            if query:
                self.logger.info(f"Single query mode: {query}")
                await self.start_browser()
                await self.initialize_strategies()
                await self._scrape_single_query(query)
            else:
                # Queue mode: load queries from file, process all
                await self.start_browser()
                await self.initialize_strategies()
                await self._process_queue()

            # Print final stats
            if self.queue_strategy:
                stats = self.queue_strategy.get_stats()
                self.logger.info("=" * 60)
                self.logger.info("Scraping Complete!")
                self.logger.info(
                    f"Completed: {stats['completed']} | Failed: {stats['failed']}"
                )
                self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"Fatal error during scraping: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()

    async def _process_queue(self):
        """Process all queries from input file via Redis queue."""
        self.logger.info("Starting queue processing...")

        # Load queries from input source
        queries = list(self.input_strategy.load_urls())
        total_queries = len(queries)
        if total_queries == 0:
            self.logger.warning("No queries to process")
            return

        self.logger.info(f"Loaded {total_queries} queries from input source")

        # Enqueue queries
        added_count = self.queue_strategy.enqueue(queries)
        self.logger.info(
            f"Enqueued {added_count} queries (skipped {total_queries - added_count} duplicates)"
        )

        # Process queue
        processed = 0
        while True:
            # Check if we need to restart browser
            if (
                self.pages_processed - self.session_start_pages
                >= self.pages_per_session
            ):
                self.logger.info(
                    f"Restarting browser after {self.pages_per_session} pages"
                )
                await self._restart_browser()
                self.session_start_pages = self.pages_processed

            # Get next query from queue
            query = self.queue_strategy.dequeue(timeout=5)

            if not query:
                # Check if queue is truly empty
                stats = self.queue_strategy.get_stats()
                if stats["pending"] == 0 and stats["processing"] == 0:
                    self.logger.info("Queue empty, all queries processed")
                    break
                else:
                    self.logger.debug("No query available, waiting...")
                    await asyncio.sleep(2)
                    continue

            # Process the query
            success = await self._scrape_single_query(query)

            if success:
                self.queue_strategy.mark_completed(query)
                self.consecutive_errors = 0
                self.pages_processed += 1
                processed += 1
            else:
                self.consecutive_errors += 1
                self.queue_strategy.mark_failed(
                    query, "Extraction failed", self.consecutive_errors
                )

            # Check for too many consecutive errors
            if self.consecutive_errors >= self.max_consecutive_errors:
                self.logger.error(
                    f"Too many consecutive errors ({self.consecutive_errors}), stopping"
                )
                break

        self.logger.info(f"Processed {processed} queries")

    async def _scrape_single_query(self, query: str) -> bool:
        """Scrape results for a single query.

        Args:
            query: Search query string

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Processing query: {query}")

        try:
            # Update output file path with sanitized query name
            await self._update_output_for_query(query)

            # Generate search URL
            search_url = await self.get_search_url(query)
            self.logger.info(f"Navigating to: {search_url[:80]}...")

            # Navigate to search URL
            await self.navigate_to_search(search_url)

            # Add delay before extraction
            await self.delay_manager.apply_delay("between_requests")

            # Scrape all results
            self.query = query  # Set current query for metadata
            await self.scrape_all_results()

            return True

        except Exception as e:
            self.logger.error(f"Failed to process query '{query}': {e}")
            return False

    async def _update_output_for_query(self, query: str):
        """Update output strategy file path for the current query."""
        import copy

        # Get the original output config
        original_output_config = self.config.get("output")
        if not original_output_config:
            return

        # Create a deep copy to avoid modifying the original config
        output_config = copy.deepcopy(original_output_config)

        # Check if we need to update file paths
        if "strategies" in output_config:
            # Composite output - update jsonl_file strategy if present
            for strategy_config in output_config.get("strategies", []):
                if strategy_config.get("strategy") == "jsonl_file":
                    config = strategy_config.get("config", {})
                    original_path = config.get("file_path", "")
                    if "{query}" in original_path:
                        sanitized = (
                            query.replace(" ", "_").replace("/", "_").replace("\\", "_")
                        )
                        config["file_path"] = original_path.replace(
                            "{query}", sanitized
                        )
                        self.logger.info(f"Output file: {config['file_path']}")
                        break
        elif output_config.get("strategy") == "jsonl_file":
            # Regular jsonl output
            config = output_config.get("config", {})
            original_path = config.get("file_path", "")
            if "{query}" in original_path:
                sanitized = query.replace(" ", "_").replace("/", "_").replace("\\", "_")
                config["file_path"] = original_path.replace("{query}", sanitized)
                self.logger.info(f"Output file: {config['file_path']}")

        # Reinitialize output strategy with updated config
        self.output_strategy = None  # Clear old reference
        if "strategies" in output_config:
            self.output_strategy = ScraperFactory.create_strategy(
                "output", "composite", output_config
            )
        elif output_config.get("strategy"):
            self.output_strategy = ScraperFactory.create_strategy(
                "output", output_config.get("strategy"), output_config
            )

    async def _restart_browser(self):
        """Restart browser and reinitialize strategies."""
        self.logger.info("Restarting browser...")
        await self.browser_manager.cleanup()
        await self.start_browser()
        await self.initialize_strategies()

    async def initialize_strategies(self):
        """Initialize all strategies based on configuration"""
        # Input strategy (optional - for queue mode)
        input_config = self.config.get("input", {})
        if input_config:
            input_strategy_name = input_config.get("strategy", "file_url_loader")
            self.input_strategy = ScraperFactory.create_strategy(
                "input", input_strategy_name, input_config
            )
            self.logger.info(f"Initialized input strategy: {input_strategy_name}")

        # Queue strategy (optional - for queue mode)
        queue_config = self.config.get("queue", {})
        if queue_config:
            queue_strategy_name = queue_config.get("strategy", "redis_queue")
            self.queue_strategy = ScraperFactory.create_strategy(
                "queue", queue_strategy_name, queue_config
            )
            self.logger.info(f"Initialized queue strategy: {queue_strategy_name}")

        # Pagination strategy
        pagination_strategy_name = self.config.get(
            "pagination_strategy", "infinite_scroll"
        )
        self.pagination_strategy = ScraperFactory.create_strategy(
            "pagination", pagination_strategy_name, self.browser_manager, self.config
        )
        self.logger.info(f"Initialized pagination strategy: {pagination_strategy_name}")

        # Extraction strategy
        extraction_strategy_name = self.config.get(
            "extraction_strategy", "generic_selector"
        )
        self.extraction_strategy = ScraperFactory.create_strategy(
            "extraction", extraction_strategy_name, self.browser_manager, self.config
        )
        self.logger.info(f"Initialized extraction strategy: {extraction_strategy_name}")

        # Output strategy - handle both composite and regular
        output_config = self.config.get("output")
        if output_config and "strategies" in output_config:
            # Composite output
            self.output_strategy = ScraperFactory.create_strategy(
                "output", "composite", output_config
            )
            self.logger.info("Initialized output strategy: composite")
        elif output_config:
            # Regular output
            output_strategy_name = output_config.get("strategy", "jsonl_file")
            self.output_strategy = ScraperFactory.create_strategy(
                "output", output_strategy_name, output_config
            )
            self.logger.info(f"Initialized output strategy: {output_strategy_name}")
        else:
            self.logger.warning("No output strategy configured")

    async def start_browser(self):
        """Start browser instance"""
        engine = self.config.get("browser_automation", "nodriver")
        headless = self.config.get("browser", {}).get("headless", self.headless)

        self.browser_manager = BrowserManager(engine=engine, headless=headless)
        await self.browser_manager.start()
        self.logger.info(f"Browser started (headless={headless})")

    async def get_search_url(self, query: str) -> str:
        """Generate search URL based on configuration"""
        url_template = self.config.get("search_url_template")
        if url_template:
            return url_template.format(query=query)
        else:
            return f"https://www.google.com/maps/search/{query}/"

    async def navigate_to_search(self, url: str):
        """Navigate to search URL"""
        if self.browser_manager:
            await self.browser_manager.navigate(url)

    async def scrape_all_results(self):
        """Scrape all results using configured strategies"""
        # Track seen items for duplicate detection
        self.seen_items = set()

        while True:
            # Check if we've reached max results
            if self.output_strategy and self.output_strategy.has_reached_limit():
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
                # Add metadata to item
                self._add_metadata(item)

                # Create unique identifier for duplicate detection
                item_id = self.get_item_id(item)
                if item_id and item_id not in self.seen_items:
                    if self.output_strategy:
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
        for field in ["url", "href", "id", "link", "name", "source_url"]:
            if field in item and item[field]:
                return str(item[field])
        return str(hash(str(item)))

    def _add_metadata(self, item: Dict):
        """Add metadata to the item"""
        # Add search query
        if self.query:
            item["query"] = self.query

        # Add source identifier
        item["source"] = "google_maps_search"

    async def cleanup(self):
        """Clean up resources"""
        if self.browser_manager:
            await self.browser_manager.cleanup()
        if self.output_strategy and hasattr(self.output_strategy, "cleanup"):
            await self.output_strategy.cleanup()
        self.logger.info("Cleanup complete")
