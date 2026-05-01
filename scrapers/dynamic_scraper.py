from base.scraper import BaseScraper
from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory
from strategies.output.null_output import NullOutputStrategy
from utils.helpers import DelayManager
import asyncio
import logging
from typing import Dict


class DynamicScraper(BaseScraper):
    """Dynamic scraper for JavaScript-heavy websites using nodriver."""

    def __init__(self, config: Dict, **kwargs):
        super().__init__(config, **kwargs)
        self.query = kwargs.get("query", "default")
        self.headless = kwargs.get("headless", True)

        self.input_strategy = None
        self.queue_strategy = None
        self.delay_manager = DelayManager(config.get("rate_limiting", {}))

        self.worker_config = config.get("workers", {})
        self.max_consecutive_errors = self.worker_config.get(
            "max_consecutive_errors", 5
        )
        self.pages_per_session = self.worker_config.get("max_pages_per_session", 100)
        self.pages_processed = 0
        self.consecutive_errors = 0
        self.session_start_pages = 0

    async def scrape(self, query: str):
        """Main scraping method."""
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting Dynamic Scraper")
            self.logger.info("=" * 60)

            if query:
                self.logger.info(f"Single query mode: {query}")
                await self.start_browser()
                await self.initialize_strategies(query)
                await self._scrape_single_query(query)
            else:
                await self.start_browser()
                await self.initialize_strategies("")
                await self._process_queue()

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

        queries = list(self.input_strategy.load_urls())
        total_queries = len(queries)
        if total_queries == 0:
            self.logger.warning("No queries to process")
            return

        self.logger.info(f"Loaded {total_queries} queries from input source")
        added_count = self.queue_strategy.enqueue(queries)
        self.logger.info(
            f"Enqueued {added_count} queries (skipped {total_queries - added_count} duplicates)"
        )

        processed = 0
        while True:
            if self.queue_strategy and hasattr(
                self.queue_strategy, "maybe_requeue_stalled"
            ):
                requeued = self.queue_strategy.maybe_requeue_stalled()
                if requeued:
                    self.logger.info(f"Requeued {requeued} stalled queries")

            if (
                self.pages_processed - self.session_start_pages
                >= self.pages_per_session
            ):
                self.logger.info(
                    f"Restarting browser after {self.pages_per_session} pages"
                )
                await self._restart_browser()
                self.session_start_pages = self.pages_processed

            query = self.queue_strategy.dequeue(timeout=5)
            if not query:
                stats = self.queue_strategy.get_stats()
                if stats["pending"] == 0 and stats["processing"] == 0:
                    self.logger.info("Queue empty, all queries processed")
                    break
                self.logger.debug("No query available, waiting...")
                await asyncio.sleep(2)
                continue

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

            if self.consecutive_errors >= self.max_consecutive_errors:
                self.logger.error(
                    f"Too many consecutive errors ({self.consecutive_errors}), stopping"
                )
                break

        self.logger.info(f"Processed {processed} queries")

    async def _scrape_single_query(self, query: str) -> bool:
        """Scrape results for a single query."""
        self.logger.info(f"Processing query: {query}")

        try:
            search_url = await self.get_search_url(query)
            self.logger.info(f"Navigating to: {search_url[:80]}...")

            await self.navigate_to_search(search_url)
            await self.delay_manager.apply_delay("between_requests")

            self.query = query
            await self.scrape_all_results()
            return True

        except Exception as e:
            self.logger.error(f"Failed to process query '{query}': {e}")
            return False

    async def _restart_browser(self):
        """Restart browser and reinitialize strategies."""
        self.logger.info("Restarting browser...")
        if self.browser_manager:
            await self.browser_manager.cleanup()
            self.browser_manager = None
        await self.start_browser()
        await self.initialize_strategies(self.query)

    async def initialize_strategies(self, query: str):
        """Initialize all strategies based on configuration."""
        if self.input_strategy and hasattr(self.input_strategy, "cleanup"):
            cleanup = self.input_strategy.cleanup()
            if asyncio.iscoroutine(cleanup):
                await cleanup
        if self.queue_strategy and hasattr(self.queue_strategy, "cleanup"):
            cleanup = self.queue_strategy.cleanup()
            if asyncio.iscoroutine(cleanup):
                await cleanup

        input_config = self.config.get("input", {})
        if input_config:
            input_strategy_name = input_config.get("strategy", "file_url_loader")
            self.input_strategy = ScraperFactory.create_strategy(
                "input", input_strategy_name, input_config
            )
            self.logger.info(f"Initialized input strategy: {input_strategy_name}")
        else:
            self.input_strategy = None

        queue_config = self.config.get("queue", {})
        if queue_config:
            queue_strategy_name = queue_config.get("strategy", "redis_queue")
            self.queue_strategy = ScraperFactory.create_strategy(
                "queue", queue_strategy_name, queue_config
            )
            self.logger.info(f"Initialized queue strategy: {queue_strategy_name}")
        else:
            self.queue_strategy = None

        pagination_strategy_name = self.config.get(
            "pagination_strategy", "infinite_scroll"
        )
        self.pagination_strategy = ScraperFactory.create_strategy(
            "pagination", pagination_strategy_name, self.browser_manager, self.config
        )

        extraction_strategy_name = self.config.get(
            "extraction_strategy", "generic_selector"
        )
        self.extraction_strategy = ScraperFactory.create_strategy(
            "extraction", extraction_strategy_name, self.browser_manager, self.config
        )

        output_config = self.config.copy()
        output_section = output_config.get("output")
        if isinstance(output_section, dict):
            if "strategies" in output_section:
                for strategy_cfg in output_section.get("strategies", []):
                    if strategy_cfg.get("strategy") == "jsonl_file":
                        file_cfg = strategy_cfg.setdefault("config", {})
                        original_path = file_cfg.get("file_path", "")
                        if "{query}" in original_path:
                            sanitized_query = (
                                query.replace(" ", "_")
                                .replace("/", "_")
                                .replace("\\", "_")
                            )
                            file_cfg["file_path"] = original_path.replace(
                                "{query}", sanitized_query
                            )
            else:
                config_section = output_section.get("config", {})
                original_path = config_section.get("file_path", "")
                if "{query}" in original_path:
                    sanitized_query = (
                        query.replace(" ", "_").replace("/", "_").replace("\\", "_")
                    )
                    config_section["file_path"] = original_path.replace(
                        "{query}", sanitized_query
                    )

        output_strategy_name = output_config.get("output_strategy", "jsonl_file")
        if output_section:
            self.output_strategy = ScraperFactory.create_strategy(
                "output", output_strategy_name, output_config
            )
            self.logger.info(f"Initialized output strategy: {output_strategy_name}")
        else:
            self.output_strategy = NullOutputStrategy({})
            self.logger.warning("No output strategy configured; using no-op output")

    async def start_browser(self):
        """Start browser instance."""
        browser_config = self.config.get("browser", {})
        engine = browser_config.get(
            "automation", self.config.get("browser_automation", "nodriver")
        )
        headless = browser_config.get("headless", self.headless)
        page_wait_seconds = browser_config.get("page_wait_seconds", 1.0)

        self.browser_manager = BrowserManager(
            engine=engine,
            headless=headless,
            page_wait_seconds=page_wait_seconds,
        )
        await self.browser_manager.start()
        self.logger.info(f"Browser started (headless={headless})")

    async def get_search_url(self, query: str) -> str:
        """Generate search URL based on configuration."""
        url_template = self.config.get("search_url_template")
        if url_template:
            return url_template.format(query=query)
        return f"https://www.google.com/maps/search/{query}/"

    async def navigate_to_search(self, url: str):
        """Navigate to search URL."""
        if self.browser_manager:
            await self.browser_manager.navigate(url)

    async def scrape_all_results(self):
        """Scrape all results using configured strategies."""
        self.seen_items = set()

        while True:
            if self.output_strategy and self.output_strategy.has_reached_limit():
                max_results = getattr(self.output_strategy, "max_results", "unknown")
                self.logger.info(f"Reached maximum results limit: {max_results}")
                break

            if not await self.pagination_strategy.has_more_results():
                self.logger.info("No more results to load")
                break

            items = await self.extraction_strategy.extract_items()
            new_items = 0
            for item in items:
                self._add_metadata(item)
                item_id = self.get_item_id(item)
                if item_id and item_id not in self.seen_items:
                    if self.output_strategy:
                        await self.output_strategy.write_item(item)
                    self.seen_items.add(item_id)
                    new_items += 1

                    if self.output_strategy and self.output_strategy.has_reached_limit():
                        break

            self.logger.info(f"Extracted {len(items)} items, {new_items} new items")

            if not await self.pagination_strategy.load_more_results():
                self.logger.info("Finished loading all results")
                break

            await asyncio.sleep(self.config.get("rate_limit", 2))

    def get_item_id(self, item: Dict) -> str:
        """Get unique identifier for an item to avoid duplicates."""
        for field in ["url", "href", "id", "link", "name", "source_url"]:
            if field in item and item[field]:
                return str(item[field])
        return str(hash(str(item)))

    def _add_metadata(self, item: Dict):
        """Add metadata to the item."""
        if self.query:
            item["query"] = self.query
        item["source"] = "google_maps_search"

    async def cleanup(self):
        """Clean up resources."""
        if self.browser_manager:
            await self.browser_manager.cleanup()
            self.browser_manager = None
        if self.output_strategy and hasattr(self.output_strategy, "cleanup"):
            await self.output_strategy.cleanup()
            self.output_strategy = None
        if self.input_strategy and hasattr(self.input_strategy, "cleanup"):
            cleanup = self.input_strategy.cleanup()
            if asyncio.iscoroutine(cleanup):
                await cleanup
            self.input_strategy = None
        if self.queue_strategy and hasattr(self.queue_strategy, "cleanup"):
            cleanup = self.queue_strategy.cleanup()
            if asyncio.iscoroutine(cleanup):
                await cleanup
            self.queue_strategy = None
        self.logger.info("Cleanup complete")
