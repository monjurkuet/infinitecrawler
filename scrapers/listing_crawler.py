"""Listing crawler for deep extraction from individual listing/detail pages"""

import asyncio
import logging
from typing import Dict, Optional

from base.scraper import BaseScraper
from base.browser_manager import BrowserManager
from factory.scraper_factory import ScraperFactory
from utils.helpers import DelayManager
from strategies.output.null_output import NullOutputStrategy


class ListingCrawler(BaseScraper):
    """
    Crawl specific listing/detail page URLs with deep extraction.
    Uses queue-based processing for resilience and scalability.
    """

    def __init__(self, config: Dict, **kwargs):
        super().__init__(config, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.instance_label = kwargs.get("instance_label", "main")

        # Initialize components
        self.input_strategy = None
        self.queue_strategy = None
        self.navigation_strategy = None
        self.secondary_output_strategy = None
        self.delay_manager = DelayManager(config.get("rate_limiting", {}))

        # Worker settings
        self.worker_config = config.get("workers", {})
        self.max_consecutive_errors = self.worker_config.get(
            "max_consecutive_errors", 5
        )
        self.pages_per_session = self.worker_config.get("max_pages_per_session", 100)

        # Retry configuration
        retry_config = config.get("retry", {})
        self.url_max_retries = retry_config.get("url_attempts", 3)
        self.url_retry_delay = retry_config.get("delay", 5)
        self.restart_on_retry_failure = retry_config.get("restart_browser", True)

        # State tracking
        self.pages_processed = 0
        self.consecutive_errors = 0
        self.session_start_pages = 0
        self.retry_counts = {}  # Track retries per URL
        self.browser_page_wait_seconds = config.get("browser", {}).get(
            "page_wait_seconds", 1.0
        )

    async def scrape(self, query: str = None):
        """
        Main crawling method.
        Note: query parameter is ignored, URLs come from input strategy.
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting Listing Crawler")
            self.logger.info(
                "Instance label: %s | one browser process per crawler instance",
                self.instance_label,
            )
            self.logger.info("=" * 60)

            # Initialize all components
            await self.start_browser()
            await self.initialize_strategies()

            # Load URLs from input source
            urls = list(self.input_strategy.load_urls())
            total_urls = len(urls)
            self.logger.info(f"Loaded {total_urls} URLs from input source")

            if total_urls == 0:
                self.logger.warning("No URLs to process")
                return

            # Enqueue URLs
            added_count = self.queue_strategy.enqueue(urls)
            self.logger.info(
                f"Added {added_count} URLs to queue (skipped {total_urls - added_count} duplicates)"
            )

            # Process queue
            await self._process_queue()

            # Print final stats
            stats = self.queue_strategy.get_stats()
            self.logger.info("=" * 60)
            self.logger.info("Crawling Complete!")
            self.logger.info(
                f"Completed: {stats['completed']} | Failed: {stats['failed']}"
            )
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"Fatal error during crawling: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()

    async def initialize_strategies(self):
        """Initialize all strategies based on configuration"""
        # Input strategy
        input_config = self.config.get("input", {})
        input_strategy_name = input_config.get("strategy", "file_url_loader")
        self.input_strategy = ScraperFactory.create_strategy(
            "input", input_strategy_name, input_config
        )
        self.logger.info(f"Initialized input strategy: {input_strategy_name}")

        # Queue strategy
        queue_config = self.config.get("queue", {})
        queue_strategy_name = queue_config.get("strategy", "redis_queue")
        self.queue_strategy = ScraperFactory.create_strategy(
            "queue", queue_strategy_name, queue_config
        )
        self.logger.info(f"Initialized queue strategy: {queue_strategy_name}")

        # Extraction strategy
        extraction_config = self.config.get("extraction", {})
        extraction_strategy_name = extraction_config.get("strategy", "multi_step")
        self.extraction_strategy = ScraperFactory.create_strategy(
            "extraction", extraction_strategy_name, self.browser_manager, self.config
        )
        self.logger.info(f"Initialized extraction strategy: {extraction_strategy_name}")

        # Primary output strategy
        output_config = self.config.get("output", {})
        if output_config:
            output_strategy_name = output_config.get("strategy", "jsonl_file")
            self.output_strategy = ScraperFactory.create_strategy(
                "output", output_strategy_name, output_config
            )
            self.logger.info(f"Initialized output strategy: {output_strategy_name}")
        else:
            self.output_strategy = NullOutputStrategy({})
            self.logger.info("Initialized output strategy: null_output")

        # Secondary output strategy (optional)
        secondary_output_config = self.config.get("secondary_output")
        if secondary_output_config:
            secondary_strategy_name = secondary_output_config.get(
                "strategy", "secondary_jsonl"
            )
            self.secondary_output_strategy = ScraperFactory.create_strategy(
                "output", secondary_strategy_name, secondary_output_config
            )
            self.logger.info(
                f"Initialized secondary output strategy: {secondary_strategy_name}"
            )

    def _cleanup_browser_bound_strategies(self):
        """Clear strategies that hold browser references before reinitializing."""
        self.extraction_strategy = None
        self.navigation_strategy = None

    async def _refresh_browser_bound_strategies(self):
        """Rebuild strategies that depend on the browser manager."""
        self._cleanup_browser_bound_strategies()
        if self.browser_manager:
            extraction_config = self.config.get("extraction", {})
            extraction_strategy_name = extraction_config.get("strategy", "multi_step")
            self.extraction_strategy = ScraperFactory.create_strategy(
                "extraction",
                extraction_strategy_name,
                self.browser_manager,
                self.config,
            )
            self.logger.info(
                f"Reinitialized extraction strategy: {extraction_strategy_name}"
            )

    async def start_browser(self):
        """Start browser instance"""
        browser_config = self.config.get("browser", {})
        engine = browser_config.get(
            "automation", self.config.get("browser_automation", "nodriver")
        )
        headless = browser_config.get("headless", self.config.get("headless", True))

        self.browser_manager = BrowserManager(
            engine=engine,
            headless=headless,
            page_wait_seconds=self.browser_page_wait_seconds,
        )
        await self.browser_manager.start()
        self.logger.info(f"Browser started (headless={headless})")

    async def navigate_to_search(self, url: str):
        """Navigate to URL - required by base class"""
        if self.browser_manager:
            await self.browser_manager.navigate(url)

    async def _process_queue(self):
        """Process URLs from queue with retry logic and rate limiting"""
        self.logger.info("Starting queue processing...")

        while True:
            if self.queue_strategy and hasattr(
                self.queue_strategy, "maybe_requeue_stalled"
            ):
                requeued = self.queue_strategy.maybe_requeue_stalled()
                if requeued:
                    self.logger.info(f"Requeued {requeued} stalled URLs")

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

            # Get next URL from queue
            url = self.queue_strategy.dequeue(timeout=5)

            if not url:
                # Check if queue is truly empty
                stats = self.queue_strategy.get_stats()
                if stats["pending"] == 0 and stats["processing"] == 0:
                    self.logger.info("Queue empty, all items processed")
                    break
                else:
                    self.logger.debug("No URL available, waiting...")
                    await asyncio.sleep(2)
                    continue

            # Process the URL
            success = await self._process_url(url)

            if success:
                self.queue_strategy.mark_completed(url)
                self.consecutive_errors = 0
                self.pages_processed += 1
            else:
                self.consecutive_errors += 1

                # Check for too many consecutive errors
                if self.consecutive_errors >= self.max_consecutive_errors:
                    self.logger.error(
                        f"Too many consecutive errors ({self.consecutive_errors}), stopping"
                    )
                    break

            # Apply rate limiting between requests
            request_delay_started = asyncio.get_running_loop().time()
            await self.delay_manager.apply_delay("between_requests")
            request_delay_elapsed = (
                asyncio.get_running_loop().time() - request_delay_started
            )
            self.logger.info(
                f"Applied between_requests delay: {request_delay_elapsed:.2f}s"
            )

    async def _process_url(self, url: str) -> bool:
        """
        Process a single URL with retry logic.
        Returns True if successful, False otherwise.
        """
        self.logger.info(f"Processing: {url[:80]}...")

        # Initialize retry count for this URL
        if url not in self.retry_counts:
            self.retry_counts[url] = 0

        for attempt in range(self.url_max_retries):
            try:
                loop = asyncio.get_running_loop()

                # Navigate to URL
                navigation_started = loop.time()
                await self.browser_manager.navigate(url)
                navigation_elapsed = loop.time() - navigation_started

                page_delay_started = loop.time()
                await self.delay_manager.apply_delay("page_load")
                page_delay_elapsed = loop.time() - page_delay_started

                # Extract data
                extraction_started = loop.time()
                items = await self.extraction_strategy.extract_items()
                extraction_elapsed = loop.time() - extraction_started

                if not items:
                    self.logger.warning(f"No data extracted from {url[:80]}...")
                    # If this is the last attempt, count as success (data might not exist)
                    if attempt == self.url_max_retries - 1:
                        return True
                    await asyncio.sleep(self.url_retry_delay)
                    continue

                # Write to outputs
                write_started = loop.time()
                for item in items:
                    # Add metadata
                    item["_crawl_meta"] = {
                        "source_url": url,
                        "pages_processed": self.pages_processed,
                        "retry_count": attempt,
                    }

                    # Primary output
                    await self.output_strategy.write_item(item)

                    # Secondary output (if configured)
                    if self.secondary_output_strategy:
                        await self.secondary_output_strategy.write_item(item)
                write_elapsed = loop.time() - write_started

                self.logger.info(
                    f"✓ Extracted {len(items)} items from {url[:60]}... (attempt {attempt + 1})"
                )
                self.logger.info(
                    f"Timing for {url[:60]}... navigate={navigation_elapsed:.2f}s page_delay={page_delay_elapsed:.2f}s extraction={extraction_elapsed:.2f}s write={write_elapsed:.2f}s total={(navigation_elapsed + page_delay_elapsed + extraction_elapsed + write_elapsed):.2f}s"
                )
                return True

            except Exception as e:
                self.logger.warning(
                    f"✗ Attempt {attempt + 1}/{self.url_max_retries} failed for {url[:60]}...: {e}"
                )

                # Check if we should restart browser
                if self.restart_on_retry_failure and attempt < self.url_max_retries - 1:
                    self.logger.info(
                        f"Restarting browser before retry {attempt + 2}..."
                    )
                    await self._restart_browser()
                    await asyncio.sleep(2)
                else:
                    # Wait before next attempt
                    if attempt < self.url_max_retries - 1:
                        await asyncio.sleep(self.url_retry_delay)

        # All retries failed
        self.logger.error(
            f"✗ All {self.url_max_retries} attempts failed for {url[:60]}..."
        )
        return False

    async def _restart_browser(self):
        """Restart browser to free memory and prevent leaks"""
        self.logger.info("Restarting browser...")
        if self.browser_manager:
            await self.browser_manager.cleanup()
            self.browser_manager = None
        await asyncio.sleep(2)
        await self.start_browser()
        await self._refresh_browser_bound_strategies()

    async def cleanup(self):
        """Clean up resources"""
        if self.browser_manager:
            await self.browser_manager.cleanup()
            self.browser_manager = None
            self.logger.info("Browser cleaned up")
        if self.output_strategy and hasattr(self.output_strategy, "cleanup"):
            await self.output_strategy.cleanup()
            self.output_strategy = None
        if self.secondary_output_strategy and hasattr(
            self.secondary_output_strategy, "cleanup"
        ):
            await self.secondary_output_strategy.cleanup()
            self.secondary_output_strategy = None
        if self.input_strategy and hasattr(self.input_strategy, "cleanup"):
            cleanup = self.input_strategy.cleanup()
            if asyncio.iscoroutine(cleanup):
                await cleanup
        if self.queue_strategy and hasattr(self.queue_strategy, "cleanup"):
            cleanup = self.queue_strategy.cleanup()
            if asyncio.iscoroutine(cleanup):
                await cleanup
        self.logger.info("Cleanup complete")
