#!/usr/bin/env python3
"""
Flexible Web Scraping Framework
Usage:
  # Batch mode (process all queries from input file)
  python main.py --config config/google_maps.yaml

  # Single query mode (CLI override)
  python main.py --config config/google_maps.yaml --query "restaurants NYC"

  # Listing crawler (URLs from file)
  python main.py --config config/gmaps_listings_working.yaml
"""

import argparse
import asyncio
import logging
import nodriver as uc
from dotenv import load_dotenv
from factory.scraper_factory import ScraperFactory


class InstanceLabelFilter(logging.Filter):
    def __init__(self, instance_label: str):
        super().__init__()
        self.instance_label = instance_label or "default"

    def filter(self, record):
        record.instance_label = self.instance_label
        return True


def setup_logging():
    """Setup logging configuration with beautiful formatting"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(instance_label)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def configure_instance_logging(instance_label: str):
    root_logger = logging.getLogger()
    filter_ = InstanceLabelFilter(instance_label)
    for handler in root_logger.handlers:
        handler.addFilter(filter_)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(instance_label)s - %(name)s - %(levelname)s - %(message)s"
            )
        )


async def main():
    """Main entry point"""
    load_dotenv()

    parser = argparse.ArgumentParser(description="Flexible Web Scraping Framework")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument(
        "--query",
        required=False,
        default=None,
        help="Search query (optional - uses input file if not provided)",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run in headless mode",
    )
    parser.add_argument(
        "--instance-label",
        default="main",
        help="Label used in logs when running multiple crawler processes",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()
    configure_instance_logging(args.instance_label)
    logger = logging.getLogger(__name__)

    try:
        # Load config
        config = ScraperFactory.load_config(args.config)
        content_type = config.get("content_type", "dynamic")

        # For dynamic scrapers:
        # - If --query is provided: single query mode (CLI override)
        # - If --query is not provided: batch mode (uses input file + queue)
        if content_type == "dynamic":
            if args.query:
                logger.info(f"Single query mode: {args.query}")
            else:
                logger.info("Batch mode: will process queries from input file")
        elif content_type == "listing_crawler":
            workers = config.get("workers", {})
            worker_count = workers.get("count", 1)
            logger.info(
                "Listing crawler runs one browser worker per process; workers.count=%s does not add in-process concurrency",
                worker_count,
            )

        # Create scraper from configuration
        scraper = ScraperFactory.create_scraper(
            args.config,
            headless=args.headless,
            query=args.query,
            instance_label=args.instance_label,
        )

        # Run scraping (scraper handles single vs batch mode)
        await scraper.scrape(args.query)

    except Exception as e:
        logger.error(f"Scraping failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Run the async function
    uc.loop().run_until_complete(main())
