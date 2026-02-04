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
from factory.scraper_factory import ScraperFactory


def setup_logging():
    """Setup logging configuration with beautiful formatting"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Flexible Web Scraping Framework")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument(
        "--query",
        required=False,
        default=None,
        help="Search query (optional - uses input file if not provided)",
    )
    parser.add_argument(
        "--headless", action="store_true", default=True, help="Run in headless mode"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()
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

        # Create scraper from configuration
        scraper = ScraperFactory.create_scraper(
            args.config, headless=args.headless, query=args.query
        )

        # Run scraping (scraper handles single vs batch mode)
        await scraper.scrape(args.query)

    except Exception as e:
        logger.error(f"Scraping failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Run the async function
    uc.loop().run_until_complete(main())
