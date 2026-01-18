#!/usr/bin/env python3
"""
Flexible Web Scraping Framework
Usage: python main.py --config config/google_maps.yaml --query "restaurants NYC"
"""

import argparse
import asyncio
import logging
import nodriver as uc
from factory.scraper_factory import ScraperFactory


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Flexible Web Scraping Framework")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--query", required=True, help="Search query")
    parser.add_argument(
        "--headless", action="store_true", default=True, help="Run in headless mode"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    try:
        # Create scraper from configuration
        scraper = ScraperFactory.create_scraper(
            args.config, headless=args.headless, query=args.query
        )

        # Run scraping
        await scraper.scrape(args.query)

    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        raise


if __name__ == "__main__":
    # Run the async function
    uc.loop().run_until_complete(main())
