import yaml
import logging
from typing import Dict, Any
from base.scraper import BaseScraper
from base.browser_manager import BrowserManager


class ScraperFactory:
    """Factory class to create scrapers based on configuration"""

    # Strategy mapping - will be populated when needed
    _STRATEGY_MAP = None

    @classmethod
    def get_strategy_map(cls):
        """Lazy load strategy map to avoid circular imports"""
        if cls._STRATEGY_MAP is None:
            # Import strategies here to avoid circular imports
            from strategies.pagination.infinite_scroll import (
                InfiniteScrollPaginationStrategy,
            )
            from strategies.pagination.next_button import NextButtonPaginationStrategy
            from strategies.extraction.generic_selector import (
                GenericSelectorExtractionStrategy,
            )
            from strategies.output.jsonl_file import JsonlFileOutputStrategy

            cls._STRATEGY_MAP = {
                # Pagination strategies
                "infinite_scroll": InfiniteScrollPaginationStrategy,
                "next_button": NextButtonPaginationStrategy,
                # Extraction strategies
                "generic_selector": GenericSelectorExtractionStrategy,
                # Output strategies
                "jsonl_file": JsonlFileOutputStrategy,
            }
        return cls._STRATEGY_MAP

    @classmethod
    def load_config(cls, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logging.error(f"Error loading config file {config_path}: {e}")
            raise

    @classmethod
    def create_scraper(cls, config_path: str, **kwargs) -> BaseScraper:
        """Create and return a scraper instance based on configuration"""
        config = cls.load_config(config_path)
        content_type = config.get("content_type", "dynamic")

        if content_type == "dynamic":
            from scrapers.dynamic_scraper import DynamicScraper

            return DynamicScraper(config, **kwargs)
        else:
            raise ValueError(f"Unsupported content type: {content_type}")

    @classmethod
    def create_strategy(cls, strategy_type: str, strategy_name: str, *args, **kwargs):
        """Create and return a strategy instance"""
        strategy_map = cls.get_strategy_map()
        if strategy_name not in strategy_map:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        strategy_class = strategy_map[strategy_name]
        return strategy_class(*args, **kwargs)
