import yaml
import logging
from typing import Dict, Any
from base.scraper import BaseScraper
from utils.config import normalize_config, validate_config


class ScraperFactory:
    """Factory class to create scrapers based on configuration"""

    # Strategy mapping - will be populated when needed
    _STRATEGY_MAP = None
    _STRATEGY_CATALOG = None

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
            from strategies.extraction.google_maps import (
                GoogleMapsExtractionStrategy,
            )
            from strategies.extraction.multi_step import MultiStepExtractionStrategy
            from strategies.output.jsonl_file import (
                JsonlFileOutputStrategy,
                SecondaryJsonlOutputStrategy,
            )
            from strategies.output.null_output import NullOutputStrategy
            from strategies.output.postgresql import (
                PostgreSQLListingDetailsUpsertStrategy,
                PostgreSQLOutputStrategy,
                PostgreSQLUpsertStrategy,
            )
            from strategies.output.composite import CompositeOutputStrategy
            from strategies.input.file_url_loader import FileInputStrategy
            from strategies.input.postgresql_uncrawled import (
                PostgreSQLUncrawledInputStrategy,
            )
            from strategies.queue.redis_queue import RedisQueueStrategy
            from strategies.navigation.tab_navigator import (
                TabNavigationStrategy,
                AccordionNavigationStrategy,
                ModalNavigationStrategy,
            )

            cls._STRATEGY_MAP = {
                # Pagination strategies
                "infinite_scroll": InfiniteScrollPaginationStrategy,
                "next_button": NextButtonPaginationStrategy,
                # Extraction strategies
                "generic_selector": GenericSelectorExtractionStrategy,
                "google_maps": GoogleMapsExtractionStrategy,
                "multi_step": MultiStepExtractionStrategy,
                # Output strategies
                "jsonl_file": JsonlFileOutputStrategy,
                "secondary_jsonl": SecondaryJsonlOutputStrategy,
                "null_output": NullOutputStrategy,
                "postgresql": PostgreSQLOutputStrategy,
                "postgresql_upsert": PostgreSQLUpsertStrategy,
                "postgresql_listing_upsert": PostgreSQLListingDetailsUpsertStrategy,
                "composite": CompositeOutputStrategy,
                # Input strategies
                "file_url_loader": FileInputStrategy,
                "postgresql_uncrawled_gmaps": PostgreSQLUncrawledInputStrategy,
                # Queue strategies
                "redis_queue": RedisQueueStrategy,
                # Navigation strategies
                "tab_navigator": TabNavigationStrategy,
                "accordion_navigator": AccordionNavigationStrategy,
                "modal_navigator": ModalNavigationStrategy,
            }
        return cls._STRATEGY_MAP

    @classmethod
    def get_strategy_catalog(cls):
        """Return valid strategy names grouped by strategy type."""
        if cls._STRATEGY_CATALOG is None:
            cls._STRATEGY_CATALOG = {
                "pagination": {"infinite_scroll", "next_button"},
                "extraction": {"generic_selector", "google_maps", "multi_step"},
                "output": {
                    "jsonl_file",
                    "secondary_jsonl",
                    "null_output",
                    "postgresql",
                    "postgresql_upsert",
                    "postgresql_listing_upsert",
                    "composite",
                },
                "input": {"file_url_loader", "postgresql_uncrawled_gmaps"},
                "queue": {"redis_queue"},
                "navigation": {
                    "tab_navigator",
                    "accordion_navigator",
                    "modal_navigator",
                },
            }
        return cls._STRATEGY_CATALOG

    @classmethod
    def load_config(cls, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            normalized = normalize_config(config or {})
            validate_config(normalized, cls.get_strategy_catalog())
            return normalized
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
        elif content_type == "listing_crawler":
            from scrapers.listing_crawler import ListingCrawler

            return ListingCrawler(config, **kwargs)
        else:
            raise ValueError(f"Unsupported content type: {content_type}")

    @classmethod
    def create_strategy(cls, strategy_type: str, strategy_name: str, *args, **kwargs):
        """Create and return a strategy instance"""
        strategy_catalog = cls.get_strategy_catalog()
        allowed = strategy_catalog.get(strategy_type)
        if allowed is None:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
        if strategy_name not in allowed:
            raise ValueError(
                f"Unknown {strategy_type} strategy '{strategy_name}'"
            )

        strategy_map = cls.get_strategy_map()
        if strategy_name not in strategy_map:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        strategy_class = strategy_map[strategy_name]
        return strategy_class(*args, **kwargs)
