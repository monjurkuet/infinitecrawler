import yaml
import logging


from utils.config import normalize_config, validate_config


class ScraperFactory:
    _STRATEGY_MAP = None

    @classmethod
    def get_strategy_map(cls):
        if cls._STRATEGY_MAP is None:
            from strategies.pagination.infinite_scroll import InfiniteScrollPaginationStrategy
            from strategies.extraction.generic_selector import GenericSelectorExtractionStrategy
            from strategies.extraction.multi_step import MultiStepExtractionStrategy
            from strategies.output.postgresql import (
                PostgreSQLListingDetailsUpsertStrategy,
                PostgreSQLOutputStrategy,
                PostgreSQLUpsertStrategy,
            )
            from strategies.queue.redis_queue import RedisQueueStrategy
            from strategies.navigation.tab_navigator import TabNavigationStrategy

            cls._STRATEGY_MAP = {
                "infinite_scroll": InfiniteScrollPaginationStrategy,
                "generic_selector": GenericSelectorExtractionStrategy,
                "multi_step": MultiStepExtractionStrategy,
                "postgresql": PostgreSQLOutputStrategy,
                "postgresql_upsert": PostgreSQLUpsertStrategy,
                "postgresql_listing_upsert": PostgreSQLListingDetailsUpsertStrategy,
                "redis_queue": RedisQueueStrategy,
                "tab_navigator": TabNavigationStrategy,
            }
        return cls._STRATEGY_MAP

    @classmethod
    def load_config(cls, config_path: str) -> dict:
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)
            normalized = normalize_config(config or {})
            validate_config(normalized, cls.get_strategy_map().keys())
            return normalized
        except Exception as e:
            logging.error(f"Error loading config file {config_path}: {e}")
            raise

    @classmethod
    def create_strategy(cls, strategy_type: str, strategy_name: str, *args, **kwargs):
        strategy_map = cls.get_strategy_map()
        if strategy_name not in strategy_map:
            raise ValueError(f"Unknown strategy: {strategy_name}")
        return strategy_map[strategy_name](*args, **kwargs)
