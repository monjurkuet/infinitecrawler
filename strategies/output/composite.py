"""Composite output strategy that supports multiple output destinations with fallback"""

import logging
from typing import Dict, List

from base.strategies import OutputStrategy


class CompositeOutputStrategy(OutputStrategy):
    """
    Composite output strategy that writes to multiple output destinations.
    Supports fallback behavior - continues to next strategy if previous fails.
    """

    def __init__(self, config: dict):
        self.config = config
        self.strategies: List[OutputStrategy] = []
        self.logger = logging.getLogger(self.__class__.__name__)

        # Import factory to create strategies
        from factory.scraper_factory import ScraperFactory

        # Get strategies list from config
        strategies_config = config.get("strategies", [])
        if not strategies_config:
            self.logger.warning("No strategies configured for composite output")
            return

        # Create strategy instances
        for strategy_config in strategies_config:
            strategy_name = strategy_config.get("strategy")
            strategy_cfg = strategy_config.get("config", {})

            # Wrap config in proper structure for factory
            wrapped_config = {
                "config": strategy_cfg,
            }

            try:
                strategy = ScraperFactory.create_strategy(
                    "output", strategy_name, wrapped_config
                )
                self.strategies.append(strategy)
                self.logger.info(f"Added {strategy_name} to composite output")
            except Exception as e:
                self.logger.error(f"Failed to create {strategy_name} strategy: {e}")

        if not self.strategies:
            self.logger.warning("No valid strategies initialized for composite output")

    async def write_item(self, item: Dict):
        """
        Write item to all configured strategies.
        If a strategy fails, log and continue to next strategy (fallback behavior).
        """
        if not self.strategies:
            self.logger.warning("No strategies available to write item")
            return

        for i, strategy in enumerate(self.strategies):
            try:
                await strategy.write_item(item)
                self.logger.debug(
                    f"Successfully wrote item to strategy {i + 1}/{len(self.strategies)}"
                )
            except Exception as e:
                self.logger.error(
                    f"Strategy {i + 1}/{len(self.strategies)} failed: {e}. "
                    f"Attempting next strategy if available."
                )
                # Continue to next strategy (fallback)

    def has_reached_limit(self) -> bool:
        """
        Check if ANY strategy has reached its limit.
        Returns True if at least one strategy reached limit.
        """
        for strategy in self.strategies:
            try:
                if strategy.has_reached_limit():
                    self.logger.info(
                        f"Strategy {strategy.__class__.__name__} reached limit"
                    )
                    return True
            except Exception as e:
                self.logger.error(f"Error checking limit for strategy: {e}")

        return False

    async def cleanup(self):
        """Cleanup all strategies"""
        self.logger.info(f"Cleaning up {len(self.strategies)} strategies")
        for i, strategy in enumerate(self.strategies):
            try:
                if hasattr(strategy, "cleanup"):
                    await strategy.cleanup()
                self.logger.debug(f"Cleaned up strategy {i + 1}")
            except Exception as e:
                self.logger.error(f"Error cleaning up strategy {i + 1}: {e}")
