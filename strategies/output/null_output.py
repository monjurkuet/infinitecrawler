"""Null output strategy used when output is optional or omitted."""

from __future__ import annotations

from typing import Dict

from base.strategies import OutputStrategy


class NullOutputStrategy(OutputStrategy):
    def __init__(self, config: dict):
        self.config = config
        self.max_results = 0

    async def write_item(self, item: Dict):
        return None

    def has_reached_limit(self) -> bool:
        return False

    async def cleanup(self):
        return None
