"""Output strategies for persisting scraped data"""

import json
import os
from typing import Dict

from base.strategies import OutputStrategy


class JsonlFileOutputStrategy(OutputStrategy):
    """Append items to a JSONL file"""

    def __init__(self, config: dict):
        # config is the output section from the main config
        # e.g., {strategy: "jsonl_file", config: {file_path: "..."}}
        self.config = config.get("config", {})
        self.file_path = self.config.get("file_path", "output/data.jsonl")
        self.max_results = self.config.get("max_results", 500)
        self.results_count = 0
        self._ensure_directory()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _ensure_directory(self):
        """Ensure output directory exists"""
        dir_path = os.path.dirname(self.file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

    def _serialize_item(self, item: Dict) -> Dict:
        """Convert item to JSON-serializable format"""
        from datetime import datetime

        serialized = {}
        for key, value in item.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized

    async def write_item(self, item: Dict):
        """Append item to JSONL file"""
        try:
            serialized = self._serialize_item(item)
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(serialized, ensure_ascii=False) + "\n")
            self.results_count += 1
        except Exception as e:
            self.logger.error(f"Failed to write item to {self.file_path}: {e}")
            raise

    def has_reached_limit(self) -> bool:
        """Check if max results limit reached"""
        return self.results_count >= self.max_results


class SecondaryJsonlOutputStrategy(JsonlFileOutputStrategy):
    """Secondary JSONL output for backup/failed items"""

    def __init__(self, config: dict):
        # config is the secondary_output section from the main config
        # e.g., {strategy: "secondary_jsonl", config: {file_path: "..."}}
        self.config = config.get("config", {})
        self.file_path = self.config.get("file_path", "output/secondary_backup.jsonl")
        self.max_results = self.config.get("max_results", 100000)
        self.results_count = 0
        self._ensure_directory()
        self.logger = logging.getLogger(self.__class__.__name__)


import logging  # noqa: E402
