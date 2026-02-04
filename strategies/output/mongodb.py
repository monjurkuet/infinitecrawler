"""MongoDB output strategy for persisting scraped data"""

import logging
from typing import Dict, Optional
from datetime import datetime, timezone

from base.strategies import OutputStrategy


class MongoDBOutputStrategy(OutputStrategy):
    """Write items to MongoDB collection"""

    def __init__(self, config: dict):
        self.config = config.get("config", {})
        self.uri = self.config.get("uri", "mongodb://localhost:27017")
        self.database = self.config.get("database", "scraping")
        self.collection = self.config.get("collection", "gmaps_listings")
        self.max_results = self.config.get("max_results", 100000)
        self.results_count = 0
        self._client = None
        self._db = None
        self._collection = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connect()

    def _connect(self):
        """Connect to MongoDB"""
        try:
            from pymongo import MongoClient

            self._client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self._db = self._client[self.database]
            self._collection = self._db[self.collection]
            self.logger.info(f"Connected to MongoDB: {self.database}.{self.collection}")
        except ImportError:
            self.logger.error("pymongo not installed. Run: pip install pymongo")
            raise
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def write_item(self, item: Dict):
        """Insert item into MongoDB collection"""
        try:
            if self.results_count >= self.max_results:
                self.logger.warning(f"Max results limit ({self.max_results}) reached")
                return

            item["_extracted_at"] = datetime.now(timezone.utc)

            if self._collection is not None:
                self._collection.insert_one(item)
                self.results_count += 1
            else:
                self.logger.error("MongoDB collection not initialized")

        except Exception as e:
            self.logger.error(f"Failed to write to MongoDB: {e}")
            raise

    def has_reached_limit(self) -> bool:
        """Check if max results limit reached"""
        return self.results_count >= self.max_results

    async def cleanup(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self.logger.info("MongoDB connection closed")


class MongoDBUpsertStrategy(OutputStrategy):
    """Upsert items to MongoDB using a configurable unique key field"""

    def __init__(self, config: dict):
        self.config = config.get("config", {})
        self.uri = self.config.get("uri", "mongodb://localhost:27017")
        self.database = self.config.get("database", "scraping")
        self.collection = self.config.get("collection", "gmaps_listings")
        self.key_field = self.config.get("key_field", "source_url")
        self.max_results = self.config.get("max_results", 100000)
        self.results_count = 0
        self._client = None
        self._db = None
        self._collection = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connect()

    def _connect(self):
        """Connect to MongoDB"""
        try:
            from pymongo import MongoClient

            self._client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self._db = self._client[self.database]
            self._collection = self._db[self.collection]
            self.logger.info(f"Connected to MongoDB: {self.database}.{self.collection}")
        except ImportError:
            self.logger.error("pymongo not installed. Run: pip install pymongo")
            raise
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def write_item(self, item: Dict):
        """Upsert item into MongoDB using configured key field"""
        try:
            if self.results_count >= self.max_results:
                self.logger.warning(f"Max results limit ({self.max_results}) reached")
                return

            item["_updated_at"] = datetime.now(timezone.utc)

            if self._collection is not None and self.key_field in item:
                key_value = item[self.key_field]
                self._collection.update_one(
                    {self.key_field: key_value}, {"$set": item}, upsert=True
                )
                self.results_count += 1
            else:
                self.logger.error(
                    f"MongoDB collection not initialized or key field '{self.key_field}' missing"
                )

        except Exception as e:
            self.logger.error(f"Failed to upsert to MongoDB: {e}")
            raise

    def has_reached_limit(self) -> bool:
        """Check if max results limit reached"""
        return self.results_count >= self.max_results

    async def cleanup(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self.logger.info("MongoDB connection closed")


import logging  # noqa: E402
