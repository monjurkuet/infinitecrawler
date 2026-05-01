"""Queue strategies for managing URL processing queue"""

import json
import logging
import time
from typing import Dict, List, Optional

from base.strategies import QueueStrategy


class RedisQueueStrategy(QueueStrategy):
    """
    Redis-based queue for distributed URL processing.
    Uses Redis lists for atomic operations and sets for deduplication.
    """

    def __init__(self, config: dict):
        # config is the queue section from the main config
        # e.g., {strategy: "redis_queue", config: {host: "..."}}
        self.config = config.get("config", {})
        self.logger = logging.getLogger(self.__class__.__name__)

        # Import redis here to make it optional
        try:
            import redis as redis_lib

            self.redis = redis_lib
        except ImportError:
            raise ImportError(
                "Redis queue requires 'redis' package. Install with: uv add redis"
            )

        # Connect to Redis
        self.client = self.redis.Redis(
            host=self.config.get("host", "localhost"),
            port=self.config.get("port", 6379),
            db=self.config.get("db", 0),
            decode_responses=True,
        )

        # Redis key names
        keys = self.config.get("keys", {})
        self.keys = {
            "pending": keys.get("pending", "crawler:pending"),
            "processing": keys.get("processing", "crawler:processing"),
            "completed": keys.get("completed", "crawler:completed"),
            "failed": keys.get("failed", "crawler:failed"),
        }

        self.visibility_timeout = self.config.get("visibility_timeout", 300)

        # Test connection
        try:
            self.client.ping()
            self.logger.info("Connected to Redis queue")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    def enqueue(self, urls: List[str]) -> int:
        """
        Add URLs to pending queue.
        Skips URLs already in completed set.
        Returns count of URLs actually added.
        """
        if not urls:
            return 0

        added = 0
        pipe = self.client.pipeline()

        for url in urls:
            # Skip if already completed
            if self.client.sismember(self.keys["completed"], url):
                continue

            # Check if already in queue
            if self.client.lpos(self.keys["pending"], url) is not None:
                continue

            pipe.lpush(self.keys["pending"], url)
            added += 1

        pipe.execute()
        self.logger.info(
            f"Enqueued {added} new URLs (skipped {len(urls) - added} duplicates/completed)"
        )
        return added

    def dequeue(self, timeout: int = 5) -> Optional[str]:
        """
        Get next URL from pending queue with atomic move to processing.
        Uses BRPOP for blocking wait.
        """
        try:
            # Atomic move from pending to processing
            result = self.client.brpoplpush(
                self.keys["pending"], self.keys["processing"], timeout=timeout
            )

            if result:
                # Store processing timestamp for visibility timeout
                self.client.hset(
                    f"{self.keys['processing']}:timestamps", result, str(time.time())
                )
                self.logger.debug(f"Dequeued URL: {result[:80]}...")

            return result

        except Exception as e:
            self.logger.error(f"Error dequeuing URL: {e}")
            return None

    def mark_completed(self, url: str):
        """Mark URL as successfully completed"""
        pipe = self.client.pipeline()
        pipe.lrem(self.keys["processing"], 0, url)
        pipe.hdel(f"{self.keys['processing']}:timestamps", url)
        pipe.sadd(self.keys["completed"], url)
        pipe.execute()
        self.logger.debug(f"Marked as completed: {url[:80]}...")

    def mark_failed(self, url: str, error: str, retry_count: int = 0):
        """Mark URL as failed with error details"""
        error_info = {"error": error, "retries": retry_count, "failed_at": time.time()}

        pipe = self.client.pipeline()
        pipe.lrem(self.keys["processing"], 0, url)
        pipe.hdel(f"{self.keys['processing']}:timestamps", url)
        pipe.hset(self.keys["failed"], url, json.dumps(error_info))
        pipe.execute()

        self.logger.warning(
            f"Marked as failed (retry {retry_count}): {url[:80]}... - {error}"
        )

    def get_stats(self) -> Dict:
        """Return queue statistics"""
        return {
            "pending": self.client.llen(self.keys["pending"]),
            "processing": self.client.llen(self.keys["processing"]),
            "completed": self.client.scard(self.keys["completed"]),
            "failed": self.client.hlen(self.keys["failed"]),
        }

    def requeue_stalled(self) -> int:
        """
        Requeue URLs that have been processing longer than visibility_timeout.
        Returns count of requeued URLs.
        """
        stalled = []
        timestamps = self.client.hgetall(f"{self.keys['processing']}:timestamps")
        current_time = time.time()

        for url, timestamp_str in timestamps.items():
            try:
                elapsed = current_time - float(timestamp_str)
                if elapsed > self.visibility_timeout:
                    stalled.append(url)
            except ValueError:
                continue

        # Requeue stalled URLs
        for url in stalled:
            pipe = self.client.pipeline()
            pipe.lrem(self.keys["processing"], 0, url)
            pipe.hdel(f"{self.keys['processing']}:timestamps", url)
            pipe.lpush(self.keys["pending"], url)
            pipe.execute()

        if stalled:
            self.logger.info(f"Requeued {len(stalled)} stalled URLs")

        return len(stalled)
