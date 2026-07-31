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
        self.ignore_completed_on_enqueue = self.config.get(
            "ignore_completed_on_enqueue", False
        )

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
        self._last_requeue_check = 0.0
        self.requeue_check_interval = self.config.get("requeue_check_interval", 30)

        self._lua_enqueue = self.client.register_script("""
            local completed_key = KEYS[1]
            local pending_key  = KEYS[2]
            local url          = ARGV[1]

            if tonumber(ARGV[2]) == 1 and
               redis.call('SISMEMBER', completed_key, url) == 1 then
                return 0
            end
            if redis.call('LPOS', pending_key, url) then
                return 0
            end
            redis.call('LPUSH', pending_key, url)
            return 1
        """)

        # Test connection
        try:
            self.client.ping()
            self.logger.info("Connected to Redis queue")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    def enqueue(self, urls: List[str]) -> int:
        if not urls:
            return 0

        added = 0
        skip_completed = 0 if self.ignore_completed_on_enqueue else 1

        for url in urls:
            try:
                result = self._lua_enqueue(
                    keys=[self.keys["completed"], self.keys["pending"]],
                    args=[url, skip_completed],
                )
                if result:
                    added += 1
            except Exception:
                self.logger.exception("enqueue Lua script failed for %s", url[:80])

        self.logger.info(
            "Enqueued %d new URLs (skipped %d duplicates/completed)",
            added, len(urls) - added,
        )
        return added

    def dequeue(self, timeout: int = 5) -> Optional[str]:
        """
        Get next URL from pending queue with atomic move to processing.
        Uses BRPOP for blocking wait.
        """
        try:
            # Atomic move from pending to processing
            result: str | None = self.client.brpoplpush(
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

    def cleanup(self):
        """Close the Redis client connection."""
        try:
            if hasattr(self.client, "close"):
                self.client.close()
        except Exception as e:
            self.logger.debug(f"Error closing Redis client: {e}")

    def maybe_requeue_stalled(self) -> int:
        current_time = time.time()
        if current_time - self._last_requeue_check < self.requeue_check_interval:
            return 0

        self._last_requeue_check = current_time
        return self.requeue_stalled()

    def requeue_stalled(self) -> int:
        """
        Requeue URLs that have been processing longer than visibility_timeout.
        Returns count of requeued URLs.
        """
        stalled: list[str] = []
        timestamps: dict[str, str] = self.client.hgetall(f"{self.keys['processing']}:timestamps")
        current_time = time.time()

        for url, timestamp_str in timestamps.items():
            try:
                elapsed = current_time - float(timestamp_str)
                if elapsed > self.visibility_timeout:
                    stalled.append(url)
            except ValueError:
                continue

        for url in stalled:
            pipe = self.client.pipeline()
            pipe.lrem(self.keys["processing"], 0, url)
            pipe.hdel(f"{self.keys['processing']}:timestamps", url)
            pipe.lpush(self.keys["pending"], url)
            pipe.execute()

        if stalled:
            self.logger.info(f"Requeued {len(stalled)} stalled URLs")

        return len(stalled)

    def requeue_stale_failed(self, max_age_hours: float = 6.0) -> int:
        """Re-enqueue failed items older than max_age_hours for retry.

        Failed items that hit transient errors (timeouts, rate caps, blips)
        get a second life after cooling down.  Permanently dead items fail
        again and re-enter the failed hash with a fresh timestamp.
        Returns count of retried items.
        """
        try:
            failed_raw = self.client.hgetall(self.keys["failed"])
        except Exception:
            return 0

        if not failed_raw:
            return 0

        cutoff = time.time() - (max_age_hours * 3600)
        to_retry: list[str] = []

        for url, raw in failed_raw.items():
            try:
                info = json.loads(raw) if isinstance(raw, str) else {}
            except Exception:
                info = {}
            if info.get("failed_at", 0) < cutoff:
                to_retry.append(url)

        if not to_retry:
            return 0

        import random
        random.shuffle(to_retry)

        try:
            for url in to_retry:
                self.client.rpush(self.keys["pending"], url)
                self.client.hdel(self.keys["failed"], url)
            self.logger.info(
                "Retried %d stale failures (older than %.1fh)",
                len(to_retry), max_age_hours,
            )
        except Exception as e:
            self.logger.warning("Stale failure retry partially failed: %s", e)

        return len(to_retry)
