"""Redis service for queue visibility and operations."""

from __future__ import annotations

import json
import logging
from typing import Optional

import redis as redis_lib

log = logging.getLogger("api.redis_service")

_client: Optional[redis_lib.Redis] = None


async def create_client() -> redis_lib.Redis:
    global _client
    client = redis_lib.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=True,
    )
    client.ping()
    _client = client
    log.info("Redis connected")
    return client


def get_client() -> redis_lib.Redis:
    if _client is None:
        raise RuntimeError("Redis not initialized")
    return _client


async def close_client():
    global _client
    if _client:
        _client.close()
        _client = None


async def check_health() -> str:
    try:
        client = get_client()
        client.ping()
        return "ok"
    except Exception as e:
        log.warning(f"Redis health check failed: {e}")
        return "error"


async def get_queue_names() -> list[str]:
    client = get_client()
    keys = client.keys("*:pending")
    return sorted(set(k.replace(":pending", "") for k in keys))


async def get_queue_stats(key: str) -> dict:
    client = get_client()
    pending = client.llen(f"{key}:pending")
    processing = client.llen(f"{key}:processing")
    completed = client.scard(f"{key}:completed")
    failed = client.hlen(f"{key}:failed") if client.type(f"{key}:failed") == "hash" else 0
    return {
        "key": key,
        "pending": pending,
        "processing": processing,
        "completed": completed,
        "failed": failed,
    }


async def get_all_queue_stats() -> list[dict]:
    names = await get_queue_names()
    stats = []
    for name in names:
        stats.append(await get_queue_stats(name))
    return stats


async def get_failed_items(key: str) -> list[dict]:
    client = get_client()
    failed_key = f"{key}:failed"
    if client.type(failed_key) != "hash":
        return []
    items = client.hgetall(failed_key)
    result = []
    for url, error_json in items.items():
        try:
            info = json.loads(error_json)
        except json.JSONDecodeError:
            info = {"error": str(error_json), "retries": 0, "failed_at": ""}
        result.append({
            "url": url,
            "error": info.get("error", "unknown"),
            "retries": info.get("retries", 0),
            "failed_at": info.get("failed_at", ""),
        })
    return result


async def requeue_stalled(key: str, timeout_secs: int = 300) -> int:
    client = get_client()
    import time
    timestamps = client.hgetall(f"{key}:processing:timestamps")
    now = time.time()
    requeued = 0
    for url, ts_str in timestamps.items():
        try:
            elapsed = now - float(ts_str)
            if elapsed > timeout_secs:
                pipe = client.pipeline()
                pipe.lrem(f"{key}:processing", 0, url)
                pipe.hdel(f"{key}:processing:timestamps", url)
                pipe.lpush(f"{key}:pending", url)
                pipe.execute()
                requeued += 1
        except (ValueError, Exception):
            continue
    return requeued


async def clear_queue(key: str, target: str) -> bool:
    client = get_client()
    targets = {"pending", "processing", "failed"}
    if target not in targets:
        return False
    full_key = f"{key}:{target}"
    if target == "failed":
        client.delete(full_key)
    elif target == "processing":
        client.delete(full_key, f"{full_key}:timestamps")
    else:
        client.delete(full_key)
    return True