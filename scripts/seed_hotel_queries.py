#!/usr/bin/env python3
"""Seed hotel-only queries directly into Redis pending queue for immediate processing.

Generates hotel-specific Google Maps search queries for all 15 BD cities
using the hotels-hospitality sector keywords, then pushes them to the
gmaps_bd_business:pending Redis queue at the front (LPUSH) so the
search daemon picks them up immediately.

Usage:
    uv run python scripts/seed_hotel_queries.py
    uv run python scripts/seed_hotel_queries.py --front  # LPUSH (front of queue, processed first)
    uv run python scripts/seed_hotel_queries.py --back   # RPUSH (back of queue, default)
"""

import argparse
import logging
import random
import re
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger("seed_hotel_queries")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

REDIS_QUEUE_KEY = "gmaps_bd_business:pending"
SEARCH_URL_TEMPLATE = "https://www.google.com/maps/search/{query}/"

# ── Hotel keywords (from software_sectors.yaml hotels-hospitality sector) ────

HOTEL_KEYWORDS_EN = [
    "hotel", "resort", "guest house", "luxury hotel",
    "boutique hotel", "budget hotel", "motel",
    "hotel & restaurant", "rest house", "boarding", "inn",
    "hostel", "hotel resort",
]

HOTEL_KEYWORDS_BN = [
    "হোটেল", "রিসোর্ট", "গেস্ট হাউস", "আবাসিক হোটেল",
    "বোর্ডিং", "রেস্ট হাউস", "মোটেল", "হোস্টেল",
    "লাক্সারি হোটেল", "বাজেট হোটেল", "বুটিক হোটেল",
]

BD_CITIES = [
    ("Dhaka", "ঢাকা"),
    ("Chattogram", "চট্টগ্রাম"),
    ("Sylhet", "সিলেট"),
    ("Khulna", "খুলনা"),
    ("Rajshahi", "রাজশাহী"),
    ("Barishal", "বরিশাল"),
    ("Rangpur", "রংপুর"),
    ("Mymensingh", "ময়মনসিংহ"),
    ("Cumilla", "কুমিল্লা"),
    ("Bogura", "বগুড়া"),
    ("Jashore", "যশোর"),
    ("Cox's Bazar", "কক্সবাজার"),
    ("Narayanganj", "নারায়ণগঞ্জ"),
    ("Gazipur", "গাজীপুর"),
    ("Feni", "ফেনী"),
    ("Narsingdi", "নরসিংদী"),
]


def generate_hotel_queries() -> list[str]:
    """Generate hotel-only queries for BD-Local and BD-National.

    Generates for each keyword × each city (BD-Local) and each keyword + Bangladesh (BD-National).
    No global queries — hotels are local businesses.
    """
    queries = set()

    # BD-Local: "{keyword} in {city}" / "{keyword} {city_bn}"
    for kw_en in HOTEL_KEYWORDS_EN:
        for city_en, city_bn in BD_CITIES:
            queries.add(f"{kw_en} in {city_en}")
            queries.add(f"{kw_en} {city_bn}")

    for kw_bn in HOTEL_KEYWORDS_BN:
        for city_en, city_bn in BD_CITIES:
            queries.add(f"{kw_bn} {city_en}")
            queries.add(f"{kw_bn} {city_bn}")

    # BD-National: "{keyword} Bangladesh"
    all_keywords = HOTEL_KEYWORDS_EN + HOTEL_KEYWORDS_BN
    for kw in all_keywords:
        queries.add(f"{kw} Bangladesh")
        if "Bangladesh" not in kw and "BD" not in kw:
            queries.add(f"{kw} outside Dhaka")

    # Normalize and deduplicate
    seen = set()
    result = []
    for q in queries:
        norm = re.sub(r"\s+", " ", q.strip().lower())
        if norm and norm not in seen and len(norm) > 5:
            seen.add(norm)
            result.append(q.strip())

    random.shuffle(result)
    return result


def push_to_redis(queries: list[str], push_front: bool = False) -> int:
    """Push queries to Redis queue. Returns count pushed."""
    redis_cmd = ["redis-cli", "LPUSH" if push_front else "RPUSH", REDIS_QUEUE_KEY]
    # redis-cli LPUSH can take multiple values
    # But to avoid arg length limits, batch in groups of 100
    batch_size = 100
    total_pushed = 0
    for i in range(0, len(queries), batch_size):
        batch = queries[i:i + batch_size]
        try:
            result = subprocess.run(
                redis_cmd + batch,
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                pushed = int(result.stdout.strip())
                total_pushed += pushed
                log.info(f"Pushed {pushed} queries (batch {i//batch_size + 1})")
            else:
                log.error(f"Redis error: {result.stderr.strip()}")
        except Exception as e:
            log.error(f"Redis command failed: {e}")
    return total_pushed


def main():
    parser = argparse.ArgumentParser(description="Seed hotel queries into Redis")
    parser.add_argument("--front", action="store_true", help="Push to FRONT of queue (LPUSH)")
    parser.add_argument("--back", action="store_true", help="Push to BACK of queue (RPUSH, default)")
    args = parser.parse_args()

    push_front = args.front and not args.back

    queries = generate_hotel_queries()
    log.info(f"Generated {len(queries)} unique hotel queries")

    # Show samples
    log.info(f"Samples: {queries[:5]}")
    log.info(f"Last samples: {queries[-5:]}")

    total = push_to_redis(queries, push_front=push_front)

    # Verify
    try:
        result = subprocess.run(
            ["redis-cli", "LLEN", REDIS_QUEUE_KEY],
            capture_output=True, text=True, timeout=10,
        )
        pending = int(result.stdout.strip())
        log.info(f"Total pushed: {total} | Queue now has {pending} pending queries")
    except Exception as e:
        log.warning(f"Could not verify queue length: {e}")


if __name__ == "__main__":
    main()
