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

# City coords matched to main query_generator.BD_CITIES order.
# Dhaka added here (not in BD_CITIES of the generator — it's implicit
# as the national center, but we include it for hotel seeding).
_BD_CITY_COORDS = [
    ("Dhaka",       "ঢাকা",       23.8103,  90.4125),
    ("Chattogram",  "চট্টগ্রাম",  22.3569,  91.7832),
    ("Sylhet",      "সিলেট",      24.8949,  91.8687),
    ("Khulna",      "খুলনা",      22.8456,  89.5403),
    ("Rajshahi",    "রাজশাহী",    24.3636,  88.6241),
    ("Barishal",    "বরিশাল",     22.7010,  90.3535),
    ("Rangpur",     "রংপুর",      25.7439,  89.2752),
    ("Mymensingh",  "ময়মনসিংহ", 24.7471,  90.4203),
    ("Cumilla",     "কুমিল্লা",   23.4607,  91.1809),
    ("Bogura",      "বগুড়া",     24.8484,  89.3733),
    ("Jashore",     "যশোর",       23.1684,  89.2123),
    ("Cox's Bazar", "কক্সবাজার",  21.4272,  92.0058),
    ("Narayanganj", "নারায়ণগঞ্জ",23.6238,  90.5000),
    ("Gazipur",     "পিজাপুর",     23.9919,  90.4203),
    ("Feni",        "ফেনী",       23.0149,  91.3953),
    ("Narsingdi",   "নরসিংদী",   23.9889,  90.4650),
]
_BD_NATIONAL_COORDS = (23.685, 90.3563)  # Bangladesh center


def generate_hotel_queries() -> list[str]:
    """Generate hotel-only queries in KEYWORD|LAT|LNG format (coords-anchored).
    Uses the same format as query_generator._build_bd_local so the daemon
    builds region-anchored search URLs automatically.
    """
    queries = set()

    # BD-Local: KEYWORD|LAT|LNG per city
    for kw in HOTEL_KEYWORDS_EN + HOTEL_KEYWORDS_BN:
        for _, _, lat, lng in _BD_CITY_COORDS:
            queries.add(f"{kw}|{lat:.4f}|{lng:.4f}")

    # BD-National: Bangladesh-center coords
    for kw in HOTEL_KEYWORDS_EN + HOTEL_KEYWORDS_BN:
        queries.add(f"{kw}|{_BD_NATIONAL_COORDS[0]:.4f}|{_BD_NATIONAL_COORDS[1]:.4f}")

    # Normalize and deduplicate
    seen = set()
    result = []
    for q in queries:
        norm = re.sub(r"\s+", " ", q.strip().lower())
        if norm and norm not in seen and len(norm) > 10:
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
