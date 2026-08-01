#!/usr/bin/env python3
"""Seed ALL BD hotels from gmaps_listings into luxury_targets.

One-shot import: reads every hotel/resort/guest house from the existing
GMaps crawl data and creates luxury_target records for LinkedIn discovery.

Usage:
    uv run python scripts/seed_all_hotels.py              # seed
    uv run python scripts/seed_all_hotels.py --dry-run     # preview
    uv run python scripts/seed_all_hotels.py --stats       # show current numbers
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from utils.pg import get_pg_config  # noqa: E402

log = logging.getLogger("seed_hotels")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - seed - %(levelname)s - %(message)s")

HOTEL_CATEGORIES = ["hotel", "resort", "guest house", "lodging", "motel", "inn", "hostel"]


def detect_city(address: str | None, lat: float | None, lon: float | None) -> str:
    """Detect city from address or coordinates."""
    if lat is not None and lon is not None:
        try:
            if 23.66 <= float(lat) <= 23.95 and 90.25 <= float(lon) <= 90.55:
                return "Dhaka"
        except (ValueError, TypeError):
            pass
    addr = (address or "").lower()
    city_map = {
        "Chattogram": ["chatto", "chitta", "ctg", "agrabad"],
        "Sylhet": ["sylhet"],
        "Khulna": ["khulna"],
        "Rajshahi": ["rajshahi"],
        "Barishal": ["barisal", "baris"],
        "Rangpur": ["rangpur"],
        "Mymensingh": ["mymen", "mymensingh"],
        "Cox's Bazar": ["cox"],
        "Bogura": ["bogur", "bogra", "bogu"],
        "Cumilla": ["cumil", "comil", "comilla"],
        "Feni": ["feni"],
        "Jashore": ["jasho", "jesso", "jashore", "jessore"],
        "Gazipur": ["gazipu"],
        "Narayanganj": ["naray"],
        "Narsingdi": ["narsi"],
    }
    for city, keywords in city_map.items():
        if any(kw in addr for kw in keywords):
            return city
    return "Other"


def detect_tier(rating: float | None, review_count: int | None) -> str:
    if rating is None:
        return "standard"
    r = float(rating)
    n = int(review_count or 0)
    if r >= 4.5 and n >= 50:
        return "luxury"
    if r >= 4.3 or (r >= 4.0 and n >= 100):
        return "premium"
    if r >= 4.0:
        return "standard"
    return "budget"


def detect_type(name: str, category: str | None) -> str:
    t = ((category or "") + " " + name).lower()
    if "resort" in t:
        return "resort"
    if "guest house" in t or "guesthouse" in t:
        return "guesthouse"
    if "hostel" in t:
        return "hostel"
    if "motel" in t:
        return "motel"
    return "hotel"


def seed_all(conn, dry_run: bool = False) -> int:
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, category, rating, review_count, address, latitude, longitude
        FROM scraper.gmaps_listings
        WHERE name IS NOT NULL AND name != ''
          AND (
            LOWER(category) LIKE '%hotel%'
            OR LOWER(category) LIKE '%resort%'
            OR LOWER(category) LIKE '%guest house%'
            OR LOWER(category) LIKE '%lodging%'
            OR LOWER(category) LIKE '%motel%'
            OR LOWER(category) LIKE '%inn%'
            OR LOWER(category) LIKE '%hostel%'
            OR LOWER(name) LIKE '%hotel%'
            OR LOWER(name) LIKE '%resort%'
          )
          AND name NOT IN (SELECT name FROM scraper.luxury_targets)
        ORDER BY rating DESC NULLS LAST
    """)
    rows = cur.fetchall()
    log.info("Found %d new hotel listings to seed", len(rows))

    if dry_run:
        log.info("DRY RUN — would seed %d targets", len(rows))
        for r in rows[:20]:
            city = detect_city(r[5], r[6], r[7])
            tier = detect_tier(r[3], r[4])
            ttype = detect_type(r[1], r[2])
            log.info("  %s | %s | %s | %s | ★%s (%d reviews)",
                     r[1][:40], city, ttype, tier,
                     r[3] or "?", r[4] or 0)
        if len(rows) > 20:
            log.info("  ... and %d more", len(rows) - 20)
        return len(rows)

    seeded = 0
    for r in rows:
        gid, name, category, rating, reviews, address, lat, lon = r
        city = detect_city(address, lat, lon)
        tier = detect_tier(rating, reviews)
        ttype = detect_type(name, category)
        try:
            cur.execute("""
                INSERT INTO scraper.luxury_targets
                    (name, city, target_type, tier, address, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING
            """, (name, city, ttype, tier, address, lat, lon))
            if cur.rowcount:
                seeded += 1
        except Exception as e:
            log.debug("Skip '%s': %s", name[:40], e)

    conn.commit()
    log.info("Seeded %d new targets", seeded)
    return seeded


def show_stats(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scraper.luxury_targets")
    total = cur.fetchone()[0]
    cur.execute("""
        SELECT target_type, tier, COUNT(*)
        FROM scraper.luxury_targets
        GROUP BY target_type, tier
        ORDER BY target_type, COUNT(*) DESC
    """)
    print(f"\n{'='*55}\n  Luxury Targets ({total} total)\n{'='*55}")
    for r in cur.fetchall():
        print(f"  {r[0]:20s} {r[1]:10s} → {r[2]:>5}")
    cur.execute("SELECT COUNT(*) FROM scraper.luxury_targets WHERE linkedin_searched = FALSE")
    pending = cur.fetchone()[0]
    print(f"\n  Pending LinkedIn search: {pending}")
    print(f"{'='*55}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()

    conn = psycopg.connect(**get_pg_config())
    conn.autocommit = False
    try:
        if args.stats:
            show_stats(conn)
            return
        seed_all(conn, dry_run=args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()