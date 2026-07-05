#!/usr/bin/env python3
"""generate_leads.py — Export qualified leads from gmaps_listings to CSV.

Scores businesses by fit for warehouse/inventory management service offering:
  - Must have phone (reachable)
  - Must have website (digital presence → likely FB ads)
  - Outside Dhaka (coord/address based)
  - E-commerce, fashion, electronics categories prioritized
  - Facebook URLs in website field = strong ad signal

Output: output/leads/YYYY-MM-DD/
  - all_leads.csv          — all qualified leads
  - by_city/<city>.csv     — grouped by detected city
  - top50_priority.csv     — top 50 sorted by lead score

Usage:
    uv run python scripts/generate_leads.py
    uv run python scripts/generate_leads.py --min-score 0.3 --limit 100
    uv run python scripts/generate_leads.py --json     # JSON output instead of CSV
"""

import argparse
import csv
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("generate_leads")

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output" / "leads"

# ---- DB connection -----------------------------------------------------------

from utils.pg import get_pg_config

DB_CONFIG = get_pg_config()

# ---- Keyword-based category scoring ------------------------------------------

HIGH_VALUE_CATEGORIES = {
    "e-commerce", "online", "fashion", "clothing", "electronics",
    "cosmetics", "beauty", "mobile", "computer", "gadget", "gadgets",
    "accessories", "watch", "jewellery", "jewelry", "gift", "toy",
    "sports", "footwear", "furniture", "home decor", "home appliances",
    "books", "stationery", "baby", "kids", "perfume", "bag", "handicraft",
    "organic", "health supplements", "automotive", "car accessories",
}

# Dhaka coordinate bounding box (loose)
DHAKA_LAT_MIN, DHAKA_LAT_MAX = 23.66, 23.95
DHAKA_LON_MIN, DHAKA_LON_MAX = 90.25, 90.55

DHAKA_KEYWORDS = {"dhaka", "ঢাকা", "gulshan", "banani", "mirpur", "uttara",
                  "bashundhara", "dhanmondi", "motijheel", "savar",
                  "narayanganj", "gazipur"}


def connect_pg():
    """Connect to PostgreSQL."""
    try:
        import psycopg
    except ImportError:
        log.error("psycopg not installed. Run: uv add psycopg[binary]")
        sys.exit(1)
    try:
        conn = psycopg.connect(**DB_CONFIG)
        conn.autocommit = True
        log.info(f"Connected to {DB_CONFIG['dbname']}")
        return conn
    except Exception as e:
        log.error(f"DB connection failed: {e}")
        sys.exit(1)


def is_outside_dhaka(row: dict) -> bool:
    """Check if business is outside Dhaka via coords or address."""
    lat = row.get("latitude")
    lon = row.get("longitude")
    if lat and lon:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            if (DHAKA_LAT_MIN <= lat_f <= DHAKA_LAT_MAX and
                DHAKA_LON_MIN <= lon_f <= DHAKA_LON_MAX):
                return False
            return True  # coords exist and outside Dhaka
        except (ValueError, TypeError):
            pass

    # Fallback: check address
    addr = (row.get("address") or "").lower()
    return not any(kw in addr for kw in DHAKA_KEYWORDS)


def detect_city(address: str) -> str:
    """Extract city name from address text."""
    if not address:
        return "unknown"
    addr_lower = address.lower()
    city_map = {
        "chattogram": ["chattogram", "chittagong", "ctg", "চট্টগ্রাম", "agrabad"],
        "sylhet": ["sylhet", "সিলেট"],
        "khulna": ["khulna", "খুলনা"],
        "rajshahi": ["rajshahi", "রাজশাহী"],
        "mymensingh": ["mymensingh", "ময়মনসিংহ"],
        "cumilla": ["cumilla", "comilla", "কুমিল্লা"],
        "barishal": ["barishal", "barisal", "বরিশাল"],
        "rangpur": ["rangpur", "রংপুর"],
        "bogura": ["bogura", "bogra", "বগুড়া"],
        "jashore": ["jashore", "jessore", "যশোর"],
        "cox's bazar": ["cox", "কক্সবাজার"],
        "feni": ["feni", "ফেনী"],
        "dinajpur": ["dinajpur", "দিনাজপুর"],
        "narayanganj": ["narayanganj", "নারায়ণগঞ্জ"],
        "tangail": ["tangail", "টাঙ্গাইল"],
        "naogaon": ["naogaon", "নওগাঁ"],
        "pabna": ["pabna", "পাবনা"],
        "kushtia": ["kushtia", "কুষ্টিয়া"],
        "noakhali": ["noakhali", "নোয়াখালী"],
    }
    for city, keywords in city_map.items():
        if any(kw in addr_lower for kw in keywords):
            return city.title()
    return "unknown"


def is_facebook_url(url: str) -> bool:
    """Check if website URL is a Facebook page."""
    if not url:
        return False
    try:
        domain = urlparse(url).netloc.lower()
        return "facebook" in domain or "fb.com" in domain
    except Exception:
        return False


def compute_lead_score(row: dict) -> float:
    """Compute lead quality score 0.0–1.0.

    Factors:
      - Has website + phone (base)
      - Facebook page → strong ad signal
      - Category fit for warehouse/inventory
      - Rating
      - Review count (business size proxy)
      - Outside Dhaka (stronger need for Dhaka hub)
    """
    score = 0.0

    # Base: has phone + website
    if row.get("phone") and row.get("website"):
        score += 0.2

    # Facebook page → actively running FB presence
    if is_facebook_url(row.get("website", "")):
        score += 0.25

    # Category fit
    category = (row.get("category") or "").lower()
    for kw in HIGH_VALUE_CATEGORIES:
        if kw in category:
            score += 0.2
            break

    # Rating (good rating = established business)
    try:
        rating = float(row.get("rating") or 0)
        score += (rating / 5.0) * 0.15
    except (ValueError, TypeError):
        pass

    # Review count (more reviews = larger business, more likely to need supply chain)
    try:
        reviews = int(row.get("review_count") or 0)
        score += min(reviews / 100, 1.0) * 0.1
    except (ValueError, TypeError):
        pass

    # Outside Dhaka (stronger need for Dhaka warehouse)
    if is_outside_dhaka(row):
        score += 0.1

    return round(min(score, 1.0), 4)


def fetch_leads(conn, min_score: float = 0.2, limit: int | None = None) -> list[dict]:
    """Fetch qualified leads from gmaps_listings."""
    query = """
        SELECT
            id, name, category, rating, review_count,
            address, phone, website, latitude, longitude,
            place_id, source_url, created_at
        FROM scraper.gmaps_listings
        WHERE phone IS NOT NULL
          AND website IS NOT NULL
          AND name IS NOT NULL
        ORDER BY
          CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
               AND (latitude < 23.66 OR latitude > 23.95
                    OR longitude < 90.25 OR longitude > 90.55)
               THEN 0 ELSE 1 END,
          review_count DESC NULLS LAST,
          rating DESC NULLS LAST
    """
    if limit:
        query += f"\nLIMIT {limit * 3}"  # fetch extra for scoring

    with conn.cursor() as cur:
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # Compute score
    scored = []
    for row in rows:
        score = compute_lead_score(row)
        if score >= min_score:
            row["lead_score"] = score
            row["has_fb"] = is_facebook_url(row.get("website", ""))
            row["city"] = detect_city(row.get("address", ""))
            row["outside_dhaka"] = is_outside_dhaka(row)
            scored.append(row)

    # Sort by score descending
    scored.sort(key=lambda r: r["lead_score"], reverse=True)

    if limit:
        scored = scored[:limit]

    return scored


def write_csv(leads: list[dict], path: Path):
    """Write leads to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "lead_score", "name", "category", "phone", "website",
        "has_fb", "address", "city", "outside_dhaka",
        "rating", "review_count", "latitude", "longitude",
        "place_id", "source_url",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    log.info(f"Wrote {len(leads)} leads to {path}")


def write_json(leads: list[dict], path: Path):
    """Write leads to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False, default=str)
    log.info(f"Wrote {len(leads)} leads to {path}")


def group_by_city(leads: list[dict]) -> dict[str, list[dict]]:
    """Group leads by detected city."""
    groups = {}
    for lead in leads:
        city = lead.get("city", "unknown")
        groups.setdefault(city, []).append(lead)
    return groups


def main():
    parser = argparse.ArgumentParser(description="Generate qualified leads from Google Maps listings")
    parser.add_argument("--min-score", type=float, default=0.2,
                        help="Minimum lead score (0-1, default: 0.2)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max leads to export (default: all)")
    parser.add_argument("--json", action="store_true",
                        help="Output JSON instead of CSV")
    args = parser.parse_args()

    conn = connect_pg()

    log.info(f"Fetching leads with min_score={args.min_score}...")
    leads = fetch_leads(conn, min_score=args.min_score, limit=args.limit)
    log.info(f"Found {len(leads)} qualified leads")

    if not leads:
        log.info("No leads to export.")
        conn.close()
        return

    # Date-based output dir
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUTPUT_DIR / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    # Summary
    fb_count = sum(1 for l in leads if l["has_fb"])
    outside_count = sum(1 for l in leads if l["outside_dhaka"])
    cities = group_by_city(leads)
    log.info(f"  Facebook pages: {fb_count}")
    log.info(f"  Outside Dhaka:  {outside_count}")
    log.info(f"  Cities covered: {len(cities)}")

    # Write full export
    summary = None
    if args.json:
        write_json(leads, out_dir / "all_leads.json")
    else:
        write_csv(leads, out_dir / "all_leads.csv")

        # Top 50
        top50 = leads[:50]
        write_csv(top50, out_dir / "top50_priority.csv")

        # By city
        city_dir = out_dir / "by_city"
        city_dir.mkdir(parents=True, exist_ok=True)
        for city, city_leads in cities.items():
            safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", city.lower())
            path = city_dir / f"{safe_name}.csv"
            write_csv(city_leads, path)

        # Write summary JSON
        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_leads": len(leads),
            "min_score": args.min_score,
            "with_facebook": fb_count,
            "outside_dhaka": outside_count,
            "cities": {city: len(cl) for city, cl in sorted(cities.items())},
            "avg_score": round(sum(l["lead_score"] for l in leads) / len(leads), 4),
        }
        summary_path = out_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        log.info(f"Summary written to {summary_path}")

    conn.close()
    if summary:
        print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
