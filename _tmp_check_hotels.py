#!/usr/bin/env python3
"""Quick check: hotel data and LinkedIn profile stats."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

from utils.pg import get_pg_config
import psycopg

pg = get_pg_config()
conn = psycopg.connect(**pg)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM scraper.gmaps_listings")
print(f"Total listings: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM scraper.linkedin_profiles")
print(f"LinkedIn profiles: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(DISTINCT listing_id) FROM scraper.linkedin_profiles")
print(f"Listings with profiles: {cur.fetchone()[0]}")

# Luxury hotel names
luxury = ["%radisson%", "%intercontinental%", "%westin%", "%pan pacific%", "%sheraton%", "%marriott%", "%ritz%", "%four seasons%", "%renaissance%"]
for pat in luxury:
    cur.execute("SELECT COUNT(*) FROM scraper.gmaps_listings WHERE LOWER(name) LIKE %s", (pat,))
    n = cur.fetchone()[0]
    if n > 0:
        print(f"\n{n} listing(s) matching '{pat}':")
        cur.execute("SELECT id, name, category, rating, phone FROM scraper.gmaps_listings WHERE LOWER(name) LIKE %s LIMIT 5", (pat,))
        for r in cur.fetchall():
            print(f"  [{r[0]}] {r[1]:45s} | {str(r[2] or ''):25s} | {str(r[3] or ''):5s} | {str(r[4] or ''):18s}")

# Hotel category count
cur.execute("SELECT COUNT(*) FROM scraper.gmaps_listings WHERE LOWER(category) LIKE '%hotel%'")
print(f"\nHotel-category listings: {cur.fetchone()[0]}")

# All listings with 'hotel' in name or category
cur.execute("SELECT id, name, category, phone, website FROM scraper.gmaps_listings WHERE LOWER(name) LIKE '%hotel%' OR LOWER(category) LIKE '%hotel%' ORDER BY rating DESC NULLS LAST LIMIT 10")
print("\nTop hotels by rating:")
for r in cur.fetchall():
    print(f"  [{r[0]}] {r[1]:45s} | {str(r[2] or ''):25s} | rating={str(r[3] or ''):5s} | {str(r[4] or '')[:30] or '':30s}")

# Check which sectors exist
try:
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='scraper' AND table_name='gmaps_listings'")
    cols = [r[0] for r in cur.fetchall()]
    print(f"\ngmaps_listings columns: {cols}")
except Exception as e:
    print(f"Schema check error: {e}")

conn.close()
