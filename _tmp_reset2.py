#!/usr/bin/env python3
"""Reset all test data from luxury_contacts and linkedin_profiles so we get a clean run."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.pg import get_pg_config
import psycopg

pg = get_pg_config()
conn = psycopg.connect(**pg)
cur = conn.cursor()

# Clean ALL luxury_contacts and luxury-sourced linkedin_profiles
cur.execute("DELETE FROM scraper.luxury_contacts")
cur.execute("DELETE FROM scraper.linkedin_profiles WHERE source = 'luxury_hotel_search'")
cur.execute("UPDATE scraper.luxury_targets SET linkedin_searched = FALSE, facebook_searched = FALSE, updated_at = NOW()")
conn.commit()

cur.execute("SELECT COUNT(*) FROM scraper.luxury_contacts")
print(f"Contacts: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE source = 'luxury_hotel_search'")
print(f"LinkedIn luxury: {cur.fetchone()[0]}")
cur.execute("SELECT id, name, linkedin_searched FROM scraper.luxury_targets ORDER BY id")
for r in cur.fetchall():
    print(f"  [{r[0]:2d}] {r[1]:45s} li={r[2]}")
conn.close()
