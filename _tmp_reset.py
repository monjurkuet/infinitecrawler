#!/usr/bin/env python3
"""Reset test data for a clean run."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.pg import get_pg_config
import psycopg

pg = get_pg_config()
conn = psycopg.connect(**pg)
cur = conn.cursor()

# Clean test data
cur.execute("DELETE FROM scraper.luxury_contacts WHERE profile_url LIKE '%test-debug%'")
cur.execute("DELETE FROM scraper.linkedin_profiles WHERE source = 'luxury_hotel_search'")
# Reset targets that got marked
cur.execute("UPDATE scraper.luxury_targets SET linkedin_searched = FALSE, facebook_searched = FALSE, updated_at = NOW() WHERE id IN (1,2,3)")
conn.commit()

cur.execute("SELECT id, name, linkedin_searched FROM scraper.luxury_targets ORDER BY id")
for r in cur.fetchall():
    print(f"  [{r[0]:2d}] {r[1]:45s} li_searched={r[2]}")

cur.execute("SELECT COUNT(*) FROM scraper.luxury_contacts")
print(f"\nContacts remaining: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE source = 'luxury_hotel_search'")
print(f"LinkedIn luxury: {cur.fetchone()[0]}")

conn.close()
