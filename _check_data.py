#!/usr/bin/env python3
"""Quick check - LinkedIn profile data quality."""
import sys, psycopg
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.pg import get_pg_config

conn = psycopg.connect(**get_pg_config())
cur = conn.cursor()

print("=== Top LinkedIn profiles ===")
cur.execute("""
    SELECT full_name, profile_title, company_name, confidence
    FROM scraper.luxury_contacts
    WHERE platform = 'linkedin'
    ORDER BY confidence DESC
    LIMIT 20
""")
for r in cur.fetchall():
    print(f"  {str(r[0] or '?'):25s} | {str(r[1] or '?')[:45]:45s} | {str(r[2] or '-')[:25]:25s} | {r[3]:.2f}")

print("\n=== Sample raw titles ===")
cur.execute("""
    SELECT profile_title FROM scraper.luxury_contacts
    WHERE platform = 'linkedin' AND profile_title IS NOT NULL
    LIMIT 20
""")
for r in cur.fetchall():
    print(f"  {r[0][:90]}")

print("\n=== ALL company names ===")
cur.execute("""
    SELECT DISTINCT company_name FROM scraper.luxury_contacts
    WHERE platform = 'linkedin' AND company_name IS NOT NULL AND company_name != ''
    ORDER BY company_name
""")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

conn.close()
