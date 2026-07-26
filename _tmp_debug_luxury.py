#!/usr/bin/env python3
"""Debug: check luxury_contacts schema and test insert."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.pg import get_pg_config
import psycopg

pg = get_pg_config()
conn = psycopg.connect(**pg)
cur = conn.cursor()

# Check luxury_contacts schema
cur.execute("""SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema='scraper' AND table_name='luxury_contacts' 
ORDER BY ordinal_position""")
print("luxury_contacts columns:")
for r in cur.fetchall():
    print(f"  {r[0]:20s} {r[1]:20s} nullable={r[2]}")

# Check constraints
cur.execute("""SELECT conname, pg_get_constraintdef(oid) 
FROM pg_constraint 
WHERE conrelid = 'scraper.luxury_contacts'::regclass""")
print("\nConstraints:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

# Test insert
try:
    cur.execute("""INSERT INTO scraper.luxury_contacts 
(target_id, full_name, platform, profile_url, profile_title, company_name, search_query, confidence, snippet, is_employee, is_guest)
VALUES (1, 'Test User', 'linkedin', 'https://linkedin.com/in/test-debug-12345', 'CEO at Test', 'Test Corp', 'test query', 0.5, 'test snippet', false, true)
RETURNING id""")
    print(f"\nTest insert OK, id={cur.fetchone()[0]}")
    conn.rollback()
except Exception as e:
    print(f"\nTest insert error: {e}")
    conn.rollback()

# Test the actual UPSERT_LUXURY_CONTACT_SQL
try:
    cur.execute("""
        INSERT INTO scraper.luxury_contacts
            (target_id, full_name, platform, profile_url, profile_title,
             company_name, search_query, confidence, snippet, is_employee, is_guest)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (profile_url) DO UPDATE SET
            confidence    = GREATEST(scraper.luxury_contacts.confidence, EXCLUDED.confidence),
            profile_title = COALESCE(EXCLUDED.profile_title, scraper.luxury_contacts.profile_title),
            company_name  = COALESCE(EXCLUDED.company_name, scraper.luxury_contacts.company_name),
            last_checked  = NOW()
    """, (1, 'Test User 2', 'linkedin', 'https://linkedin.com/in/test-debug-67890', 'CTO', 'Test Inc', 'query', 0.5, 'snippet', False, True))
    conn.commit()
    print("UPSERT test OK")
except Exception as e:
    print(f"UPSERT test error: {e}")
    conn.rollback()

# Check test data
cur.execute("SELECT * FROM scraper.luxury_contacts WHERE profile_url LIKE '%test-debug%'")
for r in cur.fetchall():
    print(f"  Found test row: id={r[0]} name={r[2]}")

conn.close()
