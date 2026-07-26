#!/usr/bin/env python3
"""schema_luxury.py — Create luxury contacts pipeline schema.

Creates/enhances tables for collecting high-income people profiles from
luxury hotels/venues via DDGS search (LinkedIn, Facebook) and nearby
GMaps business leads.

Idempotent — safe to re-run.

Usage:
    uv run python scripts/schema_luxury.py
    uv run python scripts/schema_luxury.py --dry-run   # print SQL only
"""

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import psycopg
from utils.pg import get_pg_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("schema_luxury")

# ── Fix: add missing columns to existing linkedin_profiles table ─────

FIX_LINKEDIN_PROFILES = """
    DO $$
    BEGIN
        -- Rename 'url' to 'profile_url' if old schema exists
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='url'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='profile_url'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles RENAME COLUMN url TO profile_url;
        END IF;

        -- Rename 'profile_name' to 'full_name' if old schema
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='profile_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='full_name'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles RENAME COLUMN profile_name TO full_name;
        END IF;

        -- Add 'profile_title' if missing
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='profile_title'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles ADD COLUMN profile_title TEXT;
        END IF;

        -- Add 'company_name' if missing
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='company_name'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles ADD COLUMN company_name TEXT;
        END IF;

        -- Add 'search_query' if missing
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='search_query'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles ADD COLUMN search_query TEXT;
        END IF;

        -- Add 'snippet' if missing
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='snippet'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles ADD COLUMN snippet TEXT;
        END IF;

        -- Add 'last_updated' if missing
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='last_updated'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles ADD COLUMN last_updated TIMESTAMPTZ;
        END IF;

        -- Add 'notes' if missing
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='notes'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles ADD COLUMN notes TEXT;
        END IF;

        -- Drop the old UNIQUE on url if it exists, add unique on profile_url
        IF EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_schema='scraper' AND constraint_name='linkedin_profiles_url_key'
        ) THEN
            ALTER TABLE scraper.linkedin_profiles DROP CONSTRAINT linkedin_profiles_url_key;
        END IF;
    END $$;
"""

# ── luxury_targets: the venues we're targeting ───────────────────────

LUXURY_TARGETS_TABLE = """
    CREATE TABLE IF NOT EXISTS scraper.luxury_targets (
        id              BIGSERIAL PRIMARY KEY,
        name            TEXT NOT NULL,
        alternative_names TEXT[],
        address         TEXT,
        city            TEXT DEFAULT 'Dhaka',
        target_type     TEXT DEFAULT 'hotel',
        tier            TEXT DEFAULT 'luxury',
        latitude        DOUBLE PRECISION,
        longitude       DOUBLE PRECISION,
        gmaps_place_id  TEXT,
        gmaps_visited   BOOLEAN DEFAULT FALSE,
        linkedin_searched BOOLEAN DEFAULT FALSE,
        facebook_searched BOOLEAN DEFAULT FALSE,
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW(),
        notes           TEXT
    );
"""

# ── luxury_contacts: people found via DDGS ──────────────────────────

LUXURY_CONTACTS_TABLE = """
    CREATE TABLE IF NOT EXISTS scraper.luxury_contacts (
        id              BIGSERIAL PRIMARY KEY,
        target_id       BIGINT REFERENCES scraper.luxury_targets(id) ON DELETE CASCADE,
        full_name       TEXT,
        platform        TEXT NOT NULL CHECK (platform IN ('linkedin', 'facebook', 'twitter', 'other')),
        profile_url     TEXT NOT NULL UNIQUE,
        profile_title   TEXT,
        company_name    TEXT,
        location        TEXT,
        search_query    TEXT,
        confidence      REAL DEFAULT 0.3,
        snippet         TEXT,
        email           TEXT,
        phone           TEXT,
        is_employee     BOOLEAN DEFAULT FALSE,
        is_guest        BOOLEAN DEFAULT TRUE,
        discovered_at   TIMESTAMPTZ DEFAULT NOW(),
        last_checked    TIMESTAMPTZ DEFAULT NOW(),
        notes           TEXT
    );
"""

LUXURY_CONTACTS_INDEXES = """
    CREATE INDEX IF NOT EXISTS idx_luxury_contacts_target ON scraper.luxury_contacts(target_id);
    CREATE INDEX IF NOT EXISTS idx_luxury_contacts_platform ON scraper.luxury_contacts(platform);
    CREATE INDEX IF NOT EXISTS idx_luxury_contacts_name ON scraper.luxury_contacts(full_name);
    CREATE INDEX IF NOT EXISTS idx_luxury_contacts_company ON scraper.luxury_contacts(company_name);
"""

# ── Seed luxury targets (hotels) ────────────────────────────────────

LUXURY_HOTELS_SEED = """
    INSERT INTO scraper.luxury_targets (name, alternative_names, city, target_type, tier)
    VALUES
        ('Radisson Blu Dhaka Water Garden', ARRAY['Radisson Blu', 'Radisson Water Garden'], 'Dhaka', 'hotel', 'luxury'),
        ('InterContinental Dhaka', ARRAY['Hotel Intercontinental', 'Intercontinental Dhaka'], 'Dhaka', 'hotel', 'luxury'),
        ('The Westin Dhaka', ARRAY['Westin Dhaka', 'Westin Hotel'], 'Dhaka', 'hotel', 'luxury'),
        ('Pan Pacific Sonargaon Dhaka', ARRAY['Pan Pacific', 'Sonargaon Hotel'], 'Dhaka', 'hotel', 'luxury'),
        ('Sheraton Dhaka', ARRAY['Sheraton Hotel'], 'Dhaka', 'hotel', 'luxury'),
        ('Renaissance Dhaka Gulshan Hotel', ARRAY['Renaissance Dhaka', 'Marriott Renaissance'], 'Dhaka', 'hotel', 'luxury'),
        ('Six Seasons Hotel Dhaka', ARRAY['Six Seasons'], 'Dhaka', 'hotel', 'luxury'),
        ('Amari Dhaka', ARRAY['Amari Hotel'], 'Dhaka', 'hotel', 'luxury'),
        ('Hotel Sarina Dhaka', ARRAY['Sarina Dhaka', 'Sarina Hotel'], 'Dhaka', 'hotel', 'luxury'),
        ('Crowne Plaza Dhaka Gulshan', ARRAY['Crowne Plaza'], 'Dhaka', 'hotel', 'luxury'),
        ('Holiday Inn Dhaka City Centre', ARRAY['Holiday Inn Dhaka'], 'Dhaka', 'hotel', 'premium'),
        ('Long Beach Hotel Dhaka', ARRAY['Long Beach'], 'Dhaka', 'hotel', 'premium'),
        ($$Ocean Paradise Hotel & Resort Cox's Bazar$$, ARRAY['Ocean Paradise'], 'Cox''s Bazar', 'resort', 'luxury'),
        ($$Royal Tulip Sea Pearl Beach Resort & Spa$$, ARRAY['Royal Tulip', 'Sea Pearl'], 'Cox''s Bazar', 'resort', 'luxury'),
        ('Radisson Blu Chattogram Bay View', ARRAY['Radisson Chattogram'], 'Chattogram', 'hotel', 'luxury'),
        ('Hotel Agrabad Chattogram', ARRAY['Agrabad Hotel'], 'Chattogram', 'hotel', 'premium')
    ON CONFLICT DO NOTHING;
"""

ALL_STATEMENTS = [
    ("Fix linkedin_profiles schema", FIX_LINKEDIN_PROFILES),
    ("Create luxury_targets table", LUXURY_TARGETS_TABLE),
    ("Create luxury_contacts table", LUXURY_CONTACTS_TABLE),
    ("Create luxury_contacts indexes", LUXURY_CONTACTS_INDEXES),
    ("Seed luxury hotel targets", LUXURY_HOTELS_SEED),
]


def run_migration(dry_run: bool = False) -> bool:
    pg_config = get_pg_config()
    if dry_run:
        log.info("=== DRY RUN — SQL to be executed ===")
        for label, sql in ALL_STATEMENTS:
            print(f"\n--- {label} ---")
            print(sql.strip())
        return True

    conn = psycopg.connect(**pg_config)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for label, sql in ALL_STATEMENTS:
                try:
                    cur.execute(sql)
                    log.info("%s — OK", label)
                except Exception as e:
                    log.warning("%s — %s", label, e)
        log.info("Migration complete — all tables created/enhanced and targets seeded.")
        return True
    except Exception as e:
        log.error("Migration failed: %s", e)
        return False
    finally:
        conn.close()


def verify() -> bool:
    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    try:
        cur = conn.cursor()
        checks = [
            ("scraper.luxury_targets", ["id", "name", "city", "target_type", "tier"]),
            ("scraper.luxury_contacts", ["id", "target_id", "full_name", "platform", "profile_url"]),
            ("scraper.linkedin_profiles", ["id", "listing_id", "profile_url", "full_name", "confidence"]),
        ]
        all_ok = True
        for table, cols in checks:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (table.split(".")[0], table.split(".")[1]),
            )
            existing = {r[0] for r in cur.fetchall()}
            missing = [c for c in cols if c not in existing]
            if missing:
                log.warning("%s missing columns: %s", table, missing)
                all_ok = False
            else:
                log.info("%s — OK (%d cols)", table, len(existing))

        cur.execute("SELECT COUNT(*) FROM scraper.luxury_targets")
        log.info("luxury_targets seeded: %d rows", cur.fetchone()[0])
        return all_ok
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Create luxury contacts pipeline schema")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    parser.add_argument("--verify", action="store_true", help="Check schema")
    args = parser.parse_args()

    if args.verify:
        success = verify()
    else:
        success = run_migration(dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
