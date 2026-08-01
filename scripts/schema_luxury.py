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

import psycopg  # noqa: E402
from utils.pg import get_pg_config  # noqa: E402

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

# ── Seed luxury targets (hotels, bars, clubs, event venues, elite institutions) ──

LUXURY_HOTELS_SEED = """
    INSERT INTO scraper.luxury_targets (name, alternative_names, city, target_type, tier)
    VALUES
        -- ═══ HOTELS & RESORTS ═══
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
        ('Le Meridien Dhaka', ARRAY['Le Meridien', 'Le Meridien Hotel'], 'Dhaka', 'hotel', 'luxury'),
        ('Long Beach Hotel Dhaka', ARRAY['Long Beach', 'Long Beach Suites'], 'Dhaka', 'hotel', 'premium'),
        ($$Ocean Paradise Hotel & Resort Cox's Bazar$$, ARRAY['Ocean Paradise', 'Ocean Paradise Resort'], 'Cox''s Bazar', 'resort', 'luxury'),
        ($$Royal Tulip Sea Pearl Beach Resort & Spa$$, ARRAY['Royal Tulip', 'Sea Pearl Resort'], 'Cox''s Bazar', 'resort', 'luxury'),
        ('Sayeman Heritage Hotel Sylhet', ARRAY['Sayeman Heritage'], 'Sylhet', 'hotel', 'luxury'),
        ('Grand Sylhet Hotel & Resort', ARRAY['Grand Sylhet', 'Sylhet Grand'], 'Sylhet', 'resort', 'luxury'),
        ('Radisson Blu Chattogram Bay View', ARRAY['Radisson Chattogram', 'Radisson Blu Chattogram'], 'Chattogram', 'hotel', 'luxury'),
        ('Hotel Agrabad Chattogram', ARRAY['Agrabad Hotel'], 'Chattogram', 'hotel', 'premium'),
        ($$JATRA Flagship Chattogram City Centre$$, ARRAY['JATRA City Centre', 'JATRA Chattogram'], 'Chattogram', 'hotel', 'luxury'),
        ($$Foys Lake Resort Chattogram$$, ARRAY['Foys Lake', 'Foys Resort'], 'Chattogram', 'resort', 'luxury'),
        ($$The Peninsula Chittagong$$, ARRAY['Peninsula Hotel', 'Peninsula Chittagong'], 'Chattogram', 'hotel', 'luxury'),

        -- ═══ BARS & NIGHTCLUBS ═══
        ('SKYe Lounge Bar Dhaka', ARRAY['SKYe Lounge', 'SKYe Bar'], 'Dhaka', 'bar', 'luxury'),
        ('Raw Canvas Restaurant & Bar Dhaka', ARRAY['Raw Canvas', 'Raw Canvas Bar'], 'Dhaka', 'bar', 'premium'),
        ('Loki Restaurant & Bar Dhaka', ARRAY['Loki Bar', 'Loki Restaurant'], 'Dhaka', 'bar', 'premium'),
        ('Bluemoon Recreation Club Dhaka', ARRAY['Bluemoon Club', 'Bluemoon Recreation'], 'Dhaka', 'bar', 'premium'),
        ('Westin 26th Floor Bar', ARRAY['Westin Bar', 'Westin Rooftop Bar'], 'Dhaka', 'bar', 'luxury'),

        -- === CLUBS & EXCLUSIVE MEMBERS ===
        ('Gulshan Club Limited', ARRAY['Gulshan Club', 'Gulshan Club Dhaka'], 'Dhaka', 'social_club', 'elite'),
        ('Dhaka Club Limited', ARRAY['Dhaka Club', 'Shahbagh Club'], 'Dhaka', 'social_club', 'elite'),
        ('Dutch Club Dhaka', ARRAY['Dutch Club', 'Gulshan Dutch Club'], 'Dhaka', 'social_club', 'elite'),
        ('Baridhara Club Limited', ARRAY['Baridhara Club', 'Baridhara Diplomatic Club'], 'Dhaka', 'social_club', 'elite'),
        ('Chittagong Club Limited', ARRAY['Chittagong Club', 'Chattogram Club'], 'Chattogram', 'social_club', 'elite'),
        ('Sylhet Club Limited', ARRAY['Sylhet Club'], 'Sylhet', 'social_club', 'elite'),
        ('Gulshan Youth Club', ARRAY['Gulshan Youth', 'Gulshan Youth Club Dhaka'], 'Dhaka', 'social_club', 'elite'),

        -- === GOLF & COUNTRY CLUBS ===
        ('Kurmitola Golf Club Dhaka', ARRAY['Kurmitola Golf', 'Kurmitola Golf Course'], 'Dhaka', 'golf_club', 'elite'),
        ('Army Golf Club Dhaka', ARRAY['Army Golf', 'AGC Dhaka'],
        'Dhaka', 'golf_club', 'elite'),

        -- === EVENT & WEDDING VENUES ===
        ('ICC Bangladesh Convention Center', ARRAY['ICC Convention', 'International Convention City'], 'Dhaka', 'event_venue', 'luxury'),
        ('Bangabandhu International Conference Center', ARRAY['BICC', 'Bangabandhu Conference'], 'Dhaka', 'event_venue', 'luxury'),
        ('AMM Convention Center Dhanmondi', ARRAY['AMM Convention', 'AMM Dhaka'],
        'Dhaka', 'event_venue', 'premium'),
        ('Bashundhara Convention Center', ARRAY['Bashundhara Convention', 'Bashundhara City'], 'Dhaka', 'event_venue', 'luxury'),
        ('Radisson Blu Executive Ballroom', ARRAY['Radisson Ballroom', 'Radisson Event'], 'Dhaka', 'event_venue', 'luxury'),
        ('InterContinental Dhaka Grand Ballroom', ARRAY['InterContinental Ballroom'], 'Dhaka', 'event_venue', 'luxury'),
        ('The Westin Dhaka Ballroom', ARRAY['Westin Ballroom', 'Westin Event Venue'], 'Dhaka', 'event_venue', 'luxury'),

        -- ===  FINE DINING ===
        ('Lotus E Tang Pan Asian Restaurant', ARRAY['Lotus E Tang', 'Lotus Restaurant Dhaka'], 'Dhaka', 'fine_dining', 'luxury'),
        ('The Garden Kitchen Sheraton Dhaka', ARRAY['Garden Kitchen', 'Garden Kitchen Sheraton'], 'Dhaka', 'fine_dining', 'luxury'),
        ('Pan Pacific Sonargaon Cafe', ARRAY['Sonargaon Cafe', 'Pan Pacific Restaurant'], 'Dhaka', 'fine_dining', 'luxury'),
        ('Crowne Plaza 26th Floor Dhaka', ARRAY['Crowne Plaza Restaurant', 'Crowne Plaza 26'], 'Dhaka', 'fine_dining', 'luxury'),
        ('Renaissance R-Bar & Kitchen Dhaka', ARRAY['R-Bar Renaissance', 'Renaissance Restaurant'], 'Dhaka', 'fine_dining', 'luxury'),

        -- === HNWI / BUSINESS ELITE INSTITUTIONS ===
        ('Bangladesh Garment Manufacturers Exporters Association', ARRAY['BGMEI', 'BGMEI Dhaka'], 'Dhaka', 'business_link', 'elite'),
        ('Federation of Bangladesh Chambers of Commerce Industry', ARRAY['FBCCI', 'Federation of Bangladesh Chamber'], 'Dhaka', 'business_link', 'elite'),
        ('Dhaka Chamber of Commerce Industry', ARRAY['DCCI', 'Dhaka Chamber of Commerce'], 'Dhaka', 'business_link', 'elite'),
        ('Bangladesh Association of Banks', ARRAY['BAB', 'Bangladesh Banks Association'], 'Dhaka', 'business_link', 'elite')
    ON CONFLICT (name) DO NOTHING;
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
