#!/usr/bin/env python3
"""schema_migration.py — Create new enrichment tables.

Creates scraper.emails and scraper.linkedin_profiles tables if they
don't already exist. Safe to re-run (idempotent via IF NOT EXISTS).

Usage:
    uv run python scripts/schema_migration.py                     # create tables
    uv run python scripts/schema_migration.py --dry-run            # print SQL only
    uv run python scripts/schema_migration.py --verify             # check tables exist
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_pg_config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("schema_migration")

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_EMAILS_TABLE = """
CREATE TABLE IF NOT EXISTS scraper.emails (
    id              BIGSERIAL PRIMARY KEY,
    listing_id      BIGINT REFERENCES scraper.gmaps_listings(id) ON DELETE CASCADE,
    website_url     TEXT NOT NULL,
    email           TEXT NOT NULL,
    email_type      TEXT DEFAULT 'general',
    extraction_method TEXT DEFAULT 'browser',
    is_obfuscated   BOOLEAN DEFAULT FALSE,
    context_snippet TEXT,
    discovered_at   TIMESTAMPTZ DEFAULT NOW(),
    last_verified   TIMESTAMPTZ,
    is_active       BOOLEAN DEFAULT TRUE,
    UNIQUE(listing_id, email)
);
"""

CREATE_LINKEDIN_TABLE = """
CREATE TABLE IF NOT EXISTS scraper.linkedin_profiles (
    id              BIGSERIAL PRIMARY KEY,
    listing_id      BIGINT REFERENCES scraper.gmaps_listings(id) ON DELETE CASCADE,
    full_name       TEXT,
    profile_url     TEXT NOT NULL UNIQUE,
    profile_title   TEXT,
    company_name    TEXT,
    search_query    TEXT,
    confidence      REAL DEFAULT 0.5,
    snippet         TEXT,
    checked_at      TIMESTAMPTZ DEFAULT NOW(),
    last_updated    TIMESTAMPTZ,
    notes           TEXT
);
"""

CREATE_EMAILS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_emails_listing ON scraper.emails(listing_id);
CREATE INDEX IF NOT EXISTS idx_emails_email ON scraper.emails(email);
"""

CREATE_LINKEDIN_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_linkedin_listing ON scraper.linkedin_profiles(listing_id);
CREATE INDEX IF NOT EXISTS idx_linkedin_company ON scraper.linkedin_profiles(company_name);
"""

ALL_STATEMENTS = [
    CREATE_EMAILS_TABLE,
    CREATE_LINKEDIN_TABLE,
    CREATE_EMAILS_INDEXES,
    CREATE_LINKEDIN_INDEXES,
]


def run_migration(dry_run: bool = False) -> bool:
    """Execute all DDL statements. Returns True on success."""
    pg_config = get_pg_config()

    if dry_run:
        log.info("=== DRY RUN — SQL to be executed ===")
        for stmt in ALL_STATEMENTS:
            print(stmt.strip())
            print("---")
        return True

    conn = psycopg.connect(**pg_config)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in ALL_STATEMENTS:
                log.info("Executing: %s ...", stmt.split("\n")[1].strip()[:60])
                cur.execute(stmt)  # type: ignore  # dynamic SQL string
        log.info("Migration complete — all tables and indexes created.")
        return True
    except Exception as e:
        log.error("Migration failed: %s", e)
        return False
    finally:
        conn.close()


def verify_tables() -> bool:
    """Check that required tables exist and have expected columns."""
    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    try:
        with conn.cursor() as cur:
            expected = {
                "emails": ["id", "listing_id", "email", "extraction_method"],
                "linkedin_profiles": ["id", "listing_id", "profile_url", "confidence"],
            }
            all_ok = True
            for table, columns in expected.items():
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'scraper' AND table_name = %s",
                    (table,),
                )
                existing = {row[0] for row in cur.fetchall()}
                missing = [c for c in columns if c not in existing]
                if missing:
                    log.warning("Table scraper.%(table)s missing columns: %(missing)s", {
                        "table": table, "missing": missing,
                    })
                    all_ok = False
                else:
                    log.info("Table scraper.%(table)s — OK (%(count)d columns)", {
                        "table": table, "count": len(existing),
                    })
            return all_ok
    except Exception as e:
        log.error("Verification failed: %s", e)
        return False
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Create enrichment tables")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    parser.add_argument("--verify", action="store_true", help="Check tables exist")
    args = parser.parse_args()

    if args.verify:
        success = verify_tables()
    else:
        success = run_migration(dry_run=args.dry_run)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
