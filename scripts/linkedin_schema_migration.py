#!/usr/bin/env python3
"""scripts/linkedin_schema_migration.py — Create LinkedIn jobs tables.

Additive-only DDL. Safe to re-run (IF NOT EXISTS).

Usage:
    uv run python scripts/linkedin_schema_migration.py              # apply
    uv run python scripts/linkedin_schema_migration.py --dry-run    # print SQL only
    uv run python scripts/linkedin_schema_migration.py --verify     # check tables exist
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
log = logging.getLogger("linkedin_schema_migration")

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_LINKEDIN_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS scraper.linkedin_jobs (
    id                      BIGSERIAL PRIMARY KEY,
    job_id                  BIGINT      NOT NULL UNIQUE,
    source_url              TEXT        NOT NULL,
    title                   TEXT        NOT NULL,
    company_name            TEXT,
    company_linkedin_slug   TEXT,
    company_linkedin_url    TEXT,
    location                TEXT,
    location_city           TEXT,
    listed_at               TIMESTAMPTZ,
    seniority_level         TEXT,
    employment_type         TEXT,
    job_function            TEXT,
    industries              TEXT,
    description_html        TEXT,
    description_text        TEXT,
    applicants_count        INTEGER,
    promoted                BOOLEAN,
    easy_apply              BOOLEAN,
    apply_url               TEXT,
    source_sector           TEXT,
    source_keyword          TEXT,
    sector_id               INTEGER REFERENCES scraper.sectors(id),
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_LINKEDIN_JOBS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_company_slug
    ON scraper.linkedin_jobs(company_linkedin_slug);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_sector
    ON scraper.linkedin_jobs(sector_id);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_listed
    ON scraper.linkedin_jobs(listed_at DESC);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_company
    ON scraper.linkedin_jobs(company_name);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_source_keyword
    ON scraper.linkedin_jobs(source_keyword);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_source_sector
    ON scraper.linkedin_jobs(source_sector);
"""

ALTER_LINKEDIN_COMPANIES = """
ALTER TABLE scraper.linkedin_companies
    ADD COLUMN IF NOT EXISTS linkedin_slug TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS jobs_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_job_seen_at TIMESTAMPTZ;
"""

CREATE_LINKEDIN_QUERY_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS scraper.linkedin_query_state (
    keyword         TEXT NOT NULL,
    location        TEXT NOT NULL,
    last_start      INTEGER NOT NULL DEFAULT 0,
    exhausted_at    TIMESTAMPTZ,
    last_run_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (keyword, location)
);
"""

# ── Migration helpers ────────────────────────────────────────────────────────

ALL_STATEMENTS = [
    ("linkedin_jobs table", CREATE_LINKEDIN_JOBS_TABLE),
    ("linkedin_jobs indexes", CREATE_LINKEDIN_JOBS_INDEXES),
    ("linkedin_companies ALTER", ALTER_LINKEDIN_COMPANIES),
    ("linkedin_query_state table", CREATE_LINKEDIN_QUERY_STATE_TABLE),
]


def apply_migrations(dry_run: bool = False) -> None:
    """Execute all DDL statements."""
    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    conn.autocommit = True

    try:
        for name, sql in ALL_STATEMENTS:
            log.info("Applying: %s", name)
            if dry_run:
                print(f"-- {name}\n{sql.strip()}\n")
                continue
            with conn.cursor() as cur:
                cur.execute(sql)  # type: ignore  # dynamic SQL string
            log.info("  OK")
    finally:
        conn.close()


def verify_migrations() -> None:
    """Check that tables/columns exist."""
    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)

    try:
        with conn.cursor() as cur:
            # Check linkedin_jobs
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'scraper' AND table_name = 'linkedin_jobs'
                ORDER BY ordinal_position
            """)
            cols = [r[0] for r in cur.fetchall()]
            expected = [
                "id", "job_id", "source_url", "title", "company_name",
                "company_linkedin_slug", "company_linkedin_url", "location",
                "location_city", "listed_at", "seniority_level", "employment_type",
                "job_function", "industries", "description_html", "description_text",
                "applicants_count", "promoted", "easy_apply", "apply_url",
                "source_sector", "source_keyword", "sector_id", "created_at", "updated_at"
            ]
            missing = [c for c in expected if c not in cols]
            if missing:
                log.error("linkedin_jobs missing columns: %s", missing)
            else:
                log.info("linkedin_jobs: all %d columns present", len(expected))

            # Check indexes
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'scraper' AND tablename = 'linkedin_jobs'
            """)
            idxs = [r[0] for r in cur.fetchall()]
            expected_idx = [
                "idx_linkedin_jobs_company_slug", "idx_linkedin_jobs_sector",
                "idx_linkedin_jobs_listed", "idx_linkedin_jobs_company",
                "idx_linkedin_jobs_source_keyword", "idx_linkedin_jobs_source_sector"
            ]
            for e in expected_idx:
                if e in idxs:
                    log.info("  index OK: %s", e)
                else:
                    log.warning("  index MISSING: %s", e)

            # Check linkedin_companies new columns
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'scraper' AND table_name = 'linkedin_companies'
            """)
            comp_cols = [r[0] for r in cur.fetchall()]
            for c in ("linkedin_slug", "jobs_count", "last_job_seen_at"):
                if c in comp_cols:
                    log.info("linkedin_companies: column %s present", c)
                else:
                    log.warning("linkedin_companies: column %s MISSING", c)

            # Check linkedin_query_state
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'scraper' AND table_name = 'linkedin_query_state'
            """)
            qcols = [r[0] for r in cur.fetchall()]
            expected_q = ["keyword", "location", "last_start", "exhausted_at", "last_run_at"]
            for e in expected_q:
                if e in qcols:
                    log.info("linkedin_query_state: column %s present", e)
                else:
                    log.warning("linkedin_query_state: column %s MISSING", e)

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="LinkedIn jobs schema migration")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    parser.add_argument("--verify", action="store_true", help="Verify tables exist")
    args = parser.parse_args()

    if args.verify:
        verify_migrations()
        return

    apply_migrations(dry_run=args.dry_run)

    if not args.dry_run:
        log.info("Migration complete. Run with --verify to confirm.")


if __name__ == "__main__":
    main()