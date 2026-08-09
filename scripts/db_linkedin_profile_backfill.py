#!/usr/bin/env python3
"""db_linkedin_profile_backfill.py — Re-parse stored LinkedIn profile snippets.

Every row in scraper.linkedin_profiles carries a `snippet` column populated
from the DDGS `site:linkedin.com/in/` search result when the profile was
first discovered. That snippet contains structured fields we never wrote down:

  · Location: <City, Region>
  · 500+ connections on LinkedIn
  · Experience: <company>, Education: <school>, etc.

This script re-reads those snippets and writes the structured fields into the
new columns added in 2026-08-09:
  profile_location, profile_country, connections_count, headline.

Run it once after the schema migration to backfill existing profiles, then
keep it on a 60-day re-parse loop (the snippet text never changes for the
same profile_url, but a fresh parse avoids stale regex logic on rows we
inserted before the column types stabilised).

Usage:
    uv run python scripts/db_linkedin_profile_backfill.py --max 5000
    uv run python scripts/db_linkedin_profile_backfill.py --loop --loop-gap 86400
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.linkedin_enrich import parse_profile_snippet
from utils.pg import (
    get_pg_config,
    get_profiles_to_backfill,
    update_profile_enrichment,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("db_linkedin_profile_backfill")

DEFAULT_MAX = 1000


def main():
    parser = argparse.ArgumentParser(description="Backfill LinkedIn profile enrichment fields")
    parser.add_argument("--max", type=int, default=DEFAULT_MAX)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--loop-gap", type=float, default=86400.0,
                        help="Pause seconds between cycles when --loop is set (default 24h)")
    args = parser.parse_args()

    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    conn.autocommit = False
    try:
        cycle = 0
        while True:
            cycle += 1
            profiles = get_profiles_to_backfill(conn, limit=args.max)
            if not profiles:
                log.info("[cycle %d] No profiles needing backfill.", cycle)
                if not args.loop:
                    return
                time.sleep(args.loop_gap)
                continue
            log.info("[cycle %d] Backfilling %d profiles", cycle, len(profiles))
            updated = 0
            any_field = 0
            for p in profiles:
                parsed = parse_profile_snippet(p["snippet"], p.get("profile_title"))
                if not any(parsed.values()):
                    continue
                any_field += 1
                # update_profile_enrichment is a no-op when every field is
                # null, but we still bump `enriched_at`. The intent is that a
                # profile we already inspected doesn't get re-picked-up
                # unless the snippet itself changed.
                n = update_profile_enrichment(
                    conn,
                    p["profile_url"],
                    profile_location=parsed["profile_location"],
                    profile_country=parsed["profile_country"],
                    connections_count=parsed["connections_count"],
                    headline=parsed["headline"],
                )
                updated += n
            log.info(
                "[cycle %d] Done: %d profiles with parseable fields, %d rows updated",
                cycle, any_field, updated,
            )
            if not args.loop:
                return
            time.sleep(args.loop_gap)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
