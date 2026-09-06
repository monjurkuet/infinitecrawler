#!/usr/bin/env python3
"""Sync normalized website domains from source tables into scraper.websites.

Keeps the unified table fresh so `db_email_extract --unified` always has
the latest cross-source domains:
  - scraper.gmaps_listings.website  (source='gmaps', source_id=<listing id>)
  - scraper.bbb_listings.website    (source='bbb',   source_id=<business_id>)

Social-profile links are stored with is_crawlable=false (reference only);
structurally invalid values are skipped entirely.

Usage:
    websites_sync.py --minutes 40    # incremental (systemd timer default)
    websites_sync.py --full          # one-shot backfill of everything
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import psycopg  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

from utils.urls import extract_domain, is_social_url, normalize_website  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("websites_sync")

BATCH = 2000

UPSERT_SQL = """
    INSERT INTO scraper.websites (domain, base_url, source, source_id, source_url, is_crawlable)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (domain) DO NOTHING
"""


def pg_conn():
    return psycopg.connect(
        host=os.environ.get("PG_HOST", "/var/run/postgresql"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PGPASSWORD", "changeme"),
        dbname=os.environ.get("PG_DB", "infinitecrawler"),
    )


def sync_table(conn, table: str, id_col: str, url_col: str,
               source: str, since_sql: str, ref_col: str | None = None) -> dict:
    """Upsert one source table's websites. Returns counters."""
    ref_col = ref_col or id_col
    stats = {"seen": 0, "added": 0, "social": 0, "skipped": 0}
    offset_id = 0
    with conn.cursor() as cur:
        while True:
            cur.execute(
                f"""SELECT {id_col}, {ref_col}, {url_col} FROM scraper.{table}
                    WHERE {url_col} IS NOT NULL AND {url_col} != ''
                      AND {since_sql} AND {id_col} > %s
                    ORDER BY {id_col} LIMIT %s""",
                (offset_id, BATCH),
            )
            rows = cur.fetchall()
            if not rows:
                break
            batch = []
            for rid, ref, raw in rows:
                offset_id = rid
                stats["seen"] += 1
                if is_social_url(raw):
                    domain = extract_domain(raw)
                    if domain:
                        batch.append((domain, raw.strip(), source, str(ref), raw.strip(), False))
                        stats["social"] += 1
                    else:
                        stats["skipped"] += 1
                    continue
                clean = normalize_website(raw)
                if not clean:
                    stats["skipped"] += 1
                    continue
                domain = extract_domain(clean)
                if not domain:
                    stats["skipped"] += 1
                    continue
                batch.append((domain, clean, source, str(ref), raw.strip()[:500], True))
            if batch:
                cur.executemany(UPSERT_SQL, batch)
                stats["added"] += cur.rowcount or 0
            conn.commit()
            try:
                offset_id = int(rows[-1][0])
            except (ValueError, TypeError):
                break
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=40,
                    help="incremental window (ignored with --full)")
    ap.add_argument("--full", action="store_true", help="backfill everything")
    args = ap.parse_args()

    since_g = "TRUE" if args.full else "updated_at > NOW() - INTERVAL '%d minutes'" % args.minutes
    since_b = "TRUE" if args.full else "updated_at > NOW() - INTERVAL '%d minutes'" % args.minutes

    conn = pg_conn()
    t0 = time.time()
    g = sync_table(conn, "gmaps_listings", "id", "website", "gmaps", since_g)
    b = sync_table(conn, "bbb_listings", "id", "website", "bbb", since_b, ref_col="business_id")
    dt = time.time() - t0
    log.info("sync done in %.1fs gmaps=%s bbb=%s", dt, g, b)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
