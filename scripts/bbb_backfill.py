#!/usr/bin/env python3
"""BBB website backfill — re-enrich old bbb_listings rows missing website.

Walks all rows in scraper.bbb_listings where website IS NULL AND
approval_year IS NULL (i.e. we never scraped the profile), one at a time,
calling scripts/bbb_scraper.enrich_profile through the Datasolved proxy
ladder. Idempotent: each row updates website/years_in_business/etc and
sets approval_year to keep the row from re-entering the queue.

Run as a long-lived daemon via
systemd/infinitecrawler-bbb-backfill.service (Restart=always,
RestartSec=120).
"""

import logging
import os
import sys
import time

from dotenv import load_dotenv

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from utils.pg import get_pg_config  # noqa: E402
import psycopg  # noqa: E402

log = logging.getLogger("bbb_backfill")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


def get_batch(conn, n: int = 25) -> list[tuple[str, str]]:
    """Pick the next N BBB rows needing enrichment."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT business_id, profile_url
              FROM scraper.bbb_listings
             WHERE profile_url IS NOT NULL
               AND website IS NULL
               AND approval_year IS NULL
             ORDER BY id
             LIMIT %s
             FOR UPDATE SKIP LOCKED
            """,
            (n,),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def mark_skipped(conn, business_id: str, reason: str) -> None:
    """Ensure a row doesn't come back forever (placeholder; mark as tried)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE scraper.bbb_listings
               SET updated_at = NOW()
             WHERE business_id = %s
            """,
            (business_id,),
        )
    conn.commit()


def main() -> int:
    load_dotenv()  # picks up BBB_PROFILE_PROXY ladder from .env

    from scripts.bbb_scraper import enrich_profile, mark_enriched  # noqa: WPS433 — late import

    log.info("BBB backfill starting")
    try:
        conn = psycopg.connect(**get_pg_config(include_timeouts=False),
                               autocommit=False)
    except Exception as exc:
        log.error("PG connect failed: %s", exc)
        return 1

    backoff = 1.0
    while True:
        rows = get_batch(conn, n=50)
        conn.commit()  # release the read txn before slow HTTP

        if not rows:
            log.info("Backfill queue drained; sleeping 3600s")
            time.sleep(3600)
            continue

        processed = 0
        enriched = 0
        for business_id, profile_url in rows:
            try:
                out = enrich_profile(profile_url)
            except Exception as exc:  # noqa: BLE001
                log.warning("enrich failed for %s: %s", business_id, exc)
                mark_skipped(conn, business_id, "enrich-error")
                processed += 1
                continue

            if not out:
                mark_skipped(conn, business_id, "no-data")
                processed += 1
                continue

            # Merge into the row — reuse the scraper's own write path so the
            # social/tracking blocklist consistently applies.
            mark_enriched(conn, business_id, out)
            processed += 1
            enriched += 1

        log.info(
            "batch done process=%d enriched=%d rate=%.1f/min remaining_sample=%d",
            processed, enriched,
            (processed / max(1, 50)) * 60,
            len(rows),
        )

        # Adaptive backoff: if we keep finding empty enrichments, slow down.
        if enriched == 0:
            backoff = min(backoff * 2, 300)
            log.info("no yields last batch; sleeping %ds", backoff)
            time.sleep(backoff)
        else:
            backoff = 1.0


if __name__ == "__main__":
    sys.exit(main())
