#!/usr/bin/env python3
"""db_classify.py — Classify unclassified leads directly into PostgreSQL.

Reads leads from scraper.gmaps_listings WHERE sector_id IS NULL,
classifies them via LLM + fallback, writes results back to PG.
Designed for cron: processes max_leads per run, tracks progress,
never re-classifies already-classified leads.

Usage:
    uv run python scripts/db_classify.py                     # classify up to 5000
    uv run python scripts/db_classify.py --max 2000           # classify up to 2000
    uv run python scripts/db_classify.py --dry-run            # preview
    uv run python scripts/db_classify.py --stats              # show classification stats
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("db_classify")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.llm_classifier import (
    load_sectors,
    load_training_examples,
    _single_fallback,
    build_sector_definitions,
    select_few_shot,
    classify_batch,
    build_lead_snapshot,
    save_training_examples,
    BATCH_SIZE,
    DEFAULT_SECTOR,
    METHOD_FALLBACK_RULE,
    METHOD_FALLBACK_LLM_ERROR,
    METHOD_LLM_CACHED,
    METHOD_LLM_PREFIX,
    MIN_TRAIN_CONFIDENCE,
)
from utils.pg import get_pg_config

PG_CONFIG = get_pg_config()

DEFAULT_MAX_LEADS = 5000


def get_unclassified(conn, limit: int, retry_failed: bool = False) -> list[dict]:
    """Fetch unclassified leads with phone+website, ordered by recency.

    If retry_failed=True, target leads with classification_method='fallback_llm_error'.
    """
    cols = ["id", "name", "category", "phone", "website", "address",
            "rating", "review_count", "latitude", "longitude", "place_id", "source_url"]
    with conn.cursor() as cur:
        if retry_failed:
            cur.execute(
                f"""
                SELECT {', '.join(cols)}
                FROM scraper.gmaps_listings
                WHERE classification_method = %s
                  AND phone IS NOT NULL
                  AND website IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                ("fallback_llm_error", limit),
            )
        else:
            cur.execute(
                f"""
                SELECT {', '.join(cols)}
                FROM scraper.gmaps_listings
                WHERE sector_id IS NULL
                  AND phone IS NOT NULL
                  AND website IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def update_classification(conn, lead_id: int, sector: str, confidence: float,
                          method: str, classified_at: str) -> None:
    """Write classification result to PG."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE scraper.gmaps_listings
            SET sector_id = %s,
                classification_confidence = %s,
                classification_method = %s,
                classified_at = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (sector, round(confidence, 2), method, classified_at, lead_id),
        )


def get_stats(conn) -> dict:
    """Return classification statistics."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE phone IS NOT NULL AND website IS NOT NULL) AS qualified,
                COUNT(*) FILTER (WHERE sector_id IS NOT NULL) AS classified,
                COUNT(*) FILTER (WHERE sector_id IS NULL AND phone IS NOT NULL AND website IS NOT NULL) AS remaining
            FROM scraper.gmaps_listings
            """
        )
        row = cur.fetchone()
        cur.execute(
            """
            SELECT sector_id, COUNT(*) AS cnt
            FROM scraper.gmaps_listings
            WHERE sector_id IS NOT NULL
            GROUP BY sector_id
            ORDER BY cnt DESC
            """
        )
        by_sector = dict(cur.fetchall())
    return {
        "total": row[0],
        "qualified": row[1],
        "classified": row[2],
        "remaining": row[3],
        "by_sector": by_sector,
    }


def classify_to_db(conn, leads: list[dict], sectors: dict,
                    existing_examples: list[dict],
                    model: str | None = None,
                    retry_failed: bool = False) -> tuple[int, int]:
    """Classify leads and write results to PG. Returns (classified, failed)."""
    sector_defs = build_sector_definitions(sectors)
    few_shot = select_few_shot(existing_examples, sectors)

    new_examples: list[dict] = []
    classified_count = 0
    failed_count = 0
    now = datetime.now(timezone.utc).isoformat()

    # For retry-failed mode: skip cached check, send all to LLM
    # For normal mode: check training examples for cached classifications
    llm_batch = []
    fallback_items = []

    if retry_failed:
        llm_batch = leads  # All retry-failed leads go to LLM
    else:
        classified_keys = set()
        for ex in existing_examples:
            key = (ex.get("name", ""), ex.get("website", ""))
            classified_keys.add(key)

        for lead in leads:
            key = (lead.get("name", ""), lead.get("website", ""))
            if key in classified_keys:
                stored = next(
                    (ex for ex in existing_examples
                     if ex.get("name") == lead.get("name")
                     and ex.get("website") == lead.get("website")),
                    None,
                )
                if stored:
                    update_classification(
                        conn, lead["id"], stored["sector"],
                        stored.get("confidence", 0.95),
                        METHOD_LLM_CACHED, now,
                    )
                    classified_count += 1
                else:
                    fallback_items.append(lead)
            else:
                llm_batch.append(lead)

    log.info(
        f"Cached: {classified_count}, "
        f"LLM batch: {len(llm_batch)}, "
        f"Fallback: {len(fallback_items)}"
    )

    # Process LLM-classifiable in batches
    for bstart in range(0, len(llm_batch), BATCH_SIZE):
        batch = llm_batch[bstart:bstart + BATCH_SIZE]
        log.info(
            f"LLM batch {bstart + 1}-{bstart + len(batch)} "
            f"({len(batch)} leads)..."
        )

        results = classify_batch(batch, bstart, sector_defs, few_shot, model=model)
        if results is None:
            log.warning("LLM failed, using fallback for this batch")
            for lead in batch:
                result = _single_fallback(lead, 0, sectors)
                update_classification(
                    conn, lead["id"], result["sector"],
                    result["confidence"], METHOD_FALLBACK_LLM_ERROR, now,
                )
                classified_count += 1
            failed_count += len(batch)
            conn.commit()  # persist after each batch
            continue

        for r in results:
            rel_idx = r.get("index", 0) - bstart
            if 0 <= rel_idx < len(batch):
                lead = batch[rel_idx]
                sector = r.get("sector", DEFAULT_SECTOR)
                conf = r.get("confidence", 0.5)
                # Sanitize untrusted LLM reasoning into a safe method slug:
                # keep [a-z0-9_] only, fall back to "direct" if empty.
                raw_reason = r.get("reasoning", "direct").lower()
                slug = "".join(c if c.isalnum() or c == "_" else "_" for c in raw_reason)[:20] or "direct"
                method = f"{METHOD_LLM_PREFIX}{slug}"

                update_classification(
                    conn, lead["id"], sector, conf, method, now,
                )
                classified_count += 1

                # Save high-confidence as training example
                if conf >= MIN_TRAIN_CONFIDENCE and sector:
                    new_examples.append(
                        build_lead_snapshot(
                            lead, sector, conf, r.get("reasoning", ""),
                        )
                    )

        conn.commit()  # persist after each LLM batch
        # Save training examples incrementally
        if new_examples:
            save_training_examples(new_examples)
            log.info(f"Saved {len(new_examples)} new training examples (mid-run)")
            new_examples = []  # reset for next batch

    # Fallback for remaining
    for lead in fallback_items:
        result = _single_fallback(lead, 0, sectors)
        update_classification(
            conn, lead["id"], result["sector"],
            result["confidence"], METHOD_FALLBACK_RULE, now,
        )
        classified_count += 1

    conn.commit()  # final commit for fallback batch
    return classified_count, failed_count


def main():
    parser = argparse.ArgumentParser(description="Classify leads directly into PostgreSQL")
    parser.add_argument("--max", type=int, default=DEFAULT_MAX_LEADS,
                        help=f"Max leads to classify per run (default: {DEFAULT_MAX_LEADS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without DB writes")
    parser.add_argument("--stats", action="store_true",
                        help="Show classification stats only")
    parser.add_argument("--model", type=str, default=None,
                        help="LLM model override")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry leads previously classified via fallback_llm_error")
    args = parser.parse_args()

    conn = psycopg.connect(**PG_CONFIG)
    conn.autocommit = False

    try:
        if args.stats:
            stats = get_stats(conn)
            print(f"Total listings:       {stats['total']:,}")
            print(f"Qualified (phone+web): {stats['qualified']:,}")
            print(f"Classified:           {stats['classified']:,}")
            print(f"Remaining:            {stats['remaining']:,}")
            print(f"\nBy sector:")
            for sid, cnt in sorted(stats['by_sector'].items(), key=lambda x: -x[1]):
                print(f"  {sid}: {cnt:,}")
            return

        if args.retry_failed:
            # Check for fallback_llm_error leads regardless of remaining
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM scraper.gmaps_listings "
                    "WHERE classification_method = 'fallback_llm_error'"
                )
                failed_count = cur.fetchone()[0]
            log.info(f"Found {failed_count} fallback_llm_error leads to retry")
            if failed_count == 0:
                log.info("No fallback_llm_error leads to retry.")
                return
            stats = get_stats(conn)
        else:
            stats = get_stats(conn)
            log.info(
                f"DB state: {stats['total']:,} listings, "
                f"{stats['qualified']:,} qualified, "
                f"{stats['classified']:,} classified, "
                f"{stats['remaining']:,} remaining"
            )

            if stats['remaining'] == 0:
                log.info("All qualified leads already classified. Nothing to do.")
                return

        # Load BPT sectors + training examples
        sectors = load_sectors()
        if not sectors:
            log.error("No sectors loaded. Check sectors.yaml.")
            sys.exit(1)

        existing = load_training_examples()

        # Fetch unclassified (or retry-failed) leads
        leads = get_unclassified(conn, args.max, retry_failed=args.retry_failed)
        log.info(f"Fetched {len(leads)} leads {'(retry-failed mode)' if args.retry_failed else ''}")

        if not leads:
            log.info("No leads to classify.")
            return

        if args.dry_run:
            log.info(f"[DRY-RUN] Would classify {len(leads)} leads")
            return

        classified, failed = classify_to_db(
            conn, leads, sectors, existing, model=args.model, retry_failed=args.retry_failed,
        )

        stats2 = get_stats(conn)
        log.info(
            f"Run complete: {classified} classified, {failed} LLM failures. "
            f"Total classified: {stats2['classified']:,}, "
            f"Remaining: {stats2['remaining']:,}"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()