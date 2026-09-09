#!/usr/bin/env python3
"""scripts/seed_sectors.py — Create and seed scraper.sectors table from YAML.

Run once after adding the 12 new sectors to sectors.yaml.
"""

import logging
import sys
from pathlib import Path

import psycopg
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_pg_config  # noqa: E402

log = logging.getLogger("seed_sectors")

BPT_DIR = REPO_ROOT.parent / "business-plan-template"
SECTORS_YAML = BPT_DIR / "_system" / "config" / "sectors.yaml"
SOFTWARE_SECTORS_YAML = BPT_DIR / "_system" / "config" / "software_sectors.yaml"


def load_all_sectors() -> dict:
    """Load both BPT sectors.yaml and software_sectors.yaml, return merged dict."""
    sectors: dict = {}

    # 1) BD-business sectors
    if SECTORS_YAML.exists():
        bd_data = yaml.safe_load(SECTORS_YAML.read_text())
        for sid, sc in (bd_data.get("sectors") or {}).items():
            if sc.get("status") == "active":
                sectors[sid] = {**sc, "_source": "bd_business"}
    else:
        log.warning("sectors.yaml not found at %s", SECTORS_YAML)

    # 2) Software product sectors
    if SOFTWARE_SECTORS_YAML.exists():
        sw_data = yaml.safe_load(SOFTWARE_SECTORS_YAML.read_text())
        for sid, sc in (sw_data.get("sectors") or {}).items():
            if sc.get("status") == "active":
                tbt = sc.get("target_business_types") or {}
                en_targets = tbt.get("en") or []
                bn_targets = tbt.get("bn") or []
                sectors[f"software:{sid}"] = {
                    "display_name": sc.get("display_name") or sid,
                    "product_name": sc.get("product_name") or sid,
                    "keywords": {"en": en_targets, "bn": bn_targets},
                    "subsegments": sc.get("subsegments") or [],
                    "priority_weight": sc.get("priority_weight", 0.5),
                    "status": "active",
                    "_source": "software_sectors",
                }
    else:
        log.warning("software_sectors.yaml not found at %s", SOFTWARE_SECTORS_YAML)

    log.info("Loaded %d active sectors (%d software)",
             len(sectors),
             sum(1 for s in sectors.values() if s.get("_source") == "software_sectors"))
    return sectors


CREATE_SECTORS_TABLE = """
CREATE TABLE IF NOT EXISTS scraper.sectors (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,        -- e.g. 'manufacturing-rmg'
    display_name    TEXT NOT NULL,               -- e.g. 'Manufacturing & RMG'
    source_type     TEXT NOT NULL,               -- 'bd_business' or 'software'
    product_name    TEXT,                        -- for software sectors
    keywords_en     TEXT[],                      -- English keywords array
    keywords_bn     TEXT[],                      -- Bengali keywords array
    subsegments     TEXT[],                      -- subsegment array
    priority_weight REAL DEFAULT 0.5,
    status          TEXT DEFAULT 'active',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sectors_name ON scraper.sectors(name);
CREATE INDEX IF NOT EXISTS idx_sectors_source ON scraper.sectors(source_type);
"""

UPSERT_SECTOR_SQL = """
    INSERT INTO scraper.sectors
        (name, display_name, source_type, product_name, keywords_en, keywords_bn,
         subsegments, priority_weight, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (name) DO UPDATE SET
        display_name    = EXCLUDED.display_name,
        source_type     = EXCLUDED.source_type,
        product_name    = EXCLUDED.product_name,
        keywords_en     = EXCLUDED.keywords_en,
        keywords_bn     = EXCLUDED.keywords_bn,
        subsegments     = EXCLUDED.subsegments,
        priority_weight = EXCLUDED.priority_weight,
        status          = EXCLUDED.status,
        updated_at      = NOW()
"""


def main(dry_run: bool = False):
    sectors = load_all_sectors()
    if not sectors:
        log.error("No sectors loaded")
        return 1

    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    conn.autocommit = True

    try:
        if not dry_run:
            with conn.cursor() as cur:
                cur.execute(CREATE_SECTORS_TABLE)
            log.info("Table created/verified")
        else:
            print(CREATE_SECTORS_TABLE)

        for name, sc in sectors.items():
            display = sc.get("display_name", name)
            src = sc.get("_source", "bd_business")
            product = sc.get("product_name")
            kw_en = sc.get("keywords", {}).get("en", [])
            kw_bn = sc.get("keywords", {}).get("bn", [])
            sub = sc.get("subsegments", [])
            pw = sc.get("priority_weight", 0.5)
            status = sc.get("status", "active")

            if dry_run:
                print(f"UPSERT: {name} | {display} | {src} | pw={pw}")
                continue

            with conn.cursor() as cur:
                cur.execute(UPSERT_SECTOR_SQL, (
                    name, display, src, product, kw_en, kw_bn, sub, pw, status
                ))
            log.info("Seeded: %s", name)

        if not dry_run:
            log.info("All %d sectors seeded", len(sectors))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    sys.exit(main(args.dry_run))