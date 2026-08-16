#!/usr/bin/env python3
"""Scheduled luxury lead generation — run periodically via cron/systemd timer.

Multi-stage pipeline:
  1. Seed new hotels from gmaps_listings → luxury_targets
  2. DDGS LinkedIn/Facebook discovery → luxury_contacts
  3. Export to CSV

Usage:
    uv run python scripts/schedule_luxury.py             # run all stages
    uv run python scripts/schedule_luxury.py --stage 1   # seed only
    uv run python scripts/schedule_luxury.py --stage 2   # collect only (limit 50)
    uv run python scripts/schedule_luxury.py --stage 3   # match only
    uv run python scripts/schedule_luxury.py --stage 4   # export only
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - schedule - %(levelname)s - %(message)s")
log = logging.getLogger("schedule_luxury")

SCRIPTS_DIR = REPO_ROOT / "scripts"


def run_script(name: str, *args: str) -> bool:
    cmd = ["uv", "run", "python", str(SCRIPTS_DIR / name), *args]
    log.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            log.info("%s — OK", name)
            return True
        else:
            log.warning("%s — FAILED (rc=%d): %s", name, result.returncode, result.stderr[-300:])
            return False
    except subprocess.TimeoutExpired:
        log.warning("%s — TIMEOUT", name)
        return False
    except Exception as e:
        log.error("%s — ERROR: %s", name, e)
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stage", type=int, choices=[1, 2, 3], help="Run single stage")
    p.add_argument("--max", type=int, default=50, help="Max targets for stage 2 (default 50)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    log.info("=== Luxury Lead Pipeline %s ===",
             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

    if args.dry_run:
        log.info("DRY RUN — no writes")
        return

    stages = [args.stage] if args.stage else [1, 2, 3]

    for stage in stages:
        if stage == 1:
            log.info("Stage 1: Seed new hotels from GMaps listings")
            run_script("seed_all_hotels.py")

        elif stage == 2:
            log.info("Stage 2: Collect luxury contacts (hotel-anchored, %d targets)", args.max)
            run_script("collect_luxury_contacts.py")  # auto-skips searched targets

            log.info("Stage 2b: Broader BD profile discovery")
            run_script("ddgs_profile_discovery.py")

        elif stage == 3:
            log.info("Stage 3: Export leads")
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            out_dir = REPO_ROOT / "output" / "leads" / date_str
            out_dir.mkdir(parents=True, exist_ok=True)
            run_script("generate_leads.py", "--min-score", "0.1")

    log.info("Pipeline complete")


if __name__ == "__main__":
    main()