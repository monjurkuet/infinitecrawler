#!/usr/bin/env python3
"""Blocklist GMaps CIDs that have proven non-renderable repeatedly.

Reads `gmaps:failed` (hash of url -> {"retries": N, ...}), finds URLs that
have exceeded the retry threshold (default: >=5 recorded failures), and adds
their `?cid=` form to the `gmaps:phantom:blocked` Redis set.

Once blocklisted:
  - listing_daemon._mark_phantom_url() refuses to requeue them (drops + logs)
  - scripts/phantom_sweeper.py skips them in the phantom requeue loop
  - They are PURGED from gmaps:pending / gmaps:processing / gmaps:phantom /
    gmaps:failed by this same script so they stop burning retry slots.

Usage:
    uv run python scripts/blocklist_dead_urls.py            # dry run
    uv run python scripts/blocklist_dead_urls.py --apply    # apply
    uv run python scripts/blocklist_dead_urls.py --apply --threshold 3
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import redis

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from daemons.listing_daemon import to_cid_url  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Actually write (default: dry-run)")
    ap.add_argument("--threshold", type=int, default=5, help="Min historic failures to blocklist")
    args = ap.parse_args()

    r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

    failed = r.hgetall("gmaps:failed") or {}
    if not failed:
        print("gmaps:failed is empty — nothing to blocklist")
        return 0

    to_block: list[str] = []
    for url, info_json in failed.items():
        try:
            info = json.loads(info_json)
            retries = int(info.get("retries", 0))
        except Exception:
            retries = 0
        if retries >= args.threshold:
            to_block.append(url)

    print(f"gmaps:failed entries: {len(failed)}")
    print(f"candidates with retries>={args.threshold}: {len(to_block)}")
    for u in to_block:
        print(f"  {u[:110]}")

    if not args.apply:
        print("\nDRY RUN — re-run with --apply to write")
        return 0

    # Add both the raw and cid-normalized form to the blocked set
    pipe = r.pipeline()
    for u in to_block:
        pipe.sadd("gmaps:phantom:blocked", u)
        pipe.sadd("gmaps:phantom:blocked", to_cid_url(u))
    pipe.execute()

    # Purge from every work queue so they stop circulating
    for u in to_block:
        r.lrem("gmaps:pending", 0, u)
        r.lrem("gmaps:pending", 0, to_cid_url(u))
        r.lrem("gmaps:processing", 0, u)
        r.lrem("gmaps:processing", 0, to_cid_url(u))
        r.lrem("gmaps:phantom", 0, u)
        r.lrem("gmaps:phantom", 0, to_cid_url(u))
        r.hdel("gmaps:failed", u)
        r.hdel("gmaps:failed", to_cid_url(u))

    blocked_total = r.scard("gmaps:phantom:blocked")
    print(f"\nApplied. gmaps:phantom:blocked now holds {blocked_total} entries.")
    print(f"gmaps:failed now holds {r.hlen('gmaps:failed')} entries.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
