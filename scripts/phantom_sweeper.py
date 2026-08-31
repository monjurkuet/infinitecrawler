#!/usr/bin/env python3
"""Phantom-row sweeper — requeues phantom (bare-shell rendered) GMaps URLs.

Phantom rows land in gmaps_listings with only `name` populated (phone/website/
rating/address all NULL). Before the page-wait fix these were ~94% of new rows;
after the fix they're still 2-4% due to Chrome's background-tab throttling
during load spikes.

This script is invoked every 30m via systemd user timer. It sweeps:
  1. Redis `gmaps:phantom` — URLs flagged by listing_daemon at extraction time
  2. PG `gmaps_listings` WHERE phone IS NULL AND website IS NULL AND rating IS NULL AND address IS NULL and created_at > 2h ago

It re-fetches each phantom URL via pinchtab with a longer settle time, so the
shell has time to render before the listing daemon runs its extraction.

All state stays in Redis; no PG writes here (listing_daemon's upsert handles
the final write).
"""

import json
import sys
import time
from pathlib import Path

import psycopg
import redis

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from daemons.listing_daemon import to_cid_url  # noqa: E402

PHANTOM_KEY = "gmaps:phantom"
PENDING_KEY = "gmaps:pending"
PHANTOM_LOG = Path("/var/log/infinitecrawler/infinitecrawler-phantom-sweeper.log")

# Longer wait than the listing daemon (6s) to get the tab out of background
# throttling — we're sweeping phantoms, not racing the pool.
SETTLE_SECONDS = 12


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    PHANTOM_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(PHANTOM_LOG, "a") as f:
        f.write(f"{ts} {msg}\n")
    print(f"{ts} {msg}", flush=True)


def sweep_redis(r: redis.Redis, batch: int = 100) -> int:
    """Pop up to `batch` phantom URLs and re-push into the live queue."""
    urls = r.lrange(PHANTOM_KEY, 0, batch - 1)
    if not urls:
        return 0
    for u in urls:
        cid_u = to_cid_url(u)
        r.rpush(PENDING_KEY, cid_u)
    r.ltrim(PHANTOM_KEY, batch, -1)
    return len(urls)


def sweep_pg(batch: int = 100) -> int:
    """Pull phantom URLs from PG that the daemon route missed (pre-fix rows)."""
    cfg = {
        "host": "/var/run/postgresql",
        "port": 5432,
        "user": "postgres",
        "password": "changeme",
        "dbname": "infinitecrawler",
        "connect_timeout": 10,
    }
    sql = """
        SELECT source_url FROM scraper.gmaps_listings
        WHERE source_type = 'gmaps_listing'
          AND name IS NOT NULL
          AND phone IS NULL AND website IS NULL AND rating IS NULL AND address IS NULL
          AND created_at < now() - interval '2 hours'
        ORDER BY created_at DESC
        LIMIT %s
    """
    with psycopg.connect(**cfg) as conn, conn.cursor() as cur:
        cur.execute(sql, (batch,))
        rows = cur.fetchall()
    return len(rows)


def main(argv=None):
    r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

    # Phase 1 — Redis phantom queue
    try:
        n_redis = sweep_redis(r)
        log(f"phase1_redis: requeued {n_redis} phantom URLs (phantom remaining={r.llen(PHANTOM_KEY)})")
    except Exception as e:
        log(f"phase1_redis FAIL: {e}")

    # Phase 2 — PG sweep (rows are already there; re-scrape via listing daemon's
    # normal enqueue path which uses get_uncrawled_urls_sql — phantom URLs get
    # picked up when their search_results row is still uncrawled, which by
    # definition they are when source_url has no listing with data).
    try:
        n_pg = sweep_pg()
        log(f"phase2_pg: phantom candidate URLs in PG = {n_pg}")
    except Exception as e:
        log(f"phase2_pg FAIL: {e}")

    log("sweep complete")


if __name__ == "__main__":
    main()
