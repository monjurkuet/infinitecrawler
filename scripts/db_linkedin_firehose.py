#!/usr/bin/env python3
"""db_linkedin_firehose.py — Global LinkedIn decision-maker discovery via ddgs.

Unbundled from GMaps pipeline. Generates a Cartesian query matrix from
config/linkedin_firehose.yaml, runs concurrent ddgs metasearches, parses
LinkedIn profiles from result snippets, and upserts into
scraper.linkedin_profiles with source='firehose' and listing_id=NULL.

Search engine: ddgs (https://github.com/deedy5/ddgs) — sync metasearch
aggregator (bing/google/ddg backends). Runs in a thread pool, each worker
thread owning its own DDGS() instance.

Usage:
    uv run python scripts/db_linkedin_firehose.py                    # full run
    uv run python scripts/db_linkedin_firehose.py --max-queries 500  # limit
    uv run python scripts/db_linkedin_firehose.py --dry-run          # preview only
    uv run python scripts/db_linkedin_firehose.py --stats            # stats only
    uv run python scripts/db_linkedin_firehose.py --concurrency 10
"""

import argparse
import logging
import random
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import psycopg
import yaml
from ddgs import DDGS

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from utils.linkedin_parser import parse_linkedin as _parse_linkedin  # noqa: E402
from utils.pg import get_pg_config  # noqa: E402

log = logging.getLogger("firehose")
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
# firehose logger → INFO; root stays at WARNING to suppress ddgs noise
log.handlers.clear()
_h = logging.StreamHandler()
_h.setLevel(logging.INFO)
_h.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_h)
log.setLevel(logging.INFO)
log.propagate = False

DEFAULT_CONFIG = REPO_ROOT / "config" / "linkedin_firehose.yaml"
SOURCE_TAG = "firehose"
HEARTBEAT_SEC = 30  # log progress at most every N seconds

# One DDGS() instance per worker thread (curl_cffi client is not thread-safe
# to share across threads, but thread-local reuse avoids per-call bootstrap)
_thread_local = threading.local()


# ---------------------------------------------------------------------------
# YAML config
# ---------------------------------------------------------------------------


def load_config(path: Path = DEFAULT_CONFIG) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Query matrix
# ---------------------------------------------------------------------------


def generate_queries(cfg: dict, max_queries: int | None = None) -> list[dict]:
    """Generate flat list of {query, params: {region}, family} dicts."""
    families = cfg["query_families"]
    roles = cfg["roles"]
    locations = cfg["locations"]
    industries = cfg["industries"]
    regions = cfg["regions"]
    queries: list[dict] = []

    if families.get("role_city", {}).get("enabled", True):
        tmpl = families["role_city"]["template"]
        rk = families["role_city"]["region"]
        for role in roles:
            for city in locations["bangladesh"]:
                queries.append({
                    "query": tmpl.format(role=role, city=city),
                    "params": {"region": regions[rk]},
                    "family": "role_city",
                })

    if families.get("role_city_industry", {}).get("enabled", True):
        tmpl = families["role_city_industry"]["template"]
        rk = families["role_city_industry"]["region"]
        for role in roles:
            for city in locations["global"]:
                for industry in industries:
                    queries.append({
                        "query": tmpl.format(role=role, city=city, industry=industry),
                        "params": {"region": regions[rk]},
                        "family": "role_city_industry",
                    })

    if families.get("role_only", {}).get("enabled", True):
        tmpl = families["role_only"]["template"]
        rk = families["role_only"]["region"]
        for role in roles:
            queries.append({
                "query": tmpl.format(role=role),
                "params": {"region": regions[rk]},
                "family": "role_only",
            })

    if max_queries and len(queries) > max_queries:
        queries = random.sample(queries, max_queries)

    return queries


# ---------------------------------------------------------------------------
# LinkedIn parsing — reuse utils/linkedin_parser.py
# ---------------------------------------------------------------------------


def url_norm(href: str) -> str:
    u = href.split("?")[0].rstrip("/")
    u = u.replace("https://bd.linkedin.com/", "https://www.linkedin.com/")
    return u


def parse_name(title: str) -> Optional[str]:
    for sep in [" - | ", " - ", " | "]:
        if sep in title:
            c = title.split(sep, 1)[0].strip()
            if len(c) > 2 and not c.startswith("http"):
                return c
    return None


def parse_title(title: str, body: str) -> Optional[str]:
    if " - " in title:
        m = title.split(" - ", 1)[1].strip()
        m = re.sub(r"^\|\s*", "", m).strip()
        m = re.sub(r"\s*\|.*$", "", m).strip()
        if m and len(m) > 2:
            return m
    first = body.split(".")[0].strip() if body else ""
    return first[:120] if first and len(first) > 5 else None


def parse_company(title: str, body: str) -> Optional[str]:
    m = re.search(r"\bat\s+([A-Z][A-Za-z0-9&.\s-]{2,60})(?:\||$)", title)
    if m:
        return m.group(1).strip().rstrip(" |")
    m = re.search(r"([A-Z][A-Za-z0-9&.,\s-]{2,60})\s*[·•.]\s*(Full|Self|Part)", body)
    return m.group(1).strip() if m else None


def parse_profile(result: dict, search_query: str, family: str) -> Optional[dict]:
    parsed = _parse_linkedin(result, parse_name, parse_title, parse_company, url_norm)
    if parsed is None:
        return None
    parsed["search_query"] = search_query
    parsed["family"] = family
    parsed["listing_id"] = None
    return parsed


# ---------------------------------------------------------------------------
# DB writer
# ---------------------------------------------------------------------------

UPSERT_SQL = """
    INSERT INTO scraper.linkedin_profiles
        (listing_id, full_name, profile_url, profile_title, company_name,
         search_query, confidence, snippet, source)
    VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (profile_url) DO UPDATE SET
        profile_title = COALESCE(EXCLUDED.profile_title, scraper.linkedin_profiles.profile_title),
        confidence    = GREATEST(scraper.linkedin_profiles.confidence, EXCLUDED.confidence),
        last_updated  = NOW()
    WHERE scraper.linkedin_profiles.source IN ('ddgs_discovery', 'firehose')
"""


def save_batch(conn, profiles: list[dict]) -> int:
    """Upsert a batch of profiles. Retries commit on transient DB error.

    Per-row failures are logged and skipped WITHOUT rolling back the whole
    batch (which would lose previously queued rows). Returns rows written.
    """
    if not profiles:
        return 0
    written = 0
    with conn.cursor() as cur:
        for p in profiles:
            try:
                cur.execute(UPSERT_SQL, (
                    p.get("full_name"),
                    p["profile_url"],
                    p.get("profile_title"),
                    p.get("company_name"),
                    p.get("search_query"),
                    p.get("confidence", 0.3),
                    p.get("snippet"),
                    SOURCE_TAG,
                ))
                written += cur.rowcount or 1
            except Exception as exc:
                # Skip failed row; do NOT rollback — previous rows in this
                # txn are still valid and will commit below.
                log.warning("save_batch: row skipped: %s (%s)", p.get("profile_url"), exc)
    for attempt in range(3):
        try:
            conn.commit()
            return written
        except Exception:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            log.warning("save_batch: commit failed after 3 attempts, batch lost (%d rows)", written)
            return 0


# ---------------------------------------------------------------------------
# Worker: one query via ddgs (runs on a worker thread)
# ---------------------------------------------------------------------------


def _get_ddgs(timeout: int) -> DDGS:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = DDGS(timeout=timeout)
        _thread_local.client = client
    return client


def search_one(item: dict, exec_cfg: dict, delay_seconds: float = 0.0) -> tuple[list[dict], bool]:
    """Search one query via ddgs. Returns (profiles, error).

    Tries primary backend then fallbacks. `error=True` only if every
    backend raised; `error=False` with empty list means genuinely empty
    search results. `delay_seconds` paces each worker thread between
    queries to stay under rate limits.
    """
    client = _get_ddgs(exec_cfg.get("search_timeout", 20))
    region = item["params"].get("region") or exec_cfg.get("global_region", "wt-wt")
    max_results = exec_cfg.get("max_results_per_query", 10)
    backends = ([exec_cfg.get("backend", "auto")]
                + list(exec_cfg.get("backend_fallbacks", [])))

    last_exc: Optional[Exception] = None
    try:
        for backend in backends:
            try:
                results = client.text(
                    item["query"],
                    region=region,
                    safesearch="off",
                    max_results=max_results,
                    backend=backend,
                )
                profiles = [
                    p for r in results
                    if (p := parse_profile(r, item["query"], item["family"])) is not None
                ]
                if profiles:
                    return profiles, False
            except Exception as exc:
                last_exc = exc
                continue

        if last_exc is not None:
            return [], True
        return [], False
    finally:
        if delay_seconds:
            time.sleep(delay_seconds)


# ---------------------------------------------------------------------------
# Concurrent search runner
# ---------------------------------------------------------------------------


def run_firehose(
    cfg: dict,
    queries: list[dict],
    concurrency: int,
    delay_seconds: float,
    batch_commit: int,
) -> dict:
    """Run all queries via thread pool, upsert profiles in batches.

    Returns stats dict.
    """
    conn = psycopg.connect(**get_pg_config())
    conn.autocommit = False

    exec_cfg = cfg["execution"]
    lock = threading.Lock()

    queries_done = 0
    errors = 0
    empty_results = 0
    profiles_new = 0
    profiles_dup = 0
    db_written = 0
    seen_urls: set[str] = set()
    family_new: Counter[str] = Counter()
    family_err: Counter[str] = Counter()
    family_empty: Counter[str] = Counter()
    batch_buffer: list[dict] = []
    last_heartbeat: float = time.monotonic()
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(search_one, item, exec_cfg, delay_seconds): item
                   for item in queries}

        for fut in as_completed(futures):
            item = futures[fut]
            fam = item["family"]
            try:
                profiles, err = fut.result()
            except Exception:
                profiles, err = [], True

            with lock:
                queries_done += 1
                if err:
                    errors += 1
                    family_err[fam] += 1
                elif not profiles:
                    empty_results += 1
                    family_empty[fam] += 1
                for p in profiles:
                    normed = url_norm(p["profile_url"])
                    if normed in seen_urls:
                        profiles_dup += 1
                        continue
                    seen_urls.add(normed)
                    batch_buffer.append(p)
                    profiles_new += 1
                    if len(batch_buffer) >= batch_commit:
                        n = save_batch(conn, batch_buffer)
                        db_written += n
                        batch_buffer.clear()
                if profiles:
                    family_new[fam] += len(profiles)

                # heartbeat (every HEARTBEAT_SEC)
                now = time.monotonic()
                if now - last_heartbeat >= HEARTBEAT_SEC and queries_done > 0:
                    elapsed = now - t0
                    rate = queries_done / max(elapsed, 0.001)
                    pct = queries_done / max(len(queries), 1) * 100
                    log.info(
                        "status  %d/%d (%.0f%%)  %.1fq/s  +%d profiles  "
                        "err:%d  empty:%d  db:%d  mem:%d",
                        queries_done, len(queries), pct, rate,
                        profiles_new, errors, empty_results, db_written,
                        len(seen_urls),
                    )
                    last_heartbeat = now

    # Flush remaining
    if batch_buffer:
        n = save_batch(conn, batch_buffer)
        db_written += n

    conn.close()

    log.info("family-summary %s", dict(family_new.most_common()))

    return {
        "total_queries": len(queries),
        "profiles_new": profiles_new,
        "profiles_dup": profiles_dup,
        "db_written": db_written,
        "errors": errors,
        "empty_results": empty_results,
        "time_seconds": time.monotonic() - t0,
        "families": {
            fam: {
                "profiles": family_new.get(fam, 0),
                "errors": family_err.get(fam, 0),
                "empty": family_empty.get(fam, 0),
            }
            for fam in sorted(set(family_new) | set(family_empty) | set(family_err))
        },
    }


# ---------------------------------------------------------------------------
# Stats display
# ---------------------------------------------------------------------------


def show_stats():
    conn = psycopg.connect(**get_pg_config())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE source = %s", (SOURCE_TAG,))
            total = cur.fetchone()[0]
            cur.execute("""SELECT source, COUNT(*) FROM scraper.linkedin_profiles WHERE source IS NOT NULL GROUP BY source ORDER BY 2 DESC""")
            by_source = cur.fetchall()
            cur.execute("""SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE source = %s AND checked_at > NOW() - INTERVAL '7 days'""", (SOURCE_TAG,))
            recent = cur.fetchone()[0]

        print(f"\n{'='*55}\n  LinkedIn Firehose Stats\n{'='*55}")
        print(f"  Firehose total:          {total:>6}")
        print(f"  Firehose fresh (7d):     {recent:>6}")
        print("\n  By source:")
        for src, cnt in by_source:
            print(f"    {src:<25} {cnt:>6}")
        print(f"{'='*55}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(description="Global LinkedIn firehose via ddgs")
    p.add_argument("--max-queries", type=int, default=None)
    p.add_argument("--delay-seconds", type=float, default=None)
    p.add_argument("--concurrency", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stats", action="store_true")
    p.add_argument("--loop", action="store_true",
                   help="Run continuously: after each batch, resample queries and run again forever")
    p.add_argument("--loop-gap", type=float, default=60.0,
                   help="Pause seconds between loop cycles (default 60)")
    args = p.parse_args()

    if args.stats:
        show_stats()
        return

    cfg = load_config()
    max_q = args.max_queries or cfg["execution"].get("max_queries")
    delay = args.delay_seconds if args.delay_seconds is not None else cfg["execution"].get("delay_seconds", 0.5)
    conc = args.concurrency or cfg["execution"].get("concurrency", 8)
    batch = cfg["execution"].get("batch_commit", 20)

    queries = generate_queries(cfg, max_q)
    family_counts = Counter(q["family"] for q in queries)

    log.info("start   %d queries  delay=%.1fs  workers=%d  engine=ddgs  families=%s",
             len(queries), delay, conc, dict(family_counts.most_common()))

    if args.dry_run:
        log.info("DRY RUN — no writes")
        for q in queries[:10]:
            tag = f" [region={q['params'].get('region','')}]" if q['params'].get('region') else ""
            log.info("  DRY  %s%s", q["query"][:90], tag)
        if len(queries) > 10:
            log.info("  DRY  ... and %d more", len(queries) - 10)
        return

    stats = run_firehose(cfg, queries, conc, delay, batch)

    elapsed = stats["time_seconds"]
    rate = stats["total_queries"] / max(elapsed, 0.001)
    log.info(
        "done    %dqs  +%d profiles  dup=%d  db=%d  err=%d  empty=%d  %.0fs  %.1fq/s",
        stats["total_queries"], stats["profiles_new"],
        stats["profiles_dup"], stats["db_written"],
        stats["errors"], stats["empty_results"], elapsed, rate,
    )

    # ── Continuous loop mode ────────────────────────────────────────────
    # Runs batches back-to-back forever: resample fresh queries each cycle
    # so results never exhaust. Used by the perpetual systemd service.
    cycle = 1
    while args.loop:
        gap = args.loop_gap
        log.info("loop    cycle %d complete — pausing %.0fs before next batch",
                 cycle, gap)
        time.sleep(gap)
        cycle += 1
        queries = generate_queries(cfg, max_q)
        log.info("start   cycle %d: %d queries  delay=%.1fs  workers=%d",
                 cycle, len(queries), delay, conc)

        stats = run_firehose(cfg, queries, conc, delay, batch)
        elapsed = stats["time_seconds"]
        rate = stats["total_queries"] / max(elapsed, 0.001)
        log.info(
            "done    %dqs  +%d profiles  dup=%d  db=%d  err=%d  empty=%d  %.0fs  %.1fq/s",
            stats["total_queries"], stats["profiles_new"],
            stats["profiles_dup"], stats["db_written"],
            stats["errors"], stats["empty_results"], elapsed, rate,
        )


if __name__ == "__main__":
    main()