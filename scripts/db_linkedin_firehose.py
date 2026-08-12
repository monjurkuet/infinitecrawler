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
import concurrent.futures as cf
import logging
import os
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
from utils.pg import PG_LOCK_TIMEOUT, get_pg_config  # noqa: E402
from utils.rate_gate import RateGate  # noqa: E402
from utils.transliterate import bn_to_en, contains_bengali  # noqa: E402

DDGS_BACKOFF_S = int(os.environ.get("DDGS_BACKOFF_S", "300"))
DDGS_500_THRESHOLD = int(os.environ.get("DDGS_500_THRESHOLD", "3"))
_ddgs_500_streak = 0
_ddgs_gate = RateGate()

log = logging.getLogger("firehose")
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# firehose logger → INFO; root stays at WARNING to suppress ddgs noise
log.handlers.clear()
_h = logging.StreamHandler()
_h.setLevel(logging.INFO)
_h.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
log.addHandler(_h)
log.setLevel(logging.INFO)
log.propagate = False

DEFAULT_CONFIG = REPO_ROOT / "config" / "linkedin_firehose.yaml"
SOURCE_TAG = "firehose"
HEARTBEAT_SEC = 30  # log progress at most every N seconds
DDGS_PER_CALL_TIMEOUT_S = int(os.environ.get("DDGS_PER_CALL_TIMEOUT_S", "30"))

# One DDGS() instance per worker thread (curl_cffi client is not thread-safe
# to share across threads, but thread-local reuse avoids per-call bootstrap)
_thread_local = threading.local()


# ---------------------------------------------------------------------------
# YAML config
# ---------------------------------------------------------------------------


def load_config(path: Path = DEFAULT_CONFIG) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# Query-matrix construction moved to scripts.firehose_queries (B3, 2026-08-12).
from scripts.firehose_queries import generate_queries  # noqa: E402


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

    Uses SAVEPOINT per row so a single row failure (lock_timeout,
    statement_timeout) only rolls back that row, not the entire batch.
    Without SAVEPOINT, a single INSERT failure aborts the whole txn
    and every subsequent INSERT fails with "current transaction is
    aborted".
    Returns rows written (to DB, not attempted).
    """
    if not profiles:
        return 0
    written = 0
    with conn.cursor() as cur:
        for p in profiles:
            try:
                cur.execute("SAVEPOINT sv_row")
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
                cur.execute("RELEASE SAVEPOINT sv_row")
            except Exception as exc:
                log.warning("save_batch: row skipped: %s (%s)", p.get("profile_url"), exc)
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT sv_row")
                except Exception:
                    pass
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


def _ddgs_call_with_timeout(client, q, region, max_results, backend, timeout):
    """Bound ddgs.text() with a per-call wall-clock timeout.

    ddgs's constructor timeout doesn't reliably bound slow upstreams.
    This wraps the call in a 1-worker ThreadPoolExecutor so a stalled
    upstream can't block a worker thread indefinitely.
    """
    with cf.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(
            client.text, q, region=region, safesearch="off",
            max_results=max_results, backend=backend,
        ).result(timeout=timeout)


def _ddgs_should_cooldown() -> bool:
    """True if the global DDGS cooldown window is active."""
    return _ddgs_gate.in_cooldown()


def _ddgs_record_500(count: int) -> None:
    """Increment the 500 streak; trip cooldown at threshold."""
    global _ddgs_500_streak
    _ddgs_500_streak = count
    _ddgs_gate.record_streak(count, threshold=DDGS_500_THRESHOLD, backoff_s=DDGS_BACKOFF_S)


def search_one(item: dict, exec_cfg: dict, delay_seconds: float = 0.0) -> tuple[list[dict], bool]:
    """Search one query via ddgs. Returns (profiles, error).

    Tries primary backend then fallbacks. `error=True` only if every
    backend raised; `error=False` with empty list means genuinely empty
    search results. `delay_seconds` paces each worker thread between
    queries to stay under rate limits.

    Bengali-script queries are pre-transliterated to Latin before dispatch
    because the upstream DDGS HTTP gateway returns 500 on ~15% of BN queries.
    Circuit breaker trips a global cooldown after DDGS_500_THRESHOLD 500s in a
    row.
    """
    client = _get_ddgs(exec_cfg.get("search_timeout", 20))
    region = item["params"].get("region") or exec_cfg.get("global_region", "wt-wt")
    max_results = exec_cfg.get("max_results_per_query", 10)
    backends = ([exec_cfg.get("backend", "auto")]
                + list(exec_cfg.get("backend_fallbacks", [])))

    if _ddgs_should_cooldown():
        return [], True

    q = item["query"]
    bn = contains_bengali(q)
    q_dispatched = bn_to_en(q) if bn else q
    if bn and q_dispatched != q:
        log.debug("DDGS transliterate BN→EN: %r → %r", q[:60], q_dispatched[:60])

    last_exc: Optional[Exception] = None
    streak_500 = 0
    try:
        for backend in backends:
            try:
                results = _ddgs_call_with_timeout(
                    client, q_dispatched, region, max_results, backend,
                    DDGS_PER_CALL_TIMEOUT_S,
                )
                profiles = [
                    p for r in results
                    if (p := parse_profile(r, q_dispatched, item["family"])) is not None
                ]
                if profiles:
                    return profiles, False
            except cf.TimeoutError:
                log.debug("DDGS per-call timeout (>%ds) for: %s", DDGS_PER_CALL_TIMEOUT_S, q_dispatched[:60])
                last_exc = TimeoutError("per-call timeout")
                continue
            except Exception as exc:
                last_exc = exc
                msg = str(exc).lower()
                if "500" in msg or "internal server error" in msg:
                    streak_500 += 1
                    _ddgs_record_500(streak_500)
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


def _heartbeat_loop(stop_event, t0, total_q, lock, state):
    """Wall-clock heartbeat — emits even when futures are blocked on slow DDGS reads.

    Runs as a daemon thread; takes a snapshot of counters under ``lock`` then
    formats the log line outside (to avoid holding the lock during logging).
    """
    while not stop_event.is_set():
        time.sleep(HEARTBEAT_SEC)
        with lock:
            snap = dict(state)
        elapsed = time.monotonic() - t0
        rate = snap["queries_done"] / max(elapsed, 0.001)
        pct = snap["queries_done"] / max(total_q, 1) * 100
        pending = snap["queries_done"] - snap["db_written"] - snap["errors"] - snap["empty_results"]
        log.info(
            "status  %d/%d (%.0f%%)  %.1fq/s  +%d profiles  "
            "err:%d  empty:%d  db:%d  mem:%d  pending=%d",
            snap["queries_done"], total_q, pct, rate,
            snap["profiles_new"], snap["errors"], snap["empty_results"],
            snap["db_written"], snap["seen"], pending,
        )


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
    _pg_cfg = dict(get_pg_config())
    # Firehose UPSERTs contend with linkedin-search daemon on profile_url
    # rows — use a longer lock_timeout so batches survive brief contention.
    _pg_cfg["options"] = _pg_cfg.get("options", "").replace(
        f"lock_timeout={PG_LOCK_TIMEOUT}", "lock_timeout=60s"
    ) if "options" in _pg_cfg else ""
    conn = psycopg.connect(**_pg_cfg)
    conn.autocommit = False

    exec_cfg = cfg["execution"]
    lock = threading.Lock()

    # Mutable state dict — shared between the main loop and heartbeat thread.
    state = {
        "queries_done": 0,
        "errors": 0,
        "empty_results": 0,
        "profiles_new": 0,
        "profiles_dup": 0,
        "db_written": 0,
        "seen": 0,
    }
    seen_urls: set[str] = set()
    family_new: Counter[str] = Counter()
    family_err: Counter[str] = Counter()
    family_empty: Counter[str] = Counter()
    batch_buffer: list[dict] = []
    t0 = time.monotonic()

    # Heartbeat daemon thread — emits status every HEARTBEAT_SEC regardless
    # of whether as_completed is making progress.
    hb_stop = threading.Event()
    hb = threading.Thread(
        target=_heartbeat_loop,
        args=(hb_stop, t0, len(queries), lock, state),
        daemon=True,
    )
    hb.start()

    try:
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
                    state["queries_done"] += 1
                    if err:
                        state["errors"] += 1
                        family_err[fam] += 1
                    elif not profiles:
                        state["empty_results"] += 1
                        family_empty[fam] += 1
                    for p in profiles:
                        normed = url_norm(p["profile_url"])
                        if normed in seen_urls:
                            state["profiles_dup"] += 1
                            continue
                        seen_urls.add(normed)
                        batch_buffer.append(p)
                        state["profiles_new"] += 1
                        if len(batch_buffer) >= batch_commit:
                            n = save_batch(conn, batch_buffer)
                            state["db_written"] += n
                            batch_buffer.clear()
                    if profiles:
                        family_new[fam] += len(profiles)
                    state["seen"] = len(seen_urls)
    finally:
        hb_stop.set()
        hb.join(timeout=2.0)

    # Flush remaining
    if batch_buffer:
        n = save_batch(conn, batch_buffer)
        state["db_written"] += n

    conn.close()

    log.info("family-summary %s", dict(family_new.most_common()))

    return {
        "total_queries": len(queries),
        "profiles_new": state["profiles_new"],
        "profiles_dup": state["profiles_dup"],
        "db_written": state["db_written"],
        "errors": state["errors"],
        "empty_results": state["empty_results"],
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
    import sys as _sys
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

    log.info("started version=1 args=%s", " ".join(_sys.argv[1:]))
    log.info("start   %d queries  delay=%.1fs  workers=%d  engine=ddgs  families=%s",
             len(queries), delay, conc, dict(family_counts.most_common()))

    if args.dry_run:
        log.info("DRY RUN — no writes")
        for q in queries[:10]:
            tag = f" [region={q['params'].get('region','')}]" if q['params'].get('region') else ""
            log.info("  DRY  %s%s", q["query"][:90], tag)
        if len(queries) > 10:
            log.info("  DRY  ... and %d more", len(queries) - 10)
        log.info("stopped reason=dry_run")
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
    try:
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
    except KeyboardInterrupt:
        log.info("stopped reason=SIGINT")
        return
    log.info("stopped reason=exit")


if __name__ == "__main__":
    main()