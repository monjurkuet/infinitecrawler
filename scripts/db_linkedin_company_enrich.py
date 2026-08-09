#!/usr/bin/env python3
"""db_linkedin_company_enrich.py — Free LinkedIn company enrichment.

For each company name in our pipeline (cited by either a gmaps listing or a
discovered profile), resolve its `linkedin.com/company/<slug>/` URL via DDGS,
fetch the public page, parse the schema.org Organization block + about-us
list, and persist to scraper.linkedin_companies.

This is the technique ChocoData's `linkedin-company-scraper` repo
(MIT, github.com/ChocoData-com/linkedin-company-scraper) uses — re-implemented
in this file from scratch with our logger + DB wiring. No API key, no proxy,
no headless browser.

Data we get for free from the public company page:
    industry, company_size (band), employee_count (exact),
    followers, headquarters, website, founded, specialties,
    description, logo_url.

The DDGS slug-discovery step (company name → vanity URL) is the only slow part
of the pipeline: ~2s per search.  We dispatch one search per company and only
re-verify a slug if it is missing or older than 30 days.

Usage:
    uv run python scripts/db_linkedin_company_enrich.py            # 100 companies
    uv run python scripts/db_linkedin_company_enrich.py --max 500  # bigger batch
    uv run python scripts/db_linkedin_company_enrich.py --stats    # row counts
    uv run python scripts/db_linkedin_company_enrich.py --dry-run  # preview only
    uv run python scripts/db_linkedin_company_enrich.py --loop --loop-gap 120
                                                            # perpetual backfill
    uv run python scripts/db_linkedin_company_enrich.py --only slug-missing
                                                    # restrict to companies
                                                    # with no slug yet
"""

import argparse
import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ddgs import DDGS
from utils.linkedin_enrich import fetch_company_page
from utils.pg import (  # noqa: E402
    get_companies_to_enrich,
    get_pg_config,
    mark_company_attempted,
    upsert_linkedin_company,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("db_linkedin_company_enrich")

DEFAULT_MAX = 100
DDGS_BASE_URL = "https://search.datasolved.org/search/text"  # noqa: F841 — historical
DDGS_DELAY_S = 2.0  # rate-limit DDGS to be polite
HTTP_DELAY_S = 4.0  # LinkedIn anti-bot: ChocoData observed 4s + fresh conn works
LINKEDIN_PATH_RE = re.compile(r"linkedin\.com/company/([^/?#]+)", re.I)


# ── slug discovery ──────────────────────────────────────────────────────

def _normalize(name: str) -> str:
    """Stable key for scraper.linkedin_companies.company_name_norm."""
    return re.sub(r"\s+", " ", (name or "").strip()).lower()


def _candidate_slug(href: str) -> Optional[str]:
    """Extract a vanilla /company/<slug>/ string from a DDGS href."""
    if not href:
        return None
    m = LINKEDIN_PATH_RE.search(href)
    return m.group(1) if m else None


async def resolve_slug(
    client: httpx.AsyncClient, company_name: str
) -> Optional[str]:
    """DDGS lookup to find the company's vanity /company/<slug>/ URL.

    Returns the slug string or None if not found. Uses `site:` and quotes
    the company name so the SERP is anchored to LinkedIn only.
    """
    q = f'site:linkedin.com/company/ "{company_name}"'
    # Use the ddgs library (already in use elsewhere in this codebase) for
    # consistency. It internally does its own retry and backoff.
    results: list[dict] = []
    try:
        ddgs = DDGS()
        for backend in ("duckduckgo", "yandex", "yahoo"):
            try:
                rs = await asyncio.to_thread(
                    lambda b=backend: list(
                        ddgs.text(q, region="wt-wt", safesearch="off",
                                  max_results=8, backend=b)
                    )
                )
                results.extend(rs)
            except Exception:
                continue
    except Exception as e:
        log.debug("DDGS failed for '%s': %s", company_name[:50], e)

    # Prefer the slug that contains the most company-name words.  Ties go to
    # the first non-empty slug.  This cuts down on wrong-company picks like
    # `bKash` returning the official bKash Limited page rather than a
    # similarly-named vendor.
    company_tokens = [t.lower() for t in re.split(r"[\s\-_]+", company_name)
                      if len(t) > 2]
    best, best_score = None, -1
    for r in results:
        href = r.get("href", "")
        slug = _candidate_slug(href)
        if not slug:
            continue
        slug_tokens = re.split(r"[-_]+", slug.lower())
        score = sum(1 for t in company_tokens if t in slug_tokens)
        if score > best_score:
            best, best_score = slug, score
    return best


# ── worker ──────────────────────────────────────────────────────────────

async def enrich_one(
    client: httpx.AsyncClient,
    company_name: str,
) -> tuple[str, str, Optional[dict]]:
    """Resolve slug, fetch the LinkedIn company page, return parsed record.

    Returns (company_name, status, company_or_None) where status is one of
    `parsed`, `slug_not_found`, `fetch_failed:<reason>`, `fetch_empty`.
    """
    slug = await resolve_slug(client, company_name)
    if not slug:
        return company_name, "slug_not_found", None

    await asyncio.sleep(HTTP_DELAY_S)
    company, diag = await asyncio.to_thread(fetch_company_page, slug, 25)
    if not company:
        return company_name, f"fetch_failed:{diag.get('outcome','?')}", None
    return company_name, "parsed", company


def _slug_variants(name: str) -> list[str]:
    """Generate vanilla slug candidates from a company name (no DDGS).

    Used by the script in case the DDGS search returns nothing but the
    company is well-known enough that a normalised URL guess might land on
    the canonical page (e.g. "Brain Station 23" → "brain-station-23").
    """
    s = name.strip()
    tokens = re.split(r"[\s\-_/]+", s)
    tokens = [t for t in tokens if t]
    out = []
    out.append("".join(t.lower() for t in tokens))
    out.append("-".join(t.lower() for t in tokens))
    out.append("-".join(t.lower() for t in tokens if len(t) > 2))
    out.append(s.lower().replace(" ", "-"))
    out.append(s.lower().replace(" ", ""))
    # Bangla + entity-heavy strings tend to have no good slug; de-dupe.
    seen = set()
    return [x for x in out if x and x not in seen]


async def process_batch(
    conn,
    company_names: list[str],
    concurrency: int,
    dry_run: bool,
) -> tuple[int, int, int, int]:
    """Enrich a batch of company names.

    Returns (attempted, parsed, slug_missing, fetch_failed).
    """
    sem = asyncio.Semaphore(concurrency)
    counters = {"parsed": 0, "slug_missing": 0, "fetch_failed": 0}
    counter_lock = asyncio.Lock()
    attempted = 0

    async def run(name: str):
        nonlocal attempted
        async with sem:
            name, status, company = await enrich_one(client, name)
            # Fallback: if DDGS found no slug, try a normalised URL guess
            # before giving up on the company entirely.  Only do this for
            # primarily-Latin names — slugging a Bangla name yields random
            # matches on `linkedin.com/company/<something>` pages that
            # happen to share a fragment.
            if not company and status == "slug_not_found":
                latin_chars = sum(1 for c in name if c.isascii() and c.isalpha())
                if latin_chars >= max(4, len(name) // 2):
                    for guess in _slug_variants(name):
                        if len(guess) < 2 or len(guess) > 60:
                            continue
                        await asyncio.sleep(HTTP_DELAY_S)
                        cand, diag = await asyncio.to_thread(
                            fetch_company_page, guess, 20,
                        )
                        if cand:
                            company, status = cand, "parsed"
                            break
            async with counter_lock:
                attempted += 1
                if status == "parsed":
                    counters["parsed"] += 1
                elif status == "slug_not_found":
                    counters["slug_missing"] += 1
                else:
                    counters["fetch_failed"] += 1
            if company and not dry_run:
                company["company_name"] = name
                company["company_name_norm"] = _normalize(name)
                # Decode HTML entities that leak out of the page markup
                # (e.g. "Information Technology &amp; Services").
                import html as _html
                for k, v in company.items():
                    if isinstance(v, str) and "&" in v:
                        company[k] = _html.unescape(v)
                upsert_linkedin_company(conn, company)
            else:
                # Stamp the attempt so we don't immediately retry the same
                # zero-result company on the next loop cycle.
                if not dry_run:
                    mark_company_attempted(conn, name, _normalize(name))
            log.info(
                "  [%s] %-32s → %s",
                "OK" if status == "parsed" else "..",
                name[:32], status,
            )

    client = httpx.AsyncClient(timeout=httpx.Timeout(20))
    try:
        tasks = [asyncio.create_task(run(n)) for n in company_names]
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await client.aclose()
    return (
        attempted,
        counters["parsed"],
        counters["slug_missing"],
        counters["fetch_failed"],
    )


def show_stats(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM scraper.linkedin_companies")
        total = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM scraper.linkedin_companies
            WHERE employee_count IS NOT NULL OR company_size IS NOT NULL
        """)
        with_size = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM scraper.linkedin_companies
            WHERE industry IS NOT NULL
        """)
        with_industry = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM scraper.linkedin_companies
            WHERE headquarters IS NOT NULL
        """)
        with_hq = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM scraper.linkedin_companies
            WHERE last_attempted_at > NOW() - INTERVAL '24 hours'
        """)
        attempts_24h = cur.fetchone()[0]
    print("\n" + "=" * 55)
    print("  LinkedIn Company Enrichment Stats")
    print("=" * 55)
    print(f"  Companies enriched (total):     {total:>6}")
    print(f"  With employee count or size:    {with_size:>6}")
    print(f"  With industry:                  {with_industry:>6}")
    print(f"  With headquarters:              {with_hq:>6}")
    print(f"  Attempts (24h):                 {attempts_24h:>6}")
    print("=" * 55)


def main():
    parser = argparse.ArgumentParser(description="Free LinkedIn company enrichment")
    parser.add_argument("--max", type=int, default=DEFAULT_MAX)
    parser.add_argument("--concurrency", type=int, default=2,
                        help="Parallel DDGS+LinkedIn lookups (default 2 — LinkedIn anti-bot)")
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--loop", action="store_true",
                        help="Run continuously, sleeping --loop-gap seconds between cycles")
    parser.add_argument("--loop-gap", type=float, default=120.0)
    args = parser.parse_args()

    pg_config = get_pg_config()
    conn = psycopg.connect(**pg_config)
    conn.autocommit = False
    try:
        if args.stats:
            show_stats(conn)
            return

        cycle = 0
        while True:
            cycle += 1
            companies = get_companies_to_enrich(conn, limit=args.max)
            if not companies:
                log.info("[cycle %d] No companies to enrich.", cycle)
                if not args.loop:
                    return
                time.sleep(args.loop_gap)
                continue
            names = [c["company_name"] for c in companies]
            log.info(
                "[cycle %d] Enriching %d companies (concurrency=%d, dry_run=%s)",
                cycle, len(names), args.concurrency, args.dry_run,
            )
            attempted, parsed, slug_missing, fetch_failed = asyncio.run(
                process_batch(conn, names, args.concurrency, args.dry_run)
            )
            log.info(
                "[cycle %d] Done: attempted=%d parsed=%d slug_missing=%d fetch_failed=%d",
                cycle, attempted, parsed, slug_missing, fetch_failed,
            )
            if not args.loop:
                return
            time.sleep(args.loop_gap)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
