#!/usr/bin/env python3
"""match_linkedin_to_gmaps.py — Cross-reference LinkedIn company names with GMaps.

Matches company_name from all LinkedIn/contact sources against GMaps listings.
Uses quality-weighted fuzzy matching to avoid noise.

Usage:
    uv run python scripts/match_linkedin_to_gmaps.py              # match & save
    uv run python scripts/match_linkedin_to_gmaps.py --dry-run    # preview
    uv run python scripts/match_linkedin_to_gmaps.py --stats      # show matches
    uv run python scripts/match_linkedin_to_gmaps.py --min-score 0.7  # stricter
"""

from collections import defaultdict

import argparse, logging, re, sys
from pathlib import Path

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from utils.pg import get_pg_config

log = logging.getLogger("match_li_gmaps")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - match - %(levelname)s - %(message)s")

CREATE_MATCHES_TABLE = """
    CREATE TABLE IF NOT EXISTS scraper.linkedin_gmaps_matches (
        id              BIGSERIAL PRIMARY KEY,
        profile_url     TEXT NOT NULL,
        full_name       TEXT,
        company_name    TEXT,
        profile_title   TEXT,
        gmaps_listing_id BIGINT REFERENCES scraper.gmaps_listings(id) ON DELETE CASCADE,
        gmaps_name      TEXT NOT NULL,
        gmaps_website   TEXT,
        gmaps_phone     TEXT,
        gmaps_address   TEXT,
        gmaps_category  TEXT,
        match_type      TEXT DEFAULT 'company_name',
        score           REAL DEFAULT 0.5,
        matched_at      TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(profile_url, gmaps_listing_id)
    );
    CREATE INDEX IF NOT EXISTS idx_li_gm_co ON scraper.linkedin_gmaps_matches(company_name);
    CREATE INDEX IF NOT EXISTS idx_li_gm_prof ON scraper.linkedin_gmaps_matches(profile_url);
    CREATE INDEX IF NOT EXISTS idx_li_gm_li ON scraper.linkedin_gmaps_matches(gmaps_listing_id);
"""

NOISE_WORDS = {
    'bangladesh', 'dhaka', 'gulshan', 'banani', 'mirpur', 'uttara', 'bashundhara',
    'dhanmondi', 'motijheel', 'savar', 'gazipur', 'narayanganj', 'chattogram',
    'sylhet', 'khulna', 'rajshahi', 'barisal', 'cox', 'comilla', 'mymensingh',
    'hotel', 'hotels', 'resorts', 'resort', 'ltd', 'limited', 'inc', 'corporation',
    'linkedin', 'list', 'shop',
}


def normalize(text: str) -> str:
    t = text.lower().strip()
    t = re.sub(r'[,()|:;]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def score_match(company: str, listing_name: str, listing_category: str | None) -> float:
    """Quality score 0-1. Noise words filtered before comparison."""
    c = normalize(company)
    l = normalize(listing_name)
    if not c or not l:
        return 0.0
    if c == l:
        return 1.0

    c_words = set(c.split())
    l_words = set(l.split())
    c_clean = c_words - NOISE_WORDS
    l_clean = l_words - NOISE_WORDS

    if not c_clean or not l_clean:
        return 0.0

    if c in l and len(c_clean) >= 2:
        return 0.85
    if l in c and len(l_clean) >= 1:
        return 0.75

    inter = c_clean & l_clean
    if not inter:
        return 0.0

    union = c_clean | l_clean
    if not union:
        return 0.0
    j = len(inter) / len(union)
    o = len(inter) / min(len(c_clean), len(l_clean))

    if o >= 0.5:
        return 0.45 + 0.45 * j
    if j >= 0.3:
        return 0.25 + 0.35 * j
    return 0.0


def match_companies(conn, min_score: float, dry_run: bool) -> list[dict]:
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT company_name FROM scraper.luxury_contacts
        WHERE company_name IS NOT NULL AND company_name != '' AND platform = 'linkedin'
        UNION
        SELECT DISTINCT company_name FROM scraper.discovered_profiles
        WHERE company_name IS NOT NULL AND company_name != '' AND platform = 'linkedin'
        UNION
        SELECT DISTINCT company_name FROM scraper.linkedin_profiles
        WHERE company_name IS NOT NULL AND company_name != ''
        ORDER BY company_name
    """)
    companies = [r[0] for r in cur.fetchall()]
    log.info("Distinct companies: %d", len(companies))

    cur.execute("""
        SELECT id, name, website, phone, address, category
        FROM scraper.gmaps_listings
        WHERE name IS NOT NULL AND name != ''
        ORDER BY name
    """)
    listings = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in cur.fetchall()]
    log.info("GMaps listings: %d", len(listings))

    if dry_run:
        matched = 0
        for co in companies[:20]:
            best = max((score_match(co, li[1], li[5]), li) for li in listings)
            if best[0] >= min_score:
                log.info("  [%.2f] %s -> %s", best[0], co[:40], best[1][1][:40])
                matched += 1
        log.info("  %d of 20 matched (min_score=%.1f)", matched, min_score)
        return []

    norm_listings: list[str] = [normalize(li[1]) for li in listings]
    token_idx: dict[str, list[int]] = defaultdict(list)
    for idx, norm_name in enumerate(norm_listings):
        for token in set(norm_name.split()) - NOISE_WORDS:
            token_idx[token].append(idx)

    matches = []
    skipped = set()
    for co in companies:
        if len(co) < 3 or co.lower() in NOISE_WORDS:
            skipped.add(co)
            continue
        co_norm = normalize(co)
        co_tokens = set(co_norm.split()) - NOISE_WORDS
        candidate_ids: set[int] = set()
        for token in co_tokens:
            candidate_ids.update(token_idx.get(token, ()))
        for idx in candidate_ids:
            s = score_match(co, listings[idx][1], listings[idx][5])
            if s >= min_score:
                matches.append({
                    "profile_url": None,
                    "full_name": None,
                    "company_name": co,
                    "profile_title": None,
                    "gmaps_listing_id": listings[idx][0],
                    "gmaps_name": listings[idx][1],
                    "gmaps_website": listings[idx][2],
                    "gmaps_phone": listings[idx][3],
                    "gmaps_address": listings[idx][4],
                    "gmaps_category": listings[idx][5],
                    "score": s,
                })

    log.info("Company->GMaps pairs: %d (skipped %d noise)", len(matches), len(skipped))

    cur.execute("""
        SELECT profile_url, full_name, company_name, profile_title FROM scraper.luxury_contacts
        WHERE company_name IS NOT NULL AND company_name != '' AND platform = 'linkedin'
        UNION ALL
        SELECT profile_url, full_name, company_name, profile_title FROM scraper.discovered_profiles
        WHERE company_name IS NOT NULL AND company_name != '' AND platform = 'linkedin'
    """)
    profiles = {r[2].lower().strip(): r for r in cur.fetchall()}

    match_by_co = {}
    for m in matches:
        c = m["company_name"].lower().strip()
        match_by_co.setdefault(c, []).append(m)

    result = []
    seen = set()
    for co_raw, pdata in profiles.items():
        for mc, ml in match_by_co.items():
            if co_raw in mc or mc in co_raw:
                for m in ml:
                    key = (pdata[0], m["gmaps_listing_id"])
                    if key in seen:
                        continue
                    seen.add(key)
                    result.append({
                        "profile_url": pdata[0], "full_name": pdata[1],
                        "company_name": pdata[2], "profile_title": pdata[3],
                        "gmaps_listing_id": m["gmaps_listing_id"], "gmaps_name": m["gmaps_name"],
                        "gmaps_website": m["gmaps_website"], "gmaps_phone": m["gmaps_phone"],
                        "gmaps_address": m["gmaps_address"], "gmaps_category": m["gmaps_category"],
                        "score": m["score"],
                    })

    return result


def save_matches(conn, matches: list[dict]) -> int:
    if not matches:
        return 0
    written = 0
    with conn.cursor() as cur:
        for m in matches:
            try:
                cur.execute("""
                    INSERT INTO scraper.linkedin_gmaps_matches
                        (profile_url, full_name, company_name, profile_title,
                         gmaps_listing_id, gmaps_name, gmaps_website, gmaps_phone,
                         gmaps_address, gmaps_category, score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (profile_url, gmaps_listing_id) DO UPDATE SET
                        score = GREATEST(linkedin_gmaps_matches.score, EXCLUDED.score),
                        gmaps_name = EXCLUDED.gmaps_name
                """, (
                    m["profile_url"], m["full_name"], m["company_name"], m["profile_title"],
                    m["gmaps_listing_id"], m["gmaps_name"], m["gmaps_website"], m["gmaps_phone"],
                    m["gmaps_address"], m["gmaps_category"], m["score"],
                ))
                written += 1
            except Exception as e:
                log.debug("Err: %s", e)
    return written


def show_stats(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scraper.linkedin_gmaps_matches")
    total = cur.fetchone()[0]
    print(f"\n{'='*55}\n  LinkedIn -> GMaps Cross-Reference\n{'='*55}")
    print(f"  Total matches: {total:>6}")
    if total > 0:
        cur.execute("""
            SELECT m.full_name, m.company_name, m.gmaps_name, m.gmaps_website, m.score
            FROM scraper.linkedin_gmaps_matches m
            ORDER BY m.score DESC, m.matched_at DESC
            LIMIT 25
        """)
        print("\n  Top matches:")
        for r in cur.fetchall():
            print(f"    {str(r[0]or'?'):25s} {str(r[1]or''):25s} -> {str(r[2]or''):40s} {str(r[3]or'')[:25]} [{r[4]:.2f}]")
    print(f"{'='*55}")


def main():
    p = argparse.ArgumentParser(description="Cross-reference LinkedIn companies with GMaps")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stats", action="store_true")
    p.add_argument("--min-score", type=float, default=0.5, help="Minimum match score (0-1, default 0.5)")
    args = p.parse_args()

    conn = psycopg.connect(**get_pg_config())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_MATCHES_TABLE)
        if args.stats:
            show_stats(conn)
            return
        matches = match_companies(conn, args.min_score, args.dry_run)
        if args.dry_run:
            log.info("Dry run complete")
        else:
            w = save_matches(conn, matches)
            log.info("Saved %d matches (min_score=%.2f)", w, args.min_score)
    finally:
        conn.close()


if __name__ == "__main__":
    main()