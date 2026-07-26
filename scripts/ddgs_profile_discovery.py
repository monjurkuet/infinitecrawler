#!/usr/bin/env python3
"""ddgs_profile_discovery.py — Independent DDGS profile discovery.

Searches DDGS for LinkedIn/Facebook/Instagram profiles of high-income BD
individuals: media, events, executives. NOT anchored to hotels.

Writes to scraper.discovered_profiles + mirrors LinkedIn to linkedin_profiles.

Usage:
    uv run python scripts/ddgs_profile_discovery.py              # all modes
    uv run python scripts/ddgs_profile_discovery.py --mode media  # single mode
    uv run python scripts/ddgs_profile_discovery.py --dry-run
    uv run python scripts/ddgs_profile_discovery.py --stats
"""

import argparse, asyncio, logging, re, sys
from pathlib import Path
from typing import Optional

import httpx, psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from utils.pg import get_pg_config

log = logging.getLogger("discover")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - discover - %(levelname)s - %(message)s")

DDGS = "https://search.datasolved.org/search/text"
DELAY = 2.0


STRATEGIES = {
    "li_exec": [
        'site:linkedin.com/in/ "CEO" Bangladesh',
        'site:linkedin.com/in/ "Managing Director" Bangladesh',
        'site:linkedin.com/in/ "Chairman" Bangladesh',
        'site:linkedin.com/in/ "Director" Bangladesh',
        'site:linkedin.com/in/ "Founder" Bangladesh',
        'site:linkedin.com/in/ "President" Bangladesh',
        'site:linkedin.com/in/ "Vice President" Bangladesh',
        'site:linkedin.com/in/ "General Manager" Bangladesh',
    ],
    "li_media": [
        'site:linkedin.com/in/ journalist Bangladesh',
        'site:linkedin.com/in/ reporter Dhaka',
        'site:linkedin.com/in/ anchor Bangladesh',
        'site:linkedin.com/in/ media Bangladesh',
        'site:linkedin.com/in/ editor Bangladesh',
        'site:linkedin.com/in/ "news" Bangladesh',
    ],
    "li_dhaka": [
        'site:linkedin.com/in/ Dhaka Bangladesh',
        'site:linkedin.com/in/ Gulshan Dhaka',
        'site:linkedin.com/in/ "Dhaka" Bangladesh',
        'site:linkedin.com/in/ "Bangladesh" Dhaka',
    ],
    "li_coxs": [
        'site:linkedin.com/in/ "Cox\'s Bazar"',
        'site:linkedin.com/in/ "Cox\'s Bazar" Bangladesh',
        'site:linkedin.com/in/ Chattogram Bangladesh',
    ],
    "ig_coxs": [
        'site:instagram.com "Cox\'s Bazar"',
        'site:instagram.com cox Bangladesh',
        'site:instagram.com "party" Bangladesh',
        'site:instagram.com "travel" Bangladesh',
    ],
    "bd_exec": [
        'site:linkedin.com/in/ "CEO" Dhaka',
        'site:linkedin.com/in/ "Managing Director" Dhaka',
        'site:linkedin.com/in/ "Chairman" Dhaka',
        'site:linkedin.com/in/ "Director" Bangladesh',
        'site:linkedin.com/in/ "Founder" Bangladesh',
    ],
    "luxury": [
        'site:instagram.com "luxury" Dhaka',
        'site:instagram.com "lifestyle" Dhaka',
        'site:facebook.com "luxury" Dhaka Bangladesh',
    ],
}


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
    m = re.search(r'\bat\s+([A-Z][A-Za-z0-9&.\s-]{2,60})(?:\||$)', title)
    if m:
        return m.group(1).strip().rstrip(" |")
    m = re.search(r'([A-Z][A-Za-z0-9&.,\s-]{2,60})\s*[·•.]\s*(Full|Self|Part)', body)
    return m.group(1).strip() if m else None


def parse_linkedin(result: dict) -> Optional[dict]:
    href = result.get("href", "")
    if not href.startswith("https://www.linkedin.com/in/") and not href.startswith("https://bd.linkedin.com/in/"):
        return None
    title, body = result.get("title", ""), result.get("body", "")
    return {"full_name": parse_name(title), "profile_url": url_norm(href),
            "profile_title": parse_title(title, body), "company_name": parse_company(title, body),
            "snippet": body[:500], "platform": "linkedin"}


def parse_facebook(result: dict) -> Optional[dict]:
    href = result.get("href", "")
    if "facebook.com" not in href:
        return None
    if any(x in href for x in ["/photo/", "/video/", "/story", "/messages", "/sharer", "/plugins", "/shares"]):
        return None
    title = result.get("title", "")
    name = parse_name(title) or (title.split(" | ")[0].strip() if " | " in title else None)
    return {"full_name": name, "profile_url": href.split("?")[0],
            "profile_title": "Page" if "/p/" in href or "/pages/" in href else "Profile",
            "company_name": None, "snippet": (result.get("body") or "")[:500], "platform": "facebook"}


def parse_instagram(result: dict) -> Optional[dict]:
    href = result.get("href", "")
    if "instagram.com" not in href:
        return None
    if any(x in href for x in ["/p/", "/reel/", "/explore/", "/accounts/", "/direct/"]):
        return None
    title = result.get("title", "")
    name = parse_name(title) or (href.strip("/").split("/")[-1] if href else None)
    return {"full_name": name, "profile_url": href.split("?")[0],
            "profile_title": "Instagram", "company_name": None,
            "snippet": (result.get("body") or "")[:500], "platform": "instagram"}


PARSERS = {"linkedin": parse_linkedin, "facebook": parse_facebook, "instagram": parse_instagram}


def detect_plat(query: str) -> str:
    q = query.lower()
    if "site:linkedin" in q: return "linkedin"
    if "site:facebook" in q: return "facebook"
    if "site:instagram" in q: return "instagram"
    return "linkedin"


async def search_batch(client: httpx.AsyncClient, queries: list[str], mode: str) -> list[dict]:
    all_c, seen = [], set()
    for q in queries:
        plat = detect_plat(q)
        use_site = "site:" in q.lower()
        try:
            resp = await client.get(DDGS, params={"query": q, "max_results": 8, "region": "bd-bn"},
                                    timeout=httpx.Timeout(20))
            if resp.status_code != 200:
                await asyncio.sleep(DELAY); continue
            for r in resp.json().get("results", []):
                href = r.get("href", "")
                if use_site:
                    if plat == "linkedin" and "linkedin.com" not in href: continue
                    if plat == "facebook" and "facebook.com" not in href: continue
                    if plat == "instagram" and "instagram.com" not in href: continue
                else:
                    if "linkedin.com" in href: plat = "linkedin"
                    elif "facebook.com" in href: plat = "facebook"
                    elif "instagram.com" in href: plat = "instagram"
                    else: continue
                norm = href.split("?")[0]
                if norm in seen: continue
                seen.add(norm)
                parsed = (PARSERS.get(plat) or (lambda _: None))(r)
                if not parsed: continue
                parsed["search_query"] = q
                parsed["query_type"] = mode
                all_c.append(parsed)
            await asyncio.sleep(DELAY)
        except Exception as e:
            log.warning("Search err '%s': %s", q[:50], e)
            await asyncio.sleep(DELAY)
    return all_c


CREATE_DISCOVERED_TABLE = """
    CREATE TABLE IF NOT EXISTS scraper.discovered_profiles (
        id              BIGSERIAL PRIMARY KEY,
        full_name       TEXT,
        platform        TEXT NOT NULL CHECK (platform IN ('linkedin','facebook','instagram','twitter','other')),
        profile_url     TEXT NOT NULL UNIQUE,
        profile_title   TEXT,
        company_name    TEXT,
        search_query    TEXT,
        query_mode      TEXT,
        snippet         TEXT,
        confidence      REAL DEFAULT 0.3,
        discovered_at   TIMESTAMPTZ DEFAULT NOW(),
        last_checked    TIMESTAMPTZ DEFAULT NOW(),
        notes           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_disc_plat ON scraper.discovered_profiles(platform);
    CREATE INDEX IF NOT EXISTS idx_disc_company ON scraper.discovered_profiles(company_name);
    CREATE INDEX IF NOT EXISTS idx_disc_name ON scraper.discovered_profiles(full_name);
"""


def save_profiles(conn, profiles: list[dict]) -> int:
    if not profiles: return 0
    written = 0
    with conn.cursor() as cur:
        for p in profiles:
            try:
                cur.execute("""
                    INSERT INTO scraper.discovered_profiles
                        (full_name, platform, profile_url, profile_title, company_name,
                         search_query, query_mode, snippet)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (profile_url) DO UPDATE SET
                        full_name=COALESCE(EXCLUDED.full_name,discovered_profiles.full_name),
                        profile_title=COALESCE(EXCLUDED.profile_title,discovered_profiles.profile_title),
                        last_checked=NOW()
                """, (p.get("full_name"), p["platform"], p["profile_url"],
                      p.get("profile_title"), p.get("company_name"),
                      p.get("search_query"), p.get("query_type"), p.get("snippet")))
                written += 1
                if p["platform"] == "linkedin":
                    try:
                        cur.execute("""
                            INSERT INTO scraper.linkedin_profiles
                                (listing_id,full_name,profile_url,profile_title,company_name,
                                 search_query,confidence,snippet,source)
                            VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,'ddgs_discovery')
                            ON CONFLICT (profile_url) DO UPDATE SET
                                profile_title=COALESCE(EXCLUDED.profile_title,linkedin_profiles.profile_title),
                                last_updated=NOW()
                        """, (p.get("full_name"), p["profile_url"],
                              p.get("profile_title"), p.get("company_name"),
                              p.get("search_query"), 0.5, p.get("snippet")))
                    except Exception:
                        pass
            except Exception as e:
                log.warning("Save err %s: %s", p.get("profile_url", "?")[:40], e)
    return written


def show_stats(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scraper.discovered_profiles")
    td = cur.fetchone()[0]
    print(f"\n{'='*55}\n  DDGS Profile Discovery\n{'='*55}\n  Total: {td:>6}")
    cur.execute("SELECT platform,COUNT(*) FROM scraper.discovered_profiles GROUP BY platform")
    for p, c in cur.fetchall(): print(f"    {p:12s} {c:>6}")
    cur.execute("SELECT full_name,platform,profile_title,company_name FROM scraper.discovered_profiles ORDER BY discovered_at DESC LIMIT 10")
    print("\n  Latest:")
    for r in cur.fetchall(): print(f"    {str(r[0]or'?'):30s} {r[1]:8s} {str(r[2]or''):30s} {str(r[3]or'')[:20]}")
    cur.execute("SELECT COUNT(*) FROM scraper.luxury_contacts")
    print(f"  Luxury contacts: {cur.fetchone()[0]}")
    print(f"{'='*55}")


async def run(mode: str, dry_run: bool) -> int:
    conn = psycopg.connect(**get_pg_config())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_DISCOVERED_TABLE)
        modes = list(STRATEGIES.keys()) if mode == "all" else [m.strip() for m in mode.split(",")]
        total = 0
        async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as client:
            for m in modes:
                queries = STRATEGIES.get(m)
                if not queries: continue
                log.info("Mode '%s' — %d queries", m, len(queries))
                if dry_run:
                    for q in queries[:3]: log.info("    %s", q[:70])
                    if len(queries) > 3: log.info("    ... %d more", len(queries) - 3)
                    continue
                profiles = await search_batch(client, queries, m)
                if profiles:
                    w = save_profiles(conn, profiles)
                    total += w
                    log.info("  [%s] → %d profiles (saved: %d)", m, len(profiles), w)
                else:
                    log.info("  [%s] → no profiles", m)
        return total
    finally:
        conn.close()


def main():
    p = argparse.ArgumentParser(description="Independent DDGS profile discovery")
    p.add_argument("--mode", default="all", help="Comma-separated modes: li_exec,li_media,li_dhaka,li_coxs,ig_coxs,bd_exec,luxury,all")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()
    if args.stats:
        conn = psycopg.connect(**get_pg_config())
        try: show_stats(conn)
        finally: conn.close()
        return
    found = asyncio.run(run(args.mode, args.dry_run))
    log.info("Done — %d profiles%s", found, " [DRY RUN]" if args.dry_run else "")

if __name__ == "__main__":
    main()
