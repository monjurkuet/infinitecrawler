#!/usr/bin/env python3
"""collect_luxury_contacts.py — Discover high-income profiles at luxury hotels.

Uses DDGS search to find LinkedIn/Facebook/Instagram profiles of people
associated with luxury venues in BD (guests + employees).

Usage:
    uv run python scripts/collect_luxury_contacts.py              # all pending
    uv run python scripts/collect_luxury_contacts.py --target 1   # single
    uv run python scripts/collect_luxury_contacts.py --dry-run
    uv run python scripts/collect_luxury_contacts.py --stats
"""

import argparse
import asyncio
import logging
import re
import sys
from pathlib import Path
from typing import Optional

import httpx
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from utils.linkedin_parser import parse_linkedin as _parse_linkedin
from utils.pg import get_pg_config

log = logging.getLogger("luxury")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - luxury - %(levelname)s - %(message)s")

DDGS = "https://search.datasolved.org/search/text"
DELAY = 2.5

HI_ROLES = [
    '"CEO"', '"Managing Director"', '"Chairman"',
    '"Director"', '"Founder"', '"President"',
    '"Vice President"', '"General Manager"',
    '"Chief Executive"', '"Chief Financial Officer"',
    '"Chief Operating Officer"', '"Partner"',
]

HNWI_ROLES = [
    '"Managing Director"', '"Chairman"', '"CEO"',
    '"Chief Executive"', '"Executive Director"',
    '"Group Chairman"', '"Vice Chairman"',
    '"Senior Vice President"', '"Country Head"',
    '"Managing Partner"', '"Owner"', '"Founder"',
    '"Proprietor"', '"Board Member"',
]

BUSINESS_ELITE_ROLES = [
    '"Managing Director"', '"Chairman"', '"CEO"',
    '"Board Director"', '"Chief Financial"', '"Chief Operating"',
    '"President Bangladesh"', '"Country Manager"',
    '"Group Chairman"', '"Senior Executive"',
]

DIPLOMAT_ROLES = [
    '"Ambassador"', '"High Commissioner"', '"Consul"',
    '"Deputy Ambassador"', '"Counsellor"', '"Diplomat"',
    '"First Secretary"', '"First Secretary"',
]

LAWYER_ROLES = [
    '"Barrister"', '"Advocate"', '"Supreme Court"',
    '"Senior Partner"', '"Managing Partner"', '"Law" firm"',
]

MEDIA_KW = ['"journalist"', '"media"', '"news"', '"reporter"', '"TV"', '"anchor"', '"editor"', '"broadcast"']
EVENT_KW = ['"party"', '"event"', '"gala"', '"reception"', '"wedding"', '"celebration"']
SOCIALIZE_KW = ['"socialite"', '"philanthropist"', '"influencer"', '"collector"', '"art collector"', '"patron"']


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
    c = title.split(" | ")[0].strip()
    if len(c) > 2 and not c.startswith("http"):
        return c
    return None


def parse_title(title: str, body: str) -> Optional[str]:
    if " - " in title:
        middle = title.split(" - ", 1)[1].strip()
        middle = re.sub(r"^\|\s*", "", middle).strip()
        middle = re.sub(r"\s*\|.*$", "", middle).strip()
        if middle and len(middle) > 2:
            return middle
    first = body.split(".")[0].strip() if body else ""
    if first and len(first) > 5:
        return first[:120]
    return None


def parse_company(title: str, body: str) -> Optional[str]:
    # Allow no-space "atCompany": e.g. "atIntercontinentalDhaka"
    m = re.search(r'\bat\s*([A-Z][A-Za-z0-9&.()-]{2,80})', title)
    if m:
        val = m.group(1).strip().rstrip(" |")
        val = re.sub(r'\s*(LinkedIn|Hotels|Resorts|Inc\.?|Ltd\.?|Limited)\s*$', '', val, flags=re.IGNORECASE).strip()
        if len(val) > 2:
            return val
    # "Title - Company" pattern
    m = re.search(r'-\s*([A-Z][A-Za-z0-9&\s,.()-]{3,80})', title)
    if m:
        val = m.group(1).strip()
        val = re.sub(r'\s*(LinkedIn|Inc\.?|Ltd\.?)\s*$', '', val, flags=re.IGNORECASE).strip()
        if len(val) > 2 and 'linkedin' not in val.lower():
            return val
    # Body pattern: "CompanyName · Full-time"
    m = re.search(r'([A-Z][A-Za-z0-9&.,\s-]{2,60})\s*[·•.]\s*(Full|Self|Part)', body)
    if m:
        return m.group(1).strip()
    return None


def confidence(target_name: str, parsed: dict, qtype: str) -> float:
    score = 0.3
    body = (parsed.get("snippet") or "").lower()
    name = (parsed.get("full_name") or "").lower()
    ttl = (parsed.get("profile_title") or "").lower()

    if target_name.lower() in body:
        score += 0.25
    for r in ["ceo", "cto", "cfo", "director", "managing", "chairman", "founder", "president", "vp", "general manager"]:
        if r in ttl or r in body:
            score += 0.2
            break
    if "dhaka" in body or "bangladesh" in body:
        score += 0.1
    if name and len(name.split()) >= 2:
        score += 0.1
    if "/in/" in parsed["profile_url"] and len(parsed["profile_url"].split("/")[-1].split("-")) >= 2:
        score += 0.1
    return min(score, 1.0)


def parse_linkedin(result):
    return _parse_linkedin(result, parse_name, parse_title, parse_company, url_norm)


def parse_facebook(result: dict) -> Optional[dict]:
    href = result.get("href", "")
    if "facebook.com" not in href:
        return None
    if any(x in href for x in ["/photo/", "/video/", "/story", "/messages", "/sharer", "/plugins", "/shares"]):
        return None
    title = result.get("title", "")
    name = parse_name(title)
    if not name:
        name = title.split(" | ")[0].strip() if " | " in title else None
    return {
        "full_name": name,
        "profile_url": href.split("?")[0],
        "profile_title": "Page" if "/p/" in href or "/pages/" in href else "Profile",
        "company_name": None,
        "snippet": (result.get("body") or "")[:500],
        "confidence": 0.3,
        "platform": "facebook",
    }


def parse_instagram(result: dict) -> Optional[dict]:
    href = result.get("href", "")
    if "instagram.com" not in href:
        return None
    if any(x in href for x in ["/p/", "/reel/", "/explore/", "/accounts/", "/direct/"]):
        return None
    title = result.get("title", "")
    name = parse_name(title)
    if not name:
        name = href.strip("/").split("/")[-1] if href else None
    return {
        "full_name": name,
        "profile_url": href.split("?")[0],
        "profile_title": "Instagram",
        "company_name": None,
        "snippet": (result.get("body") or "")[:500],
        "confidence": 0.3,
        "platform": "instagram",
    }


def build_queries(name: str, alt_names: list[str], city: str, target_type: str = "hotel") -> list[dict]:
    """Build DDGS queries for a venue — LinkedIn, Facebook, Instagram."""
    qs = []
    names = [name] + list(alt_names or [])[:2]

    if target_type in ("bar", "nightclub"):
        roles_for_venue = HI_ROLES[:3] + ['"Mixologist"', '"Bartender"', '"Head Chef"']
    elif target_type in ("event_venue", "convention_center"):
        roles_for_venue = HI_ROLES[:4] + ['"Event Manager"', '"Catering"', '"Wedding Planner"']
    elif target_type in ("social_club", "golf_club", "club"):
        roles_for_venue = HI_ROLES[:4] + ['"Club Manager"', '"Golf"', '"Tennis"', '"Recreation"']
    elif target_type in ("fine_dining", "restaurant"):
        roles_for_venue = HI_ROLES[:4] + ['"Chef"', '"Sommelier"', '"Restaurant Manager"']
    elif target_type in ("business_link", "chamber"):
        roles_for_venue = HI_ROLES[:4]
    else:
        roles_for_venue = HI_ROLES[:6]

    for n in names:
        short = n[:60]

        # LinkedIn: targeted hi roles
        for role in roles_for_venue[:5]:
            qs.append({"q": f'site:linkedin.com/in/ "{short}" {role}', "plat": "linkedin", "type": "li_hi"})
        qs.append({"q": f'site:linkedin.com/in/ "{short}" {city}', "plat": "linkedin", "type": "li_city"})
        qs.append({"q": f'site:linkedin.com/in/ "{short}" Bangladesh', "plat": "linkedin", "type": "li_bd"})
        qs.append({"q": f'site:linkedin.com/in/ "{short}"', "plat": "linkedin", "type": "li_gen"})

        # Facebook pages + profiles
        qs.append({"q": f'site:facebook.com "{short}" {city}', "plat": "facebook", "type": "fb_page"})
        qs.append({"q": f'site:facebook.com/p/ "{short}"', "plat": "facebook", "type": "fb_page_url"})
        qs.append({"q": f'site:facebook.com/profile.php "{short}"', "plat": "facebook", "type": "fb_profile"})
        qs.append({"q": f'site:facebook.com "{short}" Bangladesh', "plat": "facebook", "type": "fb_bd"})

        # Instagram: venue + customer content signals
        qs.append({"q": f'site:instagram.com "{short}" {city}', "plat": "instagram", "type": "ig_city"})
        qs.append({"q": f'site:instagram.com "{short}" Bangladesh', "plat": "instagram", "type": "ig_bd"})

    return qs


async def search_venue(client: httpx.AsyncClient, queries: list[dict], target_name: str) -> list[dict]:
    """Run queries → deduped contacts for one venue."""
    all_c: list[dict] = []
    seen: set[str] = set()

    for qspec in queries:
        try:
            resp = await client.get(
                DDGS, params={"query": qspec["q"], "max_results": 8, "region": "bd-bn"},
                timeout=httpx.Timeout(20),
            )
            if resp.status_code != 200:
                await asyncio.sleep(DELAY)
                continue

            for r in resp.json().get("results", []):
                href = r.get("href", "")
                if qspec["plat"] == "linkedin":
                    norm = url_norm(href)
                else:
                    norm = href.split("?")[0]
                if norm in seen:
                    continue
                seen.add(norm)

                parser = {"linkedin": parse_linkedin, "facebook": parse_facebook, "instagram": parse_instagram}.get(qspec["plat"])
                parsed = parser(r) if parser else None
                if not parsed:
                    continue

                parsed["confidence"] = confidence(target_name, parsed, qspec["type"])
                parsed["search_query"] = qspec["q"]
                parsed["query_type"] = qspec["type"]
                all_c.append(parsed)

            await asyncio.sleep(DELAY)
        except Exception as e:
            log.warning("Search error '%s': %s", qspec["q"][:50], e)
            await asyncio.sleep(DELAY)

    return all_c


def save_contacts(conn, target_id: int, contacts: list[dict]) -> int:
    """Save contacts — autocommit per batch to prevent data loss on long runs."""
    if not contacts:
        return 0
    written = 0

    with conn.cursor() as cur:
        for c in contacts:
            try:
                is_emp = c.get("query_type", "").startswith("li_emp") or c.get("query_type") in ("li_city", "li_bd", "li_gen")
                is_guest = "hi" in c.get("query_type", "")
                if "hi" in c.get("query_type", ""):
                    is_guest = True

                cur.execute("""
                    INSERT INTO scraper.luxury_contacts
                        (target_id, full_name, platform, profile_url, profile_title,
                         company_name, search_query, confidence, snippet, is_employee, is_guest)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (profile_url) DO UPDATE SET
                        confidence    = GREATEST(luxury_contacts.confidence, EXCLUDED.confidence),
                        profile_title = COALESCE(EXCLUDED.profile_title, luxury_contacts.profile_title),
                        company_name  = COALESCE(EXCLUDED.company_name, luxury_contacts.company_name),
                        last_checked  = NOW()
                """, (
                    target_id, c.get("full_name"), c["platform"], c["profile_url"],
                    c.get("profile_title"), c.get("company_name"), c.get("search_query"),
                    c.get("confidence", 0.3), c.get("snippet"), is_emp, is_guest,
                ))
                written += 1

                if c["platform"] == "linkedin":
                    try:
                        cur.execute("""
                            INSERT INTO scraper.linkedin_profiles
                                (listing_id, full_name, profile_url, profile_title, company_name,
                                 search_query, confidence, snippet, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'luxury_hotel_search')
                            ON CONFLICT (profile_url) DO UPDATE SET
                                confidence    = GREATEST(linkedin_profiles.confidence, EXCLUDED.confidence),
                                profile_title = COALESCE(EXCLUDED.profile_title, linkedin_profiles.profile_title),
                                last_updated  = NOW()
                        """, (
                            target_id, c.get("full_name"), c["profile_url"],
                            c.get("profile_title"), c.get("company_name"),
                            c.get("search_query"), c.get("confidence", 0.3), c.get("snippet"),
                        ))
                    except Exception:
                        pass

            except Exception as e:
                log.warning("Upsert err %s: %s", c.get("profile_url", "?")[:40], e)

    # Mark target as searched
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE scraper.luxury_targets SET linkedin_searched = TRUE, facebook_searched = TRUE, updated_at = NOW() WHERE id = %s",
            (target_id,),
        )
    return written


# ── Stats ──

def show_stats(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scraper.luxury_contacts")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT target_id) FROM scraper.luxury_contacts")
    tw = cur.fetchone()[0]
    cur.execute("SELECT platform, COUNT(*) FROM scraper.luxury_contacts GROUP BY platform")
    print(f"\n{'='*55}\n  Luxury Contacts Pipeline\n{'='*55}")
    print(f"  Total: {total:>6}")
    print(f"  Targets with contacts: {tw:>6}")
    for p, c in cur.fetchall():
        print(f"    {p:12s} {c:>6}")
    cur.execute("SELECT ROUND(AVG(confidence)::numeric,2) FROM scraper.luxury_contacts WHERE confidence > 0")
    print(f"  Avg confidence: {cur.fetchone()[0] or 0:>6}")
    cur.execute("""
        SELECT c.full_name, c.platform, c.profile_title, c.company_name, c.confidence, t.name
        FROM scraper.luxury_contacts c JOIN scraper.luxury_targets t ON t.id = c.target_id
        ORDER BY c.confidence DESC LIMIT 15
    """)
    print(f"\n  Top contacts:")
    for r in cur.fetchall():
        print(f"    {str(r[0] or '?'):30s} {r[1]:8s} {str(r[2] or ''):25s} {str(r[3] or ''):20s} {r[4]:.2f} [{str(r[5] or '')[:30]}]")
    cur.execute("SELECT COUNT(*) FROM scraper.luxury_targets WHERE linkedin_searched = FALSE")
    p = cur.fetchone()[0]
    print(f"\n  Pending targets: {p}")
    if p > 0:
        cur.execute("SELECT id, name, city FROM scraper.luxury_targets WHERE linkedin_searched = FALSE ORDER BY id")
        for r in cur.fetchall():
            print(f"    [{r[0]:2d}] {r[1]:45s} {r[2]}")
    print(f"{'='*55}")


# ── Main ──

async def run(target_ids: list[int] | None, dry_run: bool) -> tuple[int, int]:
    pg = get_pg_config()
    conn = psycopg.connect(**pg)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        if target_ids:
            cur.execute("SELECT id, name, alternative_names, city, target_type FROM scraper.luxury_targets WHERE id = ANY(%s) ORDER BY id", (target_ids,))
        else:
            cur.execute("SELECT id, name, alternative_names, city, target_type FROM scraper.luxury_targets WHERE linkedin_searched = FALSE OR facebook_searched = FALSE ORDER BY id")
        targets = cur.fetchall()

        if not targets:
            log.info("No pending targets.")
            return 0, 0

        log.info("Processing %d target(s)...", len(targets))
        done, total = 0, 0
        async with httpx.AsyncClient(timeout=httpx.Timeout(20)) as client:
            for t in targets:
                tid, tname, talts, tcity, ttype = t
                alt = list(talts or [])
                log.info("[%d/%d] '%s' (%s, %s)...", done + 1, len(targets), tname, tcity, ttype)

                queries = build_queries(tname, alt, tcity, target_type=ttype)
                if dry_run:
                    log.info("  [DRY-RUN] %d queries", len(queries))
                    for q in queries[:4]:
                        log.info("    %s", q["q"][:70])
                    if len(queries) > 4:
                        log.info("    ... and %d more", len(queries) - 4)
                    done += 1
                    continue

                contacts = await search_venue(client, queries, tname)
                if contacts:
                    w = save_contacts(conn, tid, contacts)
                    total += w
                    log.info("  [%d] '%s' → %d contacts (saved: %d)", tid, tname[:40], len(contacts), w)
                else:
                    cur.execute("UPDATE scraper.luxury_targets SET linkedin_searched=TRUE, facebook_searched=TRUE, updated_at=NOW() WHERE id=%s", (tid,))
                    log.info("  [%d] '%s' → no contacts", tid, tname[:40])
                done += 1
        return done, total
    finally:
        conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target", type=int, nargs="*", help="Target ID(s)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()

    if args.stats:
        conn = psycopg.connect(**get_pg_config())
        try:
            show_stats(conn)
        finally:
            conn.close()
        return

    done, found = asyncio.run(run(args.target, args.dry_run))
    if args.dry_run:
        log.info("DRY RUN: would process %d targets", done)
    else:
        log.info("Done: %d targets, %d contacts", done, found)


if __name__ == "__main__":
    main()
