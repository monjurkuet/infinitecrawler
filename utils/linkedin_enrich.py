"""utils/linkedin_enrich.py — Free LinkedIn enrichment utilities.

Two complementary scrapers, both runnable without any API key:

  1. `fetch_company_page(slug)` — fetches the public
     `https://www.linkedin.com/company/<slug>/` page and parses the schema.org
     `Organization` JSON-LD block plus the about-us definition list. Returns
     industry, company_size, employee_count, followers, headquarters, website,
     founded, specialties, description, logo. This is the technique documented
     by ChocoData's `linkedin-company-scraper` repo (MIT licensed) — we
     re-implement the parse logic from scratch in this file.

  2. `parse_profile_snippet(snippet, profile_title)` — pure-Python re-parse
     of the DDGS `site:linkedin.com/in/` search-result snippet we already
     store on every profile row. Extracts location, country, and connection
     count from patterns like
         `Experience: Stripe · Location: New York · 500+ connections on LinkedIn`
     Also pulls a cleaned headline from the profile_title field.

The slug-discovery step (resolving a company name to its `linkedin.com/company/<slug>/`
vanity URL) is handled by the DDGS layer in `db_linkedin_company_enrich.py`,
not here.

Anti-bot notes (2026-08): a fresh `requests` connection per company works
reliably; a pooled `Session` is served HTTP 999 (refusal) ~50% of the time on
this kind of fetch, per ChocoData's benchmarks. We honor that here.
"""
from __future__ import annotations

import json
import re
from typing import Optional

import requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LD_RE = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.S)
TAG_RE = re.compile(r"<[^>]+>")
# about-us__<field> — the data-test-id attributes LinkedIn renders on the
# guest company page. Each maps to a <dd> inside its own <div>.
ABOUT_TEST_IDS = {
    "industry": "about-us__industry",
    "size": "about-us__size",
    "headquarters": "about-us__headquarters",
    "founded": "about-us__foundedOn",
    "specialties": "about-us__specialties",
    "description": "about-us__description",
}
FOLLOWERS_RE = re.compile(r"([\d.,]+)\s+followers")
EMP_LITERAL_RE = re.compile(
    r'"numberOfEmployees"\s*:\s*\{\s*"value"\s*:\s*(\d+)'
)


def _strip_tags(fragment: str) -> Optional[str]:
    if not fragment:
        return None
    clean = TAG_RE.sub(" ", fragment)
    clean = (
        clean.replace("&", "&")
        .replace('"', '"')
        .replace("'", "'")
        .replace("&nbsp;", " ")
    )
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean or None


def _about(html: str, test_id: str) -> Optional[str]:
    m = re.search(
        r'data-test-id="%s"(.*?)</div>' % re.escape(test_id), html, re.S,
    )
    if not m:
        return None
    dd = re.search(r"<dd[^>]*>(.*?)</dd>", m.group(1), re.S)
    return _strip_tags(dd.group(1)) if dd else None


def _followers(html: str) -> Optional[int]:
    m = FOLLOWERS_RE.search(html)
    if not m:
        return None
    digits = re.sub(r"[.,]", "", m.group(1))
    return int(digits) if digits.isdigit() else None


def _find_org(html: str) -> Optional[dict]:
    """Return the richest schema.org Organization node in the page."""
    best, best_score = None, -1
    for block in LD_RE.findall(html):
        try:
            doc = json.loads(block)
        except json.JSONDecodeError:
            continue
        nodes = doc.get("@graph") if isinstance(doc, dict) else None
        for node in (nodes or [doc]):
            if not isinstance(node, dict):
                continue
            types = node.get("@type")
            types = types if isinstance(types, list) else [types]
            if "Organization" not in types or not node.get("name"):
                continue
            score = sum(
                bool(node.get(k))
                for k in ("description", "numberOfEmployees",
                          "logo", "address", "sameAs")
            )
            if score > best_score:
                best, best_score = node, score
    return best


def _employees(org: dict, html: str) -> Optional[int]:
    if org:
        v = org.get("numberOfEmployees")
        if isinstance(v, dict):
            v = v.get("value") or v.get("minValue") or v.get("maxValue")
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str) and v.strip().replace(",", "").isdigit():
            return int(v.replace(",", ""))
    m = EMP_LITERAL_RE.search(html)
    return int(m.group(1)) if m else None


def _logo(org: dict) -> Optional[str]:
    logo = org.get("logo") if org else None
    if isinstance(logo, list):
        logo = logo[0] if logo else None
    if isinstance(logo, str):
        return logo
    if isinstance(logo, dict):
        return logo.get("contentUrl") or logo.get("url")
    return None


def _headquarters(org: dict) -> Optional[str]:
    if not org:
        return None
    address = org.get("address")
    if isinstance(address, str):
        return address.strip() or None
    if not isinstance(address, dict):
        return None
    parts = []
    for key in ("addressLocality", "addressRegion", "addressCountry"):
        val = address.get(key)
        if isinstance(val, dict):
            val = val.get("name")
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())
    return ", ".join(parts) or None


def _website(org: dict) -> Optional[str]:
    if not org:
        return None
    candidates = org.get("sameAs")
    candidates = (
        candidates if isinstance(candidates, list)
        else ([candidates] if candidates else [])
    )
    candidates.append(org.get("url"))
    for url in candidates:
        if isinstance(url, str) and url.startswith("http") and "linkedin.com" not in url.lower():
            return url
    return None


# ── Profile-snippet parsing (DDGS result snippets) ──────────────────────

# Examples we match (English):
#   "Experience: Stripe · Location: New York · 500+ connections on LinkedIn"
#   "Experience: Cloudflare · Education: University of Colorado · Location: SF · 500+ connections"
#   "· অভিজ্ঞতা: Pathao · শিক্ষা: North South University · অবস্ধান: Dhaka · LinkedIn 500+ সংযোগ।"
LOCATION_EN_RE = re.compile(
    r"Location:\s*([^·•|.]+?)\s*(?=[·•|.]|$)", re.I
)
LOCATION_BN_RE = re.compile(
    r"অবস্ধান:\s*([^·•|।]+?)\s*(?=[·•|।]|$)"
)
# Compact form: "Location: X · LinkedIn Y connections" — but the snippet
# often appends boilerplate ("on LinkedIn", "a professional community of 1
# billion members.") so we cap the location at the first sentence boundary.
LOCATION_EN_COMPACT_RE = re.compile(
    r"Location:\s*([^·•|.]+?)\s*·\s*LinkedIn\s*([\d,+]+)\s*connections"
)
LOCATION_BN_COMPACT_RE = re.compile(
    r"অবস্ধান:\s*([^·•|।]+?)\s*·\s*LinkedIn\s*([\d,]+)\s*সংযোগ"
)
CONN_RE = re.compile(
    r"(?:LinkedIn\s*)?([\d]{1,3}(?:,\d{3})*\+?|\d{2,5}\+?)\s*(?:connections|সংযোগ)",
    re.I,
)
# Truncate trailing boilerplate that creeps into the snippet after the last
# structured token (e.g. "See mutual connections with...").
NOISE_TAIL_RE = re.compile(
    r"(?:See your mutual connections?|View mutual connections?|"
    r"a professional community of \d+ (?:billion|million) members?\.?"
    r"|on LinkedIn\.?|View .+?'s profile on LinkedIn\.?)",
    re.I,
)


def parse_profile_snippet(
    snippet: Optional[str], profile_title: Optional[str] = None
) -> dict:
    """Pull location, country, connections, and a cleaned headline out of a
    DDGS `site:linkedin.com/in/` snippet + title.

    Returns a dict with keys: profile_location, profile_country,
    connections_count, headline. Any field that could not be parsed is None.
    """
    out = {
        "profile_location": None,
        "profile_country": None,
        "connections_count": None,
        "headline": None,
    }
    if not snippet:
        return out

    # Strip trailing LinkedIn boilerplate that bleeds into the snippet after
    # the last `·` separator (e.g. "View X's profile on LinkedIn. a
    # professional community of 1 billion members.").
    snippet_clean = NOISE_TAIL_RE.sub("", snippet).rstrip(" .·|")

    # --- connections ---
    m = CONN_RE.search(snippet_clean)
    if m:
        out["connections_count"] = m.group(1)

    # --- location ---
    # Prefer the labelled form first, then fall back to the "Location: X ·
    # LinkedIn Y connections" compact form.
    loc_match = (
        LOCATION_EN_RE.search(snippet_clean)
        or LOCATION_BN_RE.search(snippet_clean)
    )
    if not loc_match:
        compact = (
            LOCATION_EN_COMPACT_RE.search(snippet_clean)
            or LOCATION_BN_COMPACT_RE.search(snippet_clean)
        )
        if compact:
            loc_match = re.match(r".+", compact.group(1))
            if loc_match:
                out["profile_location"] = compact.group(1).strip(" .·|")
    if loc_match:
        out["profile_location"] = loc_match.group(1).strip(" .·|,")

    # country = last comma-separated token of the location, with a small
    # lookup table for the most common single-token forms we see in DDGS
    # snippets (where the snippet only carries the city or country).
    loc = out["profile_location"]
    COUNTRY_HINTS = {
        "bangladesh", "india", "pakistan", "nepal", "sri lanka",
        "united states", "usa", "united kingdom", "uk", "canada",
        "australia", "singapore", "malaysia", "indonesia", "uae",
        "saudi arabia", "qatar", "germany", "france", "spain",
        "italy", "netherlands", "sweden", "switzerland", "japan",
        "south korea", "china", "philippines", "thailand", "vietnam",
        "myanmar", "afghanistan", "kenya", "nigeria", "south africa",
        "egypt", "turkey", "brazil", "mexico", "argentina", "ireland",
        "new zealand", "poland", "portugal", "russia", "ukraine",
        "hong kong", "taiwan",
    }
    if loc:
        parts = [p.strip() for p in loc.split(",") if p.strip()]
        last = parts[-1]
        if last.lower() in COUNTRY_HINTS:
            out["profile_country"] = last
        elif len(parts) >= 2:
            # First token is the city; last is likely the country even if not
            # in our hint set — store it, downstream code can normalise.
            out["profile_country"] = last
        else:
            # Single-token location that is not a known country — leave
            # country null so we don't confuse cities for countries.
            out["profile_country"] = None

    # --- headline (from title only) ---
    # title format: "Name - Title at Company | LinkedIn" → drop " - Title at
    # Company | LinkedIn" and use the role portion as headline.
    if profile_title:
        t = re.sub(r"\s*\|.*$", "", profile_title)
        m = re.match(r"^.+?\s*-\s*(.+?)(?:\s+at\s+.+)?$", t)
        if m:
            out["headline"] = m.group(1).strip()
        else:
            out["headline"] = t.strip()

    return out


# ── LinkedIn company page fetch ────────────────────────────────────────

def fetch_company_page(slug: str, timeout: int = 30) -> tuple[Optional[dict], dict]:
    """Fetch one `linkedin.com/company/<slug>/` page and parse it.

    Returns (company_dict_or_None, diagnostics). Opens a fresh connection per
    call; reusing a session halves the success rate per ChocoData's
    benchmarks (linkedin-company-scraper, MIT, 2026-07).

    Outcome strings on the diagnostics dict:
      "parsed", "refused" (HTTP 999 or empty page), "not_found" (404),
      "authwall", "no_org_block" (200 but unparseable markup).
    """
    url = f"https://www.linkedin.com/company/{slug}/"
    diag: dict = {"slug": slug, "url": url}
    try:
        # Fresh connection per call — see module docstring.
        with requests.get(
            url, headers=HEADERS, timeout=timeout, stream=False
        ) as response:
            html = response.text
            diag.update({
                "status": response.status_code,
                "chars": len(html),
            })
    except requests.RequestException as exc:
        diag.update({"outcome": "error", "detail": str(exc)})
        return None, diag

    org = _find_org(html)
    about_present = any(_about(html, tid) for tid in ABOUT_TEST_IDS.values())

    if org or about_present:
        size = _about(html, ABOUT_TEST_IDS["size"])
        industry = _about(html, ABOUT_TEST_IDS["industry"])
        headquarters = (
            _about(html, ABOUT_TEST_IDS["headquarters"]) or _headquarters(org)
        )
        founded = _about(html, ABOUT_TEST_IDS["founded"])
        specialties = _about(html, ABOUT_TEST_IDS["specialties"])
        description = (
            (org or {}).get("description")
            or _about(html, ABOUT_TEST_IDS["description"])
        )
        linkedin_url = (
            (org or {}).get("url") if isinstance((org or {}).get("url"), str)
            else f"https://www.linkedin.com/company/{slug}"
        )
        if isinstance(linkedin_url, str) and "?" in linkedin_url:
            linkedin_url = linkedin_url.split("?", 1)[0]

        company = {
            "slug": slug,
            "linkedin_url": linkedin_url,
            "name": (org or {}).get("name") or slug,
            "industry": industry,
            "company_size": size,
            "employee_count": _employees(org or {}, html),
            "followers": _followers(html),
            "headquarters": headquarters,
            "website": _website(org or {}),
            "founded": founded,
            "specialties": specialties,
            "description": description,
            "logo_url": _logo(org or {}),
        }
        diag["outcome"] = "parsed"
        return company, diag

    head = html[:4000].lower()
    if response.status_code == 999 or (
        response.status_code == 200 and len(html) < 5000
    ):
        diag["outcome"] = "refused"
    elif "authwall" in head or "join linkedin" in head:
        diag["outcome"] = "authwall"
    elif response.status_code == 404:
        diag["outcome"] = "not_found"
    else:
        diag["outcome"] = "no_org_block"
    diag["detail"] = (
        f"HTTP {response.status_code}, {len(html)} chars; no Organization or "
        "about-us markers in markup."
    )
    return None, diag
