#!/usr/bin/env python3
"""Query Generator — Infinite three-tier rotation: BD-Local, BD-National, Global.

Generates Google Maps search queries from BPT sector configs (software_sectors.yaml).
Targets customer business types that would BUY the software, not the software vendors.
Rotates through 15 BD cities × 15 software sectors × buyer keywords, plus Bangladesh-level
and international-market queries. Never exhausts — shuffles and restarts cycle.

Mix ratio (per batch of 50):
  70% BD-Local  — "{keyword} in {city}" / "{keyword} {city_bn}"
  10% BD-National — "{keyword} Bangladesh" / "{keyword} outside Dhaka"
  20% Global    — "{keyword} {country}" for export-eligible keywords

Global eligibility heuristic: keyword or sector suggests exportable services.
"""

import random
import os
import re
from pathlib import Path
from typing import Optional


# ── Configuration ──────────────────────────────────────────────────────────

BD_CITIES = [
    # (english, bangla, lat, lng) — coords anchor the GMaps search region
    ("Chattogram",  "চট্টগ্রাম",   22.3569,  91.7832),
    ("Sylhet",      "সিলেট",       24.8949,  91.8687),
    ("Khulna",      "খুলনা",       22.8456,  89.5403),
    ("Rajshahi",    "রাজশাহী",     24.3636,  88.6241),
    ("Barishal",    "বরিশাল",      22.7010,  90.3535),
    ("Rangpur",     "রংপুর",       25.7439,  89.2752),
    ("Mymensingh",  "ময়মনসিংহ",  24.7471,  90.4203),
    ("Cumilla",     "কুমিল্লা",    23.4607,  91.1809),
    ("Bogura",      "বগুড়া",      24.8484,  89.3733),
    ("Jashore",     "যশোর",        23.1684,  89.2123),
    ("Cox's Bazar", "কক্সবাজার",   21.4272,  92.0058),
    ("Narayanganj", "নারায়ণগঞ্জ", 23.6238,  90.5000),
    ("Gazipur",     "গাজীপুর",     23.9919,  90.4203),
    ("Feni",        "ফেনী",        23.0149,  91.3953),
    ("Narsingdi",   "নরসিংদী",    23.9889,  90.4650),
]

# GMaps search zoom level.  13z covers a city + suburbs (~15 km radius).
# 11z–13z all yield ~5x more results than the unanchored text-only form
# (verified 2026-08-01 against headless Chrome 144).  For national/global
# we use a wider zoom (6z for country, 7z for smaller country, 5z for large).
CITY_SEARCH_ZOOM = 13
NATIONAL_ZOOM = 7
GLOBAL_ZOOM = 5

# National / international coordinates (lat, lng, zoom) per market.
# Used to build region-anchored searches; format: KEYWORD|lat|lng|zoom
BD_COORD = (23.685, 90.3563, 7)
INTERNATIONAL_MARKETS = [
    ("USA", 37.0902, -95.7129, 5),
    ("UK", 55.3781, -3.4360, 6),
    ("Australia", -25.2744, 133.7751, 4),
    ("Canada", 56.1304, -106.3468, 4),
    ("UAE", 23.4241, 53.8478, 6),
    ("Saudi Arabia", 23.8859, 45.0792, 6),
    ("Germany", 51.1657, 10.4515, 6),
    ("France", 46.6034, 1.8883, 6),
    ("Italy", 41.8719, 12.5674, 6),
    ("Netherlands", 52.1326, 5.2913, 7),
    ("Belgium", 50.8503, 4.3517, 7),
    ("Sweden", 60.1282, 18.6435, 5),
    ("Switzerland", 46.8182, 8.2275, 7),
    ("Austria", 47.5162, 14.5501, 7),
    ("Denmark", 56.2639, 9.5018, 6),
    ("Norway", 60.4720, 8.4689, 5),
    ("Singapore", 1.3521, 103.8198, 10),
    ("Malaysia", 4.2105, 101.9758, 6),
    ("Japan", 36.2048, 138.2529, 5),
    ("South Korea", 35.9078, 127.7669, 6),
    ("Hong Kong", 22.3193, 114.1694, 10),
    ("Qatar", 25.3548, 51.1839, 8),
    ("Oman", 21.4735, 55.9754, 6),
    ("Kuwait", 29.3117, 47.4818, 8),
    ("Bahrain", 25.9304, 50.6378, 9),
    ("India", 20.5937, 78.9629, 5),
    ("South Africa", -30.5595, 22.9375, 5),
    ("Brazil", -14.2350, -51.9253, 4),
]

MIX_RATIO = {"bd_local": 0.70, "bd_national": 0.10, "global": 0.20}

# All queries use KEYWORD|LAT|LNG format.  This seed sentinel string must
# appear in every query — the daemon uses it to detect coordinated queries.
# If absent, the query flows as a plain text fallback (legacy).
QUERY_SEP = "|"

REPO_ROOT = Path(__file__).resolve().parents[1]
_SECTORS_YAML_DEFAULT = REPO_ROOT.parent / "business-plan-template" / "_system" / "config" / "software_sectors.yaml"
SECTORS_YAML_PATH = Path(os.environ["SECTORS_YAML_PATH"]) if os.environ.get("SECTORS_YAML_PATH") else _SECTORS_YAML_DEFAULT


# ── Global eligibility heuristic ────────────────────────────────────────────

GLOBAL_INDICATOR_WORDS = {
    "outsourcing", "service", "consulting", "consultant", "development",
    "agency", "software", "b2b", "manufacturer", "factory", "export",
    "production", "modeling", "coordination", "documentation",
    "developer", "design", "marketing", "seo", "content",
    "video production", "web", "app", "it",
}

GLOBAL_ELIGIBLE_SECTORS = {
    "bim-global-outreach",
    "media-marketing-digital",
    "electronics-gadgets",
    "clothing-fashion",
    "travel-tourism",
    # New sectors (2026-07-03) — export/outsourcing potential
    "healthcare-pharma",      # pharma manufacturing, medical transcription
    "food-beverage",          # food processing, spice export, tea
    "education-training",     # online education, corporate training
    "logistics-transport",    # freight forwarding, shipping
    "agriculture-agro",       # agro-processing, shrimp, jute, tea
    "construction-real-estate",  # developer, contractor — exportable services
    "service-agents-distribution",  # C&F, importers, trade agents, commission agents
}

# Keywords that are TOO technical/niche for BD-local city queries.
# These only make sense as international-market queries (global pool).
# Google Maps won't have "MEP coordination in Khulna" — it's a B2B service,
# not a Maps-indexed local business.
GLOBAL_ONLY_KEYWORDS = {
    # BIM ultra-technical terms
    "mep coordination", "mep design", "mep consultant",
    "scan to bim", "scan-to-bim", "bim production partner",
    "bim outsourcing", "bim consulting", "bim consultant",
    "revit modeling service", "architectural bim service",
    "construction documentation", "bim modeling",
    "bim coordination",
    # New sectors (2026-07-03) — B2B services, not Maps-indexed locally
    "3pl logistics bd", "supply chain company bangladesh",
    "medical transcription", "pharmaceutical manufacturing",
    "food processing company bd", "agro processing company bd",
    # Service agents — B2B trade/international terms, not Maps-indexed locally
    "indenting agent bd", "buying house bangladesh",
    "procurement agent dhaka", "commercial importer bangladesh",
}


def _is_global_eligible(keyword: str, sector_id: str) -> bool:
    """Heuristic: does this keyword target exportable services?"""
    kw = keyword.lower().strip()
    # Sector-level: these sectors have global potential
    if sector_id in GLOBAL_ELIGIBLE_SECTORS:
        return True
    # Keyword-level: contains a global-market indicator word
    if any(w in kw for w in GLOBAL_INDICATOR_WORDS):
        return True
    return False


# ── Sector loader ───────────────────────────────────────────────────────────

# Built-in keyword fallback used when the BPT sectors.yaml is missing
# (the file lives in a sibling repo, `business-plan-template`, which is not
# always present).  Keeps the daemon productive instead of crash-looping on
# empty pools.  These mirror the buyer business types from the sector configs.
DEFAULT_KEYWORDS_EN = [
    "manufacturing company", "factory", "warehouse", "logistics company",
    "transport company", "trucking company", "freight forwarder",
    "buying house", "apparel sourcing", "garments factory", "textile mill",
    "tailor shop", "cloth store", "departmental store", "supermarket",
    "grocery store", "wholesale market", "pharmacy", "medical store",
    "hospital", "clinic", "diagnostic center", "dental clinic",
    "eye hospital", "nursing home", "hotel", "restaurant", "cafe",
    "coffee shop", "fast food", "bakery", "sweet shop", "catering service",
    "guest house", "resort", "motel", "rest house", "travel agency",
    "airline agency", "tour operator", "bus service", "car rental",
    "tire shop", "auto workshop", "car servicing", "motorcycle showroom",
    "bike service center", "electronics shop", "mobile phone shop",
    "computer shop", "computer training center", "online service",
    "it company", "software company", "web design agency",
    "digital marketing agency", "seo agency", "printing press",
    "stationery shop", "book shop", "school", "college", "training center",
    "coaching center", "bank", "atm", "insurance company", "finance company",
    "remittance service", "microfinance", "nbfi", "stock broker",
    "real estate developer", "construction company", "interior design",
    "architect", "engineering consultant", "event venue", "convention hall",
    "community center", "gym", "salon", "spa", "jewelry shop",
    "furniture shop", "hardware store", "paint shop", "cement supplier",
    "steel supplier", "agro farm", "poultry farm", "fish farm", "dairy farm",
    "feed mill", "seed store", "fertilizer shop", "cold storage",
    "food processing", "rice mill", "flour mill", "oil mill", "ice factory",
    "beverage distributor", "cosmetics shop", "perfume shop", "toy shop",
    "sports shop", "gift shop", "pet shop", "laundry", "dry cleaner",
    "security service", "cleaning service", "pest control",
    "cctv installation", "electrical shop", "ac service center",
    "plumbing service", "packaging company", "ceramics factory",
    "brick factory", "plastic factory", "pharmaceutical company",
    "exporter", "importer", "supplier", "distributor", "dealer",
    "showroom", "agency", "office", "head office", "branch office",
]
DEFAULT_KEYWORDS_BN = [
    "উৎপাদন কারখানা", "ফ্যাক্টরি", "গুদাম", "পরিবহন কোম্পানি",
    "ট্রাকিং কোম্পানি", "ফ্রেইট ফরওয়ার্ডার", "বায়িং হাউস",
    "গার্মেন্টস ফ্যাক্টরি", "টেক্সটাইল মিল", "দর্জি দোকান",
    "কাপড়ের দোকান", "ডিপার্টমেন্টাল স্টোর", "সুপারমার্কেট",
    "মুদি দোকান", "পাইকারি বাজার", "ফার্মেসি", "মেডিকেল স্টোর",
    "হাসপাতাল", "ক্লিনিক", "ডায়াগনস্টিক সেন্টার", "ডেন্টাল ক্লিনিক",
    "চক্ষু হাসপাতাল", "নার্সিং হোম", "হোটেল", "রেস্টুরেন্ট", "ক্যাফে",
    "কফি শপ", "ফাস্ট ফুড", "বেকারি", "মিষ্টির দোকান", "ক্যাটারিং সার্ভিস",
    "গেস্ট হাউস", "রিসোর্ট", "মোটেল", "রেস্ট হাউস", "ট্রাভেল এজেন্সি",
    "এয়ারলাইন এজেন্সি", "ট্যুর অপারেটর", "বাস সার্ভিস", "কার রেন্টাল",
    "টায়ার শপ", "অটো ওয়ার্কশপ", "কার সার্ভিসিং", "মোটরসাইকেল শোরুম",
    "বাইক সার্ভিস সেন্টার", "ইলেকট্রনিক্স শপ", "মোবাইল ফোন শপ",
    "কম্পিউটার শপ", "কম্পিউটার প্রশিক্ষণ কেন্দ্র", "অনলাইন সার্ভিস",
    "আইটি কোম্পানি", "সফটওয়্যার কোম্পানি", "ওয়েব ডিজাইন এজেন্সি",
    "ডিজিটাল মার্কেটিং এজেন্সি", "সিও এজেন্সি", "প্রিন্টিং প্রেস",
    "স্টেশনারি দোকান", "বইয়ের দোকান", "স্কুল", "কলেজ",
    "প্রশিক্ষণ কেন্দ্র", "কোচিং সেন্টার", "ব্যাংক", "এটিএম",
    "ইনস্যুরেন্স কোম্পানি", "ফাইন্যান্স কোম্পানি", "রেমিট্যান্স সার্ভিস",
    "মাইক্রোফাইন্যান্স", "এনবিএফআই", "স্টক ব্রোকার",
    "রিয়েল এস্টেট ডেভেলপার", "কনস্ট্রাকশন কোম্পানি", "ইন্টেরিয়র ডিজাইন",
    "স্থপতি", "ইঞ্জিনিয়ারিং কনসালট্যান্ট", "ইভেন্ট ভেন্যু",
    "কনভেনশন হল", "কমিউনিটি সেন্টার", "জিম", "সেলুন", "স্পা",
    "জুয়েলারি শপ", "ফার্নিচার শপ", "হার্ডওয়্যার দোকান", "পেইন্ট শপ",
    "সিমেন্ট সাপ্লায়ার", "স্টিল সাপ্লায়ার", "এগ্রো ফার্ম",
    "পোল্ট্রি ফার্ম", "মাছের খামার", "ডেইরি ফার্ম", "ফিড মিল",
    "বীজের দোকান", "সার দোকান", "কোল্ড স্টোরেজ",
    "ফুড প্রসেসিং", "রাইস মিল", "ফ্লাওয়ার মিল", "অয়েল মিল",
    "আইস ফ্যাক্টরি", "বেভারেজ ডিস্ট্রিবিউটর", "কসমেটিক্স শপ",
    "পারফিউম শপ", "খেলনার দোকান", "স্পোর্টস শপ", "গিফট শপ",
    "পোষা প্রাণীর দোকান", "লন্ড্রি", "ড্রাই ক্লিনার", "সিকিউরিটি সার্ভিস",
    "ক্লিনিং সার্ভিস", "পেস্ট কন্ট্রোল", "সিসিটিভি ইনস্টলেশন",
    "ইলেকট্রিক্যাল শপ", "এসি সার্ভিস সেন্টার", "প্লাম্বিং সার্ভিস",
    "প্যাকেজিং কোম্পানি", "সিরামিকস ফ্যাক্টরি", "ইটের কারখানা",
    "প্লাস্টিক ফ্যাক্টরি", "ফার্মাসিউটিক্যাল কোম্পানি", "রপ্তানিকারক",
    "আমদানিকারক", "সাপ্লায়ার", "ডিস্ট্রিবিউটর", "ডিলার", "শোরুম",
    "এজেন্সি", "অফিস", "প্রধান কার্যালয়", "শাখা অফিস",
]

FALLBACK_SECTOR_CONFIG = {
    "fallback": {
        "status": "active",
        "target_business_types": {
            "en": DEFAULT_KEYWORDS_EN,
            "bn": DEFAULT_KEYWORDS_BN,
        },
    },
}


def _load_sectors() -> dict:
    """Load active sectors from BPT sectors.yaml."""
    import yaml
    if not SECTORS_YAML_PATH.exists():
        return dict(FALLBACK_SECTOR_CONFIG)
    try:
        data = yaml.safe_load(SECTORS_YAML_PATH.read_text())
    except Exception:
        return dict(FALLBACK_SECTOR_CONFIG)
    raw = (data or {}).get("sectors", {})
    loaded = {k: v for k, v in raw.items() if v.get("status") == "active"}
    # If the file exists but has no active sectors, fall back to built-ins.
    return loaded or dict(FALLBACK_SECTOR_CONFIG)


def _extract_keywords(sector_config: dict) -> list[str]:
    """Pull searchable keywords from a sector config (buyer business types preferred)."""
    kw = []
    tbt = sector_config.get("target_business_types") or {}
    has_tbt = bool(tbt.get("en") or tbt.get("bn"))
    if has_tbt:
        kw.extend(tbt.get("en", []))
        kw.extend(tbt.get("bn", []))
        return kw
    # Fallback: legacy keywords
    kd = sector_config.get("keywords", {})
    kw.extend(kd.get("en", []))
    kw.extend(kd.get("bn", []))
    kw.extend(sector_config.get("subsegments", []))
    return kw


# ── Query builder ───────────────────────────────────────────────────────────

def _build_bd_local(keyword: str, city_en: str, city_bn: str,
                     lat: float = 0.0, lng: float = 0.0) -> list[str]:
    """City-level queries.

    Format: `KEYWORD|LAT|LNG` — the daemon splits on `|` and builds the
    region-anchored URL `/search/KEYWORD/@lat,lng,13z`.  Verified 2026-08-01:
    keyword WITHOUT city text + coords yields 120 results vs 26 with city
    text in the query and 22 for the unanchored form.  The keyword already
    carries its own language (EN or BN from the sector config), so one query
    per city suffices.
    """
    return [f"{keyword}|{lat:.4f}|{lng:.4f}"]


def _build_bd_national(keyword: str) -> list[str]:
    """Bangladesh-level queries.  Uses Bangladesh-center coords."""
    lat, lng, _ = BD_COORD
    return [f"{keyword}|{lat:.4f}|{lng:.4f}"]


def _build_global(keyword: str) -> list[str]:
    """International-market queries with country-center coords."""
    queries = []
    for market, lat, lng, _ in INTERNATIONAL_MARKETS:
        queries.append(f"{keyword}|{lat:.4f}|{lng:.4f}")
    return queries


# ── Cycle builder ───────────────────────────────────────────────────────────

def _build_full_cycle(sectors: dict) -> dict[str, list[str]]:
    """Build all query pools. Returns {pool_name: [query_strings]}."""
    pools = {"bd_local": [], "bd_national": [], "global": []}

    for sector_id, sc in sectors.items():
        keywords = _extract_keywords(sc)
        for kw in keywords:
            kw_norm = kw.strip()
            if not kw_norm or len(kw_norm) < 3:
                continue

            kw_lower = kw_norm.lower()

            # Global-only keywords skip BD-local + BD-national —
            # they're too technical for Maps results in Bangladeshi cities.
            is_global_only = kw_lower in GLOBAL_ONLY_KEYWORDS

            # BD-local: city-level queries (skip global-only keywords)
            if not is_global_only:
                for city_en, city_bn, lat, lng in BD_CITIES:
                    pools["bd_local"].extend(
                        _build_bd_local(kw_norm, city_en, city_bn, lat, lng))

                # BD-national
                pools["bd_national"].extend(_build_bd_national(kw_norm))

            # Global: only for export-eligible keywords
            if _is_global_eligible(kw_norm, sector_id):
                pools["global"].extend(_build_global(kw_norm))

    # Deduplicate each pool
    for pool_name in pools:
        seen = set()
        deduped = []
        for q in pools[pool_name]:
            norm = re.sub(r"\s+", " ", q.strip().lower())
            if norm and norm not in seen and len(norm) > 5:
                seen.add(norm)
                deduped.append(q.strip())
        pools[pool_name] = deduped

    return pools


# ── Infinite Query Generator ────────────────────────────────────────────────

class InfiniteQueryGenerator:
    """Generate infinite Google Maps search queries cycling through:
    BD-Local (70%), BD-National (10%), Global (20%).

    Each pool is a shuffled cycle. When exhausted, reshuffle and restart.
    """

    def __init__(self, sectors: Optional[dict] = None):
        if sectors is None:
            sectors = _load_sectors()
        self._pools = _build_full_cycle(sectors)
        self._indexes: dict[str, int] = {}
        self._cycles: dict[str, list[str]] = {}
        self._total_generated = 0

        for pool_name, queries in self._pools.items():
            if queries:
                random.shuffle(queries)
                self._cycles[pool_name] = queries
                self._indexes[pool_name] = 0

    @property
    def pool_sizes(self) -> dict[str, int]:
        return {k: len(v) for k, v in self._pools.items()}

    @property
    def total_unique_queries(self) -> int:
        return sum(len(v) for v in self._pools.values())

    def _next_from_pool(self, pool_name: str) -> Optional[str]:
        """Get next query from a pool. Reshuffle on cycle exhaustion."""
        cycle = self._cycles.get(pool_name)
        if not cycle:
            return None
        idx = self._indexes[pool_name]
        if idx >= len(cycle):
            random.shuffle(cycle)
            idx = 0
        query = cycle[idx]
        self._indexes[pool_name] = idx + 1
        return query

    def next_batch(self, n: int = 50) -> list[str]:
        """Return next n queries respecting mix ratio.
        Falls back to available pools if a pool is empty.
        """
        batch = []
        # Determine how many from each pool per batch
        per_pool = {}
        for pool_name, ratio in MIX_RATIO.items():
            if pool_name in self._cycles:
                per_pool[pool_name] = max(1, int(n * ratio))

        # Adjust if total doesn't match n (rounding)
        assigned = sum(per_pool.values())
        if assigned < n:
            # Give remainder to the largest pool
            largest = max(per_pool, key=lambda k: len(self._cycles.get(k, [])))
            per_pool[largest] += n - assigned
        elif assigned > n:
            # Trim from the largest pool
            largest = max(per_pool, key=lambda k: len(self._cycles.get(k, [])))
            per_pool[largest] -= assigned - n

        # Pull from each pool
        remaining = {}
        for pool_name, count in per_pool.items():
            for _ in range(count):
                q = self._next_from_pool(pool_name)
                if q:
                    batch.append(q)
                else:
                    remaining.setdefault(pool_name, 0)
                    remaining[pool_name] += 1

        # Fill shortfall from any available pool
        need = n - len(batch)
        available = [p for p in self._cycles if p not in remaining or remaining[p] < n]
        for _ in range(need):
            for pool_name in available:
                q = self._next_from_pool(pool_name)
                if q:
                    batch.append(q)
                    break
            if len(batch) >= n:
                break

        self._total_generated += len(batch)
        return batch

    def stats(self) -> dict:
        """Return current generator stats."""
        return {
            "total_generated": self._total_generated,
            "pool_sizes": self.pool_sizes,
            "total_unique": self.total_unique_queries,
            "current_indexes": dict(self._indexes),
        }


# ── CLI for testing ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    gen = InfiniteQueryGenerator()
    st = gen.stats()
    print(f"Pools: {st['pool_sizes']}")
    print(f"Total unique queries: {st['total_unique']}")
    print()

    # Show first 3 batches (all use KEYWORD|LAT|LNG format — pools indistinguishable by text)
    for i in range(3):
        batch = gen.next_batch(50)
        print(f"Batch {i+1}: {len(batch)} queries")
        print(f"  Samples: {batch[:5]}")
        print()