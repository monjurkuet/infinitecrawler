#!/usr/bin/env python3
"""Query Generator — Infinite three-tier rotation: BD-Local, BD-National, Global.

Generates Google Maps search queries from BPT sector configs (sectors.yaml).
Rotates through 15 BD cities × 7 sectors × keywords, plus Bangladesh-level
and international-market queries. Never exhausts — shuffles and restarts cycle.

Mix ratio (per batch of 50):
  70% BD-Local  — "{keyword} in {city}" / "{keyword} {city_bn}"
  10% BD-National — "{keyword} Bangladesh" / "{keyword} outside Dhaka"
  20% Global    — "{keyword} {country}" for export-eligible keywords

Global eligibility heuristic: keyword or sector suggests exportable services.
"""

import random
import re
from pathlib import Path
from typing import Optional


# ── Configuration ──────────────────────────────────────────────────────────

BD_CITIES = [
    ("Chattogram", "চট্টগ্রাম"),
    ("Sylhet", "সিলেট"),
    ("Khulna", "খুলনা"),
    ("Rajshahi", "রাজশাহী"),
    ("Barishal", "বরিশাল"),
    ("Rangpur", "রংপুর"),
    ("Mymensingh", "ময়মনসিংহ"),
    ("Cumilla", "কুমিল্লা"),
    ("Bogura", "বগুড়া"),
    ("Jashore", "যশোর"),
    ("Cox's Bazar", "কক্সবাজার"),
    ("Narayanganj", "নারায়ণগঞ্জ"),
    ("Gazipur", "গাজীপুর"),
    ("Feni", "ফেনী"),
    ("Narsingdi", "নরসিংদী"),
]

INTERNATIONAL_MARKETS = ["USA", "UK", "Australia", "Canada", "UAE", "Saudi Arabia"]

MIX_RATIO = {"bd_local": 0.70, "bd_national": 0.10, "global": 0.20}

SECTORS_YAML_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "business-plan-template"
    / "_system"
    / "config"
    / "sectors.yaml"
)


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

def _load_sectors() -> dict:
    """Load active sectors from BPT sectors.yaml."""
    import yaml
    if not SECTORS_YAML_PATH.exists():
        return {}
    data = yaml.safe_load(SECTORS_YAML_PATH.read_text())
    raw = data.get("sectors", {})
    return {k: v for k, v in raw.items() if v.get("status") == "active"}


def _extract_keywords(sector_config: dict) -> list[str]:
    """Pull all searchable keywords from a sector config."""
    kw = []
    kd = sector_config.get("keywords", {})
    kw.extend(kd.get("en", []))
    kw.extend(kd.get("bn", []))
    for sub in sector_config.get("subsegments", []):
        kw.append(sub)
    return kw


# ── Query builder ───────────────────────────────────────────────────────────

def _build_bd_local(keyword: str, city_en: str, city_bn: str) -> list[str]:
    """City-level queries."""
    queries = []
    queries.append(f"{keyword} in {city_en}")
    queries.append(f"{keyword} {city_bn}")
    return queries


def _build_bd_national(keyword: str) -> list[str]:
    """Bangladesh-level queries."""
    queries = [f"{keyword} Bangladesh"]
    if "Bangladesh" not in keyword.lower() and "bd" not in keyword.lower():
        queries.append(f"{keyword} outside Dhaka")
    return queries


def _build_global(keyword: str) -> list[str]:
    """International-market queries."""
    queries = []
    for market in INTERNATIONAL_MARKETS:
        queries.append(f"{keyword} {market}")
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
                for city_en, city_bn in BD_CITIES:
                    pools["bd_local"].extend(_build_bd_local(kw_norm, city_en, city_bn))

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

    # Show first 3 batches
    for i in range(3):
        batch = gen.next_batch(50)
        bd_local = sum(1 for q in batch if any(c[0] in q for c in BD_CITIES))
        bd_nat = sum(1 for q in batch if "Bangladesh" in q or "outside Dhaka" in q)
        global_q = len(batch) - bd_local - bd_nat
        print(f"Batch {i+1}: {len(batch)} queries")
        print(f"  BD-Local: {bd_local}, BD-National: {bd_nat}, Global: {global_q}")
        print(f"  Samples: {batch[:5]}")
        print()