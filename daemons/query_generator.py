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

import logging
import random
import os
import re
from pathlib import Path
from typing import Optional

log = logging.getLogger("query_generator")

# Data-only literal arrays moved to query_keywords in B3 (2026-08-12).
from daemons.query_keywords import (  # noqa: E402
    BD_CITIES,
    BD_COORD,
    FALLBACK_SECTOR_CONFIG,
    INTERNATIONAL_MARKETS,
)


# ── Configuration ──────────────────────────────────────────────────────────

# Data-only literal arrays (BD_CITIES, BD_COORD, INTERNATIONAL_MARKETS,
# DEFAULT_KEYWORDS_EN, DEFAULT_KEYWORDS_BN, FALLBACK_SECTOR_CONFIG) now live
# in daemons.query_keywords (B3 split, 2026-08-12).

# GMaps search zoom level.  13z covers a city + suburbs (~15 km radius).
# 11z–13z all yield ~5x more results than the unanchored text-only form
# (verified 2026-08-01 against headless Chrome 144).  For national/global
# we use a wider zoom (6z for country, 7z for smaller country, 5z for large).
CITY_SEARCH_ZOOM = 13
NATIONAL_ZOOM = 7
GLOBAL_ZOOM = 5

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

# Built-in keyword fallback + FALLBACK_SECTOR_CONFIG now live in
# daemons.query_keywords (B3 split, 2026-08-12).



def _load_sectors() -> dict:
    """Load active sectors from BPT sectors.yaml."""
    import yaml
    if not SECTORS_YAML_PATH.exists():
        return dict(FALLBACK_SECTOR_CONFIG)
    try:
        data = yaml.safe_load(SECTORS_YAML_PATH.read_text())
    except Exception:
        log.debug("query_generator: sectors yaml unreadable", exc_info=True)
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