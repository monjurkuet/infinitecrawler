"""utils/linkedin_jobs_config.py — Load LinkedIn jobs config from sectors.yaml.

Reads the `linkedin_jobs` block we appended to business-plan-template/_system/config/sectors.yaml
and produces:
  - locations: list[str] (e.g., ["Dhaka, Bangladesh", "Chattogram, Bangladesh", "Bangladesh"])
  - sector_keywords: dict[sector_key, list[str]]
  - universal_keywords: list[str]
  - all_queries: list[tuple(keyword, location, sector_or_none)] — ready to iterate

Used by the search daemon to generate the query matrix without any extra config files.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SECTORS_PATH = (
    Path(__file__).resolve().parents[2]
    / "business-plan-template"
    / "_system"
    / "config"
    / "sectors.yaml"
)
SECTORS_YAML_PATH = Path(os.environ.get("SECTORS_YAML_PATH", str(DEFAULT_SECTORS_PATH)))


def _load_sectors() -> dict[str, Any]:
    """Load and parse sectors.yaml, returning the whole document."""
    if not SECTORS_YAML_PATH.exists():
        raise FileNotFoundError(f"sectors.yaml not found at {SECTORS_YAML_PATH}")
    with SECTORS_YAML_PATH.open() as f:
        data = yaml.safe_load(f)
    if not data or not isinstance(data, dict):
        raise ValueError("sectors.yaml parsed to empty/invalid structure")
    return data


def load_linkedin_jobs_config() -> dict:
    """Return the full linkedin_jobs config dict (locations, sector_keywords, universal_keywords)."""
    data = _load_sectors()
    lj = data.get("linkedin_jobs")
    if not lj or not isinstance(lj, dict):
        raise ValueError("sectors.yaml missing `linkedin_jobs:` top-level block")
    # Normalize and validate
    locations = lj.get("locations", [])
    sector_keywords = lj.get("sector_keywords", {})
    universal_keywords = lj.get("universal_keywords", [])
    global_expansion_sectors = lj.get("global_expansion_sectors", [])
    if not locations:
        raise ValueError("linkedin_jobs.locations must be non-empty")
    return {
        "locations": locations,
        "sector_keywords": sector_keywords,
        "universal_keywords": universal_keywords,
        "global_expansion_sectors": global_expansion_sectors,
    }


def build_keyword_location_pairs() -> list[tuple[str, str, str | None]]:
    """Return [(keyword, location, sector_key_or_None), ...] for the full matrix.

    BD locations (anything matching "Bangladesh") get the FULL keyword matrix:
    universal + every sector key, since niche vocab is BD-flavored.

    Non-BD (global) locations get only universal keywords + the sectors
    listed under `global_expansion_sectors:` — BD-local vocabulary
    (merchandiser, MFS, garments officer) returns empty pages overseas.
    """
    cfg = load_linkedin_jobs_config()
    pairs: list[tuple[str, str, str | None]] = []

    bd_locations = [loc for loc in cfg["locations"] if "bangladesh" in loc.lower()]
    global_locations = [loc for loc in cfg["locations"] if "bangladesh" not in loc.lower()]
    global_sectors = set(cfg.get("global_expansion_sectors") or [])

    # Sector-keywords: all sectors × BD, only global_expansion_sectors × global
    for sector_key, kws in cfg["sector_keywords"].items():
        for kw in kws:
            for loc in bd_locations:
                pairs.append((kw, loc, sector_key))
            if sector_key in global_sectors:
                for loc in global_locations:
                    pairs.append((kw, loc, sector_key))

    # Universal keywords: every location regardless of BD/global
    for kw in cfg["universal_keywords"]:
        for loc in cfg["locations"]:
            pairs.append((kw, loc, None))

    return pairs


def build_queries_paginated() -> list[tuple[str, str, int, str | None]]:
    """Return [(keyword, location, start, sector_key), ...] for *every page*.

    LinkedIn guest API caps at start=975 (max 98 pages of 10).
    We yield start=0,10,20,...,970 for each (keyword, location) pair.
    """
    pairs = build_keyword_location_pairs()
    out: list[tuple[str, str, int, str | None]] = []
    for kw, loc, sector in pairs:
        for start in range(0, 976, 10):  # 0..970 inclusive
            out.append((kw, loc, start, sector))
    return out


# Convenience for monitoring / stats
def count_queries() -> dict:
    """Return dict with query counts for dashboard/monitoring."""
    cfg = load_linkedin_jobs_config()
    sector_pairs = sum(len(kws) * len(cfg["locations"]) for kws in cfg["sector_keywords"].values())
    universal_pairs = len(cfg["universal_keywords"]) * len(cfg["locations"])
    total_pairs = sector_pairs + universal_pairs
    return {
        "locations": len(cfg["locations"]),
        "sector_keyword_count": sum(len(kws) for kws in cfg["sector_keywords"].values()),
        "universal_keyword_count": len(cfg["universal_keywords"]),
        "unique_kw_loc_pairs": total_pairs,
        "paginated_requests": total_pairs * 98,  # 98 pages max (0..970)
    }


if __name__ == "__main__":
    import json

    cfg = load_linkedin_jobs_config()
    pairs = build_keyword_location_pairs()
    print(json.dumps({
        "locations": cfg["locations"],
        "sector_keywords": {k: len(v) for k, v in cfg["sector_keywords"].items()},
        "universal_keywords": len(cfg["universal_keywords"]),
        "unique_kw_loc_pairs": len(pairs),
        "total_paginated": len(pairs) * 98,
    }, indent=2))