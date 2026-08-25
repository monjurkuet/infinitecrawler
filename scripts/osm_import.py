#!/usr/bin/env python3
"""scripts/osm_import.py — OSM Geofabrik → filter → osm2pgsql → gmaps_listings merge pipeline.

Downloads Geofabrik .osm.pbf extracts, filters for business POIs with osmium,
imports to temporary PostGIS tables with osm2pgsql --slim --drop, then merges
the filtered OSM business data into scraper.gmaps_listings using the existing
ON CONFLICT (source_url) DO UPDATE upsert pattern — identical to how the
Nearby Scanner daemon inserts its results.

OSM rows use source_type='osm' and source_url='osm:node/{id}' or 'osm:way/{id}',
which never collides with Google source_urls.
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

# ── constants ─────────────────────────────────────────────────────────────

GEOFABRIK_BASE = "https://download.geofabrik.de"
GEOFABRIK_REGIONS: dict[str, dict] = {
    # Region key          Geofabrik path                          MB (approx)
    "bangladesh":         {"path": "asia/bangladesh-latest.osm.pbf",                 "size_mb": 336},
    "india":              {"path": "asia/india-latest.osm.pbf",                      "size_mb": 1638},
    "thailand":           {"path": "asia/thailand-latest.osm.pbf",                   "size_mb": 311},
    "vietnam":            {"path": "asia/vietnam-latest.osm.pbf",                    "size_mb": 311},
    "indonesia":          {"path": "asia/indonesia-latest.osm.pbf",                   "size_mb": 1638},
    "my-sg-brunei":       {"path": "asia/malaysia-singapore-brunei-latest.osm.pbf",   "size_mb": 238},
    "great-britain":      {"path": "europe/great-britain-latest.osm.pbf",             "size_mb": 2048},
    "gcc-states":        {"path": "asia/gcc-states-latest.osm.pbf",                   "size_mb": 240},
    "canada-ontario":     {"path": "north-america/canada/ontario-latest.osm.pbf",     "size_mb": 924},
    "us-new-york":       {"path": "north-america/us/new-york-latest.osm.pbf",        "size_mb": 472},
}
# Which regions use "only enriched" (phone/web filter) vs "all businesses"
ENRICHED_ONLY_REGIONS = {
    "bangladesh", "india", "thailand", "vietnam", "indonesia",
    "my-sg-brunei", "gcc-states", "canada-ontario", "us-new-york",
}
# great-britain uses all-business import (higher website coverage ~38%)

# OSM tags to keep during osmium pre-filter
OSMIUM_FILTER_TAGS = ["shop", "office", "amenity"]

# OSM tag → readable category mapping (subset, most common)
OSM_CATEGORY_MAP = {
    "shop=clothes": "Clothing Store", "shop=electronics": "Electronics Store",
    "shop=mobile_phone": "Mobile Phone Shop", "shop=furniture": "Furniture Store",
    "shop=supermarket": "Supermarket", "shop=convenience": "Convenience Store",
    "shop=bakery": "Bakery", "shop=butcher": "Butcher",
    "shop=hardware": "Hardware Store", "shop=bookstore": "Bookstore",
    "shop=computer": "Computer Store", "shop=jewelry": "Jewelry Store",
    "shop=hairdresser": "Hair Salon", "shop=beauty": "Beauty Salon",
    "shop=optician": "Optician", "shop=car": "Car Dealership",
    "shop=car_repair": "Car Repair", "shop=shoes": "Shoe Store",
    "shop=sports": "Sports Store", "shop=toys": "Toy Store",
    "shop=florist": "Florist", "shop=gift": "Gift Shop",
    "shop=alcohol": "Liquor Store", "shop=kiosk": "Kiosk",
    "shop=mobile_money": "Mobile Money Agent",
    "office=company": "Company Office", "office=it": "IT Company",
    "office=lawyer": "Law Firm", "office=accountant": "Accounting Firm",
    "office=insurance": "Insurance Office", "office=estate_agent": "Real Estate",
    "office=architect": "Architecture Firm", "office=educational_institution": "Educational Institution",
    "office=advertising_agency": "Advertising Agency",
    "office=government": "Government Office", "office=ngo": "NGO Office",
    "office=telecommunication": "Telecom Office",
    "amenity=restaurant": "Restaurant", "amenity=cafe": "Cafe",
    "amenity=fast_food": "Fast Food", "amenity=bar": "Bar",
    "amenity=pharmacy": "Pharmacy", "amenity=bank": "Bank",
    "amenity=hospital": "Hospital", "amenity=clinic": "Clinic",
    "amenity=dentist": "Dentist", "amenity=doctors": "Doctors Office",
    "amenity=school": "School", "amenity=university": "University",
    "amenity=college": "College", "amenity=library": "Library",
    "amenity=fuel": "Gas Station", "amenity=cinema": "Cinema",
    "amenity=gym": "Gym", "amenity=hotel": "Hotel", "amenity=hostel": "Hostel",
    "amenity=printing": "Printing Service",
}

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "osm"
DEFAULT_REGIONS = list(GEOFABRIK_REGIONS.keys())

# ── logging ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("osm_import")

# ── helpers ───────────────────────────────────────────────────────────────


@dataclass
class RegionResult:
    """Tracks per-region import metrics."""
    region: str
    downloaded: bool = False
    filtered: bool = False
    imported: bool = False
    merged: int = 0
    deleted_stale: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def pg_connect() -> psycopg.Connection:
    """Connect to PostgreSQL using .env settings."""
    cfg = {
        "host": os.environ.get("PG_HOST", "/var/run/postgresql"),
        "port": os.environ.get("PG_PORT", "5432"),
        "dbname": os.environ.get("PG_DB", "infinitecrawler"),
        "user": os.environ.get("PG_USER", "postgres"),
        "password": os.environ.get("PG_PASSWORD", "changeme"),
    }
    return psycopg.connect(**cfg)  # type: ignore[call-overload]


def run_subprocess(cmd: list[str], desc: str, timeout: int = 600) -> tuple[int, str]:
    """Run a subprocess, capture output, log result."""
    log.info("%s: %s", desc, " ".join(cmd[:4]) + "...")
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired:
        log.error("%s timed out after %ds", desc, timeout)
        return 1, f"TIMEOUT after {timeout}s"
    if result.returncode != 0:
        stderr_tail = result.stderr[-500:] if result.stderr else ""
        log.error("%s failed (exit %d): %s", desc, result.returncode, stderr_tail)
    return result.returncode, result.stderr or ""


def osm_type_prefix(osm_type: str) -> str:
    """Map osm2pgsql geometry type to OSM source_url prefix."""
    return {"point": "node", "polygon": "way", "line": "way"}.get(osm_type, "node")


def _build_address(tags: dict) -> str | None:
    """Concatenate addr:* tags into a single address string."""
    parts = []
    hn = tags.get("addr:housenumber")
    st = tags.get("addr:street")
    if hn:
        parts.append(hn)
    if st:
        parts.append(st)
    city = tags.get("addr:city")
    if city:
        parts.append(city)
    pc = tags.get("addr:postcode")
    if pc:
        parts.append(pc)
    return ", ".join(parts) if parts else None


def _map_category(tags: dict) -> str | None:
    """Map OSM tags to a readable category label."""
    for tag_key in ("shop", "office", "amenity"):
        val = tags.get(tag_key)
        if val:
            mapped = OSM_CATEGORY_MAP.get(f"{tag_key}={val}")
            if mapped:
                return mapped
            # Fallback: title-case the value
            return val.replace("_", " ").title()
    return None


# ── pipeline steps ────────────────────────────────────────────────────────


def step_download(region_key: str, dest: Path) -> bool:
    """Download the Geofabrik .osm.pbf extract for a region."""
    info = GEOFABRIK_REGIONS[region_key]
    url = f"{GEOFABRIK_BASE}/{info['path']}"

    # Resume support: if partial file exists, continue
    cmd = [
        "curl", "-L", "-C", "-", "--retry", "5", "--retry-delay", "10",
        "-o", str(dest), url,
    ]
    code, stderr = run_subprocess(cmd, f"[{region_key}] download", timeout=1200)
    if code != 0:
        log.error("[%s] download failed: %s", region_key, stderr[-200:])
        return False
    if not dest.exists() or dest.stat().st_size < 100_000:
        log.error("[%s] download produced tiny/missing file", region_key)
        return False
    log.info("[%s] downloaded %s (%.0f MB)", region_key, dest.name, dest.stat().st_size / 1_048_576)
    return True


def step_filter(region_key: str, input_pbf: Path, output_pbf: Path) -> bool:
    """Run osmium tags-filter to keep only shop/office/amenity elements."""
    # Fail fast with an actionable message if the osmium CLI is unavailable.
    import shutil
    if shutil.which("osmium") is None:
        log.error(
            "[%s] 'osmium' CLI not found on PATH. Install it to run the Geofabrik "
            "import path: `sudo pacman -S osmium-tool` (Arch) or "
            "`sudo apt-get install osmium-tool` (Debian/Ubuntu). "
            "NOTE: osmium-tool requires boost >= 1.92 at build time; on hosts with "
            "boost 1.91 the AUR/build may fail to link — use the Overpass path "
            "(--overpass-city) as a dependency-free alternative, or install a "
            "prebuilt osmium binary.",
            region_key,
        )
        return False
    cmd = ["osmium", "tags-filter", str(input_pbf)] + OSMIUM_FILTER_TAGS + ["-o", str(output_pbf), "--overwrite"]
    code, stderr = run_subprocess(cmd, f"[{region_key}] osmium filter", timeout=300)
    if code != 0:
        log.error("[%s] osmium filter failed: %s", region_key, stderr[-200:])
        # If the input PBF is corrupt (e.g. from a interrupted -C - resume),
        # osmium reports "failed to uncompress data". Delete the raw PBF so
        # the next run re-downloads fresh instead of resuming corruption.
        if "uncompress" in stderr.lower() or "buffer error" in stderr.lower():
            log.warning("[%s] corrupt PBF detected — deleting %s for fresh re-download", region_key, input_pbf.name)
            input_pbf.unlink(missing_ok=True)
        return False
    if not output_pbf.exists() or output_pbf.stat().st_size < 1_000:
        log.warning("[%s] filter produced tiny file (no business data?)", region_key)
        # Not necessarily an error — some regions might be empty
        return True
    log.info("[%s] filtered %s → %s (%.1f MB → %.1f KB)",
             region_key, input_pbf.name, output_pbf.name,
             input_pbf.stat().st_size / 1_048_576 if input_pbf.exists() else 0,
             output_pbf.stat().st_size / 1024)
    return True


def step_import_osm2pgsql(region_key: str, pbf: Path, prefix: str) -> bool:
    """Import the filtered PBF into temporary PG tables with osm2pgsql."""
    db = os.environ.get("PG_DB", "infinitecrawler")
    host = os.environ.get("PG_HOST", "/var/run/postgresql")
    user = os.environ.get("PG_USER", "postgres")
    cmd = [
        "osm2pgsql", "--create", "--slim", "--drop",
        "-d", db, "-H", host, "-U", user,
        "--prefix", prefix,
        "-k", "-l",
        str(pbf),
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = os.environ.get("PG_PASSWORD", "changeme")
    log.info("[%s] osm2pgsql importing with prefix=%s", region_key, prefix)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800,
                                env=env, check=False)
    except subprocess.TimeoutExpired:
        log.error("[%s] osm2pgsql timed out", region_key)
        return False
    if result.returncode != 0:
        log.error("[%s] osm2pgsql failed: %s", region_key, result.stderr[-300:])
        return False
    log.info("[%s] osm2pgsql import complete", region_key)
    return True


def step_merge_to_gmaps(
    conn: psycopg.Connection,
    region_key: str,
    prefix: str,
    enriched_only: bool,
) -> int:
    """Merge data from the temp osm2pgsql tables into scraper.gmaps_listings.

    This runs the INSERT ... ON CONFLICT (source_url) DO UPDATE query —
    the same upsert pattern used by nearby_scanner_daemon.
    """
    point_tbl = f"{prefix}_point"
    poly_tbl = f"{prefix}_polygon"

    # Check that temp tables exist — they might not if the PBF had no data
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname='public' AND c.relname=%s AND c.relkind='r')
        """, (point_tbl,))
        has_point = cur.fetchone()[0]
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname='public' AND c.relname=%s AND c.relkind='r')
        """, (poly_tbl,))
        has_poly = cur.fetchone()[0]

    if not has_point and not has_poly:
        log.warning("[%s] no temp tables found, skipping merge", region_key)
        return 0

    # The WHERE clause: enriched-only regions need phone or website;
    # high-coverage regions import all named businesses.
    where_clause = ""
    if enriched_only:
        where_clause = (
            "AND (tags ? 'phone' OR tags ? 'contact:phone' "
            "OR tags ? 'website' OR tags ? 'contact:website')"
        )

    # Build the polygon UNION clause only if polygon table exists
    poly_union = ""
    if has_poly:
        poly_union = (
            f" UNION ALL SELECT osm_id, name, shop, office, amenity, tags, "
            f"ST_Y(ST_Centroid(way)) AS lat, ST_X(ST_Centroid(way)) AS lon, "
            f"'polygon' AS geom_type FROM {poly_tbl} "
            f"WHERE name IS NOT NULL {where_clause}"
        )

    # Build the merge SQL — uses the same INSERT ... ON CONFLICT (source_url)
    # DO UPDATE pattern as nearby_scanner_daemon. Category is stored as the
    # raw OSM tag value (e.g. "clothes", "company"); the db_classify daemon
    # will assign sector_id later. Full OSM tags preserved in payload JSONB.
    merge_sql = f"""
    INSERT INTO scraper.gmaps_listings (
        place_id, source_url, key_value, source_type, name, category,
        rating, review_count, address, phone, website, booking_url,
        social_links, plus_code, is_claimed, latitude, longitude,
        crawl_retry_count, crawl_pages_processed, sector_id,
        classification_confidence, classification_method, classified_at,
        payload, created_at, updated_at
    )
    WITH osm_raw AS (
        SELECT osm_id, name, shop, office, amenity, tags,
               ST_Y(way) AS lat, ST_X(way) AS lon, 'point' AS geom_type
        FROM {point_tbl}
        WHERE name IS NOT NULL {where_clause}
        {poly_union}
    ),
    osm_union AS (
        -- Deduplicate by source_url: osm2pgsql polygon table can have
        -- the same osm_id twice (way + multipolygon relation). Keep one row.
        SELECT DISTINCT ON (CASE WHEN geom_type = 'point'
                            THEN 'osm:node/' || osm_id
                            ELSE 'osm:way/' || osm_id END)
               osm_id, name, shop, office, amenity, tags, lat, lon, geom_type
        FROM osm_raw
        ORDER BY CASE WHEN geom_type = 'point'
                 THEN 'osm:node/' || osm_id
                 ELSE 'osm:way/' || osm_id END,
                 geom_type  -- prefer 'point' over 'polygon' if somehow both
    )
    SELECT
        NULL,
        CASE WHEN geom_type = 'point' THEN 'osm:node/' || osm_id ELSE 'osm:way/' || osm_id END,
        CASE WHEN geom_type = 'point' THEN 'osm:node:' || osm_id ELSE 'osm:way:' || osm_id END,
        'osm',
        name,
        COALESCE(shop, office, amenity),
        NULL, NULL,
        TRIM(CONCAT_WS(', ', tags->'addr:housenumber', tags->'addr:street', tags->'addr:city', tags->'addr:postcode')),
        COALESCE(tags->'phone', tags->'contact:phone'),
        COALESCE(tags->'website', tags->'contact:website'),
        NULL,
        CASE
            WHEN tags ? 'contact:facebook' OR tags ? 'contact:twitter' OR tags ? 'contact:instagram'
            THEN jsonb_build_object('facebook', tags->'contact:facebook', 'twitter', tags->'contact:twitter', 'instagram', tags->'contact:instagram')
            ELSE NULL
        END,
        NULL, NULL,
        lat, lon,
        0, 1,
        NULL, NULL, NULL, NULL,
        jsonb_build_object('osm_id', osm_id, 'osm_type', CASE WHEN geom_type='point' THEN 'node' ELSE 'way' END, 'osm_tags', hstore_to_jsonb(tags), 'import_region', %s::text, 'imported_at', to_char(NOW(),'YYYY-MM-DD"T"HH24:MI:SS"Z"')),
        NOW(), NOW()
    FROM osm_union
    ON CONFLICT (source_url) DO UPDATE SET
        key_value = COALESCE(EXCLUDED.key_value, gmaps_listings.key_value),
        name = EXCLUDED.name,
        category = EXCLUDED.category,
        address = COALESCE(EXCLUDED.address, gmaps_listings.address),
        phone = COALESCE(EXCLUDED.phone, gmaps_listings.phone),
        website = COALESCE(EXCLUDED.website, gmaps_listings.website),
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        social_links = COALESCE(EXCLUDED.social_links, gmaps_listings.social_links),
        payload = EXCLUDED.payload,
        updated_at = NOW()
    """

    try:
        with conn.cursor() as cur:
            cur.execute(merge_sql, (region_key,))
            merged = cur.rowcount
        conn.commit()
        log.info("[%s] merged %d rows into gmaps_listings (source_type='osm')", region_key, merged)
        return merged
    except Exception as e:
        conn.rollback()
        log.error("[%s] merge failed: %s", region_key, e)
        return 0


def step_delete_stale(conn: psycopg.Connection, region_key: str, prefix: str = "osm_tmp") -> int:
    """Delete OSM rows that no longer exist in the fresh extract.

    This handles POIs that were deleted from OSM between refreshes.
    Only deletes rows with source_url starting with 'osm:' that were
    imported from this region (tracked via payload->>'import_region').
    """
    point_tbl = f"{prefix}_point"
    poly_tbl = f"{prefix}_polygon"
    sql = f"""
        DELETE FROM scraper.gmaps_listings
        WHERE source_type = 'osm'
          AND payload->>'import_region' = %s
          AND source_url NOT IN (
              SELECT 'osm:node/' || osm_id FROM {point_tbl}
              UNION
              SELECT 'osm:way/' || osm_id FROM {poly_tbl}
          )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (region_key,))
            deleted = cur.rowcount
        conn.commit()
        if deleted:
            log.info("[%s] deleted %d stale OSM rows", region_key, deleted)
        return deleted
    except Exception as e:
        conn.rollback()
        log.error("[%s] stale deletion failed: %s", region_key, e)
        return 0


def step_cleanup_temp_tables(conn: psycopg.Connection, prefix: str = "osm_tmp") -> None:
    """Drop temporary osm2pgsql tables."""
    for suffix in ["_point", "_polygon", "_line", "_roads"]:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {prefix}{suffix} CASCADE")
    conn.commit()
    log.info("Dropped temporary osm2pgsql tables (prefix=%s)", prefix)


def step_cleanup_pbf_files(*paths: Path) -> None:
    """Delete raw and filtered PBF files from disk."""
    for p in paths:
        if p and p.exists():
            p.unlink()
            log.info("Deleted %s", p.name)


# ── Overpass API (for US cities — skip huge Geofabrik USA PBF) ─────────────

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

OVERPASS_CITIES: dict[str, dict] = {
    # City key            bbox (south, west, north, east)
    "los-angeles":   {"bbox": (33.70, -118.60, 34.35, -118.10), "region": "us-la"},
    "san-francisco": {"bbox": (37.60, -122.60, 37.85, -122.35), "region": "us-sf"},
}


def overpass_query_biz(bbox: tuple[float, float, float, float]) -> str:
    """Build an Overpass QL query for businesses with phone or website in a bbox."""
    south, west, north, east = bbox
    # Query nodes and ways tagged shop/office/amenity with website or phone
    return f"""[out:json][timeout:180];
(
  nwr["shop"]["website"]({south},{west},{north},{east});
  nwr["shop"]["phone"]({south},{west},{north},{east});
  nwr["office"]["website"]({south},{west},{north},{east});
  nwr["office"]["phone"]({south},{west},{north},{east});
  nwr["amenity"]["website"]({south},{west},{north},{east});
  nwr["amenity"]["phone"]({south},{west},{north},{east});
);
out center;"""


def overpass_fetch(query: str) -> list[dict] | None:
    """POST to Overpass API, return parsed elements list."""
    import json
    import urllib.parse
    import urllib.request

    data = urllib.parse.urlencode({"data": query}).encode()
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            req = urllib.request.Request(endpoint, data=data, method="POST")
            req.add_header("User-Agent", "InfiniteCrawler-OSM/1.0")
            log.info("Overpass: trying %s", endpoint)
            with urllib.request.urlopen(req, timeout=200) as resp:
                raw = json.loads(resp.read())
                els = raw.get("elements", [])
                log.info("Overpass: got %d elements", len(els))
                return els
        except Exception as e:
            log.warning("Overpass %s failed: %s", endpoint, e)
    log.error("All Overpass endpoints failed")
    return None


def overpass_merge(conn: psycopg.Connection, city_key: str, elements: list[dict]) -> int:
    """Merge Overpass API results directly into gmaps_listings.

    Overpass returns JSON elements with tags — no osm2pgsql needed.
    source_url = 'osm:overpass/{type}/{id}' to distinguish from Geofabrik imports.
    """
    from psycopg.types.json import Jsonb

    region_label = OVERPASS_CITIES[city_key]["region"]
    merged = 0
    with conn.cursor() as cur:
        for el in elements:
            osm_id = el["id"]
            osm_type = el["type"]  # node, way, relation
            tags = el.get("tags", {})
            name = tags.get("name")
            if not name:
                continue
            # Coords: nodes use lat/lon, ways use center
            lat = el.get("lat") or el.get("center", {}).get("lat")
            lon = el.get("lon") or el.get("center", {}).get("lon")
            if lat is None or lon is None:
                continue

            source_url = f"osm:overpass/{osm_type}/{osm_id}"
            key_value = f"osm:overpass:{osm_type}:{osm_id}"
            category = tags.get("shop") or tags.get("office") or tags.get("amenity")
            phone = tags.get("phone") or tags.get("contact:phone")
            website = tags.get("website") or tags.get("contact:website")
            address_parts = [tags.get("addr:housenumber"), tags.get("addr:street"),
                             tags.get("addr:city"), tags.get("addr:postcode")]
            address = ", ".join(p for p in address_parts if p) or None

            social = {}
            for s in ("facebook", "twitter", "instagram"):
                v = tags.get(f"contact:{s}")
                if v:
                    social[s] = v
            social_json = Jsonb(social) if social else None

            payload = Jsonb({
                "osm_id": osm_id, "osm_type": osm_type,
                "osm_tags": tags, "import_region": region_label,
                "imported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })

            cur.execute("""
                INSERT INTO scraper.gmaps_listings (
                    place_id, source_url, key_value, source_type, name, category,
                    rating, review_count, address, phone, website, booking_url,
                    social_links, plus_code, is_claimed, latitude, longitude,
                    crawl_retry_count, crawl_pages_processed, sector_id,
                    classification_confidence, classification_method, classified_at,
                    payload, created_at, updated_at
                ) VALUES (
                    NULL, %s, %s, 'osm', %s, %s,
                    NULL, NULL, %s, %s, %s, NULL,
                    %s, NULL, NULL, %s, %s,
                    0, 1, NULL, NULL, NULL, NULL,
                    %s, NOW(), NOW()
                )
                ON CONFLICT (source_url) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    address = COALESCE(EXCLUDED.address, gmaps_listings.address),
                    phone = COALESCE(EXCLUDED.phone, gmaps_listings.phone),
                    website = COALESCE(EXCLUDED.website, gmaps_listings.website),
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
            """, (source_url, key_value, name, category, address, phone, website,
                  social_json, lat, lon, payload))
            merged += 1
    conn.commit()
    log.info("[overpass/%s] merged %d rows", city_key, merged)
    return merged


def process_overpass_city(conn: psycopg.Connection, city_key: str) -> RegionResult:
    """Import a US city via Overpass API (no PBF download, no osm2pgsql)."""
    result = RegionResult(region=city_key)
    city = OVERPASS_CITIES[city_key]

    query = overpass_query_biz(city["bbox"])
    elements = overpass_fetch(query)
    if elements is None:
        result.errors.append("overpass fetch failed")
        return result

    result.merged = overpass_merge(conn, city_key, elements)
    return result

# ── main pipeline ────────────────────────────────────────────────────────


def process_region(conn: psycopg.Connection, region_key: str, keep_pbf: bool = False) -> RegionResult:
    """Run the full download→filter→import→merge→cleanup pipeline for one region."""
    result = RegionResult(region=region_key)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_pbf = DATA_DIR / f"{region_key}-latest.osm.pbf"
    filtered_pbf = DATA_DIR / f"{region_key}_biz.pbf"

    # Step 1: Download
    if keep_pbf and raw_pbf.exists() and raw_pbf.stat().st_size > 100_000:
        log.info("[%s] reusing existing %s", region_key, raw_pbf.name)
        result.downloaded = True
    else:
        result.downloaded = step_download(region_key, raw_pbf)
        if not result.downloaded:
            result.errors.append("download failed")
            return result

    # Step 2: Filter with osmium
    result.filtered = step_filter(region_key, raw_pbf, filtered_pbf)
    if not result.filtered:
        result.errors.append("osmium filter failed")
        return result

    if not filtered_pbf.exists() or filtered_pbf.stat().st_size < 1_000:
        log.warning("[%s] filtered file is empty, skipping import", region_key)
        step_cleanup_pbf_files(raw_pbf, filtered_pbf)
        return result

    # Use a per-region prefix so parallel runs don't collide on temp tables
    tbl_prefix = f"osm_{region_key.replace('-', '_')}"

    # Step 3: Import to temp tables with osm2pgsql
    result.imported = step_import_osm2pgsql(region_key, filtered_pbf, prefix=tbl_prefix)
    if not result.imported:
        result.errors.append("osm2pgsql import failed")
        step_cleanup_pbf_files(raw_pbf, filtered_pbf)
        return result

    # Step 4: Merge into scraper.gmaps_listings
    enriched_only = region_key in ENRICHED_ONLY_REGIONS
    result.merged = step_merge_to_gmaps(conn, region_key, tbl_prefix, enriched_only)

    # Step 5: Delete stale OSM rows for this region
    result.deleted_stale = step_delete_stale(conn, region_key, prefix=tbl_prefix)

    # Step 6: Cleanup
    step_cleanup_temp_tables(conn, prefix=tbl_prefix)
    if not keep_pbf:
        step_cleanup_pbf_files(raw_pbf, filtered_pbf)

    return result


def show_status(conn: psycopg.Connection) -> None:
    """Print current OSM data status in gmaps_listings."""
    print("\n=== OSM Data in scraper.gmaps_listings ===\n")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COALESCE(payload->>'import_region', 'unknown') AS region,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE phone IS NOT NULL AND phone != '') AS with_phone,
                COUNT(*) FILTER (WHERE website IS NOT NULL AND website != '') AS with_website,
                COUNT(*) FILTER (WHERE email_scanned_at IS NOT NULL) AS email_scanned,
                MAX(updated_at) AS last_refresh
            FROM scraper.gmaps_listings
            WHERE source_type = 'osm'
            GROUP BY 1
            ORDER BY 2 DESC
        """)
        rows = cur.fetchall()
    if not rows:
        print("  (no OSM data imported yet)")
        return
    print(f"  {'Region':<20} {'Total':>7} {'Phone':>7} {'Website':>8} {'Email':>7}  Last Refresh")
    print(f"  {'─'*20} {'─'*7} {'─'*7} {'─'*8} {'─'*7}  {'─'*20}")
    for region, total, phone, web, email, refresh in rows:
        refresh_str = refresh.strftime("%Y-%m-%d %H:%M") if refresh else "never"
        print(f"  {region:<20} {total:>7} {phone:>7} {web:>8} {email:>7}  {refresh_str}")

    # Total across all sources for comparison
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_type, COUNT(*) AS total
            FROM scraper.gmaps_listings
            GROUP BY source_type
            ORDER BY 2 DESC
        """)
        all_rows = cur.fetchall()
    print("\n  All sources in gmaps_listings:")
    for st, total in all_rows:
        print(f"    {st:<20} {total:>7}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", "-r", nargs="+", default=None,
                        help=f"Region(s) to process (default: all). Choices: {', '.join(DEFAULT_REGIONS)}")
    parser.add_argument("--overpass-city", "-c", nargs="+", default=None,
                        help=f"US city(s) to import via Overpass API. Choices: {', '.join(OVERPASS_CITIES.keys())}")
    parser.add_argument("--status", "-s", action="store_true",
                        help="Show OSM data status and exit (no import)")
    parser.add_argument("--keep-pbf", action="store_true",
                        help="Keep raw PBF files on disk (delete is default)")
    parser.add_argument("--list-regions", action="store_true",
                        help="List available Geofabrik regions and exit")
    args = parser.parse_args()

    if args.list_regions:
        print("Available Geofabrik regions:")
        for key, info in GEOFABRIK_REGIONS.items():
            print(f"  {key:<20} {info['path']:<55} ~{info['size_mb']:>5} MB")
        print("\nAvailable Overpass US cities:")
        for key, info in OVERPASS_CITIES.items():
            print(f"  {key:<20} bbox={info['bbox']}")
        return 0

    # Connect to PG
    try:
        conn = pg_connect()
    except Exception as e:
        log.error("PG connection failed: %s", e)
        return 1

    if args.status:
        show_status(conn)
        conn.close()
        return 0

    # ── Overpass city imports (pure API, no PBF) ──
    overpass_cities = args.overpass_city or []
    for c in overpass_cities:
        if c not in OVERPASS_CITIES:
            log.error("Unknown Overpass city: %s (use --list-regions to see choices)", c)
            return 1

    # ── Geofabrik region imports ──
    regions = args.region if args.region else []

    if not regions and not overpass_cities:
        # Default: all Geofabrik regions + all Overpass cities
        regions = DEFAULT_REGIONS
        overpass_cities = list(OVERPASS_CITIES.keys())

    # Validate region names
    for r in regions:
        if r not in GEOFABRIK_REGIONS:
            log.error("Unknown region: %s (use --list-regions to see choices)", r)
            return 1

    total_cities = len(regions) + len(overpass_cities)
    all_names = regions + [f"overpass:{c}" for c in overpass_cities]
    log.info("Starting OSM import for %d region(s)/city(s): %s", total_cities, ", ".join(all_names))
    results: list[RegionResult] = []

    # Process Geofabrik regions
    for region_key in regions:
        log.info("=" * 70)
        t0 = time.time()
        result = process_region(conn, region_key, keep_pbf=args.keep_pbf)
        elapsed = time.time() - t0
        results.append(result)
        status = "OK" if result.ok else "FAILED"
        log.info("[%s] %s in %.0fs — merged %d, deleted %d stale",
                 region_key, status, elapsed, result.merged, result.deleted_stale)
        if not result.ok:
            for err in result.errors:
                log.error("[%s] error: %s", region_key, err)

    # Process Overpass cities
    for city_key in overpass_cities:
        log.info("=" * 70)
        t0 = time.time()
        result = process_overpass_city(conn, city_key)
        elapsed = time.time() - t0
        results.append(result)
        status = "OK" if result.ok else "FAILED"
        log.info("[overpass/%s] %s in %.0fs — merged %d",
                 city_key, status, elapsed, result.merged)
        if not result.ok:
            for err in result.errors:
                log.error("[overpass/%s] error: %s", city_key, err)

    # Summary
    conn.close()
    log.info("=" * 70)
    log.info("SUMMARY")
    total_merged = sum(r.merged for r in results)
    total_stale = sum(r.deleted_stale for r in results)
    total_errors = sum(len(r.errors) for r in results)
    ok = sum(1 for r in results if r.ok)
    fail = len(results) - ok
    log.info("  Regions: %d ok, %d failed", ok, fail)
    log.info("  Total merged into gmaps_listings: %d", total_merged)
    log.info("  Total stale rows deleted: %d", total_stale)
    if total_errors:
        log.info("  Total errors: %d", total_errors)
    return 1 if fail else 0



if __name__ == "__main__":
    sys.exit(main())
