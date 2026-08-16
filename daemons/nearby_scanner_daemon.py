#!/usr/bin/env python3
"""
nearby_scanner_daemon.py — Google Places Nearby Search grid-scanner.

DISCOVERY mode: covers entire cities with a grid of overlapping radius
circles, calling `places:searchNearby` (New API) which returns up to 20
fully-detailed places per call — phone, rating, address, website, etc.

Each API call enriches up to 20 NEW businesses in one shot, vs the old
browser daemon which enriched 1 per ~8 seconds.

GRID STRATEGY:
  - Each city gets a hex grid of lat/lng points (spacing ~1.4km for 1km radii)
  - For each grid point, Nearby Search is called with type batches
  - Overlapping circles ensure NO places are missed at edges
  - Dedup by place_id across overlapping circles (ON CONFLICT on source_url)

TYPE BATCHING:
  - Each grid point is scanned with 3 type batches (food, retail+services, health+other)
  - This avoids the 20-result cap cutting off dense areas when too many types are in one call

MULTI-KEY ROTATION:
  - Round-robins across N keys, marks 429'd keys exhausted for nearby_search method
  - Separate quota from GetPlace + Text Search in places_api_daemon
  - Sleeps when all keys exhausted, resumes on quota reset

GRID TRACKING:
  - scraper.nearby_scan_grid table tracks which grid cells are done
  - Daemon resumes from incomplete cells after restart

systemd unit: ~/.config/systemd/user/infinitecrawler-nearby-scanner.service
"""

import asyncio
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import psycopg
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from utils.pg import get_pg_config  # noqa: E402
from daemons.common import install_signal_handlers  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

# ── Config ───────────────────────────────────────────────────────────────────

CONFIG_PATH = REPO_ROOT / "config" / "nearby_scanner.yaml"

NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"

FIELD_MASK = (
    "places.id,places.displayName,places.formattedAddress,"
    "places.internationalPhoneNumber,places.nationalPhoneNumber,"
    "places.rating,places.userRatingCount,places.websiteUri,"
    "places.types,places.location,places.plusCode"
)

REQUEST_TIMEOUT = 15
CONCURRENCY = 5
ALL_EXHAUSTED_SLEEP = int(os.environ.get("NEARBY_EXHAUSTED_SLEEP", "600"))

# Type batches — 3 calls per grid cell covering all business categories.
# Split to avoid the 20-result cap truncating dense areas.
TYPE_BATCHES = [
    # Batch A: Food + Hospitality
    [
        "restaurant", "cafe", "bar", "bakery", "meal_takeaway",
        "meal_delivery", "liquor_store", "lodging", "hotel", "hostel",
    ],
    # Batch B: Retail + Services + Professional
    [
        "store", "clothing_store", "electronics_store", "shopping_mall",
        "beauty_salon", "hair_care", "spa", "gym", "fitness_center",
        "real_estency_agency", "lawyer", "accounting", "insurance_agency",
        "travel_agency", "car_dealer", "car_repair", "car_rental",
        "plumber", "electrician", "general_contractor",
    ],
    # Batch C: Health + Education + General + Government
    [
        "hospital", "pharmacy", "doctor", "dentist", "veterinary_care",
        "school", "university", "bank", "atm", "gas_station",
        "supermarket", "grocery_store", "convenience_store",
        "tourist_attraction", "museum", "park", "place_of_worship",
        "storage", "funeral_home", "post_office", "courthouse",
        "city_hall", "embassy", "local_government_office",
        "fire_station", "police",
    ],
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("nearby_scanner")

# ── PG connection ─────────────────────────────────────────────────────────────

_pg = get_pg_config()
PG_HOST, PG_PORT = _pg["host"], _pg["port"]
PG_USER, PG_PASSWORD, PG_DB = _pg["user"], _pg["password"], _pg["dbname"]


def _connect_pg() -> psycopg.Connection:
    kwargs = dict(host=PG_HOST, user=PG_USER, password=PG_PASSWORD,
                  dbname=PG_DB, connect_timeout=10)
    if "/" not in str(PG_HOST):
        kwargs["port"] = PG_PORT
    return psycopg.connect(**kwargs)


# ── Key state tracking ────────────────────────────────────────────────────────

class KeyState:
    def __init__(self, key: str, label: str):
        self.key = key
        self.label = label
        self.exhausted: dict[str, float] = {}
        self.request_count: dict[str, int] = {"nearby": 0}

    def is_available(self, method: str = "nearby") -> bool:
        ts = self.exhausted.get(method)
        if ts is None:
            return True
        if time.time() - ts > 25 * 3600:
            self.exhausted.pop(method, None)
            self.request_count[method] = 0
            return True
        return False

    def mark_exhausted(self, method: str = "nearby"):
        if method not in self.exhausted:
            self.exhausted[method] = time.time()
            log.warning("Key %s exhausted for %s", self.label, method)

    def __repr__(self):
        return f"KeyState({self.label}, {'ok' if self.is_available() else 'ex'})"


# ── Grid generation ─────────────────────────────────────────────────────────

def hex_grid(bbox: dict, radius_m: int = 1000) -> list[tuple[float, float]]:
    """Generate a hexogonal grid of lat/lng points covering a bounding box.

    Returns a list of (lat, lng) tuples. The hex grid ensures every point
    in the area is within `radius_m` of at least one grid center.

    bbox = {"north": float, "south": float, "east": float, "west": float}
    """
    north, south = bbox["north"], bbox["south"]
    east, west = bbox["east"], bbox["west"]

    # Earth radius in meters
    R = 6371000.0
    # Spacing between grid centers (hex grid for full coverage)
    # For hex packing, spacing = radius * 1.5 vertically, radius * sqrt(3) horizontally
    spacing_y = radius_m * 1.5  # meters between rows
    spacing_x = radius_m * math.sqrt(3)  # meters between columns (hex offset)

    points = []
    lat = south
    row = 0
    while lat <= north:
        # Calculate lng step at this latitude
        lat_rad = math.radians(lat)
        dlng = spacing_x / (R * math.cos(lat_rad))
        # Offset every other row by half spacing (hex pattern)
        lng_start = west if row % 2 == 0 else west + dlng / 2
        lng = lng_start
        while lng <= east:
            points.append((round(lat, 6), round(lng, 6)))
            lng += dlng
        # Step lat by spacing_y meters
        dlat = spacing_y / R * 180 / math.pi
        lat += dlat
        row += 1

    return points


# ── Grid tracking DB table ─────────────────────────────────────────────────

GRID_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scraper.nearby_scan_grid (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    radius_m INTEGER NOT NULL DEFAULT 1000,
    batch_idx INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    result_count INTEGER DEFAULT 0,
    error_msg TEXT,
    scanned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS nearby_grid_cell_uidx
    ON scraper.nearby_scan_grid (city, latitude, longitude, batch_idx);
CREATE INDEX IF NOT EXISTS nearby_grid_status_idx
    ON scraper.nearby_scan_grid (status, city);
"""


# ── Daemon ──────────────────────────────────────────────────────────────────

class NearbyScannerDaemon:
    def __init__(self, config: dict):
        self.config = config
        self.api_keys: list[KeyState] = []
        self.pg_conn: Optional[psycopg.Connection] = None
        self.shutdown_requested = False
        self.total_calls = 0
        self.total_places_found = 0
        self.total_places_written = 0
        self.total_duplicates = 0
        self.last_heartbeat = time.monotonic()
        self.last_heartbeat_count = 0
        self._rr_idx = 0
        self.radius_m: int = config.get("radius_m", 1000)
        self.cells_per_run: int = config.get("cells_per_run", 50)
        self.cities: list[dict] = config.get("cities", [])

        keys = config.get("api_keys", [])
        if not keys:
            env_keys = os.environ.get("PLACES_API_KEYS", "")
            keys = [k.strip() for k in env_keys.split(",") if k.strip()]
        for i, k in enumerate(keys):
            self.api_keys.append(KeyState(k, f"key{i+1}"))

        self.http_client: Optional[httpx.AsyncClient] = None
        self._seen_place_ids: set[str] = set()

    def _next_key(self) -> Optional[KeyState]:
        n = len(self.api_keys)
        for _ in range(n):
            ks = self.api_keys[self._rr_idx % n]
            self._rr_idx += 1
            if ks.is_available():
                return ks
        return None

    def _all_exhausted(self) -> bool:
        return all(not ks.is_available() for ks in self.api_keys)

    async def _init(self):
        self.http_client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
        self.pg_conn = _connect_pg()
        self.pg_conn.autocommit = True
        self._ensure_schema()
        self._populate_grid()
        # Load already-seen place_ids to avoid re-processing
        self._load_seen_ids()
        log.info("PG connected, grid table ready")
        log.info("Loaded %d API keys", len(self.api_keys))
        log.info("Cities: %s", ", ".join(c["name"] for c in self.cities))

    def _ensure_schema(self):
        with self.pg_conn.cursor() as cur:
            cur.execute(GRID_TABLE_SQL)

    def _populate_grid(self):
        """Generate grid points for all configured cities and insert into DB."""
        for city in self.cities:
            name = city["name"]
            bbox = city["bbox"]
            points = hex_grid(bbox, self.radius_m)
            inserted = 0
            for batch_idx in range(len(TYPE_BATCHES)):
                for lat, lng in points:
                    try:
                        with self.pg_conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO scraper.nearby_scan_grid
                                    (city, latitude, longitude, radius_m, batch_idx, status)
                                VALUES (%s, %s, %s, %s, %s, 'pending')
                                ON CONFLICT (city, latitude, longitude, batch_idx) DO NOTHING
                            """, (name, lat, lng, self.radius_m, batch_idx))
                            inserted += cur.rowcount
                    except Exception:
                        pass
            log.info("Grid for %s: %d points × %d batches = %d cells (inserted %d new)",
                     name, len(points), len(TYPE_BATCHES),
                     len(points) * len(TYPE_BATCHES), inserted)

    def _load_seen_ids(self):
        """Load all existing place_ids from gmaps_listings to skip duplicates."""
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute("SELECT place_id FROM scraper.gmaps_listings WHERE place_id IS NOT NULL")
                self._seen_place_ids = {r[0] for r in cur.fetchall()}
            log.info("Loaded %d existing place_ids for dedup", len(self._seen_place_ids))

            # Also count pending/done cells
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT status, count(*) FROM scraper.nearby_scan_grid GROUP BY status
                """)
                stats = dict(cur.fetchall())
                log.info("Grid status: %s", stats)
        except Exception as e:
            log.warning("Could not load seen place_ids: %s", e)
            self._seen_place_ids = set()

    def _fetch_pending_cells(self, limit: int) -> list[dict]:
        """Fetch pending grid cells, prioritized by city order."""
        self._pg_reconnect()
        if not self.pg_conn:
            return []
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT id, city, latitude, longitude, radius_m, batch_idx
                    FROM scraper.nearby_scan_grid
                    WHERE status = 'pending'
                    ORDER BY city, batch_idx, latitude, longitude
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
            return [{"grid_id": r[0], "city": r[1], "lat": r[2], "lng": r[3],
                     "radius": r[4], "batch_idx": r[5]} for r in rows]
        except Exception as e:
            log.error("Fetch pending cells failed: %s", e)
            self.pg_conn = None
            return []

    def _mark_cell(self, grid_id: int, status: str, result_count: int = 0,
                   error_msg: str = None):
        if not self.pg_conn:
            return
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    UPDATE scraper.nearby_scan_grid
                    SET status = %s, result_count = %s, error_msg = %s,
                        scanned_at = NOW()
                    WHERE id = %s
                """, (status, result_count, error_msg, grid_id))
        except Exception:
            pass

    async def _search_nearby(self, key: str, lat: float, lng: float,
                             radius: int, type_batch: list[str]) -> list[dict]:
        """Call Nearby Search API. Returns list of places or error marker."""
        headers = {
            "X-Goog-Api-Key": key,
            "X-Goog-FieldMask": FIELD_MASK,
            "Content-Type": "application/json",
        }
        body = {
            "includedTypes": type_batch,
            "maxResultCount": 20,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": float(radius),
                }
            },
            "languageCode": "en",
        }
        try:
            r = await self.http_client.post(NEARBY_URL, headers=headers,
                                           json=body, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json().get("places", [])
            elif r.status_code == 429:
                return [{"_error": "429"}]
            else:
                log.debug("searchNearby status=%s body=%s", r.status_code, r.text[:200])
                return []
        except Exception as e:
            log.debug("searchNearby exception: %s", e)
            return []

    def _normalize_place(self, place: dict, city: str) -> Optional[dict]:
        """Convert a Nearby Search result into gmaps_listings row format."""
        if not place or place.get("_error"):
            return None

        display_name = place.get("displayName", {}).get("text", "")
        if not display_name:
            return None

        place_id = place.get("id")
        if not place_id:
            return None

        # Skip if we've already processed this place_id
        if place_id in self._seen_place_ids:
            return None

        loc = place.get("location", {})
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        plus_code = place.get("plusCode", {}).get("globalCode")

        phone = place.get("internationalPhoneNumber") or place.get("nationalPhoneNumber")
        website = place.get("websiteUri")
        rating = place.get("rating")
        review_count = place.get("userRatingCount")
        address = place.get("formattedAddress")

        # Build a synthetic source_url for this discovered place
        source_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

        # Category
        types = place.get("types", [])
        generic = {"point_of_interest", "establishment", "food", "health"}
        category = None
        for t in types:
            if t not in generic:
                category = t.replace("_", " ").title()
                break
        if not category and types:
            category = types[0].replace("_", " ").title()

        return {
            "place_id": place_id,
            "source_url": source_url,
            "key_value": f"nearby:{place_id}",
            "source_type": "nearby_search",
            "name": display_name,
            "category": category,
            "rating": rating,
            "review_count": review_count,
            "address": address,
            "phone": phone,
            "website": website,
            "booking_url": None,
            "social_links": None,
            "plus_code": plus_code,
            "is_claimed": None,
            "latitude": lat,
            "longitude": lng,
            "crawl_retry_count": 0,
            "crawl_pages_processed": 1,
            "sector_id": None,
            "classification_confidence": None,
            "classification_method": None,
            "classified_at": None,
            "_payload": {
                **place,
                "_method": "nearby_search",
                "_city": city,
                "_discovered_at": datetime.now(timezone.utc).isoformat(),
            },
        }

    def _upsert_listing(self, item: dict) -> bool:
        """Upsert a listing into gmaps_listings."""
        if not self.pg_conn:
            self._pg_reconnect()
            if not self.pg_conn:
                return False
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scraper.gmaps_listings (
                        place_id, source_url, key_value, source_type, name, category,
                        rating, review_count, address, phone, website, booking_url,
                        social_links, plus_code, is_claimed, latitude, longitude,
                        crawl_retry_count, crawl_pages_processed, sector_id,
                        classification_confidence, classification_method, classified_at,
                        payload, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, NOW(), NOW()
                    )
                    ON CONFLICT (source_url) DO UPDATE SET
                        key_value = COALESCE(EXCLUDED.key_value, gmaps_listings.key_value),
                        place_id = EXCLUDED.place_id,
                        name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        rating = EXCLUDED.rating,
                        review_count = EXCLUDED.review_count,
                        address = EXCLUDED.address,
                        phone = EXCLUDED.phone,
                        website = EXCLUDED.website,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        plus_code = EXCLUDED.plus_code,
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                """, (
                    item["place_id"], item["source_url"], item["key_value"],
                    item["source_type"], item["name"], item["category"],
                    item["rating"], item["review_count"], item["address"],
                    item["phone"], item["website"], item["booking_url"],
                    json.dumps(item["social_links"]) if item["social_links"] else None,
                    item["plus_code"], item["is_claimed"], item["latitude"],
                    item["longitude"], item["crawl_retry_count"],
                    item["crawl_pages_processed"], item["sector_id"],
                    item["classification_confidence"], item["classification_method"],
                    item["classified_at"],
                    psycopg.types.json.Jsonb(item["_payload"]),
                ))
            # Track the place_id in our in-memory set
            self._seen_place_ids.add(item["place_id"])
            return True
        except psycopg.errors.UniqueViolation:
            return True
        except Exception as e:
            log.error("Upsert failed for place_id %s: %s",
                      item.get("place_id", "?")[:20], e)
            return False

    def _pg_reconnect(self):
        if self.pg_conn is not None:
            try:
                with self.pg_conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return
            except (psycopg.OperationalError, ConnectionError):
                log.info("PG stale — reconnecting...")
                try:
                    self.pg_conn.close()
                except Exception:
                    pass
        try:
            self.pg_conn = _connect_pg()
            self.pg_conn.autocommit = True
            log.info("PG reconnected")
        except Exception as e:
            log.error("PG reconnect failed: %s", e)
            self.pg_conn = None

    async def _process_cell(self, cell: dict) -> bool:
        """Process one grid cell: one Nearby Search call, upsert all results."""
        key_state = self._next_key()
        if not key_state:
            return False

        type_batch = TYPE_BATCHES[cell["batch_idx"]]

        places = await self._search_nearby(
            key_state.key, cell["lat"], cell["lng"], cell["radius"], type_batch
        )

        if places and places[0].get("_error") == "429":
            key_state.mark_exhausted()
            self._mark_cell(cell["grid_id"], "pending")  # retry later
            return False

        key_state.request_count["nearby"] += 1
        self.total_calls += 1

        written = 0
        found = len(places)
        self.total_places_found += found

        for place_data in places:
            if place_data.get("_error"):
                continue
            item = self._normalize_place(place_data, cell["city"])
            if item is None:
                self.total_duplicates += 1
                continue
            if self._upsert_listing(item):
                written += 1
                self.total_places_written += 1
            else:
                self.total_duplicates += 1

        self._mark_cell(cell["grid_id"], "done", result_count=found)
        log.info("Cell [%s %.4f,%.4f batch=%d] → %d found, %d new, %d dup",
                 cell["city"], cell["lat"], cell["lng"], cell["batch_idx"],
                 found, written, found - written)
        return True

    async def _process_batch(self, cells: list[dict]):
        """Process a batch of grid cells with controlled concurrency."""
        sem = asyncio.Semaphore(CONCURRENCY)

        async def _bounded(cell):
            async with sem:
                return await self._process_cell(cell)

        results = await asyncio.gather(
            *(_bounded(c) for c in cells), return_exceptions=True
        )
        for cell, result in zip(cells, results):
            if isinstance(result, Exception):
                log.error("Cell %s raised: %s", cell["grid_id"], result)
                self._mark_cell(cell["grid_id"], "failed", error_msg=str(result))

    def _heartbeat(self):
        now = time.monotonic()
        if now - self.last_heartbeat < 60:
            return
        elapsed = now - self.last_heartbeat
        delta = self.total_places_written - self.last_heartbeat_count
        rate = int(delta / elapsed * 3600) if elapsed > 0 else 0
        available = sum(1 for ks in self.api_keys if ks.is_available())
        log.info(
            "heartbeat calls=%d found=%d written=%d dup=%d rate=%d/hr keys=%d/%d grid_calls=%d",
            self.total_calls, self.total_places_found,
            self.total_places_written, self.total_duplicates,
            rate, available, len(self.api_keys),
            self.total_calls
        )
        self.last_heartbeat = now
        self.last_heartbeat_count = self.total_places_written

    async def run(self):
        await self._init()
        log.info("Nearby scanner started | radius=%dm, concurrency=%d, cells_per_run=%d",
                 self.radius_m, CONCURRENCY, self.cells_per_run)

        while not self.shutdown_requested:
            try:
                self._heartbeat()

                if self._all_exhausted():
                    log.warning("All keys exhausted. Sleeping %ds...", ALL_EXHAUSTED_SLEEP)
                    await asyncio.sleep(ALL_EXHAUSTED_SLEEP)
                    continue

                # Fetch pending cells
                cells = self._fetch_pending_cells(self.cells_per_run)
                if not cells:
                    log.info("No pending grid cells. Grid scan complete! Sleeping 300s...")
                    await asyncio.sleep(300)
                    continue

                t0 = time.time()
                await self._process_batch(cells)
                elapsed = time.time() - t0
                log.info("Batch done: %d cells in %.1fs (%.1f/s) | calls=%d written=%d total",
                         len(cells), elapsed,
                         len(cells)/elapsed if elapsed > 0 else 0,
                         self.total_calls, self.total_places_written)

                await asyncio.sleep(0.3)

            except Exception as e:
                log.error("Loop error: %s", e, exc_info=True)
                await asyncio.sleep(10)

        log.info("Shutdown. calls=%d found=%d written=%d dup=%d",
                 self.total_calls, self.total_places_found,
                 self.total_places_written, self.total_duplicates)
        if self.http_client:
            await self.http_client.aclose()
        if self.pg_conn:
            try:
                self.pg_conn.close()
            except Exception:
                pass
        log.info("Cleanup complete.")


# ── Entry point ──────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    import yaml
    with open(path) as f:
        return yaml.safe_load(f)


async def main():
    config = load_config(str(CONFIG_PATH))
    daemon = NearbyScannerDaemon(config)
    install_signal_handlers(daemon)
    await daemon.run()


if __name__ == "__main__":
    asyncio.run(main())
