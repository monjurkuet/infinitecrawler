#!/usr/bin/env python3
"""
places_api_daemon.py — Multi-key Google Places API (New) listing daemon.

Replaces the browser-based listing_daemon for places where we already have
the ChIJ place_id in the gmaps_search_results URL.  Uses the Places API
(New) v1 directly — no browser, no pinchtab, 100x faster.

DUAL STRATEGY:
  1. GetPlace  (places/{place_id})  — 100% accurate, 200/key/day quota
  2. Text Search (places:searchText)   — ~90% accurate, separate 200/key/day quota
  Falls back to Text Search when GetPlace quota exhausted on all keys.

MULTI-KEY ROTATION:
  Round-robins across N keys. When a key 429s on a method, it's marked
  exhausted for the day and skipped. When GetPlace is exhausted across
  all keys, Text Search takes over. When Text Search is also exhausted
  across all keys, the daemon sleeps until midnight UTC (quota reset).

SAFE RESTARTS:
  Uses the same scraper.gmaps_listings table + output strategy as the
  browser daemon. Stop this daemon, start the browser daemon, no data loss.

systemd unit: ~/.config/systemd/user/infinitecrawler-places-api.service
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
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

CONFIG_PATH = REPO_ROOT / "config" / "places_api_daemon.yaml"

# API endpoints
GETPLACE_URL = "https://places.googleapis.com/v1/places/{place_id}"
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Field mask — everything we need for listing extraction
FIELD_MASK_GETPLACE = (
    "id,displayName,formattedAddress,internationalPhoneNumber,"
    "nationalPhoneNumber,rating,userRatingCount,websiteUri,types,"
    "location,plusCode"
)
FIELD_MASK_TEXT_SEARCH = (
    "places.id,places.displayName,places.formattedAddress,"
    "places.internationalPhoneNumber,places.nationalPhoneNumber,"
    "places.rating,places.userRatingCount,places.websiteUri,"
    "places.types,places.location,places.plusCode"
)

# ChIJ place_id extraction from GMaps URLs
CHIJ_RE = re.compile(r"ChIJ[A-Za-z0-9_-]{10,40}")

# Per-request timeout
REQUEST_TIMEOUT = 15

# Concurrency: how many parallel requests per batch
CONCURRENCY = 5

# Sleep when all keys exhausted (seconds).  Default: 10 min.
# The daemon rechecks quota state after this sleep.
ALL_EXHAUSTED_SLEEP = int(os.environ.get("PLACES_EXHAUSTED_SLEEP", "600"))

# Batch size for PG URL fetch
URL_FETCH_BATCH = 200

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("places_api_daemon")

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
    """Tracks per-key, per-method daily quota state."""

    def __init__(self, key: str, label: str):
        self.key = key
        self.label = label
        # exhausted_at[method] = monotonic timestamp when 429 first hit
        self.exhausted: dict[str, float] = {}
        self.request_count: dict[str, int] = {"getplace": 0, "text_search": 0}
        # Track approximate reset time — Google resets daily quotas at
        # midnight Pacific Time (~08:00 UTC). We use a conservative 24h
        # rolling window from first exhaustion.
        self._first_exhausted: dict[str, float] = {}

    def is_available(self, method: str) -> bool:
        """Check if this key is available for the given method."""
        ts = self.exhausted.get(method)
        if ts is None:
            return True
        # Reset after 25 hours (conservative — Google resets ~24h)
        if time.time() - ts > 25 * 3600:
            self.exhausted.pop(method, None)
            self._first_exhausted.pop(method, None)
            self.request_count[method] = 0
            return True
        return False

    def mark_exhausted(self, method: str):
        if method not in self.exhausted:
            self.exhausted[method] = time.time()
            self._first_exhausted[method] = time.time()
            log.warning("Key %s exhausted for %s", self.label, method)

    def __repr__(self):
        return (f"KeyState({self.label}, g={'ok' if self.is_available('getplace') else 'ex'}, "
                f"t={'ok' if self.is_available('text_search') else 'ex'})")


# ── Daemon ──────────────────────────────────────────────────────────────────

class PlacesAPIDaemon:
    def __init__(self, config: dict):
        self.config = config
        self.api_keys: list[KeyState] = []
        self.pg_conn: Optional[psycopg.Connection] = None
        self.shutdown_requested = False
        self.total_processed = 0
        self.total_success = 0
        self.total_failures = 0
        self.last_heartbeat = time.monotonic()
        self.last_heartbeat_count = 0
        self.method_stats = {"getplace": 0, "text_search": 0, "text_search_fallback": 0, "no_match": 0}
        self._rr_idx = 0  # round-robin index

        # Load API keys from config or env
        keys = config.get("api_keys", [])
        if not keys:
            # Try env: PLACES_API_KEYS=key1,key2,key3
            env_keys = os.environ.get("PLACES_API_KEYS", "")
            keys = [k.strip() for k in env_keys.split(",") if k.strip()]
        for i, k in enumerate(keys):
            self.api_keys.append(KeyState(k, f"key{i+1}"))

        self.http_client: Optional[httpx.AsyncClient] = None

    def _next_key_for_method(self, method: str) -> Optional[KeyState]:
        """Round-robin to the next available key for the given method."""
        n = len(self.api_keys)
        for _ in range(n):
            ks = self.api_keys[self._rr_idx % n]
            self._rr_idx += 1
            if ks.is_available(method):
                return ks
        return None

    def _all_exhausted(self, method: str) -> bool:
        return all(not ks.is_available(method) for ks in self.api_keys)

    def _any_available(self, method: str) -> bool:
        return any(ks.is_available(method) for ks in self.api_keys)

    async def _init(self):
        """Initialize HTTP client + PG connection."""
        self.http_client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
        self.pg_conn = _connect_pg()
        self.pg_conn.autocommit = True
        log.info("PG connected: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
        log.info("Loaded %d API keys: %s", len(self.api_keys),
                 ", ".join(ks.label for ks in self.api_keys))

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

    def _fetch_uncrawled(self, limit: int = URL_FETCH_BATCH) -> list[dict]:
        """Fetch uncrawled URLs with business names from PG.
        Returns list of {url, name, chij_id} dicts.
        """
        self._pg_reconnect()
        if not self.pg_conn:
            return []
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT s.payload->>'url' AS url,
                           s.payload->>'name' AS name,
                           s.id
                    FROM scraper.gmaps_search_results s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM scraper.gmaps_listings l
                        WHERE l.source_url = s.payload->>'url'
                    )
                      AND s.payload->>'name' IS NOT NULL
                    ORDER BY s.updated_at DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
        except Exception as e:
            log.error("PG URL fetch failed: %s", e)
            self.pg_conn = None
            return []

        results = []
        for url, name, sr_id in rows:
            if not url or not url.startswith("https://www.google.com/maps/"):
                continue
            m = CHIJ_RE.search(url)
            if not m:
                continue
            results.append({
                "url": url,
                "name": name,
                "chij_id": m.group(0),
                "sr_id": sr_id,
            })
        return results

    async def _getplace(self, key: str, place_id: str) -> dict | None:
        """Call GetPlace API. Returns parsed place dict or None."""
        headers = {
            "X-Goog-Api-Key": key,
            "X-Goog-FieldMask": FIELD_MASK_GETPLACE,
            "Accept-Language": "en",
        }
        url = GETPLACE_URL.format(place_id=place_id)
        try:
            r = await self.http_client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                return {"_error": "429", "_body": r.text[:300]}
            else:
                log.debug("GetPlace %s status=%s", place_id[:20], r.status_code)
                return None
        except Exception as e:
            log.debug("GetPlace exception: %s", e)
            return None

    async def _text_search(self, key: str, query: str) -> list[dict]:
        """Call Text Search API. Returns list of place dicts."""
        headers = {
            "X-Goog-Api-Key": key,
            "X-Goog-FieldMask": FIELD_MASK_TEXT_SEARCH,
            "Content-Type": "application/json",
        }
        body = {"textQuery": query, "languageCode": "en"}
        try:
            r = await self.http_client.post(TEXT_SEARCH_URL, headers=headers,
                                           json=body, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json().get("places", [])
            elif r.status_code == 429:
                return [{"_error": "429"}]
            else:
                log.debug("TextSearch '%s' status=%s", query[:30], r.status_code)
                return []
        except Exception as e:
            log.debug("TextSearch exception: %s", e)
            return []

    def _normalize_place(self, place: dict, source_url: str, chij_id: str) -> Optional[dict]:
        """Convert a Places API response dict into the gmaps_listings row format.
        This mirrors what listing_details.py expects in _map_row().
        """
        if not place or place.get("_error"):
            return None

        display_name = place.get("displayName", {}).get("text", "")
        if not display_name:
            return None

        # Extract lat/lng
        loc = place.get("location", {})
        lat = loc.get("latitude")
        lng = loc.get("longitude")

        # plusCode
        plus_code = place.get("plusCode", {}).get("globalCode")

        # Phone (prefer international )
        phone = place.get("internationalPhoneNumber") or place.get("nationalPhoneNumber")

        # Website
        website = place.get("websiteUri")

        # Rating
        rating = place.get("rating")
        review_count = place.get("userRatingCount")

        # Address
        address = place.get("formattedAddress")

        # Category (first type that isn't generic)
        types = place.get("types", [])
        generic_types = {"point_of_interest", "establishment", "food"}
        category = None
        for t in types:
            if t not in generic_types:
                category = t.replace("_", " ").title()
                break
        if not category and types:
            category = types[0].replace("_", " ").title()

        return {
            "place_id": place.get("id", chij_id),
            "source_url": source_url,
            "key_value": source_url,
            "source_type": "gmaps_listing",
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
            "_chij_id": chij_id,
        }

    def _upsert_listing(self, item: dict):
        """Upsert a single listing into gmaps_listings. Mirrors listing_details.py SQL."""
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
                        source_url = EXCLUDED.source_url,
                        source_type = EXCLUDED.source_type,
                        name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        rating = EXCLUDED.rating,
                        review_count = EXCLUDED.review_count,
                        address = EXCLUDED.address,
                        phone = EXCLUDED.phone,
                        website = EXCLUDED.website,
                        booking_url = EXCLUDED.booking_url,
                        social_links = COALESCE(EXCLUDED.social_links, gmaps_listings.social_links),
                        plus_code = EXCLUDED.plus_code,
                        is_claimed = EXCLUDED.is_claimed,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        crawl_retry_count = EXCLUDED.crawl_retry_count,
                        crawl_pages_processed = EXCLUDED.crawl_pages_processed,
                        sector_id = COALESCE(EXCLUDED.sector_id, gmaps_listings.sector_id),
                        classification_confidence = COALESCE(EXCLUDED.classification_confidence, gmaps_listings.classification_confidence),
                        classification_method = COALESCE(EXCLUDED.classification_method, gmaps_listings.classification_method),
                        classified_at = COALESCE(EXCLUDED.classified_at, gmaps_listings.classified_at),
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
            return True
        except psycopg.errors.UniqueViolation:
            return True  # already in DB
        except Exception as e:
            log.error("Upsert failed for %s: %s", item["source_url"][:60], e)
            return False

    async def _process_one(self, record: dict) -> bool:
        """Process a single uncrawled listing.
        Try GetPlace first (most accurate), fall back to Text Search.
        Returns True on success.
        """
        chij_id = record["chij_id"]
        url = record["url"]
        name = record["name"]

        # PHONE SAFETY: Never type/log the API key value itself.

        # Strategy 1: GetPlace (most accurate)
        key_state = self._next_key_for_method("getplace")
        if key_state:
            result = await self._getplace(key_state.key, chij_id)
            if result and not result.get("_error"):
                key_state.request_count["getplace"] += 1
                item = self._normalize_place(result, url, chij_id)
                if item:
                    item["_payload"] = {
                        **result, "_method": "getplace", "_chij_id": chij_id,
                        "_source_url": url, "business_name": name,
                    }
                    if self._upsert_listing(item):
                        self.total_success += 1
                        self.method_stats["getplace"] += 1
                        return True
            elif result and result.get("_error") == "429":
                key_state.mark_exhausted("getplace")
                # Fall through to text search for this record

        # Strategy 2: Text Search fallback
        key_state = self._next_key_for_method("text_search")
        if key_state:
            query = f"{name} Bangladesh"
            places = await self._text_search(key_state.key, query)
            if places and not places[0].get("_error"):
                key_state.request_count["text_search"] += 1
                # Try to find exact match by ChIJ ID
                matched = None
                for p in places:
                    if p.get("id") == chij_id:
                        matched = p
                        break
                # Fall back to name match
                if not matched:
                    best = places[0]
                    best_name = best.get("displayName", {}).get("text", "").lower()
                    if name and (name.lower() in best_name or best_name in name.lower()):
                        matched = best

                if matched:
                    item = self._normalize_place(matched, url, chij_id)
                    if item:
                        item["_payload"] = {
                            **matched, "_method": "text_search",
                            "_chij_id": chij_id, "_source_url": url,
                            "business_name": name, "_search_query": query,
                        }
                        if self._upsert_listing(item):
                            self.total_success += 1
                            self.method_stats["text_search_fallback"] += 1
                            return True
                else:
                    self.method_stats["no_match"] += 1
            elif places and places[0].get("_error") == "429":
                key_state.mark_exhausted("text_search")

        # Both strategies failed for this record
        self.total_failures += 1
        return False

    async def _process_batch(self, records: list[dict]):
        """Process a batch of records with controlled concurrency."""
        sem = asyncio.Semaphore(CONCURRENCY)

        async def _bounded(rec):
            async with sem:
                return await self._process_one(rec)

        results = await asyncio.gather(
            *(_bounded(rec) for rec in records),
            return_exceptions=True
        )
        self.total_processed += len(records)
        for rec, result in zip(records, results):
            if isinstance(result, Exception):
                log.error("Exception processing %s: %s", rec["url"][:60], result)
                self.total_failures += 1

    def _heartbeat(self):
        """Log stats every 60 seconds."""
        now = time.monotonic()
        if now - self.last_heartbeat < 60:
            return
        elapsed = now - self.last_heartbeat
        delta = self.total_success - self.last_heartbeat_count
        rate = delta / elapsed * 3600 if elapsed > 0 else 0
        total_cap = sum(1 for ks in self.api_keys if ks.is_available("getplace")) \
                   + sum(1 for ks in self.api_keys if ks.is_available("text_search"))
        log.info("heartbeat processed=%d success=%d fail=%d rate=%d/hr keys_available=%d/%d "
                 "methods{g:%d t:%d t_fb:%d no_match:%d}",
                 self.total_processed, self.total_success, self.total_failures,
                 int(rate), total_cap, len(self.api_keys) * 2,
                 self.method_stats["getplace"],
                 self.method_stats["text_search"],
                 self.method_stats["text_search_fallback"],
                 self.method_stats["no_match"])
        self.last_heartbeat = now
        self.last_heartbeat_count = self.total_success

    async def run(self):
        """Main eternal loop."""
        await self._init()

        log.info("Places API daemon started with %d keys", len(self.api_keys))
        log.info("Concurrency: %d, URL batch: %d", CONCURRENCY, URL_FETCH_BATCH)

        while not self.shutdown_requested:
            try:
                self._heartbeat()

                # Check if everything is exhausted
                if self._all_exhausted("getplace") and self._all_exhausted("text_search"):
                    log.warning("All keys exhausted for both methods. Sleeping %ds...",
                                ALL_EXHAUSTED_SLEEP)
                    await asyncio.sleep(ALL_EXHAUSTED_SLEEP)
                    continue

                # Skip fetch if already exhausted — sleep first
                if self._all_exhausted("getplace") and self._all_exhausted("text_search"):
                    log.warning("All keys exhausted for both methods. Sleeping %ds...", ALL_EXHAUSTED_SLEEP)
                    await asyncio.sleep(ALL_EXHAUSTED_SLEEP)
                    continue
                # Fetch a batch of uncrawled URLs
                records = self._fetch_uncrawled()
                if not records:
                    log.info("No uncrawled URLs remaining. Sleeping 60s...")
                    await asyncio.sleep(60)
                    continue

                # Process the batch
                t0 = time.time()
                await self._process_batch(records)
                elapsed = time.time() - t0
                log.info("Batch done: %d records in %.1fs (%.1f/s) | total: %d ok, %d fail",
                         len(records), elapsed, len(records)/elapsed if elapsed > 0 else 0,
                         self.total_success, self.total_failures)

                # Small delay between batches to avoid hammering
                await asyncio.sleep(0.5)

            except Exception as e:
                log.error("Loop error: %s", e, exc_info=True)
                await asyncio.sleep(10)

        # Shutdown
        log.info("Shutdown requested. Total: %d processed, %d success, %d fail",
                 self.total_processed, self.total_success, self.total_failures)
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
    """Load YAML config."""
    import yaml
    with open(path) as f:
        return yaml.safe_load(f)


async def main():
    config = load_config(str(CONFIG_PATH))
    daemon = PlacesAPIDaemon(config)
    install_signal_handlers(daemon)
    await daemon.run()


if __name__ == "__main__":
    asyncio.run(main())
