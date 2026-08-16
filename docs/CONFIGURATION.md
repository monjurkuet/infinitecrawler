# Configuration Reference

The framework uses YAML files to define scraper behavior. Configs containing API keys (`places_api_daemon.yaml`, `nearby_scanner.yaml`) are gitignored — `.yaml.example` templates are tracked, and real keys are provided via the `PLACES_API_KEYS` environment variable.

## Schema Reference

### Root Object

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `name` | string | Yes | Human-readable name |
| `content_type` | enum | Yes | `dynamic` (GMaps search) or `listing_crawler` |

### Browser-based Daemon Config (`gmaps_listings_working.yaml`)

Used by `daemons/listing_daemon.py` (legacy browser daemon).

```yaml
name: Google Maps Listing Crawler (Working Config)
content_type: listing_crawler
browser:
  automation: pinchtab
  headless: true
  page_wait_seconds: 5.0
queue:
  strategy: redis_queue
  config:
    host: localhost
    port: 6379
    db: 0
    keys:
      pending: gmaps:pending
      processing: gmaps:processing
      completed: gmaps:completed
      failed: gmaps:failed
    visibility_timeout: 300
output:
  strategy: postgresql_listing_upsert
  config:
    host: "/var/run/postgresql"
    port: 5432
    user: postgres
    database: infinitecrawler
    schema: scraper
    table: gmaps_listings
    key_field: place_id
    source_type: gmaps_listing
    max_results: 100000
    recreate_table: false
rate_limiting:
  page_load: [0.5, 1.0]
  between_requests: [1, 2]
  between_tabs: [0.3, 0.8]
  distribution: random
  max_retries: 3
  retry_delay: [30, 60]
workers:
  count: 3
  max_consecutive_errors: 15
  max_pages_per_session: 100
```

### Places API Daemon Config (`places_api_daemon.yaml`)

Used by `daemons/places_api_daemon.py` — enriches existing URLs via Places API (New) HTTP calls. No browser needed.

```yaml
# API keys — override with PLACES_API_KEYS env var (comma-separated).
# Each key from a separate Google Cloud project for independent 200/day quota.
api_keys:
  - "YOUR_API_KEY_1"
  - "YOUR_API_KEY_2"
  - "YOUR_API_KEY_3"
  - "YOUR_API_KEY_4"
  - "YOUR_API_KEY_5"

concurrency: 5           # parallel API requests per batch
batch_size: 200          # how many uncrawled URLs to fetch per loop iteration
exhausted_sleep: 600     # seconds to sleep when all keys exhausted
request_timeout: 15      # per-request timeout (seconds)

text_search:
  enabled: true
  country_suffix: "Bangladesh"   # appended to business name for search accuracy
  strict_match: false             # accept name matches if ChIJ ID doesn't match
```

### Nearby Scanner Config (`nearby_scanner.yaml`)

Used by `daemons/nearby_scanner_daemon.py` — discovers NEW places by grid-scanning cities with Nearby Search.

```yaml
# API keys — override with PLACES_API_KEYS env var (comma-separated)
api_keys:
  - "YOUR_API_KEY_1"  # ... up to 5 keys

radius_m: 2000          # radius per grid circle (meters). 2km = good coverage
concurrency: 5          # parallel API calls
cells_per_run: 50       # grid cells fetched per loop iteration

# City bounding boxes (north, south, east, west in decimal degrees)
# Each city gets a hex grid of overlapping circles covering its urban area.
cities:
  - name: "Dhaka"
    bbox: {north: 23.8967, south: 23.6400, east: 90.5600, west: 90.3200}
  - name: "Chittagong"
    bbox: {north: 22.4400, south: 22.1600, east: 91.8800, west: 91.7800}
  # ... see nearby_scanner.yaml.example for the full list of 29 cities
```

### Selectors (dynamic search configs)

```yaml
selectors:
  items: "a.hfpxzc"
  fields:
    name: "aria-label"
    source_url: "href"
```

### Pagination

```yaml
pagination_strategy: "infinite_scroll"
pagination:
  container: "div[role='feed']"
  scroll_script: "..."
  max_scroll_attempts: 500
  items_selector: "a.hfpxzc"
```

### Output Strategies

**PostgreSQL Upsert (search results):**
```yaml
output:
  strategy: "postgresql_upsert"
  config:
    database: "infinitecrawler"
    schema: "scraper"
    table: "gmaps_search_results"
    key_field: "source_url"
    max_results: 10000
```

**PostgreSQL Listing Details Upsert:**
```yaml
output:
  strategy: "postgresql_listing_upsert"
  config:
    database: "infinitecrawler"
    schema: "scraper"
    table: "gmaps_listings"
    key_field: "place_id"
    source_type: "gmaps_listing"
    recreate_table: false
```

### Workers

```yaml
workers:
  count: 3
  max_consecutive_errors: 15
  max_pages_per_session: 100
```

### Rate Limiting

```yaml
rate_limiting:
  between_requests:
    - 1
    - 2
  distribution: "random"

rate_limit: 2
```

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PG_HOST` | PostgreSQL host (socket path or TCP) | Required (no default — import fails without it) |
| `PG_PORT` | PostgreSQL port | 5432 |
| `PG_USER` | PostgreSQL user | postgres |
| `PG_PASSWORD` | PostgreSQL password | changeme |
| `PG_DB` | PostgreSQL database | infinitecrawler |
| `PLACES_API_KEYS` | Comma-separated Google Places API keys | (from config YAML if env not set) |
| `PLACES_EXHAUSTED_SLEEP` | Sleep seconds when all API keys exhausted | 600 |
| `NEARBY_EXHAUSTED_SLEEP` | Sleep seconds when all scanner keys exhausted | 600 |
| `FLUSH_INTERVAL_SEC` | Listing daemon flush interval | 5.0 |
| `FLUSH_ON_REQUIRED_FIELD` | Flush batch when phone/website found | 0 |
| `LOG_LEVEL` | Logging level | INFO |

## Grid Table Schema (`scraper.nearby_scan_grid`)

The Nearby Scanner daemon auto-creates this table on startup to track grid cell progress:

```sql
CREATE TABLE scraper.nearby_scan_grid (
    id BIGSERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    radius_m INTEGER NOT NULL DEFAULT 1000,
    batch_idx INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending | done | failed
    result_count INTEGER DEFAULT 0,
    error_msg TEXT,
    scanned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Unique constraint: (city, latitude, longitude, batch_idx)
-- Index on (status, city) for efficient pending-cell queries
```
