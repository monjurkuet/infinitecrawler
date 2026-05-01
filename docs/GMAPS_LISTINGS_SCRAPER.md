# Google Maps Listing Scraper

A production-grade scraper for extracting business listings from Google Maps. Extracts contact info, ratings, reviews, categories, and more.

## Quick Start

```bash
# Activate virtual environment
source .venv/bin/activate

# Run with working configuration
python main.py --config config/gmaps_listings_working.yaml

# Run headless (no browser window)
python main.py --config config/gmaps_listings_working.yaml --headless

# Run 4 parallel crawler instances
uv run python scripts/run_listing_crawlers.py --instances 4 --config config/gmaps_listings_working.yaml
```

## What It Extracts

| Field | Description | Example |
|-------|-------------|---------|
| `name` | Business name | "Akademy of Entrepreneurs" |
| `category` | Business type | "Marketing agency" |
| `rating` | Star rating | "4.5" |
| `review_count` | Number of reviews | "42" |
| `address` | Physical address | "123 Main St, City, State" |
| `phone` | Phone number | "+1 555-123-4567" |
| `website` | Website URL | "https://example.com" |
| `booking_url` | Booking/scheduling link | "https://calendly.com/..." |
| `is_claimed` | Claimed by owner | true/false |
| `plus_code` | Google plus code | "ABC1+DEF City" |
| `latitude` | Latitude coordinate | "33.5438227" |
| `longitude` | Longitude coordinate | "-112.046985" |

## Requirements

### System Dependencies
- **Python 3.12+**
- **Google Chrome** (or Chromium)
- **Redis** (for queue management)
- **PostgreSQL** (for output storage)

### Python Dependencies
```bash
uv pip install -e .
uv pip install redis psycopg[binary]
```

## Configuration

### Main Configuration File

```yaml
# config/gmaps_listings_working.yaml

name: "Google Maps Listing Crawler"
content_type: "listing_crawler"

browser:
  automation: "nodriver"
  headless: true  # Set to false for debugging

input:
  strategy: "postgresql_uncrawled_gmaps"
  config:
    database: "infinitecrawler"
    schema: "scraper"
    search_results_table: "gmaps_search_results"
    listings_table: "gmaps_listings"
    source_url_field: "source_url"
    batch_size: 1000

queue:
  strategy: "redis_queue"
  config:
    host: "localhost"
    port: 6379
    ignore_completed_on_enqueue: true
    keys:
      pending: "gmaps:pending"
      completed: "gmaps:completed"
      failed: "gmaps:failed"

extraction:
  strategy: "multi_step"
  config:
    steps:
      - action: "extract"
        fields:
          name:
            selector: "h1"
            type: "text"
          rating:
            selector: "span.ceNzKf[aria-label*='stars']"
            type: "attribute"
            attribute: "aria-label"
            regex: "([0-9.]+)"
          # ... more fields
  retry:
    enabled: true
    attempts: 3
    delay: 2
    backoff: "exponential"
  timeouts:
    page_load: 10
    element: 5

output:
  strategy: "jsonl_file"
  config:
    file_path: "output/gmaps_listings.jsonl"

secondary_output:
  strategy: "postgresql_listing_upsert"
  config:
    database: "infinitecrawler"
    schema: "scraper"
    table: "gmaps_listings"
    key_field: "place_id"
    source_type: "gmaps_listing"
    recreate_table: false

rate_limiting:
  between_requests:
    - 8  # minimum seconds
    - 15 # maximum seconds
  distribution: "random"

workers:
  count: 3
  max_consecutive_errors: 5
  max_pages_per_session: 100
```

### Parallel Execution

For higher throughput, run 4 separate crawler processes against the same Redis queue using the launcher script:

```bash
uv run python scripts/run_listing_crawlers.py --instances 4 --config config/gmaps_listings_working.yaml
```

This uses process-level parallelism. It does not rely on `workers.count` for concurrency, because the current implementation runs one browser/extraction pipeline per process.

Each process gets its own `--instance-label` so logs are easy to distinguish.

Operational notes:

- each instance launches its own Chrome process
- all instances share the same Redis queue keys
- all instances upsert into `scraper.gmaps_listings`
- throughput improves, but CPU, memory, and Google Maps rate limits may become the limiting factor

### Database-Backed Listing Input

The listing crawler now reads directly from PostgreSQL search results and processes only uncrawled listing URLs.

An URL is considered uncrawled when:

- the search result has a `source_url`
- no row exists in `scraper.gmaps_listings` with the same `source_url`

Representative query shape:

```sql
SELECT DISTINCT sr.payload->>'source_url' AS source_url
FROM scraper.gmaps_search_results sr
LEFT JOIN scraper.gmaps_listings gl
  ON gl.source_url = sr.payload->>'source_url'
WHERE sr.payload->>'source_url' IS NOT NULL
  AND gl.source_url IS NULL
ORDER BY source_url;
```

If you still want to seed URLs manually for debugging, `file_url_loader` remains available, but it is no longer the normal workflow.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Google Maps Listing Scraper                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │   Input     │───▶│    Queue     │───▶│    Workers       │  │
│  │  (URLs)     │    │  (Redis)    │    │  (3 parallel)   │  │
│  └──────────────┘    └──────────────┘    └────────┬─────────┘  │
│                                                   │             │
│                                                   ▼             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Browser Manager                        │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │                   nodriver                          │ │   │
│  │  │  - Headless Chrome automation                       │ │   │
│  │  │  - Automatic retry on failures                     │ │   │
│  │  │  - Browser restart every 100 pages                  │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                    │
│                           ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                 Extraction Strategy                       │   │
│  │  ┌──────────────────────────────────────────────────┐ │   │
│  │  │  Multi-Step Pipeline                                │ │   │
│  │  │  1. Extract core fields (name, rating, address)  │ │   │
│  │  │  2. Extract contact info (phone, website)           │ │   │
│  │  │  3. Extract metadata (coordinates, category)        │ │   │
│  │  │  4. Check claimed status & booking links            │ │   │
│  │  │  5. Retry failed fields with fallback selectors    │ │   │
│  │  └──────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                    │
│                           ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                   Output Handlers                        │   │
│  │  ┌─────────────────┐    ┌─────────────────────────┐    │   │
│  │  │  JSONL File      │    │  PostgreSQL Table      │    │   │
│  │  │  (backup)        │    │  (primary storage)     │    │   │
│  │  └─────────────────┘    └─────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Components

#### 1. Input Strategy (`postgresql_uncrawled_gmaps`)
- Loads listing URLs from `scraper.gmaps_search_results`
- Filters out any URL that already exists in `scraper.gmaps_listings`
- Deduplicates by SQL `DISTINCT`

#### 2. Queue Strategy (`redis_queue`)
- Manages URL processing order
- Prevents duplicate processing
- Tracks completion/failure status
- Can be configured to ignore stale Redis `completed` state when PostgreSQL should be authoritative

#### 3. Browser Manager (`nodriver`)
- Controls Chrome browser
- Handles page navigation
- Manages browser lifecycle
- Auto-restart every 100 pages

#### 4. Extraction Strategy (`multi_step`)
- Multi-stage data extraction
- Retry logic with exponential backoff
- Fallback selectors

#### 5. Output Strategies
- **JSONL File**: Line-delimited JSON backup
- **PostgreSQL**: Canonical storage with typed columns plus raw JSONB payload

## How It Works

### Extraction Pipeline

1. **Page Load**
   ```
   URL → Browser → Wait for page load (10s timeout)
   ```

2. **Field Extraction** (with retry)
   ```
   For each field:
   1. Try primary selector
   2. If fail, wait exponential backoff (2s, 4s, 8s)
   3. Try fallback selectors
   4. If all fail, log warning and continue
   ```

3. **URL Processing** (with restart)
   ```
   For each URL:
   1. Navigate to page
   2. Extract all fields
   3. If fail, retry up to 3 times
   4. On retry, restart browser
   5. Save to outputs
   ```

### Retry Logic

#### Element-Level Retry
```python
# 3 attempts with exponential backoff
for attempt in range(3):
    for selector in [primary, fallback1, fallback2]:
        try:
            value = await extract(selector)
            if value: return value
        except:
            continue
    await asyncio.sleep(2 ** attempt)  # 2s, 4s, 8s
```

#### URL-Level Retry
```python
# 3 attempts with browser restart
for attempt in range(3):
    try:
        await extract_all_fields()
        return True
    except:
        if attempt < 2:  # Restart before retry 2 & 3
            await restart_browser()
        await asyncio.sleep(5 * (attempt + 1))
```

## Output Formats

### JSONL File (`output/gmaps_listings.jsonl`)

```jsonl
{"name": "Akademy of Entrepreneurs", "rating": "5.0", "review_count": "14", "address": "7301 N 16th St, Phoenix, AZ 85020", "phone": "+1 480-331-5207", "website": "https://example.com", "category": "Marketing agency", "is_claimed": true, "booking_url": "https://calendly.com/...", "latitude": "33.5438227", "longitude": "-112.046985", "source_url": "https://google.com/maps/place/..."}
{"name": "ABM Parking Services", "rating": "1.0", "review_count": "5", "address": "211B Elm St, Rockford, IL", "phone": "+1 815-968-5294", "website": "https://park-rockford.com", "category": "Parking lot", "is_claimed": true, "latitude": "42.2703391", "longitude": "-89.0944358", "source_url": "https://google.com/maps/place/..."}
```

### PostgreSQL Table

The table is relational and typed. A representative row looks like this:

```sql
SELECT place_id, source_url, name, category, rating, review_count, address
FROM scraper.gmaps_listings;
```

The full raw record remains in `payload` as JSONB.

## Monitoring

### Redis Queue Stats
```bash
redis-cli KEYS gmaps:*
redis-cli LLEN gmaps:pending
redis-cli LLEN gmaps:completed
redis-cli LLEN gmaps:failed
```

To force a rerun, clear the listing-detail queue keys:

```bash
redis-cli DEL gmaps:pending gmaps:processing gmaps:completed gmaps:failed
```

### PostgreSQL Query Examples
```sql
SELECT COUNT(*) FROM scraper.gmaps_listings;

SELECT name, source_url
FROM scraper.gmaps_listings
WHERE is_claimed IS FALSE;

SELECT category, COUNT(*) AS count, AVG(rating) AS avg_rating
FROM scraper.gmaps_listings
GROUP BY category
ORDER BY count DESC;
```

## Troubleshooting

### Common Issues

#### 1. "No data extracted"
**Cause**: Page didn't load properly
**Solution**:
```bash
# Run in headed mode to see what's happening
python main.py --config config/gmaps_listings_working.yaml --headless false
```

#### 2. "Redis connection refused"
**Cause**: Redis not running
**Solution**:
```bash
# Start Redis
sudo systemctl start redis
# Or run Redis in container
docker run -p 6379:6379 redis:alpine
```

#### 3. "PostgreSQL connection failed"
**Cause**: PostgreSQL not running or wrong credentials
**Solution**:
```bash
# Start PostgreSQL
sudo systemctl start postgresql
# Or check URI in config
```

#### 4. Chrome crashes / "DevToolsActivePort"
**Cause**: Chrome installation issues
**Solution**:
```bash
# Install Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb

# Or use Chromium
sudo apt install chromium-browser
```

### Logs

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Performance Tuning

#### Increase Throughput
```yaml
workers:
  count: 5  # More parallel workers

rate_limiting:
  between_requests:
    - 3  # Faster between requests
    - 5
```

#### Reduce Memory
```yaml
workers:
  max_pages_per_session: 50  # Restart browser more often
```

#### Increase Reliability
```yaml
extraction:
  retry:
    attempts: 5  # More retries
    delay: 3

workers:
  max_consecutive_errors: 10  # Allow more errors before stopping
```

## Project Structure

```
infinitecrawler/
├── config/
│   ├── gmaps_listings_working.yaml    # Main config
│   └── gmaps_listings.yaml
├── strategies/
│   ├── extraction/
│   │   └── multi_step.py              # Extraction logic
│   ├── output/
│   │   ├── jsonl_file.py
│   │   └── postgresql.py              # PostgreSQL output
│   └── navigation/
│       └── tab_navigator.py
├── scrapers/
│   └── listing_crawler.py              # Main crawler
├── base/
│   ├── browser_manager.py              # Browser control
│   └── strategies.py                   # Base classes
├── main.py                             # Entry point
├── output/
│   └── gmaps_urls.txt                 # Input URLs
└── docs/
    └── GMAPS_LISTINGS_SCRAPER.md       # This file
```

## License

See project root LICENSE file.
