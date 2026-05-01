# Google Maps Search Scraper

A production-grade scraper for extracting business listings from Google Maps search results. Uses infinite scroll pagination to discover and extract business names and URLs. Supports both single query mode and batch processing via Redis queue.

## Quick Start

```bash
# Activate virtual environment
source .venv/bin/activate

# Create input file with queries
echo -e "restaurants in NYC\npizza in LA" > input/search_queries.txt

# Run in batch mode (process all queries from file)
python main.py --config config/google_maps.yaml

# Run with single query (CLI override)
python main.py --config config/google_maps.yaml --query "pizza in chicago"

# Run headless (no browser window)
python main.py --config config/google_maps.yaml --query "pizza in chicago" --headless
```

## Two Running Modes

### Mode 1: Batch Mode (Default)

Process multiple queries from `input/search_queries.txt` via Redis queue.

```bash
# 1. Edit queries file
nano input/search_queries.txt

# 2. Run (processes all queries from file)
python main.py --config config/google_maps.yaml
```

### Mode 2: Single Query Mode (CLI Override)

Process a single query, bypasses Redis queue.

```bash
python main.py --config config/google_maps.yaml --query "restaurants in NYC"
```

---

## What It Extracts

| Field | Description | Example |
|-------|-------------|---------|
| `name` | Business name | "Joe's Pizza" |
| `source_url` | Google Maps URL | "https://www.google.com/maps/place/..." |
| `query` | Search term used | "pizza in chicago" |
| `source` | Data source identifier | "google_maps_search" |
| `_extracted_at` | Timestamp of extraction | "2026-02-04T10:00:00Z" |

---

## Requirements

### System Dependencies
- **Python 3.12+**
- **Google Chrome** (or Chromium)
- **Redis** (for queue management)
- **MongoDB** (for output storage, optional)

### Python Dependencies
```bash
uv pip install -e .
uv pip install pymongo redis
```

---

## Configuration

### Main Configuration File

```yaml
# config/google_maps.yaml

name: "Google Maps Search"
content_type: "dynamic"
browser_automation: "nodriver"
headless: true

# Input: Load queries from file
input:
  strategy: "file_url_loader"
  config:
    file_path: "input/search_queries.txt"
    deduplicate: true

# Queue: Track query processing
queue:
  strategy: "redis_queue"
  config:
    host: "localhost"
    port: 6379
    db: 0
    keys:
      pending: "gmaps_search:pending"
      processing: "gmaps_search:processing"
      completed: "gmaps_search:completed"
      failed: "gmaps_search:failed"
    visibility_timeout: 300

pagination_strategy: "infinite_scroll"
extraction_strategy: "generic_selector"

# Output: MongoDB + JSONL fallback
output_strategy: "composite"
output:
  strategies:
    - strategy: "mongodb_upsert"
      config:
        uri: "mongodb://localhost:27017"
        database: "scraping"
        collection: "gmaps_search_results"
        key_field: "source_url"
        max_results: 10000
    - strategy: "jsonl_file"
      config:
        file_path: "output/google_maps_{query}.jsonl"
        max_results: 10000

search_url_template: "https://www.google.com/maps/search/{query}/"
rate_limit: 2

# Workers: Parallel processing
workers:
  count: 3
  max_consecutive_errors: 5
  max_pages_per_session: 100
```

---

## Input File Format

Create `input/search_queries.txt` with one query per line:

```text
# Comments (lines starting with #)
# Empty lines are ignored

restaurants in NYC
pizza in LA
coffee shops in Seattle
plumbers in Chicago
dentists in Miami
gyms in Austin
bars in Denver
pubs in Portland
```

---

## Output Configuration Options

### MongoDB Only

```yaml
output_strategy: "mongodb_upsert"
output:
  uri: "mongodb://localhost:27017"
  database: "scraping"
  collection: "gmaps_search_results"
  key_field: "source_url"
```

### JSONL File Only

```yaml
output_strategy: "jsonl_file"
output:
  file_path: "output/google_maps_{query}.jsonl"
  max_results: 10000
```

### MongoDB + JSONL Fallback (Recommended)

```yaml
output_strategy: "composite"
output:
  strategies:
    - strategy: "mongodb_upsert"
      config:
        uri: "mongodb://localhost:27017"
        database: "scraping"
        collection: "gmaps_search_results"
        key_field: "source_url"
        max_results: 10000
    - strategy: "jsonl_file"
      config:
        file_path: "output/google_maps_{query}.jsonl"
        max_results: 10000
```

---

## Key Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `input.file_path` | Path to queries file | `input/search_queries.txt` |
| `queue.keys.pending` | Redis key for pending queries | `gmaps_search:pending` |
| `queue.keys.completed` | Redis key for completed | `gmaps_search:completed` |
| `queue.keys.failed` | Redis key for failed | `gmaps_search:failed` |
| `workers.count` | Parallel workers | 3 |
| `workers.max_pages_per_session` | Pages before browser restart | 100 |
| `search_url_template` | URL template with `{query}` | Google Maps search URL |
| `pagination_strategy` | How to load more results | `infinite_scroll` |
| `extraction_strategy` | How to extract data | `generic_selector` |
| `rate_limit` | Seconds between scroll operations | 2 |
| `max_scroll_attempts` | Maximum scroll attempts | 500 |
| `max_results` | Maximum results to extract | 10000 |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Google Maps Search Scraper                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. INPUT                                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  input/search_queries.txt                               │   │
│  │  (one query per line)                                  │   │
│  │                        ↓                                │   │
│  │  file_url_loader.load_urls()                            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                    │
│                            ↓                                    │
│  2. QUEUE (Redis)                                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  gmaps_search:pending     → processing → completed     │   │
│  │  gmaps_search:failed      (failed queries)             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                    │
│                            ↓                                    │
│  3. WORKERS (Parallel Processing)                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  For each worker:                                         │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │  Browser Manager (nodriver)                         │ │   │
│  │  │  ┌─────────────────────────────────────────────┐   │ │   │
│  │  │  │  1. Get query from queue                    │   │ │   │
│  │  │  │  2. Navigate to search URL                  │   │ │   │
│  │  │  │  3. Infinite scroll pagination              │   │ │   │
│  │  │  │  4. Extract business names + URLs           │   │ │   │
│  │  │  │  5. Write to outputs                        │   │ │   │
│  │  │  │  6. Mark as completed                       │   │ │   │
│  │  │  └─────────────────────────────────────────────┘   │ │   │
│  │  │  Restart browser every 100 pages                 │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                    │
│                            ↓                                    │
│  4. OUTPUT                                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  MongoDB (primary) + JSONL (fallback)                    │   │
│  │  Collection: scraping.gmaps_search_results                │   │
│  │  File: output/google_maps_{query}.jsonl                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Components

#### 1. Input Strategy (`file_url_loader`)
- Loads queries from text file
- Skips comments (#) and empty lines
- Optional deduplication

#### 2. Queue Strategy (`redis_queue`)
- Manages query processing state
- Tracks pending, processing, completed, failed
- Prevents duplicate processing

#### 3. Workers (Parallel Processing)
- Multiple workers process queries in parallel
- Each worker: browser → navigate → scrape → output
- Browser restart every 100 pages
- Error handling with retry limits

#### 4. Pagination Strategy (`infinite_scroll`)
- Scrolls to load more search results
- Detects end of results
- Configurable max scroll attempts

#### 5. Extraction Strategy (`generic_selector`)
- Simple CSS selector-based extraction
- Extracts name from `aria-label` attribute
- Extracts URL from `href` attribute

#### 6. Output Strategies
- **MongoDB**: Primary storage with upsert by `source_url`
- **JSONL**: Fallback on MongoDB failure
- **Composite**: Tries MongoDB first, falls back to JSONL

---

## How It Works

### Batch Mode (Default)

1. **Load Queries**
   ```
   Read input/search_queries.txt
   ↓
   file_url_loader.load_urls() → ["restaurants in NYC", "pizza in LA", ...]
   ```

2. **Enqueue Queries**
   ```
   queue.enqueue(queries)
   ↓
   Redis: LPUSH gmaps_search:pending "restaurants in NYC", ...
   ```

3. **Process Queue (Parallel Workers)**
   ```
   For each worker:
     while queue not empty:
       query = queue.dequeue()
       if query:
         navigate_to(search_url + query)
         scrape_all_results()
         output.write(items)
         queue.mark_completed(query)
   ```

4. **Output**
   ```
   For each extracted item:
     - Add metadata (query, source, timestamp)
     - Write to MongoDB (primary)
     - Write to JSONL (fallback on failure)
   ```

### Single Query Mode (CLI Override)

```
--query "restaurants in NYC"
↓
Skip input/queue
↓
Directly scrape single query
↓
Exit after completion
```

---

## Redis Queue Commands

```bash
# Check queue status
redis-cli LLEN gmaps_search:pending    # Waiting to process
redis-cli LLEN gmaps_search:processing # Currently processing
redis-cli LLEN gmaps_search:completed # Successfully processed
redis-cli LLEN gmaps_search:failed    # Failed (with error info)

# View completed queries
redis-cli SMEMBERS gmaps_search:completed

# View failed queries with errors
redis-cli HGETALL gmaps_search:failed

# Clear all queues (reset)
redis-cli DEL gmaps_search:pending gmaps_search:processing gmaps_search:completed gmaps_search:failed

# View pending queries (without removing)
redis-cli LRANGE gmaps_search:pending 0 -1
```

---

## Output Formats

### JSONL File (`output/google_maps_{query}.jsonl`)

```jsonl
{"name": "Joe's Pizza", "source_url": "https://www.google.com/maps/place/Joe's+Pizza/...", "query": "pizza in chicago", "source": "google_maps_search", "_updated_at": "2026-02-04T10:00:00Z"}
{"name": "Lou Malnati's", "source_url": "https://www.google.com/maps/place/Lou+Malnatis/...", "query": "pizza in chicago", "source": "google_maps_search", "_updated_at": "2026-02-04T10:00:01Z"}
{"name": "Giordano's", "source_url": "https://www.google.com/maps/place/Giordanos/...", "query": "pizza in chicago", "source": "google_maps_search", "_updated_at": "2026-02-04T10:00:02Z"}
```

### MongoDB Collection

```javascript
// scraping.gmaps_search_results

{
  "_id": ObjectId("..."),
  "name": "Joe's Pizza",
  "source_url": "https://www.google.com/maps/place/Joe's+Pizza/data=...",
  "query": "pizza in chicago",
  "source": "google_maps_search",
  "_updated_at": ISODate("2026-02-04T10:00:00Z")
}

// Upserted document (no duplicates by source_url)
{
  "_id": ObjectId("..."),
  "name": "Joe's Pizza Updated",
  "source_url": "https://www.google.com/maps/place/Joe's+Pizza/data=...",
  "query": "pizza in chicago",
  "source": "google_maps_search",
  "_updated_at": ISODate("2026-02-04T11:00:00Z")
}
```

---

## Monitoring

### Redis Queue Stats

```bash
# Quick status check
redis-cli KEYS gmaps_search:*

# Detailed stats
redis-cli LLEN gmaps_search:pending   # Pending
redis-cli LLEN gmaps_search:completed # Done
redis-cli LLEN gmaps_search:failed    # Failed
```

### MongoDB Query Examples

```javascript
// Count total results
db.gmaps_search_results.countDocuments({})

// Find results for specific query
db.gmaps_search_results.find({ query: "pizza in chicago" })

// Get unique businesses
db.gmaps_search_results.distinct("source_url")

// Count by query
db.gmaps_search_results.aggregate([
  { $group: { _id: "$query", count: { $sum: 1 } } }
])

// Sample businesses by query
db.gmaps_search_results.aggregate([
  { $match: { query: "pizza in chicago" } },
  { $sample: { size: 10 } }
])

// Find duplicate URLs (shouldn't happen with upsert)
db.gmaps_search_results.aggregate([
  { $group: { _id: "$source_url", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } }
])
```

### Check Output Files

```bash
# List all output files
ls output/google_maps_*.jsonl

# Count lines in a file
wc -l output/google_maps_pizza_in_chicago.jsonl

# View first entries
head -20 output/google_maps_pizza_in_chicago.jsonl

# Search for specific business
grep "Joe's Pizza" output/google_maps_pizza_in_chicago.jsonl
```

---

## Troubleshooting

### Common Issues

#### 1. "No items extracted"
**Cause**: Page didn't load or selectors changed
**Solution**:
```bash
# Run in headed mode
python main.py --config config/google_maps.yaml --query "pizza" --headless false
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

#### 3. "MongoDB connection failed"
**Cause**: MongoDB not running or wrong URI
**Solution**:
```bash
# Start MongoDB
sudo systemctl start mongod
# Or use JSONL only
# Change output_strategy to "jsonl_file"
```

#### 4. "Scrolling stops before extracting all results"
**Cause**: Rate limiting or page responsiveness
**Solution**:
```yaml
rate_limit: 3  # Wait longer between scrolls
pagination:
  max_scroll_attempts: 1000
```

#### 5. "Chrome crashes / DevToolsActivePort"
**Cause**: Chrome installation issues
**Solution**:
```bash
# Install Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb
```

#### 6. "Too many consecutive errors"
**Cause**: Multiple failures in a row
**Solution**:
```yaml
workers:
  max_consecutive_errors: 10  # Allow more errors
```

### Logs

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## Performance Tuning

### Increase Throughput

```yaml
workers:
  count: 5  # More parallel workers

rate_limit: 1  # Faster scrolling

pagination:
  max_scroll_attempts: 1000  # More results
```

### Reduce Memory

```yaml
workers:
  max_pages_per_session: 50  # Restart browser more often

output:
  max_results: 1000  # Limit results per query
```

### More Reliable Extraction

```yaml
workers:
  max_consecutive_errors: 10  # Allow more errors before stopping

rate_limit: 3  # Wait longer between scrolls

pagination:
  max_scroll_attempts: 500
```

---

## Customization

### Changing Selectors

```yaml
selectors:
  items: "a.hfpxzc"  # CSS selector for business links
  fields:
    name: "aria-label"    # Attribute for business name
    source_url: "href"    # Attribute for URL
```

### Custom Search URL

```yaml
search_url_template: "https://www.google.com/maps/search/{query}/@40.7128,-74.0060,15z"
```

### Custom Output Collection

```yaml
output:
  strategies:
    - strategy: "mongodb_upsert"
      config:
        collection: "my_custom_collection"
        key_field: "source_url"
```

### Custom Redis Keys

```yaml
queue:
  config:
    keys:
      pending: "myapp:search:pending"
      processing: "myapp:search:processing"
      completed: "myapp:search:completed"
      failed: "myapp:search:failed"
```

---

## Project Structure

```
infinitecrawler/
├── config/
│   ├── google_maps.yaml              # Main config (input + queue + output)
│   └── yelp_example.yaml             # Example config
├── strategies/
│   ├── extraction/
│   │   └── generic_selector.py       # Extraction logic
│   ├── input/
│   │   └── file_url_loader.py        # Input strategy
│   ├── output/
│   │   ├── composite.py              # Composite output
│   │   ├── jsonl_file.py            # JSONL output
│   │   └── mongodb.py               # MongoDB output
│   ├── pagination/
│   │   └── infinite_scroll.py        # Pagination logic
│   └── queue/
│       └── redis_queue.py            # Queue management
├── scrapers/
│   └── dynamic_scraper.py            # Main dynamic scraper
├── base/
│   ├── browser_manager.py            # Browser control
│   └── strategies.py                # Base classes
├── input/
│   └── search_queries.txt           # Input queries file
├── main.py                           # Entry point
├── output/
│   └── google_maps_*.jsonl          # JSONL output files
└── docs/
    └── GMAPS_SEARCH_SCRAPER.md      # This file
```

---

## Differences from Listing Scraper

| Aspect | Search Scraper | Listing Scraper |
|--------|---------------|-----------------|
| **Input** | Queries from file | URLs from file |
| **Queue** | Redis (query tracking) | Redis (URL tracking) |
| **Processing** | Query → search URL → results | URL → full details |
| **Data** | Name + URL only | Full business details |
| **Pagination** | Infinite scroll | Tab navigation |
| **Use Case** | Lead discovery | Data enrichment |

---

## License

See project root LICENSE file.
