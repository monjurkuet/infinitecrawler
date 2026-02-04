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
- **MongoDB** (for output storage)

### Python Dependencies
```bash
uv pip install -e .
uv pip install pymongo redis
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
  strategy: "file_url_loader"
  config:
    file_path: "output/gmaps_urls.txt"  # Your input URLs
    deduplicate: true
    batch_size: 1000

queue:
  strategy: "redis_queue"
  config:
    host: "localhost"
    port: 6379
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
  strategy: "mongodb"
  config:
    uri: "mongodb://localhost:27017"
    database: "scraping"
    collection: "gmaps_listings"

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

### Input File Format

Create a text file with one Google Maps URL per line:

```text
output/gmaps_urls.txt
```

Example:
```
https://www.google.com/maps/place/ABM+Parking+Services/data=!4m7!3m6!1s0x88089525299ea079:0x4654c1299cd61e25!8m2!3d42.2703391!4d-89.0944358!16s%2Fg%2F11t2q75g6t
https://www.google.com/maps/place/That!+Company/data=!4m7!3m6!1s0x88e7ead80dcd345d:0x1e15a3c2ecb21448!8m2!3d28.5471243!4d-81.379912
https://www.google.com/maps/place/Darwin+AI/data=!4m7!3m6!1s0x880e2d535715b269:0x72da2df56c5577a0!8m2!3d41.8774387!4d-87.6356423
```

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
│  │  │  JSONL File      │    │  MongoDB Collection     │    │   │
│  │  │  (backup)        │    │  (primary storage)     │    │   │
│  │  └─────────────────┘    └─────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Components

#### 1. Input Strategy (`file_url_loader`)
- Loads URLs from text file
- Deduplicates URLs
- Batches for queue

#### 2. Queue Strategy (`redis_queue`)
- Manages URL processing order
- Prevents duplicate processing
- Tracks completion/failure status

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
- **MongoDB**: Primary storage with upsert

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

### MongoDB Collection

```javascript
// scraping.gmaps_listings

{
  "_id": ObjectId("..."),
  "source_url": "https://google.com/maps/place/...",
  "name": "Akademy of Entrepreneurs",
  "category": "Marketing agency",
  "rating": "5.0",
  "review_count": "14",
  "address": "7301 N 16th St, Phoenix, AZ 85020",
  "phone": "+1 480-331-5207",
  "website": "https://example.com",
  "booking_url": "https://calendly.com/...",
  "is_claimed": true,
  "plus_code": "GXV3+G6 Phoenix, Arizona, USA",
  "latitude": "33.5438227",
  "longitude": "-112.046985",
  "_extracted_at": ISODate("2026-02-04T10:00:00Z"),
  "_crawl_meta": {
    "pages_processed": 5,
    "retry_count": 0
  }
}
```

## Monitoring

### Redis Queue Stats
```bash
redis-cli KEYS gmaps:*
redis-cli LLEN gmaps:pending
redis-cli LLEN gmaps:completed
redis-cli LLEN gmaps:failed
```

### MongoDB Query Examples
```javascript
// Count total
db.gmaps_listings.countDocuments({})

// Find unclaimed businesses
db.gmaps_listings.find({ is_claimed: false })

// Find businesses with rating >= 4
db.gmaps_listings.find({ $expr: { $gte: [{ $toDouble: "$rating" }, 4] } })

// Aggregate by category
db.gmaps_listings.aggregate([
  { $group: { _id: "$category", count: { $sum: 1 }, avgRating: { $avg: { $toDouble: "$rating" } } } }
])
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

#### 3. "MongoDB connection failed"
**Cause**: MongoDB not running or wrong URI
**Solution**:
```bash
# Start MongoDB
sudo systemctl start mongod
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
│   │   └── mongodb.py                 # MongoDB output
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
