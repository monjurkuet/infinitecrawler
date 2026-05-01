# Configuration Reference

The framework uses YAML files to define scraper behavior. This allows for rapid iteration without code changes.

## Schema Reference

### Root Object

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `name` | string | Yes | Human-readable name of the scraper |
| `content_type` | enum | Yes | `dynamic` (nodriver) or `listing_crawler` |
| `browser_automation` | string | No | Browser engine (default: `nodriver`) |
| `headless` | boolean | No | Run browser headless (default: true) |

### Input Section

Loads URLs or queries from a source.

```yaml
input:
  strategy: "file_url_loader"  # Required: strategy name
  config:
    file_path: "input/urls.txt"  # Path to input file
    deduplicate: true  # Skip duplicate URLs/queries
```

### Queue Section

Manages processing state with Redis.

```yaml
queue:
  strategy: "redis_queue"  # Required: strategy name
  config:
    host: "localhost"  # Redis host
    port: 6379          # Redis port
    db: 0               # Redis database
    keys:
      pending: "scraper:pending"      # Queue key
      processing: "scraper:processing" # In-progress key
      completed: "scraper:completed"   # Completed key
      failed: "scraper:failed"        # Failed key
    visibility_timeout: 300  # Seconds before requeue
```

### Selectors

Maps data fields to CSS selectors.

```yaml
selectors:
  items: "a.hfpxzc"              # Container selector
  fields:
    name: "aria-label"           # Text or attribute name
    source_url: "href"           # Extract from href attribute
```

### Pagination

Controls how the scraper navigates.

**Infinite Scroll:**
```yaml
pagination_strategy: "infinite_scroll"
pagination:
  container: "div[role='feed']"        # Scrollable container
  scroll_script: "..."                 # Custom scroll JS
  max_scroll_attempts: 500           # Safety limit
  items_selector: "a.hfpxzc"           # Items to count
```

**Next Button:**
```yaml
pagination_strategy: "next_button"
pagination:
  next_button_selector: "a.next"      # Pagination button
  max_pages: 50                       # Maximum pages
```

### Output

Controls where data is saved.

**Single Output:**
```yaml
output_strategy: "jsonl_file"
output:
  file_path: "output/data_{query}.jsonl"
  max_results: 1000
```

**MongoDB:**
```yaml
output_strategy: "mongodb"
output:
  uri: "mongodb://localhost:27017"
  database: "scraping"
  collection: "results"
  max_results: 10000
```

**MongoDB Upsert:**
```yaml
output_strategy: "mongodb_upsert"
output:
  uri: "mongodb://localhost:27017"
  database: "scraping"
  collection: "results"
  key_field: "source_url"  # Deduplicate by this field
  max_results: 10000
```

**Composite (Multiple Outputs):**
```yaml
output_strategy: "composite"
output:
  strategies:
    - strategy: "mongodb_upsert"
      config:
        uri: "mongodb://localhost:27017"
        database: "scraping"
        collection: "results"
        key_field: "source_url"
    - strategy: "jsonl_file"
      config:
        file_path: "output/results.jsonl"
```

### Workers

Parallel processing settings.

```yaml
workers:
  count: 3                    # Parallel workers
  max_consecutive_errors: 5    # Stop after N errors
  max_pages_per_session: 100   # Restart browser after N pages
```

### Rate Limiting

```yaml
rate_limiting:
  between_requests:
    - 5   # Min seconds
    - 15  # Max seconds
  distribution: "random"  # random, normal, fixed

rate_limit: 2  # Simple delay between requests
```

## Example Configuration (Search Scraper)

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

# Pagination
pagination_strategy: "infinite_scroll"
pagination:
  container: "div[role='feed']"
  scroll_script: "document.querySelector('div[role=\"feed\"]').scrollTo(0, document.querySelector('div[role=\"feed\"]').scrollHeight)"
  max_scroll_attempts: 500

# Extraction
extraction_strategy: "generic_selector"
selectors:
  items: "a.hfpxzc"
  fields:
    name: "aria-label"
    source_url: "href"

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

# Settings
search_url_template: "https://www.google.com/maps/search/{query}/"
rate_limit: 2
workers:
  count: 3
  max_consecutive_errors: 5
  max_pages_per_session: 100
```

## Example Configuration (Listing Crawler)

```yaml
# config/gmaps_listings_working.yaml

name: "Google Maps Listing Crawler"
content_type: "listing_crawler"

browser:
  automation: "nodriver"
  headless: true

input:
  strategy: "file_url_loader"
  config:
    file_path: "output/gmaps_urls.txt"
    deduplicate: true

queue:
  strategy: "redis_queue"
  config:
    host: "localhost"
    port: 6379
    keys:
      pending: "gmaps:pending"
      completed: "gmaps:completed"
      failed: "gmaps:failed"

navigation:
  strategy: "tab_navigator"
  config:
    tabs:
      - name: "overview"
        selector: '[role="tab"][aria-selected="true"]'
        required: true

extraction:
  strategy: "multi_step"
  config:
    steps:
      - action: "extract"
        fields:
          name:
            selector: "h1"
            type: "text"

output:
  strategy: "jsonl_file"
  secondary_output:
    strategy: "mongodb"
    config:
      database: "scraping"
      collection: "gmaps_listings"

workers:
  count: 3
  max_pages_per_session: 100
```
