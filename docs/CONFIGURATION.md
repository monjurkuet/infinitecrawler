# Configuration Reference

The framework uses YAML files to define scraper behavior.

## Schema Reference

### Root Object

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `name` | string | Yes | Human-readable name |
| `content_type` | enum | Yes | `dynamic` (GMaps search) or `listing_crawler` |

### Input Section

For listing crawling, URLs are loaded directly from PostgreSQL:

```yaml
input:
  strategy: "postgresql_uncrawled_gmaps"
  config:
    database: "infinitecrawler"
    schema: "scraper"
    search_results_table: "gmaps_search_results"
    listings_table: "gmaps_listings"
    source_url_field: "source_url"
    batch_size: 1000
```

### Queue Section

```yaml
queue:
  strategy: "redis_queue"
  config:
    host: "localhost"
    port: 6379
    db: 0
    keys:
      pending: "gmaps:pending"
      processing: "gmaps:processing"
      completed: "gmaps:completed"
      failed: "gmaps:failed"
    visibility_timeout: 300
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

### Output

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
