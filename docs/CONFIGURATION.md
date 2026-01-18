# Configuration Reference

The framework uses YAML files to define scraper behavior. This allows for rapid iteration without code changes.

## Schema Reference

### Root Object

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `name` | string | Yes | Human-readable name of the scraper. |
| `content_type` | enum | Yes | `dynamic` (nodriver) or `static` (requests). |
| `pagination_strategy` | enum | Yes | `infinite_scroll`, `next_button`, etc. |
| `extraction_strategy` | enum | Yes | Usually `generic_selector`. |
| `search_url_template` | string | Yes | Python f-string for the target URL. Use `{query}` placeholder. |

### Selectors

Maps data fields to CSS selectors.

```yaml
selectors:
  items: "div.result-card"      # The container for a single result
  fields:
    name: "h1.title"            # Text content of this element
    url: "a.link"               # 'href' attribute (auto-detected for 'url'/'href' keys)
    rating: "span.stars"
```

### Pagination

Controls how the scraper navigates.

**Infinite Scroll:**
```yaml
pagination:
  container: "div.feed"         # The scrollable element
  max_scroll_attempts: 500      # Safety limit
```

**Next Button:**
```yaml
pagination:
  next_button_selector: "a.next"
  max_pages: 50
```

### Output

Controls where data is saved.

```yaml
output:
  file_path: "output/data_{query}.jsonl"
  max_results: 1000
```

## Example Configuration

```yaml
name: "Google Maps"
content_type: "dynamic"
browser_automation: "nodriver"
headless: true

# Strategy Selection
pagination_strategy: "infinite_scroll"
extraction_strategy: "generic_selector"
output_strategy: "jsonl_file"

# Navigation
search_url_template: "https://www.google.com/maps/search/{query}/"

# Data Mapping
selectors:
  items: "a.hfpxzc"
  fields:
    name: "aria-label"
    url: "href"

# Pagination Settings
pagination:
  container: "div[role='feed']"
  max_scroll_attempts: 500

# Runtime Controls
rate_limit: 2
```
