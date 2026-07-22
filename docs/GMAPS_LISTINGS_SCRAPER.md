# Google Maps Listing Scraper

Extracts business listings from Google Maps — contact info, ratings, categories, and more.

## Quick Start

```bash
systemctl --user start infinitecrawler-listing
systemctl --user status infinitecrawler-listing --no-pager -l -n 50
```

## Architecture (Daemon Mode)

The listing scraper runs as an eternal systemd service:

- **Live PG feed**: reads uncrawled URLs directly from `gmaps_search_results`, no file export step
- **Automatic browser lifecycle**: reconnects to pinchtab every 1h or 100 pages
- **In-stream fallback classification**: rule-based sector detection runs inline before DB write
- **24/7 operation**: systemd auto-restarts on crash, memory capped at 3G

## What It Extracts

| Field | Description |
|-------|-------------|
| `name` | Business name |
| `category` | Business type |
| `rating` | Star rating |
| `review_count` | Number of reviews |
| `address` | Physical address |
| `phone` | Phone number |
| `website` | Website URL |
| `booking_url` | Booking/scheduling link |
| `is_claimed` | Claimed by owner |
| `plus_code` | Google plus code |
| `latitude` | Latitude coordinate |
| `longitude` | Longitude coordinate |

## Requirements

- Python 3.12+
- Google Chrome (or Chromium)
- Redis (localhost)
- PostgreSQL (remote VPS)

## Configuration

See `config/gmaps_listings_working.yaml`.

## Extraction Pipeline

1. **Navigate** to listing URL (30s timeout)
2. **Extract fields** with retry logic and fallback selectors
3. **Transform** data (clean phone, normalize digits)
4. **Upsert** to `scraper.gmaps_listings` with in-stream fallback classification

### Retry Logic

- Element-level: 3 attempts with exponential backoff, fallback selectors
- URL-level: 3 attempts with browser restart between retries

## Output

`scraper.gmaps_listings` — typed columns + JSONB payload, upsert by `place_id`.

## Monitoring

```bash
redis-cli LLEN gmaps:pending
redis-cli SCARD gmaps:completed
redis-cli HLEN gmaps:failed
```
