# Google Maps Search Scraper

Extracts business names and URLs from Google Maps search results using infinite scroll pagination.

## Quick Start

```bash
systemctl --user start infinitecrawler-search
systemctl --user status infinitecrawler-search --no-pager -l -n 50
```

## Architecture (Daemon Mode)

Runs as an eternal systemd service:

- **Infinite query generation**: reads from BPT sectors × cities, auto-refills Redis queue
- **Automatic browser lifecycle**: reconnects to pinchtab every 1h or 100 pages
- **24/7 operation**: systemd auto-restarts on crash, memory capped at 3G

## What It Extracts

| Field | Description |
|-------|-------------|
| `name` | Business name |
| `source_url` | Google Maps URL |
| `query` | Search term used |
| `source` | Data source identifier |

## Requirements

- Python 3.12+
- Google Chrome (or Chromium)
- Redis (localhost)
- PostgreSQL (remote VPS)

## Configuration

See `config/gmaps_bd_business_search.yaml`.

## How It Works

1. **Query generation**: `InfiniteQueryGenerator` cycles through 23,460 queries in three tiers (BD-Local 70%, BD-National 10%, Global 20%)
2. **Queue**: Redis queue (`gmaps_bd_business:*`) for processing state
3. **Search**: navigate to Google Maps search URL, scroll results
4. **Extract**: CSS selector-based extraction (`a.hfpxzc`) for name + URL
5. **Upsert**: PostgreSQL upsert to `scraper.gmaps_search_results`

## Monitoring

```bash
redis-cli LLEN gmaps_bd_business:pending
redis-cli SCARD gmaps_bd_business:completed
redis-cli HLEN gmaps_bd_business:failed
```
