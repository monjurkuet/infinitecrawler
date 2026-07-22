# InfiniteCrawler — Continuous Google Maps Lead Generation

A modular, configuration-driven Google Maps scraping framework running 24/7 via systemd daemons. Generates qualified business leads from 16 BPT sectors.

## Architecture: Two Eternal Daemons

```
search-daemon                           listing-daemon
  │ Generates infinite query cycle        │ Reads uncrawled URLs from PG
  │ from BPT's 15 sectors (23,460        │ → Redis queue
  │ unique queries, 3-tier mix)           │ Deep extraction (phone, website,
  │ GMaps scroll → PG upsert              │ rating, category, place_id)
  │                                      │ PG upsert
```

**Query mix:** 70% BD-Local (city×keyword), 10% BD-National, 20% Global (6 international markets)

## Quick Start

```bash
systemctl --user enable --now infinitecrawler-search infinitecrawler-listing
uv run python scripts/monitor_pipeline.py
uv run python -m api.main
```

## Storage

| Table | Source | Content |
|-------|--------|---------|
| `scraper.gmaps_search_results` | search-daemon | Business name + URL per query |
| `scraper.gmaps_listings` | listing-daemon | Full profile: phone, website, address, rating, coordinates, sector_id |

PostgreSQL on remote VPS. Redis on localhost for queue management.

## Key Features

- **24/7 Continuous** — systemd-supervised eternal loops, never exhausts queries
- **Auto-classification** — In-stream fallback (rule-based) + offline LLM (DeepSeek V4 Flash)
- **Anti-bot resistant** — pinchtab (Chrome-based) with browser restarts every hour
- **Three-tier queries** — BD city-level, Bangladesh-national, and international (USA, UK, AU, CA, UAE, KSA)
- **REST API** — 30+ routes on port 8015 (Bearer auth)
- **Health monitoring** — Hermes cron watchdog every 60m

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Google Chrome
- Redis (localhost)
- PostgreSQL (remote VPS)
