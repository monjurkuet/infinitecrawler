# InfiniteCrawler — Continuous Google Maps Lead Generation

![Built with uv](https://img.shields.io/badge/Built%20with-uv-purple)

A modular, configuration-driven Google Maps scraping framework running 24/7 via systemd daemons. Generates qualified business leads from 15 BD sectors defined in [business-plan-template](https://github.com/monjurkuet/business-plan-template).

## Architecture: Two Eternal Daemons

```
search-daemon                           listing-daemon
  │ Generates infinite query cycle        │ Reads uncrawled URLs from PG
  │ from BPT's 15 sectors (21,356         │ → Redis queue
  │ unique queries, 3-tier mix)           │ Deep extraction (phone, website,
  │ GMaps scroll → PG upsert              │ rating, category, place_id)
  │                                      │ PG upsert
```

**Query mix:** 70% BD-Local (city×keyword), 10% BD-National, 20% Global (6 international markets)

## Quick Start

```bash
# Start daemons
systemctl --user enable --now infinitecrawler-search infinitecrawler-listing

# Health check
uv run python scripts/monitor_pipeline.py

# REST API (port 8015, Bearer auth)
uv run python -m api.main
```

## Storage

| Table | Source | Content |
|-------|--------|---------|
| `scraper.gmaps_search_results` | search-daemon | Business name + URL per query |
| `scraper.gmaps_listings` | listing-daemon | Full profile: phone, website, address, rating, coordinates, **sector_id** (classified) |

PostgreSQL on remote VPS. Redis on localhost for queue management (`gmaps_bd_business:*` for search, `gmaps:*` for listing).

## Sector Coverage (15 Sectors)

9 researched sectors + 6 pipeline-only sectors defined in BPT `sectors.yaml`. See [AGENTS.md](AGENTS.md) for full query pool breakdown.

## Key Features

- **24/7 Continuous** — systemd-supervised eternal loops, never exhausts queries
- **Auto-classification** — In-stream fallback (rule-based) + offline LLM (DeepSeek V4 Flash) assigns every listing to a BPT sector
- **Anti-bot resistant** — nodriver (Chrome-based) with browser restarts every hour
- **Three-tier queries** — BD city-level, Bangladesh-national, and international (USA, UK, AU, CA, UAE, KSA)
- **REST API** — Full programmatic access on port 8015 (31 routes, Bearer auth)
- **Health monitoring** — Hermes cron watchdog every 60m + 1m API keepalive

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Google Chrome
- Redis (localhost)
- PostgreSQL (remote VPS)