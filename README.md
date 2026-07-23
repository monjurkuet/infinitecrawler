# InfiniteCrawler — Continuous Google Maps Lead Generation

24/7 Google Maps scraping pipeline. Two systemd daemons extract business listings from Bangladesh (15 BPT sectors × 15 cities), enrich with emails/LinkedIn, and classify by sector — all running eternally with zero manual intervention.

## Architecture

```
search-daemon                            listing-daemon
  │ Generates infinite query cycle         │ Reads uncrawled URLs from PG
  │ from 15 BPT sectors × 15 cities        │ → Redis queue
  │ (23,460 unique queries, 3-tier mix)    │ Deep extraction (phone, website,
  │ GMaps scroll → PG upsert               │ rating, category, coordinates)
  │                                        │ PG upsert + in-stream fallback
  │                                        │ classification
                                           │
                            enrichment (timers)
                              ├─ db_email_extract.py  (every 2h)
                              ├─ db_linkedin_search.py (every 4h)
                              └─ db_classify.py        (offline LLM cron)
```

**Query mix:** 70% BD-Local (city×keyword), 10% BD-National, 20% Global (6 international markets)

## Quick Start

```bash
systemctl --user enable --now infinitecrawler-search infinitecrawler-listing
uv run python scripts/monitor_pipeline.py
uv run python -m api.main
```

## Current Stats (2026-07-23)

| Metric | Count |
|--------|-------|
| Search results (PG) | 72,962 |
| Listings (PG) | 28,132 |
| Qualified (phone+website) | 10,003 |
| Emails extracted | 2,539 (15.1% coverage) |
| LinkedIn profiles | 7,707 (1,088 listings) |
| Classified (17 sectors) | 12,351 |
| Search queue pending | ~2,600 |
| Listing queue completed | 3,565 |

## Storage

| Table | Source | Content |
|-------|--------|---------|
| `scraper.gmaps_search_results` | search-daemon | Business name + URL per search query |
| `scraper.gmaps_listings` | listing-daemon | Full profile: phone, website, address, rating, coordinates, sector_id |
| `scraper.emails` | db_email_extract | Extracted emails from business websites |
| `scraper.linkedin_profiles` | db_linkedin_search | Discovered LinkedIn profiles |

PostgreSQL on remote VPS. Redis on localhost for queue management.

## Key Features

- **24/7 Continuous** — systemd-supervised eternal loops, never exhausts queries
- **Auto-classification** — In-stream fallback (rule-based) + offline LLM (DeepSeek V4 Flash)
- **Anti-bot resistant** — pinchtab (Chrome-based) with browser restarts every hour
- **Three-tier queries** — BD city-level, Bangladesh-national, and international (USA, UK, AU, CA, UAE, KSA)
- **REST API** — 30+ routes on port 8015 (Bearer auth)
- **Health monitoring** — Pipeline monitor script + Hermes cron watchdog (60m)

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Google Chrome
- Redis (localhost)
- PostgreSQL (remote VPS)
- pinchtab v0.15+ (Chrome lifecycle manager)

## Commands

### Service management
```bash
systemctl --user status   infinitecrawler-search       # search daemon
systemctl --user status   infinitecrawler-listing      # listing daemon
systemctl --user status   pinchtab                      # Chrome provider
systemctl --user list-timers | grep infinitecrawler     # enrichment timers
```

### Pipeline health
```bash
uv run python scripts/monitor_pipeline.py               # human-readable
uv run python scripts/monitor_pipeline.py --json         # machine-readable
```

### Redis queues
```bash
redis-cli LLEN  gmaps_bd_business:pending    # search pending
redis-cli SCARD gmaps_bd_business:completed   # search completed
redis-cli HLEN  gmaps_bd_business:failed      # search failed (HASH!)
redis-cli LLEN  gmaps:pending                 # listing pending
redis-cli SCARD gmaps:completed               # listing completed
```

### Enrichment backfill
```bash
uv run python scripts/db_email_extract.py --max 500
uv run python scripts/db_linkedin_search.py --max 200
uv run python scripts/db_classify.py --retry-failed --max 1000
```

## Browser Engine: pinchtab

Daemons connect to an external `pinchtab server` (bridge port 9868). Pinchtab manages Chrome's lifecycle — the daemons only issue HTTP commands:

```bash
# Quick health check
PINCHTAB_TOKEN=$(python3 -c "import json;print(json.load(open('/root/.pinchtab/config.json'))['server']['token'])")
curl -s -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health
```

**Stability:** Chrome must be configured with `--max_old_space_size=2048 --renderer-process-limit=5` in `/root/.pinchtab/config.json` to avoid OOM crashes on Google Maps.
