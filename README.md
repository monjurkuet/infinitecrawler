# InfiniteCrawler — Continuous Google Maps Lead Generation

24/7 Google Maps scraping pipeline. Two systemd daemons extract business listings from Bangladesh (15 BPT sectors × 15 cities), enrich with emails/LinkedIn, and classify by sector — all running eternally with zero manual intervention.

## Architecture

```
search-daemon                            listing-daemon
  │ Generates infinite query cycle         │ Reads uncrawled URLs from PG
  │ from 15 BPT sectors × 15 cities        │ → Redis queue
  │ (23,460 unique queries, 3-tier mix)    │ Deep extraction (phone, website,
  │ GMaps scroll → PG upsert               │ rating, category, coordinates)
  │                                        │ PG upsert (50-row batch + 5s
  │                                        │ timer-based flush)
                                            │
                            enrichment (timers + perpetual loops)
                              ├─ db_email_extract --loop       (perpetual, 30s gap)
                              ├─ db_email_extract              (every 2h, safety net)
                              ├─ db_linkedin_search             (every 4h)
                              ├─ db_linkedin_match              (every 6h)
                              ├─ db_linkedin_firehose --loop    (perpetual, ~225 profiles/hr)
                              ├─ db_classify                    (daily, LLM)
                              └─ monitor_pipeline + watchdog    (every 15min, auto-heal)
```

**Query mix:** 70% BD-Local (city×keyword), 10% BD-National, 20% Global (6 international markets)

## Quick Start

```bash
systemctl --user enable --now infinitecrawler-search infinitecrawler-listing \
    infinitecrawler-linkedin-firehose-loop infinitecrawler-email-extract-loop pinchtab
uv run python scripts/monitor_pipeline.py
uv run python -m api.main
```

## Current Stats (2026-08-09)

| Metric | Count | Pace |
|--------|-------|------|
| Search results (PG) | 79,336 | ~400/hr |
| Listings (PG) | 19,366 | ~91–154/hr |
| Qualified (phone+website) | 6,772 | — |
| Emails extracted | 6,535 | ~25–36/hr |
| LinkedIn profiles | 51,927 | ~226/hr |
| LinkedIn↔GMaps matches (verified ≥0.7) | 1,096 / 70,071 | — |
| Classified (17 sectors) | 18,004 | — |
| Unclassified leads with phone+website | 0 | done |
| Listings pending email extraction | 3,624 | clearing via loop |

## Storage

| Table | Source | Content |
|-------|--------|---------|
| `scraper.gmaps_search_results` | search-daemon | Business name + URL per search query |
| `scraper.gmaps_listings` | listing-daemon | Full profile: phone, website, address, rating, coordinates, sector_id |
| `scraper.emails` | db_email_extract (loop + 2h timer) | Extracted emails from business websites |
| `scraper.linkedin_profiles` | db_linkedin_search + firehose loop | Discovered LinkedIn profiles |
| `scraper.linkedin_gmaps_matches` | db_linkedin_match (every 6h) | Company-matched profiles with score + is_verified |

PostgreSQL on local socket (or TCP 127.0.0.1:5432). Redis on localhost for queue management.

## Key Features

- **24/7 Continuous** — systemd-supervised eternal loops, never exhausts queries
- **Auto-classification** — In-stream fallback (rule-based) + offline LLM (DeepSeek V4 Flash) with health gate
- **Anti-bot resistant** — pinchtab (Chrome-based) with browser restarts every hour
- **Three-tier queries** — BD city-level, Bangladesh-national, and international (USA, UK, AU, CA, UAE, KSA)
- **REST API** — 30+ routes on port 8015 (Bearer auth)
- **Health monitoring** — Pipeline monitor script + systemd watchdog (15min) with auto-heal
- **File logging** — All systemd units log to `/var/log/infinitecrawler/`

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Google Chrome
- Redis (localhost)
- PostgreSQL (local socket or TCP 127.0.0.1:5432)
- pinchtab v0.15+ (Chrome lifecycle manager)

## Commands

### Service management
```bash
systemctl --user status   infinitecrawler-search                       # search daemon
systemctl --user status   infinitecrawler-listing                      # listing daemon
systemctl --user status   infinitecrawler-linkedin-firehose-loop      # perpetual LinkedIn discovery
systemctl --user status   infinitecrawler-email-extract-loop           # perpetual email backfill
systemctl --user status   pinchtab                                      # Chrome provider
systemctl --user list-timers | grep infinitecrawler                     # enrichment timers
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
uv run python scripts/db_email_extract.py --max 200          # one-shot
uv run python scripts/db_email_extract.py --loop --loop-gap 30 # perpetual
uv run python scripts/db_linkedin_search.py --max 200
uv run python scripts/db_linkedin_match.py --dry-run         # preview matches
uv run python scripts/db_classify.py --retry-failed --max 1000
```

### Log files
```bash
ls -la /var/log/infinitecrawler/        # 9 log files (one per unit)
tail -f /var/log/infinitecrawler/infinitecrawler-listing.log
```

## Browser Engine: pinchtab

Daemons connect to an external `pinchtab server` (bridge port 9868). Pinchtab manages Chrome's lifecycle — the daemons only issue HTTP commands:

```bash
# Quick health check
PINCHTAB_TOKEN=$(python3 -c "import json;print(json.load(open('/root/.pinchtab/config.json'))['server']['token'])")
curl -s -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health
```

**Stability:** Chrome must be configured with `--max_old_space_size=2048 --renderer-process-limit=5` in `/root/.pinchtab/config.json` to avoid OOM crashes on Google Maps. If bridge lands on port 9869 (port drift), restart pinchtab: `systemctl --user restart pinchtab`.

## Documentation

- [`.agents/knowledge-base.md`](.agents/knowledge-base.md) — Self-healing pipeline audit playbook (KEY FACTS TABLE + 6 phases)
- [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) — YAML schema reference
- [`docs/AUDIT-2026-07-31.md`](docs/AUDIT-2026-07-31.md) — Historical codebase audit (resolved)
- [`DOCKER-README.md`](DOCKER-README.md) — Docker Hub image documentation
- [`DOCKER-DEPLOYMENT.md`](DOCKER-DEPLOYMENT.md) — Full deployment guide
