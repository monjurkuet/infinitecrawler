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
| Emails extracted | 6,566 | ~25–36/hr (HTTP) → ≥50/hr target with `--mode both` |
| LinkedIn profiles | 51,927 | ~226/hr (firehose) |
| LinkedIn↔GMaps matches (verified ≥0.7) | 1,096 / 70,071 | — |
| Classified (17 sectors) | 18,004 | — |
| Unclassified leads with phone+website | 0 | done |
| Listings pending email extraction | 3,624 | clearing via loop |

> Backend PG timeouts are now enforced (`idle_in_transaction_session_timeout=30s`, `statement_timeout=120s`, `lock_timeout=10s`); see `.agents/knowledge-base.md` for env knobs.

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

## Pipeline Hardening (2026-08-09)

DDGS Bengali transliteration gate (`utils/transliterate.py`) — Bengali-script queries are pre-transliterated to Latin before being dispatched to `search.datasolved.org`, which has been returning 500 on ~15% of BN queries. A 500-streak circuit breaker trips a 300s cooldown after 3 consecutive failures. Brand-name safety: queries that yield 0 Latin letters after transliteration are dispatched unchanged.

PG session hardening — every psycopg connection (sync + async pool) now applies `idle_in_transaction_session_timeout=30s`, `statement_timeout=120s`, `lock_timeout=10s` via the libpq-standard `options=` kwarg. Env-overridable.

Pinchtab browser fallback for email extraction — `db_email_extract.py --mode both` runs the HTTP crawl first, then queues zero-result listings for a browser pass using rendered DOM + mailto hrefs. Targets ≥10% email hit-rate vs 3.6% HTTP-only. Concurrency 3, 0.2s nav delay (mitigates the documented Facebook/Instagram CLOSE-WAIT stall). New `--force-rescan` flag drains the existing backlog; `--max` default raised 500→2000 for loop mode.

Listing daemon diagnostics — per-cycle `cycle_summary` line every 5 min (`success{phone,website,plus_code,category} retries{…}`); on final retry failure, `ERROR` line with URL + last-failure-kind. Set `LOG_SAMPLE_HTML=1` for first-500-chars HTML forensics. Set `FLUSH_ON_REQUIRED_FIELD=1` + `FLUSH_INTERVAL_SEC=1` to flush each successful listing immediately.

Standardized logging — listing + search daemons + firehose use `%(asctime)s - %(name)s - %(levelname)s - %(message)s` with `started version=1 args=…` / `stopped reason=…` lifecycle markers.

## Documentation

- [`.agents/knowledge-base.md`](.agents/knowledge-base.md) — Self-healing pipeline audit playbook (KEY FACTS TABLE + 6 phases)
- [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) — YAML schema reference
- [`docs/AUDIT-2026-07-31.md`](docs/AUDIT-2026-07-31.md) — Historical codebase audit (resolved)
- [`DOCKER-README.md`](DOCKER-README.md) — Docker Hub image documentation
- [`DOCKER-DEPLOYMENT.md`](DOCKER-DEPLOYMENT.md) — Full deployment guide
