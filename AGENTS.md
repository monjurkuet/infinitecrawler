# InfiniteCrawler — Lead Generation Pipeline

## Architecture: Continuous 24/7 Scraping (systemd daemons)

Two eternal systemd user services run continuously:

```
search-daemon (infinitecrawler-search.service)   listing-daemon (infinitecrawler-listing.service)
    │                                                     │
    │ Generates queries from BPT sectors:                  │ Reads uncrawled URLs from PG directly:
    │  70% BD-Local (city×keyword)                         │  SELECT FROM gmaps_search_results
    │  10% BD-National (Bangladesh-wide)                   │  LEFT JOIN gmaps_listings WHERE NULL
    │  20% Global (6 international markets)                 │  LIMIT 100 → Redis queue
    │                                                     │
    │ Searches Google Maps, scrolls results,               │ Navigates each URL, extracts via
    │ extracts listing URLs, upserts to PG                 │ multi-step extraction (phone, website,
    │                                                     │ rating, category), upserts to PG
    │                                                     │
    │ 1h browser restart (or 100 pages)                    │ In-stream fallback classification
    │ Per-query timeout: 30s navigation                    │ (rule-based, runs inline before write)
    │                                                     │ 1h browser restart (or 100 pages)
    │                                                     │ Per-URL timeout: 30s nav + 45s extraction
    │                                                     │
    │                                                     │ Offline LLM classification (cron)
    │                                                     │ db_classify.py upgrades fallback → llm_*
```

Redis queues:
- Search: `gmaps_bd_business:pending/processing/completed/failed`
- Listing: `gmaps:pending/processing/completed/failed`

Database: PostgreSQL (remote VPS) — `scraper.gmaps_search_results` and `scraper.gmaps_listings`

## Commands

### Systemd
```bash
systemctl --user start    infinitecrawler-search infinitecrawler-listing   # start daemons
systemctl --user stop     infinitecrawler-search                          # stop one
systemctl --user restart  infinitecrawler-listing                         # restart one
systemctl --user status   infinitecrawler-search --no-pager -l -n 50      # logs
systemctl --user is-active infinitecrawler-search                         # quick check
```

### Redis
```bash
redis-cli LLEN  gmaps_bd_business:pending    # search pending queries
redis-cli SCARD gmaps_bd_business:completed   # search completed queries
redis-cli HLEN  gmaps_bd_business:failed      # search failed (HASH — use HLEN not LLEN)
redis-cli LLEN  gmaps:pending                 # listing pending URLs
redis-cli SCARD gmaps:completed               # listing completed URLs
```

### PostgreSQL
```bash
PGPASSWORD=changeme psql -h 100.92.181.21 -U postgres -d infinitecrawler
  -c "SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour'"
  -c "SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour'"
  -c "SELECT COUNT(*) FILTER (WHERE phone IS NOT NULL) AS with_phone FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour'"

# Classification stats
  -c "SELECT classification_method, COUNT(*) FROM scraper.gmaps_listings WHERE classification_method IS NOT NULL GROUP BY classification_method ORDER BY 2 DESC"
  -c "SELECT COUNT(*) FILTER (WHERE sector_id IS NULL AND phone IS NOT NULL AND website IS NOT NULL) AS unclassified FROM scraper.gmaps_listings"
```

### Classification
```bash
# Offline classification (cron or manual)
uv run python scripts/db_classify.py                     # classify up to 5000
uv run python scripts/db_classify.py --max 2000           # custom limit
uv run python scripts/db_classify.py --dry-run            # preview without DB writes
uv run python scripts/db_classify.py --stats              # classification stats only
uv run python scripts/db_classify.py --retry-failed --max 1300  # retry LLM error leads
```

### Health Monitor
```bash
uv run python scripts/monitor_pipeline.py          # human-readable report
uv run python scripts/monitor_pipeline.py --json    # machine-readable JSON
uv run python scripts/monitor_pipeline.py --stats   # classification stats only
```

### Stuck Chrome Detection
```bash
# Check for Chrome processes stuck on newtab (should be on Google Maps)
uv run bash scripts/check-stuck-chrome.sh
# Or: systemctl --user status infinitecrawler-search infinitecrawler-listing --no-pager -l
```

## Query Generator

`daemons/query_generator.py` — infinite three-tier rotation from BPT sectors:

| Pool | Size | Description |
|------|------|-------------|
| BD-Local | 17,130 | "{keyword} in {city}" × 15 cities × 15 sectors × en+bn |
| BD-National | 1,094 | "{keyword} Bangladesh" / "{keyword} outside Dhaka" |
| Global | 3,132 | "{keyword} {country}" × 6 countries × 11 export-eligible sectors |
| **Total** | **21,356** | Shuffled cycle, reshuffles on exhaustion (~50h at 40q/h) |

Global-eligible sectors (11): BIM, Media-Marketing-Digital, Electronics-Gadgets, Clothing-Fashion, Travel-Tourism, Healthcare-Pharma, Food-Beverage, Education-Training, Logistics-Transport, Agriculture-Agro, Construction-Real-Estate.

Ultra-technical BIM keywords (MEP design, scan-to-BIM, BIM outsourcing) are global-only — they have no Google Maps results in Bangladeshi cities.

## Files

| Path | Purpose |
|------|---------|
| `daemons/search_daemon.py` | Eternal search loop: query generation → GMaps scroll → PG upsert |
| `daemons/listing_daemon.py` | Eternal listing loop: PG URL feed → deep extraction → PG upsert |
| `daemons/query_generator.py` | Infinite three-tier query rotation engine |
| `scripts/monitor_pipeline.py` | Health monitor (Redis + PG + systemd checks) |
| `scripts/llm_classifier.py` | LLM classifier module: prompt building, fallback, few-shot, training examples |
| `scripts/db_classify.py` | Offline cron: reads unclassified leads from PG, calls LLM, writes back |
| `api/` | FastAPI REST server (port 8015, bearer auth) |
| `~/.config/systemd/user/infinitecrawler-*.service` | systemd unit files |
| `~/.hermes/scripts/bd-watchdog.sh` | Hermes cron watchdog (every 60m, no_agent) |

## Important Notes

- `gmaps_bd_business:failed` is a **HASH** (not LIST). Use `HLEN` not `LLEN`.
- Search config uses `rate_limit: 2` (int), not `rate_limiting: {...}` (dict). Daemon handles this.
- Both daemons have no restart limits (`StartLimitIntervalSec=0`). systemd never gives up.
- Browser restarts every hour OR 100 pages — whichever hits first. Chrome temp dirs cleaned.
- Memory capped at 3G per daemon via systemd `MemoryMax`.
