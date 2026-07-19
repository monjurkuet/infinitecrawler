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

The `source_type` column on each row identifies which run wrote it:
- `gmaps_search` / `gmaps_listing` = legacy / current pinchtab-era
- (former side-by-side `gmaps_search_pinchtab` was retired when nodriver was removed.)

## Commands

### Systemd
```bash
systemctl --user start    infinitecrawler-search infinitecrawler-listing   # start daemons
systemctl --user stop     infinitecrawler-search                          # stop one
systemctl --user restart  infinitecrawler-listing                         # restart one
systemctl --user status   infinitecrawler-search --no-pager -l -n 50      # logs
systemctl --user is-active infinitecrawler-search                         # quick check

# Pinchtab browser server (the daemon's Chrome provider — required dependency).
# Both daemon units declare After=pinchtab.service, so starting pinchtab means
# you don't have to worry about starting it first.
systemctl --user status   pinchtab.service                                # status
systemctl --user restart  pinchtab.service                                # restart it
tail -f /root/.pinchtab/server.log                                          # raw server log

# Timer services (email every 2h, LinkedIn every 4h)
systemctl --user start    infinitecrawler-email-extract.timer             # start email timer
systemctl --user start    infinitecrawler-linkedin-search.timer           # start LinkedIn timer
systemctl --user list-timers --no-pager | grep infinitecrawler            # next fire times
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

### Pinchtab health

```bash
# Quick server health (no daemon restart needed)
PINCHTAB_TOKEN=$(cat /root/.pinchtab/config.json | python3 -c "import sys,json;print(json.load(sys.stdin)['server']['token'])")
curl -s -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health

# Should see tab count, recent crashes, restart counts
# "crashes.recent" > 0 is normal — pinchtab's supervisor handles it
# "tabs": 0 means the always-on supervisor hasn't restarted Chrome yet

systemctl --user status pinchtab.service --no-pager -l -n 20
tail -50 /root/.pinchtab/server.log
```

## Query Generator

`daemons/query_generator.py` — infinite three-tier rotation from BPT sectors:

| Pool | Size | Description |
|------|------|-------------|
| BD-Local | 18,780 | "{keyword} in {city}" × 15 cities × 16 sectors × en+bn |
| BD-National | 1,194 | "{keyword} Bangladesh" / "{keyword} outside Dhaka" |
| Global | 3,486 | "{keyword} {country}" × 6 countries × 12 export-eligible sectors |
| **Total** | **23,460** | Shuffled cycle, reshuffles on exhaustion (~55h at 40q/h) |

Global-eligible sectors (12): BIM, Media-Marketing-Digital, Electronics-Gadgets, Clothing-Fashion, Travel-Tourism, Healthcare-Pharma, Food-Beverage, Education-Training, Logistics-Transport, Agriculture-Agro, Construction-Real-Estate, Service-Agents-Distribution.

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
| `scripts/db_email_extract.py` | Offline HTTP email extraction backfill (runs every 2h via timer) |
| `scripts/db_linkedin_search.py` | Offline LinkedIn profile discovery via DDGS (runs every 4h via timer) |
| `api/` | FastAPI REST server (port 8015, bearer auth) |
| `~/.config/systemd/user/infinitecrawler-*.service` | systemd unit files |
| `~/.config/systemd/user/infinitecrawler-*.timer` | systemd timer unit files |
| `~/.hermes/scripts/bd-watchdog.sh` | Hermes cron watchdog (every 60m, no_agent) |

## Important Notes

- `gmaps_bd_business:failed` is a **HASH** (not LIST). Use `HLEN` not `LLEN`.
- Search config uses `rate_limit: 2` (int), not `rate_limiting: {...}` (dict). Daemon handles this.
- Both daemons have no restart limits (`StartLimitIntervalSec=0`). systemd never gives up.
- Browser reconnects every 1h OR 100 pages — reconnect just releases the HTTP session and grabs a fresh tab.  Pinchtab's `always-on` supervisor handles Chrome crashes (instant restart); the daemon never touches Chrome directly.
- Memory capped at 3G per daemon via systemd `MemoryMax`; pinchtab itself gets 6G for Chrome + its always-on supervisor.

## Browser Engine: pinchtab

The crawler daemons talk to a separate `pinchtab server` process (port 9868 by default)
over a thin async HTTP client at `base/pinchtab_client.py`.  Pinchtab manages
Chrome's lifecycle and crashes automatically — the daemons are pure observers
that issue `POST /navigate` and `POST /evaluate` requests.

| Component | Path |
|---|---|
| HTTP client + Tab adapter | `base/pinchtab_client.py` |
| Browser wrapper | `base/browser_manager.py` (no nodriver branch — pinchtab only) |
| Pinchtab binary | `/root/.pinchtab/bin/0.15.0/pinchtab-linux-amd64` |
| Pinchtab config | `/root/.pinchtab/config.json` |
| Pinchtab systemd unit | `~/.config/systemd/user/pinchtab.service` |

The Tab adapter shims nodriver's `Tab.evaluate() / select() / select_all() /
wait()` interface so the existing `strategies/extraction/*.py`,
`strategies/pagination/*.py`, and `strategies/navigation/*.py` files work
unchanged.

### Pinchtab Chrome stability patch — REQUIRED

Pinchtab 0.15 ships Chrome with `--max_old_space_size=512
--renderer-process-limit=1` which OOM-crashes on Google Maps every 1-3
navigations.  Override via `extraFlags` in
`/root/.pinchtab/config.json`:

```json
"browser": { "extraFlags": "--max_old_space_size=2048 --renderer-process-limit=5" }
```

After this flag is set, Chromium uses 2GB V8 heap + 5 renderer processes and
stays alive through the daemon's 1h/100-page cycle.  Without it the
auto-recovery loop kicks in 3-5 times per minute and throughput drops ~40%.

See the `pinchtab-chrome-stability` Hermes skill for the full diagnosis.

