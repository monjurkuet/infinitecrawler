# InfiniteCrawler — Continuous Google Maps Lead Generation

24/7 Google Maps lead generation pipeline for Bangladesh (15 BPT sectors × 15 cities) and 29 global cities. Three extraction strategies run side by side: browser-based scroll crawling, Places API multi-key enrichment, and Nearby Search grid-scanning — all writing to the same PostgreSQL `gmaps_listings` table.

## Architecture

```
search-daemon (browser)
  │ Generates infinite query cycle from 15 BPT sectors × 15 cities
  │ (23,460 unique queries, 3-tier mix)
  │ GMaps scroll → PG upsert (gmaps_search_results)
  │
places-api-daemon (HTTP, multi-key)
  │ Enriches uncrawled URLs from gmaps_search_results
  │ GetPlace first (100% accurate, ChIJ place_id match)
  │ Text Search fallback (90% accurate, name + country query)
  │ 5 keys × 2 methods × 200/day = 1,600 listings/day
  │ No browser needed — pure HTTP, 40x faster than browser daemon
  │
nearby-scanner-daemon (HTTP, grid discovery)
  │ Discovers NEW places by grid-scanning 29 cities
  │ Hex grid of overlapping 2km circles
  │ Each call returns 20 fully-detailed places
  │ 5 keys × 200/day × 20 places = 20,000 NEW listings/day
  │ Writes directly to gmaps_listings (bypasses gmaps_search_results)
  │
listing-daemon (browser, legacy)
  │ Reads uncrawled URLs → Redis queue → deep extraction
  │ PG upsert (50-row batch + 5s timer-based flush)
  │ Still useful for fallback when API quotas exhausted
  │
                            enrichment (timers + perpetual loops)
                              ├─ db_email_extract --loop       (perpetual, 30s gap)
                              ├─ db_email_extract              (every 2h, safety net)
                              ├─ db_classify                   (daily, LLM)
                              └─ monitor_pipeline + watchdog    (every 15min, auto-heal)
```

**Query mix:** 70% BD-Local (city×keyword), 10% BD-National, 20% Global (6 international markets)

**City coverage (Nearby Scanner):** 15 BD cities + 14 global cities = 29 cities, 240,408 grid cells

## Quick Start

```bash
# 1. Set up secrets
cp .env.example .env  # Edit with your PG, Redis, and API keys
cp config/places_api_daemon.yaml.example config/places_api_daemon.yaml
cp config/nearby_scanner.yaml.example config/nearby_scanner.yaml

# 2. Install deps
uv sync

# 3. Enable and start all daemons
systemctl --user enable --now infinitecrawler-search infinitecrawler-listing \
    infinitecrawler-places-api infinitecrawler-nearby-scanner \
    infinitecrawler-email-extract-loop pinchtab

# 4. Enable enrichment timers
systemctl --user enable --now infinitecrawler-email-extract.timer \
    infinitecrawler-classify.timer

# 5. Start dashboard
uv run python -m api.main
```

## Storage

| Table | Source | Content |
|-------|--------|---------|
| `scraper.gmaps_search_results` | search-daemon | Business name + URL per search query |
| `scraper.gmaps_listings` | all 3 listing daemons | Full profile: phone, website, address, rating, coordinates, sector_id |
| `scraper.emails` | db_email_extract (loop + 2h timer) | Extracted emails from business websites |
| `scraper.nearby_scan_grid` | nearby-scanner-daemon | Grid cell tracking (city, lat, lng, status) |
| `scraper.linkedin_profiles` | firehose + search + backfill | DDGS-discovered LinkedIn profiles (name, title, company, location, connections, headline) |
| `scraper.linkedin_companies` | company enrichment loop | Company cards: industry, size, employees, followers, HQ, website, founded, specialties, logo |
| `scraper.app_users` | premium dashboard | Subscribers (bcrypt+JWT credentials, entitlement) |
| `scraper.auth_attempts` | premium dashboard | Login audit log + rate-limit source (success/fail per IP+email) |

PostgreSQL on local socket (or TCP 127.0.0.1:5432). Redis on localhost for queue management.

## Key Features

- **Three extraction strategies** — Browser scroll, Places API (GetPlace + Text Search), Nearby Search grid-scanner
- **Multi-key API rotation** — Round-robins across 5 keys, each separate Google Cloud project with independent 200/day quota per method
- **Three independent quota buckets per key** — GetPlace, Text Search, and Nearby Search each have separate 200/day caps (free tier)
- **Grid-based discovery** — Hex grid of overlapping 2km circles covers 29 cities (15 BD + 14 global), returns up to 20 places per call
- **24/7 Continuous** — systemd-supervised eternal loops, survive reboot
- **Auto-classification** — In-stream fallback (rule-based) + offline LLM (DeepSeek V4 Flash)
- **Dedup on overlap** — Nearby Search deduplicates by place_id across overlapping grid circles
- **Safe coexistence** — All daemons upsert to the same table with `ON CONFLICT (source_url) DO UPDATE`
- **REST API** — 30+ routes on port 8015 (Bearer auth)
- **Premium dashboard** — self-serve subscriber SPA on `:5173` + JWT API on `:8016` (see `PREMIUM_DASHBOARD.md`)
- **LinkedIn enrichment** — three loops: profile backfill (6h, re-parses DDGS snippets for location/country/connections/headline), company loop (30min, slug → public-page → industry/size/employees/followers/HQ/website), firehose (decision-maker discovery)
- **Health monitoring** — Pipeline monitor script + systemd watchdog (15min) with auto-heal
- **Ops dashboard (admin)** — static SPA on `:8015/admin` (units, queues, tables, services)

## LinkedIn loops (systemd, no API key)

```
# profiles: re-parse DDGS snippets every 6h → location/country/connections/headline
infinitecrawler-linkedin-backfill.service

# companies: resolve slug → fetch public page → industry/size/employees/followers
infinitecrawler-linkedin-company-loop.service   # 30min, batch=500
```

Both log into `/var/log/infinitecrawler/` and respect `Restart=on-failure`.

## Places API Daemon (`daemons/places_api_daemon.py`)

Replaces the browser-based listing daemon for places where we already have the ChIJ place_id in the `gmaps_search_results` URL. Pure HTTP — no browser, no Chrome, no pinchtab.

**Dual strategy per record:**
1. **GetPlace** (`places/{place_id}`) — 100% accurate, exact ChIJ match
2. **Text Search** (`places:searchText`) — fallback when GetPlace quota exhausted, searches by business name + "Bangladesh", verifies ChIJ or name match

**Throughput:** 5 keys × 2 methods × 200/day = 1,600 listings/day (vs browser daemon's ~7/min = ~10,000/day but with Chrome overhead)

**Config:** `config/places_api_daemon.yaml` (or `.example` template). API keys from `PLACES_API_KEYS` env var override config.

## Nearby Scanner Daemon (`daemons/nearby_scanner_daemon.py`)

Discovers NEW businesses by grid-scanning entire cities with Nearby Search. Each API call returns up to 20 fully-detailed places (phone, rating, address, website, coordinates).

**Grid strategy:**
- Hex grid of lat/lng points with 2km radius circles
- 3 type batches per grid point (food/hospitality, retail/services, health/education/general) to avoid 20-result cap truncating dense areas
- Overlapping circles ensure full coverage — no gaps
- Dedup by place_id across overlapping circles
- Grid tracking in `scraper.nearby_scan_grid` — resumes from incomplete cells after restart

**Throughput:** 5 keys × 200 calls/day × 20 places = 20,000 NEW listings/day

**Coverage:** 29 cities, 240,408 grid cells (BD cities ~44k, global ~196k)

**Config:** `config/nearby_scanner.yaml` (or `.example` template). API keys from `PLACES_API_KEYS` env var override config.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Google Chrome (for browser daemon + pinchtab)
- Google Places API (New) keys — 5 separate Google Cloud projects for max free-tier quota
- Redis (localhost)
- PostgreSQL (local socket or TCP 127.0.0.1:5432)
- pinchtab v0.15+ (Chrome lifecycle manager)

## Commands

### Service management
```bash
systemctl --user status   infinitecrawler-search               # search daemon (browser)
systemctl --user status   infinitecrawler-listing              # listing daemon (browser, legacy)
systemctl --user status   infinitecrawler-places-api           # Places API daemon (HTTP)
systemctl --user status   infinitecrawler-nearby-scanner       # Nearby grid-scanner (HTTP)
systemctl --user status   infinitecrawler-email-extract-loop   # perpetual email backfill
systemctl --user status   pinchtab                              # Chrome provider
systemctl --user list-timers | grep infinitecrawler            # enrichment timers
```

### Pipeline health
```bash
uv run python scripts/monitor_pipeline.py               # human-readable
uv run python scripts/monitor_pipeline.py --json         # machine-readable
```

### Nearby scanner grid status
```sql
SELECT city, status, count(*) FROM scraper.nearby_scan_grid GROUP BY city, status ORDER BY city;
-- Or check of newly discovered places:
SELECT count(*) FROM scraper.gmaps_listings WHERE source_type = 'nearby_search';
```

### Enrichment
```bash
uv run python scripts/db_email_extract.py --loop --loop-gap 30   # perpetual
uv run python scripts/db_classify.py --retry-failed --max 1000    # LLM classification
```

### Log files
```bash
ls -la /var/log/infinitecrawler/        # all daemon logs
tail -f /var/log/infinitecrawler/places-api-daemon.log
tail -f /var/log/infinitecrawler/infinitecrawler-nearby-scanner.log
```

## Browser Engine: pinchtab

The browser-based daemons (search, listing) connect to an external `pinchtab server` (bridge port 9868). Pinchtab manages Chrome's lifecycle — the daemons only issue HTTP commands. The Places API and Nearby Scanner daemons do NOT use pinchtab — they are pure HTTP.

**Launch (this host):** pinchtab runs as a systemd user unit (`infinitecrawler-pinchtab.service`, `Restart=always`). **Do NOT run it as a manual background process** — it dies on logout/reboot, silently killing the whole browser tier.

```bash
systemctl --user enable --now infinitecrawler-pinchtab.service
```

**Required `~/.pinchtab/config.json` settings:**
- `server.token` must match `.env` `PINCHTAB_TOKEN`
- `security.allowEvaluate: true`; `security.allowedDomains` must include `google.com`, `www.google.com`, `maps.google.com` for the listing/search daemons
- **For the email browser pass** (`scripts/db_email_extract.py --mode both`) the allowlist must be widened: `idpi.enabled=false` and `allowedDomains=[]` (or include BD/CTG/common TLDs) — otherwise the fallback extracts 0 emails because the bridge refuses to navigate to non-Google domains.
- `browser.extraFlags` must include `--disable-background-timer-throttling --disable-backgrounding-occluded-windows --disable-renderer-backgrounding` — without these, background tabs throttle and the listing daemon extracts bare shells ("Extracted 1 fields")
- `instanceDefaults.maxTabs` must be ≥ `LISTING_TAB_POOL + 1` (currently 8) or LRU eviction drops worker tabs mid-batch

**CRITICAL — listing daemon URL form:** the listing daemon navigates Google Maps place URLs. Headless Chrome cannot render the `/maps/place/<name>/data=!...` detail view (Google strips the CID and returns a bare map shell, so every selector misses). `daemons/listing_daemon.py` rewrites each queued URL to `https://www.google.com/maps/place/?cid=<decimal>` via `to_cid_url()` — this loads the full panel and populates name/rating/phone/address. Do NOT "fix" this back to the `/place/data=` form.

**CRITICAL — page settle time:** even with `?cid=`, the detail panel renders asynchronously. `config/gmaps_listings_working.yaml` sets `browser.page_wait_seconds: 6.0` and `max_wait: 12` on the Overview tab. Lower values extract from the half-rendered shell (name only, no phone/website/rating).

## Self-healing: phantom-row sweeper

Google Maps occasionally bounces headless requests to a bare shell (title="Google Maps", only `name` renders). Before the current fixes these "phantom rows" were 94% of new inserts. Now:

- **Listing daemon** (`daemons/listing_daemon.py`) detects phantom rows (name + no phone/website/rating/address/plus_code/category) and routes the URL to Redis list **`gmaps:phantom`** instead of persisting the bare row or marking it completed.
- **Phantom sweeper** (`scripts/phantom_sweeper.py`, systemd `infinitecrawler-phantom-sweeper.timer`, every 30min) re-pushes phantom URLs back into `gmaps:pending` and also sweeps legacy PG phantom rows (older than 2h).
- **Watchdog** (`scripts/monitor_pipeline.py`) calls `backfill_phantom()` each run alongside the existing backlog backfill.

Health check: `redis-cli LLEN gmaps:phantom` should be ≤ 50; `SELECT count(*) FROM scraper.gmaps_listings WHERE source_type='gmaps_listing' AND created_at > now()-interval '1 hour' AND phone IS NULL AND website IS NULL AND rating IS NULL AND address IS NULL` should be < 5% of hourly rows.

**Logs:** on this host daemon logs go to `/var/log/infinitecrawler/` (not `~/.cache/infinitecrawler/logs/`).

## OSM Enrichment (OpenStreetMap)

Free, quota-less business discovery/backfill complementary to the Google pipelines. `scripts/osm_import.py` downloads Geofabrik `.osm.pbf` extracts (or queries the Overpass API for US cities), filters business POIs, imports them into PostGIS, and merges them into `scraper.gmaps_listings` with `source_type='osm'` (never collides with Google `source_url`s).

**Prerequisites (system packages):** `osmium-tool`, `osm2pgsql`, and the PostGIS + hstore extensions in PostgreSQL.

**Run a single region (Geofabrik):**
```bash
uv run python scripts/osm_import.py --region bangladesh
# keep the PBF on disk for reuse: add --keep-pbf
```

**US cities via Overpass (no download, no osm2pgsql):**
```bash
uv run python scripts/osm_import.py --overpass-city san-francisco
```

**Status / what's imported:**
```bash
uv run python scripts/osm_import.py --status
```

**Weekly refresh:** `systemd/infinitecrawler-osm-import.{service,timer}` (runs `scripts/osm_import.py` every Monday 03:00 UTC). Enable with `systemctl --user enable --now infinitecrawler-osm-import.timer`.

> OSM phone/website coverage in South Asia is sparse (~2-3% phone, ~1-2% website) — best used for discovery (name + location + category), not enrichment. The Google Nearby Scanner already covers enrichment better. See `docs/OSM-RESEARCH-2026-08-17.md` for the full analysis.

## Backup (DB → Mega)

The `infinitecrawler` PG database is backed up by the sibling **`data-archive`** codebase, 4× per day, and uploaded to Mega cloud.

- **Tool**: `/run/media/growloop/codebase/data-archive/backup_all.sh` (runs PG + MongoDB + Hermes + OpenCode, then `mega_sync.sh`)
- **Schedule**: systemd user timer `data-archive.timer` — 0/6/12/18 UTC daily (Persistent=true)
- **Local cache**: `data-archive/database/postgresql/localhost/localhost_infinitecrawler_full_*.dump.zst` (7-day retention)
- **Cloud archive**: `/backups/postgresql/localhost/YYYY-MM-DD/` on Mega

Restore:
```bash
zstd -d /run/media/growloop/codebase/data-archive/database/postgresql/localhost/localhost_infinitecrawler_full_<TS>.dump.zst -o /tmp/ic.dump
sudo -u postgres pg_restore -d infinitecrawler --no-owner -1 /tmp/ic.dump  # drop -c to preserve
```

Manual trigger:
```bash
bash /run/media/growloop/codebase/data-archive/backup_all.sh          # normal cycle
bash /run/media/growloop/codebase/data-archive/backup_all.sh --full   # force all PG DBs
```

The legacy `backups/ic_pg_*.dump.zst` files at the repo root are an outdated manual format — superseded by `data-archive` and not uploaded; safe to ignore (or `rm`).

## Documentation

- [`AGENTS.md`](AGENTS.md) — Agent operating guide (stack, conventions, gotchas)
- [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) — YAML schema reference
- [`docs/AUDIT-2026-07-31.md`](docs/AUDIT-2026-07-31.md) — Historical codebase audit (resolved)
- [`.agents/knowledge-base.md`](.agents/knowledge-base.md) — Self-healing pipeline audit playbook
- [`DOCKER-README.md`](DOCKER-README.md) — Docker Hub image documentation
- [`DOCKER-DEPLOYMENT.md`](DOCKER-DEPLOYMENT.md) — Full deployment guide

## License

Proprietary. See repository settings for details.
