# InfiniteCrawler — Codebase Overview

> A 24/7 lead-generation engine that continuously harvests business data from
> Google Maps, LinkedIn, OpenStreetMap, and BBB into a single PostgreSQL
> warehouse, enriches it (emails, classification, company data), and serves it
> through REST APIs and subscriber dashboards. Everything runs as self-healing
> systemd user daemons that survive reboots.

---

## 1. What It Does (Core Purpose)

InfiniteCrawler is a **perpetual B2B/B2C lead-generation pipeline**. Its
primary goal: build and maintain a massive, continuously-refreshed database of
businesses — starting with **Bangladesh** (15 BPT industry sectors × 15 cities,
~23,460 unique search queries) and expanding to **29 global cities** — then
sell/deliver access to that data through a paywalled dashboard.

It is not a one-off scraper; it is an **always-on data factory** with quota
management, deduplication, self-healing, and monitoring built in.

---

## 2. Architecture at a Glance

```
DISCOVERY LAYER
  ├─ search-daemon          (browser) GMaps scroll → gmaps_search_results (URL+name)
  ├─ places-api-daemon      (HTTP)    GetPlace + Text Search enrichment
  ├─ nearby-scanner-daemon  (HTTP)    grid-scans 29 cities, finds NEW places
  └─ listing-daemon         (browser) legacy deep-extraction via Redis queue

ENRICHMENT LAYER
  ├─ db_email_extract       (loop 30s + 2h timer) scrapes emails from biz websites
  ├─ db_classify            (daily)  LLM-based sector classification
  ├─ LinkedIn suite         profiles firehose, jobs, companies (guest API)
  ├─ OSM import             (weekly) free quota-less POI backfill via PostGIS
  └─ BBB.org vertical (30 niches × 29,731 US cities)
     ├─ bbb-scraper.service       search + listings (direct HTTP, no proxy — the /api/search JSON
     │                           endpoint is not Cloudflare-walled from the host's egress)
     └─ bbb-backfill.service      enriches legacy rows lacking website/years_in_business
                                  via a 3-flavor Datasolved proxy ladder (-cf → -res → plain).

PROXY POLICY (locked 2026-09-15)
  - Any host with Cloudflare challenge: route through `datasolved-CF` over plain HTTP.
    Pinchtab/Playwright is reserved for pages that need JS or session cookies, NOT CF bypass.
  - BBB profile enrichment: 3-flavor rotation (BBB_PROFILE_PROXY=_CF, _RES, _PLAIN), one
    shared password in `.env`. Each profile failure on a CF wall quietly rotates.
  - LinkedIn jobs: DATASOLVED_PROXY(=_CF) — both must point at -cf; job search is the CF-heavy path.
  - No Datasolved var is set for search-side traffic deliberately — BBB /api/search is reached
    direct from the host IP (saves ~4x latency and monthly bytes, has never 403'd).

STORAGE
  └─ PostgreSQL `scraper.*` schema (gmaps_listings, linkedin_jobs,
     linkedin_profiles, linkedin_companies, emails, nearby_scan_grid, ...)
     Redis for queues (gmaps:pending, gmaps:phantom, linkedin:jobs:pending, ...)

SERVING LAYER
  ├─ Internal REST API        :8015 (Bearer, 30+ routes, ops/metrics)
  ├─ Premium API              :8016 (JWT paywall, self-serve signup)
  ├─ Subscriber SPA (React)   :5173
  └─ Ops admin SPA (React)    :5174

OPS LAYER
  ├─ monitor_pipeline + watchdog (every 15 min, auto-heal)
  ├─ phantom-row sweeper         (every 30 min, requeues failed renders)
  ├─ data-archive backups        (4×/day PG dump → Mega cloud)
  └─ All daemons = systemd user units + linger (boot-persistent)
```

## 3. Capabilities (Detail)

### 3.1 Google Maps — three parallel extraction strategies
| Daemon | Method | Speed | Throughput | Use |
|---|---|---|---|---|
| search-daemon | Browser scroll of GMaps results | ~slow | feeds URL queue | Generates the worklist |
| places-api-daemon | Places API (New): GetPlace + Text Search | 40× faster than browser | 1,600/day (5 keys) | Enrich known URLs, 100%/90% accuracy |
| nearby-scanner-daemon | Nearby Search on hex grid (2 km circles) | 20 places/call | **20,000 new listings/day** | Discovers places never searched |
| listing-daemon | Browser deep-dive (`?cid=` URLs) | ~7 places/min | ~10,000/day, no API cap | Fallback / quota-independent |

Key engineering:
- **Multi-key rotation**: 5 Google Cloud projects × 3 separate 200/day method
  quotas = 3,000 free API calls/day, with 25 h rolling-window quota tracking.
- **Grid coverage**: 29 cities, 240,408 grid cells, 3 type batches per point
  (food / retail / health) to dodge the 20-result cap; state persisted in
  `nearby_scan_grid` so scans resume after restart.
- **Dedup**: by `place_id` across overlapping circles; all daemons upsert with
  `ON CONFLICT (source_url) DO UPDATE`, so strategies coexist safely.

### 3.2 Self-healing data quality
- **Phantom-row detection**: headless Chrome sometimes gets a bare GMaps shell.
  The listing daemon detects name-only rows and routes them to `gmaps:phantom`;
  a sweeper timer requeues them automatically instead of persisting garbage.
- **Watchdog/monitor**: `scripts/monitor_pipeline.py` + 15-min systemd watchdog
  restart stalled units and backfill missed queues.
- **URL fixing**: listing daemon rewrites `/maps/place/data=...` URLs to
  `?cid=<decimal>` form — the only variant headless Chrome renders fully.

### 3.3 Email enrichment
`db_email_extract` runs perpetually (30 s gap) plus a 2-hourly safety net:
visits each listing's website (browser fallback for JS sites) and extracts
contact emails into `scraper.emails`.

### 3.4 LinkedIn suite
- **Profiles firehose**: DDGS-based discovery of decision-maker profiles
  (name, title, company, location, connections).
- **Jobs pipeline** (3 daemons): guest-API search (keyword × location matrix
  from sectors.yaml) → job detail fetch → company-page enrichment, all through
  a residential-proxy pool with per-worker pacing and 429/999 backoff.
- **Company loop**: resolves `linkedin.com/company/{slug}` for industry, size,
  employee count, HQ, website, followers; slug-first upsert merges with
  firehose rows without false-name collisions.

### 3.5 OpenStreetMap import
Weekly Geofabrik `.osm.pbf` (or Overpass for US cities) → PostGIS → merge into
`gmaps_listings` as `source_type='osm'`. Free, quota-less discovery layer —
sparser contact data, so used for discovery rather than enrichment.

### 3.6 Dashboards & monetization
- **Premium dashboard**: self-serve signup, bcrypt+JWT auth, rate-limited
  login with audit trail (`auth_attempts`), subscriber SPA on :5173.
- **Internal REST API** (:8015): 30+ routes for stats, listings query,
  daemon status, queue depths.
- **Ops admin SPA** (:5174): units/queues/logs for operators.

### 3.7 Vertical / one-off collectors
`scripts/` includes BBB scraping/backfill, luxury-hotel seeding
(`seed_all_hotels`, `collect_luxury_contacts`), website re-sync, lead export
(`generate_leads.py`), and schema migrations — all additive-only by
convention.

---

## 4. Tech Stack

- **Python 3.12+**, deps via `uv`; ruff + pyright + pytest
- **FastAPI** (both APIs), React/Vite SPAs
- **PostgreSQL** (+ PostGIS for OSM), **Redis** for queues
- **pinchtab**-managed Chrome for browser daemons (HTTP bridge on :9868)
- **curl_cffi/httpx** with residential proxies (datasolved) for LinkedIn
- **systemd user units + linger** for boot-persistent 24/7 operation
- Docker packaging available (Dockerfile, docker-compose, Hub push scripts)

---

## 5. Use Cases

1. **Lead database SaaS** — primary: subscribers query/filter/export fresh BD
   + global business leads with emails via the premium dashboard.
2. **Sales prospecting lists** — one-off exports via `generate_leads.py`
   (sector × city × has-email filters).
3. **Market mapping** — sector penetration per city from grid-scan density;
   `nearby_scan_grid` doubles as a coverage map.
4. **Recruitment intelligence** — LinkedIn jobs corpus (titles, seniority,
   hiring volume per company/sector/city).
5. **Company enrichment API** — LinkedIn company cards + GMaps listings +
   emails = single entity profile, reusable by other products.
6. **Cold-outreach fuel** — emails table is the deliverable for mail campaigns.

---

## 6. Future Expandability

Designed-in extension points:

- **New geographies**: add a city to the nearby-scanner city list /
  sectors.yaml query matrix — grid cells generate automatically. No code
  change needed.
- **New sectors/keywords**: the query engine is data-driven off `sectors.yaml`;
  adding a sector instantly creates city × keyword work across search daemon
  and LinkedIn jobs.
- **More API keys**: quota scales linearly; each new Google Cloud project adds
  600 calls/day (3 methods × 200). Rotation logic is already N-key generic.
- **New sources**: the pipeline pattern (daemon → Redis queue → PG upsert →
  monitor hook) is established; strategies/ + factory/ exist for registering
  new site scrapers. Obvious candidates: Facebook Pages, local directories
  (e.g. BDTradeInfo), industry registries, Crunchbase-style startup data.
- **Entity resolution / master record**: LinkedIn companies ↔ GMaps listings ↔
  OSM POIs currently merged only via slug/URL — a fuzzy-match/master-entity
  layer (phone, address geohash, name similarity) is a natural next step to
  produce one golden record per business.
- **Richer enrichment**: website tech-stack detection, social handles,
  review-sentiment scoring, WhatsApp/phone validation.
- **Monetization depth**: usage metering per subscriber, Stripe billing, API
  key tiers on the premium API, webhook exports (new-email alerts).
- **LLM layer**: `db_classify` already uses an LLM; extendable to lead
  scoring, personalized outreach draft generation, and anomaly detection on
  pipeline stats.
- **Horizontal scaling**: daemons are stateless w.r.t. each other and keyed by
  Redis/PG — a second box can run extra listing/detail workers against the
  same queues with no code change (mind Chrome/pinchtab placement).
- **Known gaps flagged in-repo**: LinkedIn 975-result search cap (mitigate via
  sub-keyword expansion), slug-less firehose companies long tail, OSM's sparse
  BD contact data.

---

## 7. Operational Posture

- **Boot-persistent**: every unit `WantedBy=default.target`, `Restart=on-failure`, linger enabled.
- **Backups**: 4×/day full PG dump (zstd) → Mega, 7-day local retention, documented restore.
- **Observability**: `scripts/ic_status.sh` live dashboard, monitor JSON mode,
  per-daemon logs in `/var/log/infinitecrawler/`.
- **Conventions**: additive-only schema migrations, secrets only in `.env`
  (never committed), API keys via `PLACES_API_KEYS` env, dashboards and
  daemons all systemd-managed.

## 8. Repo Map (quick)

```
daemons/     the 7 always-on workers (search, listing, places-api, nearby,
             linkedin-search, linkedin-job-detail, linkedin-company)
api/         internal REST :8015      api_premium/  JWT API :8016
web/         subscriber SPA :5173     web-admin/    ops SPA :5174
services/    classification           strategies/   per-site scrape logic
base/, factory/  scraper framework    config/       YAML configs (.example tracked)
scripts/     ops, enrichment, migrations, one-off collectors
utils/       pg, redis, proxy helpers systemd/      all unit + timer definitions
docs/        audits, plans, OSM research tests/       pytest
```
