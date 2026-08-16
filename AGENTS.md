# AGENTS.md — InfiniteCrawler

24/7 Google Maps lead generation pipeline for Bangladesh (15 BPT sectors × 15 cities) plus 29 global cities. Three extraction strategies run side by side: browser scroll, Places API multi-key enrichment, and Nearby Search grid-scanning — all writing to the same PostgreSQL `gmaps_listings` table.

## Stack & Tooling

- **Python 3.12+**, deps via `uv` (`uv run python ...`, `uv sync`, `uv add`)
- **ruff** for linting (`ruff check .`), **pyright** for type checking, **pytest** for tests
- **PostgreSQL** (local socket `/var/run/postgresql:5432`, pw `changeme`, `PG_HOST` env required at import time)
- **Redis** on `6379` (queue + transient state)
- **FastAPI** dashboard/API at `:8015` (`uv run python -m api.main`)
- **Google Places API (New)** — multi-key rotation, `PLACES_API_KEYS` env var (comma-separated)
- `max_results` cap = 100,000

## Architecture

```
search-daemon (browser) → GMaps scroll → PG upsert (gmaps_search_results)
places-api-daemon (HTTP) → GetPlace + Text Search → PG upsert (gmaps_listings)
nearby-scanner-daemon (HTTP) → grid-scanning 29 cities → PG upsert (gmaps_listings)
listing-daemon (browser, legacy) → Redis queue → deep extraction → PG upsert (gmaps_listings)
enrichment:
  db_email_extract --loop     (perpetual, 30s gap)
  db_email_extract            (every 2h, safety net)
  db_classify                 (daily, LLM)
  monitor_pipeline + watchdog  (every 15min, auto-heal)
```

Query mix: 70% BD-Local (city×keyword), 10% BD-National, 20% Global (6 international markets). 23,460 unique queries sourced from business-plan-template `sectors.yaml`.

### Places API (New) — Free Tier Quota

Each Google Cloud project gets **separate 200/day caps per API method** (free tier, no billing required):
- GetPlace (`places/{place_id}`): 200/key/day — 100% accurate via ChIJ place_id
- Text Search (`places:searchText`): 200/key/day — ~90% accurate, searches by business name
- Nearby Search (`places:searchNearby`): 200/key/day — returns up to 20 places per call, grid discovery

With 5 keys: 1,000 GetPlace + 1,000 Text Search + 1,000 Nearby Search = 3,000 API calls/day.
Nearby Search yields 20 places/call → **20,000 NEW listings/day**.

## Repository Layout

```
infinitecrawler/
├── daemons/         # search-daemon, listing-daemon, places-api-daemon,
│                    # nearby-scanner-daemon, enrichment loops
├── api/             # FastAPI dashboard + REST
├── base/            # shared base classes (scraper, daemon)
├── config/          # pipeline config + .yaml.example templates
├── factory/         # scraper factory + strategy registration
├── strategies/      # per-site scroll/extraction strategies
├── services/        # domain services (email, classify)
├── utils/           # shared utilities
├── systemd/         # user service units
├── scripts/         # monitor, ops, one-off tools
├── tests/           # pytest
└── docs/            # design docs + plans
```

## Conventions

- Always `uv run python ...` — system `python3` lacks project deps.
- Scripts in `scripts/` are standalone (existing convention); package code lives in `daemons/`, `api/`, `services/`, etc.
- `output/`, `logs/`, `backups/` are gitignored — do not commit.
- Secrets in `.env` (chmod 600), sourced by systemd `EnvironmentFile=`. Never inline in service units.
- API keys in `PLACES_API_KEYS` env var (comma-separated). Config YAMLs with keys are gitignored; only `.yaml.example` templates are tracked.
- DB schema migrations must be additive — never break an existing database.
- Daemons are `systemctl --user` units with linger enabled. They survive reboot.
- `.agents/knowledge-base.md` contains accumulated operational knowledge — read it before extending.
- The three listing daemons coexist safely — all upsert to `gmaps_listings` with `ON CONFLICT (source_url) DO UPDATE`.

## Gotchas

1. **`PG_HOST` env var is required at import time** — set it in `.env` or the import itself fails, not just the connection.
2. **Ruff rule set is intentionally minimal** (`E4/E7/E9 + F` only). Adding `I`/`UP`/`B` all-at-once surfaced 727 pre-existing warnings. Expand selectively, not all at once.
3. **Chrome instance sharing**: marketplace scraper and IC both use Chrome — stagger runs to avoid collision. Per-query lock files prevent collision within IC. The Places API and Nearby Scanner daemons do NOT use Chrome.
4. **Places API daily cap is the bottleneck**, not QPM (6,000 QPM ceiling). Each key: 200/day per method. When all keys exhausted, daemons sleep 600s and retry.
5. **Places API quota resets at ~midnight Pacific** (~08:00 UTC). The daemons use a conservative 25h rolling window for auto-reset.
6. **Nearby Search returns max 20 places/call** — use type batching (3 batches: food, retail, health) to avoid truncating dense areas. Grid circles overlap, so dedup by place_id.
7. **email_extract has two modes**: `--loop` (perpetual, 30s gap) and one-shot (every 2h safety net). Both must stay enabled.
8. **Config files with API keys are gitignored** — `config/places_api_daemon.yaml` and `config/nearby_scanner.yaml` are not tracked. Use the `.yaml.example` templates and `PLACES_API_KEYS` env var.

## API Daemon vs Browser Daemon

| Feature | Places API Daemon | Listing Daemon (browser) |
|---------|-------------------|--------------------------|
| Speed | 40x faster (pure HTTP) | ~7 places/min |
| Accuracy | 90-100% (GetPlace exact, Text Search ~90%) | Variable (depends on GMaps HTML) |
| Browser | None | Chrome via pinchtab |
| Daily cap | 1,600/day (5 keys × 2 methods × 200) | ~10,000/day (no API cap) |
| Source | gmaps_search_results (existing URLs) | gmaps_search_results (existing URLs) |

| Feature | Nearby Scanner Daemon |
|---------|----------------------|
| Speed | 20 places/call, ~0.1s/call |
| Accuracy | 100% (API returns structured data) |
| Browser | None |
| Daily cap | 20,000/day (5 keys × 200 × 20) |
| Source | Grid scan (discovers NEW places, not from gmaps_search_results) |

## Workflow Checklist

1. Schema change? Additive only — `IF NOT EXISTS` or `ALTER TABLE ADD COLUMN`.
2. New daemon? Mirror the existing systemd unit pattern (`systemd/`), bind `0.0.0.0`, add `After=tailscaled.service`.
3. Did you run `uv run ruff check .` and `uv run pytest`?
4. Did you restart the relevant daemon via `systemctl --user restart <unit>`?
5. `git status` clean or only intended changes?
6. API keys in env var, not committed to git?
