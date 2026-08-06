# InfiniteCrawler — Agent Knowledge Base

> **Self-healing pipeline audit playbook.** Run top-to-bottom when: pipeline seems stuck, data stops flowing, or after any code change. Each phase reports findings — phases marked **BLOCK** halt the audit; phases marked **REPORT** continue regardless.

---

## KEY FACTS TABLE

| Fact | Value | Source / Why |
|------|-------|--------------|
| Pinchtab bridge port | `9868` | `/root/.pinchtab/config.json` → `server.port` |
| Pinchtab API token | `123456` | `/root/.pinchtab/config.json` → `server.token` |
| Redis namespace (search) | `gmaps_bd_business:*` | Search daemon queue prefix |
| Redis namespace (listing) | `gmaps:*` | Listing daemon queue prefix |
| Postgres host | `/var/run/postgresql` (unix socket) | TCP bind to 127.0.0.1:5432 fails on this WSL host; use unix socket. Configs updated. |
| Postgres db | `infinitecrawler` | `scraper.*` schema |
| Postgres user | `postgres` | Default |
| Postgres password | `PG_PASSWORD` (not set in shell) | Set in `.env`; systemd services source it via EnvironmentFile. For interactive commands use `PGPASSWORD=changeme` or `source .env` first. Commands below hardcode the value. |
| LLM classification key | `LLM_API_KEY` | Required by `scripts/db_classify.py` / `scripts/llm_classifier.py`. Not set in `.env` currently — skip Phase 4c if key missing. |
| Failed queues use HASHES | `HLEN` not `LLEN` | `failed` queues are hash maps |
| Chrome CDP debug port | `9869` | Do NOT use for daemons — daemons talk to pinchtab on `9868` |
| Repository root | `/root/codebase/vhd/infinitecrawler` | All commands relative to this |
| Emails table missing cols | `is_obfuscated`, `context_snippet` | Added 2026-07-25; upsert was silently failing |
| Emails column rename | `source_type` → `extraction_method` | Fixed 2026-08-03; UPSERT_EMAIL_SQL + caller in db_email_extract.py + stats query all updated |
| Emails unique key | `(listing_id, email)` | Added 2026-07-25 |
| LinkedIn unique key | `profile_url` | Added 2026-07-25 |
| Search config needs | `ignore_completed_on_enqueue: true` | Prevents query exhaustion when all queries are in completed set |
| Listing daemon bug | `await restart_browser()` in eternal loop | Removed 2026-07-25; was restarting browser on EVERY failed URL |
| Postgres config drift | `listen_addresses` may revert to `localhost` | If cluster restarts externally, socket path breaks. Set to `''` for unix-socket-only. |
| Postgres config drift | `port` may revert to `5433` | Same restart issue. Keep `port = 5432`. |
| Enrichment services | `email-extract`, `linkedin-search`, `linkedin-firehose` | `email-extract` + `linkedin-search` run as systemd timers (every 2h/4h). **`linkedin-firehose` is PERPETUAL: runs continuously as `infinitecrawler-linkedin-firehose-loop.service`** (`--loop --loop-gap 60`, restart=always) — one 8000-query cycle ends, 60s pause, fresh random 8000-query sample runs again, forever. Monitor via `systemctl --user status infinitecrawler-linkedin-firehose-loop`. Firehose uses ddgs metasearch engine v9.14.4 (`pip install ddgs`) — not the http proxy. Source=`firehose` in `scraper.linkedin_profiles`. Concurrency=8, ThreadPoolExecutor with thread-local DDGS instances. |
| Firehose config | `config/linkedin_firehose.yaml` | Query matrix: 56 roles × (20 BD cities + 15 global hubs × 20 industries + 1 role-only) = ~18k. Capped at 8000/cycle random sample. Engine=ddgs with `auto` backend + `[yahoo, yandex]` fallbacks. Region=wt-wt (global) / bd-bn (Bangladesh). SyslogIdentifier=ic-firehose. |
| LinkedIn profiles columns | 13 cols: `id, listing_id, profile_url, full_name, confidence, source, checked_at, profile_title, company_name, search_query, snippet, last_updated, notes` | `source` and `notes` added by scripts/schema_luxury.py FIX_LINKEDIN_PROFILES block (~line 102). Not in schema_migration.py DDL. |
| Phase 3a drift trap | `-h 127.0.0.1` silently fails | 2026-07-29 audit: caused false-positive `search_1h=0`. Always use `-h /var/run/postgresql`. |
| Phase 3 listings_1h false-alarm | `listings_1h=0` in first ~30 min after daemon restart is normal | 2026-08-07 audit: `PostgreSQLListingDetailsUpsertStrategy._BATCH_SIZE=50`; in-memory `_write_batch` is flushed only on 50 items or shutdown. With ~16-48s/URL, the first batch flush lands 13-40 min after startup. Check `MAX(updated_at)` age, not `COUNT` — if MAX age > batch_interval × 2, *then* it's a real stall. |
| Listing daemon write path | `_write_batch` → `_flush_write_batch(executemany)` | Verified 2026-08-07: standalone test wrote 50 rows in <1s. Path is healthy; the listing daemon's apparent "no writes" was a Phase 3 false-alarm, not a real bug. |
| Phase 2.5 drift trap | `--no-clean` invalid psql flag | Use `-t` (tuples-only). |
| infinite_scroll probe JS quote bug | `json.dumps(['div[role="feed"]',...])` injected into JS source produced `["div[role="feed"]",...]` with unbalanced quotes → `SyntaxError` every probe → 0 scroll, hits `max_scroll_attempts=500` (~33min/query). Fixed 2026-08-01: wrap in `JSON.parse(json.dumps(...))` so quotes stay escaped. |
| GMaps scroll needs event dispatch | Setting `el.scrollTop = el.scrollHeight` alone does NOT trigger GMaps IntersectionObserver. Must also `el.dispatchEvent(new Event('scroll', {bubbles: true}))`. Without it, cards stay at 10 forever even though more exist. Verified 2026-08-01. |
| pinchtab 0.15 `/action kind:close` | Returns 400 `unknown action kind: close`. Same for `DELETE /tabs/:id` (404 page not found). Pinchtab has no tab-close endpoint — daemon cleanup must use `navigate("about:blank")` to reuse the tab and avoid leak. Verified 2026-08-01. |
| Logging pattern | Root logger WARNING, app logger INFO + dedicated handler, `propagate=False` | db_linkedin_firehose.py (2026-08-03) and db_email_extract.py (2026-08-03) use this pattern to suppress httpx/ddgs noise. All enrichment scripts should follow this — `basicConfig(level=INFO)` floods logs. |
| DB commit durability | 3x retry `conn.commit()` with 0.5s/1s backoff, `conn.rollback()` on row failure | Firehose `save_batch()` and `upsert_emails()` both use this pattern (2026-08-03). Prevents silent data loss on transient PG outages. |
| SQL NULL trap — linkedin-search | `NOT IN` with nullable column dead since 2026-07-25 | Fixed 2026-08-03: `utils/pg.py` FETCH_UNPROCESSED_LINKEDIN_SQL and FETCH_UNPROCESSED_EMAILS_SQL now use `NOT EXISTS`. Firehose NULL listing_ids caused `NOT IN` subquery to return 0 rows always. `db_linkedin_search.py` sector-filter clause also patched. |
| Shutdown flush order | `daemons/common.py` flush BEFORE cleanup | Fixed 2026-08-03: reordered so `output_strategy.flush_batch()` calls before `output_strategy.cleanup()` — prevents losing up to 49 batched rows on graceful shutdown. |
| Email extract httpx leak | New client per listing with no limits | Fixed 2026-08-03: one shared `httpx.AsyncClient` per batch with `Limits(max_connections=concurrency)`, pidfile guard (`_system/email_extract.pid`), heartbeat logs every 30s, per-task `asyncio.timeout(FETCH_TIMEOUT+5)`, `MAX_HTML_BYTES=2MB` input cap. Service unit has `TimeoutStartSec=3600`, `Restart=on-failure`. |
| Email extract ReDoS | `OBFUSCATED_PATTERNS` pattern 1 had catastrophic backtracking | Fixed 2026-08-03: bounded character classes `{1,64}`, required explicit `[at]`/`(at)` marker (no longer optional), `{2,16}` TLD cap. Created `MAX_SCAN_BYTES=2_000_000` input truncation in `scan_text_for_emails()`. |
| LLM classifier key | `os.environ["LLM_API_KEY"]` → crash on import | Fixed 2026-08-03: `LLM_API_KEY = os.environ.get("LLM_API_KEY", "")` with graceful fallback warning. |
| Classify timer | Not scheduled, daily at 03:00 with 10min jitter | Created 2026-08-03: `infinitecrawler-classify.{service,timer}` user units. Runs `uv run python scripts/db_classify.py`. |
| Watchdog timer | Not scheduled, every 15 min | Created 2026-08-03: `infinitecrawler-watchdog.{service,timer}`. Runs `scripts/watchdog.sh`. Monitor extended: email-extract freshness, firehose-loop status, API health (port 8015), PG reachability. Auto-restarts firehose and email-extract in `--restart` mode. |
| Monitor pipeline checks | Only `listing_daemon` pgrepped | Extended 2026-08-03: now checks email-extract staleness (max `discovered_at` from `scraper.emails`), `infinitecrawler-linkedin-firehose-loop` service active, API `http://127.0.0.1:8015/` responding, PG via `max(discovered_at)` query. |
| Tests | Only `test_cli_and_config.py` existed | Added 2026-08-03: `test_pg_queries.py` (NULL trap regression), `test_email_extract.py` (26 tests: regex, obfuscation, noise, dedup, large-input ReDoS), `test_shutdown_order.py` (flush-before-cleanup ordering). All 39 tests pass. |
| Dead-code guard | Was missing since 2026-08-03 (playbook kept flagging it) | Created 2026-08-07: `scripts/assert_dead_code.sh` — a guard-list of intentionally-removed files; fails if any reappears. Currently guards 5 paths (AGENTS.md, docs/GMAPS_*.md, scripts/check-stuck-chrome.sh, strategies/input/__init__.py). Add a line whenever you delete a file so the next Phase 5c run guards against re-regression. |
| Lint config | None (ruff used defaults) | Added 2026-08-07: `[tool.ruff]` + `[tool.ruff.lint]` in `pyproject.toml`. Selects `E4/E7/E9/F` (same as ruff defaults) so adding `W/I/UP/B` is opt-in only. Also excludes `*.sh/*.yaml/*.md/*.json` so non-Python files don't trigger parse-error false positives (the report surfaced 41 bogus errors scanning `assert_dead_code.sh`). |

# PHASE 0 — REPOSITORY DRIFT DISCOVERY (BLOCK)

Before trusting ANY knowledge in this document, verify that the repository still matches it.

Discover automatically:

- services
- daemons
- queues
- config files
- scripts
- migrations
- cron jobs
- timers
- systemd units
- CLI entrypoints
- database schemas
- output strategies
- browser providers
- infrastructure

Compare discoveries against this document.

For every mismatch:

1. Verify which version is correct.
2. Update this document.
3. Remove obsolete knowledge.
4. Record the change in the Knowledge Updates section.

Never continue using stale assumptions.

---

## BEFORE YOU START: PRE-FLIGHT

Run these 3 checks first. If any fail, fix them BEFORE running Phases 1-6:

```bash
# PF1. Redis alive?
redis-cli PING               # Expected: PONG

# PF2. Postgres reachable
PGPASSWORD=$PG_PASSWORD psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "SELECT 1" --no-align --tuples-only
# Expected: 1

# PF3. Pinchtab has tabs
PINCHTAB_TOKEN=$(python3 -c "import json; print(json.load(open('/root/.pinchtab/config.json'))['server']['token'])")
curl -s --connect-timeout 5 -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('tabs',0))"
# Expected: integer (tab count, any value OK)
```

**If any pre-flight fails → fix that service, then proceed.**

---

## REPOSITORY EVOLUTION PROTOCOL

This document is part of the repository.

Treat it exactly like production code.

Whenever you execute this audit you MUST review BOTH:

- the repository
- this document

If either can be improved, improve it.

After every audit:

• update discovered facts
• remove obsolete facts
• rewrite outdated sections
• simplify duplicated sections
• merge overlapping checks
• improve diagnostics
• improve repair actions
• improve command reliability
• improve readability
• improve organization
• improve maintainability

Never preserve obsolete information.

If a command fails,
replace it with a working command.

If a better diagnostic exists,
replace the old one.

If a better repair exists,
replace the old one.

If a better organization exists,
rewrite the document.

The goal is that this document becomes more accurate after every execution.
---

## PHASE 1: SERVICE & DAEMON LIFECYCLE CHECK — BLOCK

Run these commands in order. If any service is dead, restart it:

```bash
# 1a. All service statuses (with recent logs)
systemctl --user status infinitecrawler-search infinitecrawler-listing \
  infinitecrawler-email-extract infinitecrawler-linkedin-search \
  infinitecrawler-linkedin-firehose-loop \
  pinchtab --no-pager -l --lines=20

# 1b. Scheduled enrichment timers (email/linkedin/pg-backup run periodically)
systemctl --user list-timers --no-pager | grep infinitecrawler
# Expected: email-extract ~2h, linkedin-search ~4h, pg-backup daily. linkedin-firehose-loop is PERPETUAL (no timer).

# 1c. Pinchtab health (the Chrome provider both daemons depend on)
PINCHTAB_TOKEN=$(python3 -c "import json; print(json.load(open('/root/.pinchtab/config.json'))['server']['token'])")
curl -s --connect-timeout 5 -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health

# Expected: all three services active (running) + firehose-loop active. Health returns tab count, crashes stats.
# Pinchtab bridge port: 9868 (NOT 9869). Token: 123456.
```

**EXIT_CODE: BLOCK** — if any service is dead or unreachable, repair it before continuing.

---

## PHASE 2: REDIS QUEUE DIAGNOSTICS — BLOCK

Check both queue namespaces. `failed` queues are **HASHES** — use `HLEN` not `LLEN`:

```bash
# 2a. Search daemon queue (gmaps_bd_business:*)
redis-cli LLEN  gmaps_bd_business:pending
redis-cli LLEN  gmaps_bd_business:processing
redis-cli SCARD gmaps_bd_business:completed
redis-cli HLEN  gmaps_bd_business:failed

# 2b. Listing daemon queue (gmaps:*)
redis-cli LLEN  gmaps:pending
redis-cli LLEN  gmaps:processing
redis-cli SCARD gmaps:completed
redis-cli HLEN  gmaps:failed

# 2c. Full pipeline monitor (JSON for parsing)
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/monitor_pipeline.py --json
```

**Red flags (add new ones here when discovered):**
- `processing` > 0 but no daemon running → stalled items
- `completed` not growing over time → daemon not processing
- `failed` > 50 → extraction issues or dead listings
- Search queue `completed` near 0 despite `pending` > 1000 → **REGRESSION: search daemon not calling `mark_completed`** (historic bug `e6cbb0d`)
- `processing` > 0 AND `completed` NOT growing AND daemon IS running → tab stuck/crashed, daemon restart needed
- `pending` dropping but `completed` flat → output strategy PG connection dropped (fixed in `dd8fece` — but verify)
- Search enqueued 0 new queries when pending is low → all generated queries are already in `completed` set; set `ignore_completed_on_enqueue: true` in search config
- Daemon logs `Reconnecting to pinchtab (pages=1, uptime=<2min)` every few seconds → **REGRESSION: listing daemon restarts browser on every URL failure** (fixed 2026-07-25 by removing `await restart_browser()` from eternal loop)
- search_1h = 0 while daemon navigates every 6s and reconnects every 100 pages → **all queries generate identical GMaps results; mapping static → ON CONFLICT DO UPDATE yields no new `updated_at`**. Query diversity exhaustion. Expand query pool or add new markets.
- Daemon navigates 100 pages in 15min → each page produces zero new DB writes → output_strategy `ON CONFLICT` matched; probably same queries/same results.
- **New drift trap (2026-07-29/30): `Phase 3a` using `-h 127.0.0.1` silently returns 0 for all queries.** Always use `-h /var/run/postgresql`. If `search_1h=0` while pages are being processed, switching to Unix socket is the first diagnostic.
- **New drift trap (2026-08-01): `InfiniteScrollPaginationStrategy` logs `Scroll N: M cards, 0 new` for ALL iterations, then hits `max_scroll_attempts=500` and exits with `Reached the end (10 unique)` even though GMaps actually returned more.** Two-part cause: (a) `_scroll_pane` probe JS has unbalanced quotes from naive `json.dumps()` into JS source — fix uses `JSON.parse(json.dumps(sels))`. (b) Even with probe fixed, programmatic `scrollTop` assignment doesn't fire GMaps' IntersectionObserver — must add `el.dispatchEvent(new Event('scroll', {bubbles:true}))` after. Verify scroll health by looking at `Scroll N: M cards, K new (cumulative M)` — `K > 0` in early iterations.

**EXIT_CODE: REPORT** — anomalies logged; continue audit.

---

## PHASE 2.5: SYSTEM RESOURCE HEALTH — REPORT

```bash
# 2.5a. Disk usage (catch log/DB outages)
df -h / | tail -1
# Expected: Use% < 90%

# 2.5b. Memory pressure
free -h | awk 'NR==1||NR==2'
# Expected: available > 500M

# 2.5c. PG connection count (spikes indicate leak) — use unix socket, -t for tuples-only
# Note: --no-clean is NOT a valid psql flag. Use -t (--tuples-only).
PGPASSWORD=$PG_PASSWORD psql -h /var/run/postgresql -U postgres -d infinitecrawler -t -c \
  "SELECT count(*) FROM pg_stat_activity WHERE datname='infinitecrawler'"
# Expected: < 20 (high count with low daemon activity → connection leak)
```

**EXIT_CODE: REPORT** — flag anomalies but continue.

---

## PHASE 3: DATABASE INGESTION VELOCITY — REPORT

Verify data is actually being written to PostgreSQL in real-time:

```bash
# 3a. Per-table freshness — the authoritative stall signal.
# Uses MAX(updated_at)-vs-NOW(), NOT a 1-hour COUNT, because listing/email
# enrichment upserts are batched (50 rows in-memory) and don't move
# updated_at until a flush. An old MAX is the real symptom; a 0-row 1h
# window is often a *false alarm* when the daemon uptime is < batch
# interval (see drift trap below). MUST use unix socket.
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "
SELECT 'gmaps_search_results' AS table,
       NOW() - MAX(updated_at) AS age_max,
       MAX(updated_at) AS last_write
FROM scraper.gmaps_search_results
UNION ALL
SELECT 'gmaps_listings', NOW() - MAX(updated_at), MAX(updated_at)
FROM scraper.gmaps_listings
UNION ALL
SELECT 'emails', NOW() - MAX(discovered_at), MAX(discovered_at)
FROM scraper.emails
UNION ALL
SELECT 'linkedin_profiles', NOW() - MAX(checked_at), MAX(checked_at)
FROM scraper.linkedin_profiles
ORDER BY age_max DESC;
"
# Expected:
#   gmaps_search_results.age_max < 10 min (search daemon writes ~2/s)
#   gmaps_listings.age_max     < (daemon uptime < 1.5h ? 30 min : 10 min)
#                                ↑ see drift trap below
#   emails.age_max             < 2h (timer every 2h; mid-run writes are continuous)
#   linkedin_profiles.age_max  < 5 min (perpetual firehose)

# 3b. Row-count velocity (last hour / 2h / 4h windows) — secondary signal.
# These can be 0 immediately after a daemon restart even when the daemon is
# healthy — see drift traps below. Use 3a above (max age) as the primary
# decision signal for stall detection.
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour') as search_1h,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour') as listings_1h,
  (SELECT COUNT(*) FROM scraper.emails WHERE discovered_at > NOW() - INTERVAL '2 hours') as emails_2h,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE checked_at > NOW() - INTERVAL '4 hours') as linkedin_4h;
"

# 3c. Full counts (unix socket — same reason)
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results) as search_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings) as listings_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL AND website IS NOT NULL) as qualified,
  (SELECT COUNT(*) FROM scraper.emails) as emails_total,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles) as linkedin_total;
"
```

**Expected (3a max-age — primary):** search ≤ 10 min, linkedin ≤ 5 min, emails ≤ 2h, listings ≤ 10 min UNLESS listing-daemon uptime < 1.5h (see drift trap).
**Expected (3b row counts — secondary):** search_1h > 0. listings_1h is allowed to be 0 in the first ~30 min after a listing-daemon restart.
**Drift trap:** If `search_1h = 0` while daemon is processing, you probably used `-h 127.0.0.1`. Always use `-h /var/run/postgresql`. Verify with the 3a query (max age should be < 10 min).
**Drift trap (2026-08-07):** `listings_1h = 0` is expected when the listing daemon's uptime < ~30 min — `PostgreSQLListingDetailsUpsertStrategy` uses an in-memory `_write_batch` with `_BATCH_SIZE=50` that is flushed only when the buffer fills or the daemon shuts down. At ~16-30s/URL that's ~13-25 min for the first flush. Distinguish from a real stall with the 3a query: if `gmaps_listings.age_max` < 1h AND daemon uptime < 1.5h, it's a normal batch interval, not a bug. After uptime > 1.5h, age_max > 30 min IS a real stall (PG connection dropped silently — check pg_stat_activity and daemon logs for "PG reconnect failed").
**EXIT_CODE: REPORT**

---

## PHASE 4: ENRICHMENT COMPLETENESS — REPORT

Verify the offline enrichment scripts are producing results:

```bash
# 4a. Email coverage
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_email_extract.py --stats

# 4b. LinkedIn coverage (company-anchored)
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_linkedin_search.py --stats

# 4b-bis. LinkedIn firehose (global DDGS, no GMaps anchor)
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_linkedin_firehose.py --stats

# 4c. Classification coverage (requires LLM_API_KEY in .env; skip if not set)
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_classify.py --stats 2>/dev/null || echo "WARN: LLM_API_KEY not set — skipping classification check"
```

**Expected:** Email coverage growing every 2h. LinkedIn profiles growing every 4h. Firehose profiles growing continuously (perpetual service). Classification skipped if LLM_API_KEY missing.
**EXIT_CODE: REPORT**

---

## PHASE 5: CODEBASE SANITY (Quick Checks) — REPORT

```bash
cd /root/codebase/vhd/infinitecrawler

# 5a. Lint
uv run ruff check .
# Ruff rule set is configured in pyproject.toml [tool.ruff.lint].select —
# keep it minimal (E4/E7/E9/F). Adding 'W'/'I'/'UP'/'B' surfaced 727 pre-existing
# warnings on the 2026-08-07 audit; only broaden after a sweep cleans them.

# 5b. Tests
uv run python -m pytest tests/ -v

# 5c. Dead-code assertion (self-documenting guard list)
# scripts/assert_dead_code.sh lists every file intentionally removed in past
# refactors. It FAILS if any of them reappears (regression detection).
# When you delete a file, add a line to DEAD_FILES in that script — the
# next playbook run will then guard against accidental reintroduction.
bash scripts/assert_dead_code.sh || echo "WARN: dead-code target reappeared — see output above"

# 5d. Pinchtab port integrity — configs must NOT reference wrong ports
grep -rn "9869\|e03c" config/ base/ daemons/ 2>&1
# Expected: zero matches. Port 9869 is the Chrome CDP debug port, not for daemons.
# Token must be 123456, never the old e03c... placeholder.

# 5e. Config YAMLs loadable (all 3 enrichment configs)
uv run python -c "
from factory.scraper_factory import ScraperFactory
c1 = ScraperFactory.load_config('config/gmaps_bd_business_search.yaml')
c2 = ScraperFactory.load_config('config/gmaps_listings_working.yaml')
print(f'Search config OK ({len(c1)} keys)')
print(f'Listing config OK ({len(c2)} keys)')
"
# Note: linkedin_firehose.yaml is NOT loaded via ScraperFactory — it's read
# directly by scripts/db_linkedin_firehose.py. Verify it parses separately:
uv run python -c "
import yaml
cfg = yaml.safe_load(open('config/linkedin_firehose.yaml'))
roles = cfg.get('roles', [])
print(f'Firehose config OK ({len(roles)} roles)')
"

# 5f. All core imports working (auto-discovered — no hardcoded list)
uv run python -c "
try:
    from daemons.search_daemon import DaemonState as SearchState
except: print('WARN: search_daemon'), None

try:
    from daemons.listing_daemon import DaemonState as ListingState
except: print('WARN: listing_daemon'), None

try:
    from base.browser_manager import BrowserManager
except: print('WARN: browser_manager'), None

try:
    from base.pinchtab_client import PinchtabClient, PinchtabConfig
except: print('WARN: pinchtab_client'), None

try:
    from strategies.queue.redis_queue import RedisQueueStrategy
except: print('WARN: redis_queue'), None

try:
    from strategies.output.postgresql import PostgreSQLOutputStrategy, PostgreSQLUpsertStrategy, PostgreSQLListingDetailsUpsertStrategy
except: print('WARN: postgresql strategy'), None

try:
    from utils.pg import get_pg_config, get_uncrawled_urls_sql
except: print('WARN: utils.pg'), None

try:
    from utils.helpers import DelayManager
except: print('WARN: utils.helpers'), None

print('Core import audit complete — warnings above indicate missing modules')
"

# 5g. DB schema integrity — verify tables have columns/constraints the code expects
PGPASSWORD=$PG_PASSWORD psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "
SELECT column_name FROM information_schema.columns WHERE table_schema='scraper' AND table_name='emails' AND column_name IN ('is_obfuscated','context_snippet','extraction_method');
SELECT conname FROM pg_constraint WHERE conrelid='scraper.emails'::regclass AND conname='emails_listing_id_email_key';
SELECT conname FROM pg_constraint WHERE conrelid='scraper.linkedin_profiles'::regclass AND conname='linkedin_profiles_profile_url_key';
SELECT column_name FROM information_schema.columns WHERE table_schema='scraper' AND table_name='linkedin_profiles' AND column_name='source';
"
# Expected: 3 columns (is_obfuscated, context_snippet, extraction_method), 1 email unique constraint, 1 linkedin unique constraint, 1 source column
```

**EXIT_CODE: REPORT**

---

## PHASE 6: REPAIR ACTIONS (run only if phases 1-5 detect issues)

After every successful repair:

Determine

Why it happened.

How it could have been detected earlier.

How to prevent recurrence.

Update:

Red Flags

Diagnostics

Repair section

Knowledge Base

Playbook

If prevention is possible,
add it.

> **WARNING:** This section is destructive. Run phases 1-5 first, then apply ONLY the repairs needed.

```bash
# === DRY-RUN FIRST (read-only, safe) ===

# 6aa. Preview stuck search daemon items
echo "=== Stuck search items (dry-run): ==="
redis-cli LLEN gmaps_bd_business:processing && redis-cli --raw LRANGE gmaps_bd_business:processing 0 5

# 6ab. Preview stuck listing daemon items
echo "=== Stuck listing items (dry-run): ==="
redis-cli LLEN gmaps:processing && redis-cli --raw LRANGE gmaps:processing 0 5

# === APPLY (run only after reviewing dry-run output) ===

# 6a. Drain stuck search daemon processing items → push back to pending
redis-cli --raw LRANGE gmaps_bd_business:processing 0 -1 | while IFS= read -r item; do
  redis-cli LPUSH gmaps_bd_business:pending "$item" > /dev/null
  redis-cli LREM gmaps_bd_business:processing 1 "$item" > /dev/null
done
redis-cli DEL "api.search_bd_business:processing:timestamps"

# 6b. Drain stuck listing daemon items
redis-cli --raw LRANGE gmaps:processing 0 -1 | while IFS= read -r url; do
  redis-cli LPUSH gmaps:pending "$url" > /dev/null
  redis-cli LREM gmaps:processing 1 "$url" > /dev/null
done
redis-cli DEL "api.search_bd_business:processing:timestamps"

# 6bb. Clear stale failure queues when daemons were crashing
# Use only when failures accumulated from a past outage and daemons are now healthy.
redis-cli DEL gmaps_bd_business:failed
redis-cli DEL gmaps:failed

# 6c. Fix search query exhaustion (when all queries are in completed set)
# Add ignore_completed_on_enqueue: true to queue.config in config/gmaps_bd_business_search.yaml
# This allows the search daemon to re-search queries (GMaps results change over time)

# 6d. Fix DB schema if upserts are silently failing
PGPASSWORD=$PG_PASSWORD psql -h /var/run/postgresql -U postgres -d infinitecrawler <<'EOSQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'scraper.emails'::regclass AND attname = 'is_obfuscated') THEN
        ALTER TABLE scraper.emails ADD COLUMN is_obfuscated BOOLEAN DEFAULT false;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'scraper.emails'::regclass AND attname = 'context_snippet') THEN
        ALTER TABLE scraper.emails ADD COLUMN context_snippet TEXT;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'scraper.emails'::regclass AND conname = 'emails_listing_id_email_key') THEN
        ALTER TABLE scraper.emails ADD CONSTRAINT emails_listing_id_email_key UNIQUE (listing_id, email);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'scraper.linkedin_profiles'::regclass AND conname = 'linkedin_profiles_profile_url_key') THEN
        ALTER TABLE scraper.linkedin_profiles ADD CONSTRAINT linkedin_profiles_profile_url_key UNIQUE (profile_url);
    END IF;
END $$;
EOSQL

# 6e. Fix broken get_uncrawled_urls_sql if it references non-existent columns
# The SQL in utils/pg.py must use LEFT JOIN on source_url, not id NOT IN (SELECT listing_id...)

# 6f. Restart services (safe — systemd handles pinchtab dependency)
systemctl --user restart infinitecrawler-search
systemctl --user restart infinitecrawler-listing

# 6g-bis. Fix scroll regression (InfiniteScrollPaginationStrategy reports
#         "Scroll N: 10 cards, 0 new" forever, hits max_scroll_attempts)
# (a) _scroll_pane probe JS — replace naive `json.dumps()`-into-JS-source
#     with `JSON.parse(json.dumps(sels))` so quotes don't break.
# (b) After `el.scrollTop = el.scrollHeight`, ALSO
#     `el.dispatchEvent(new Event('scroll', {bubbles: true}))` —
#     GMaps' IntersectionObserver does not fire from programmatic scroll
#     alone.
# Files: strategies/pagination/infinite_scroll.py

# 6g. Restart pinchtab (only if Chrome is truly crashed and supervisor isn't recovering)
systemctl --user restart pinchtab.service

# 6h. Enrichment backlog recovery
uv run python scripts/db_email_extract.py --max 500
uv run python scripts/db_linkedin_search.py --max 200
uv run python scripts/db_linkedin_firehose.py --max-queries 2000
uv run python scripts/db_classify.py --retry-failed --max 1000

# 6i. Full pipeline health snapshot (always run last)
uv run python scripts/monitor_pipeline.py --json
```

**EXIT_CODE: BLOCK** — only proceed if phase 1-5 flagged anomalies.

---

## PHASE 7: SCROLL HEALTH (REPORT) — added 2026-08-01

Scroll regression detectors. Run when `search_1h` looks low, daemon shows
steady activity, but `extracted N items (0 new)` repeats forever.

```bash
# 7a. Live tail — verify scroll is producing new URLs
systemctl --user status infinitecrawler-search --no-pager -l --lines=50 \
  | grep -E "Scroll|extracted"
# Expected (healthy): "Scroll 2: 20 cards, 10 new (cumulative 20)" etc.
# Bad (regression):    "Scroll N: 10 cards, 0 new (8/8)" — stall_threshold hit
#                      but cumulative stays at ~10 → scroll never loads more.

# 7b. End-of-results sanity (queries should reach varied totals, not all 10)
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d \
  infinitecrawler -t -c "
SELECT
  COUNT(DISTINCT key_value) AS queries,
  COUNT(*) AS rows,
  ROUND(AVG(cnt)) AS avg_per_query,
  MIN(cnt) AS min_per_query,
  MAX(cnt) AS max_per_query
FROM (
  SELECT key_value, COUNT(*) AS cnt
  FROM scraper.gmaps_search_results
  WHERE updated_at > NOW() - INTERVAL '1 hour'
  GROUP BY key_value
) sub;
"
# Expected: min_per_query > 10 (note: with ON CONFLICT upsert, each key_value may have 1 row;
# verify scroll health via Phase 7a live-tail instead). If min_per_query = 10 across many distinct
# key_value entries (new query with only 10 results), the scroll bug may be back.

# 7c. Pinchtab /action 400s (close_tab regression)
TOKEN=$(python3 -c 'import json; print(json.load(open("/root/.pinchtab/config.json"))["server"]["token"])')
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9868/health \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
recent = d.get('failures', {}).get('recent', [])
n400 = sum(1 for f in recent if f.get('path')=='/action' and f.get('status')==400)
print(f'/action 400s in last batch: {n400}')
"
# Expected: 0. If > 0, daemon's close_tab is calling /action kind:close
# (pinchtab 0.15 has no such action). Fix: navigate("about:blank") instead.

**EXIT_CODE: REPORT**

---

## QUICK-REFERENCE: Command Cheatsheet

```bash
# Pinchtab health
curl -s --connect-timeout 5 -H "Authorization: Bearer $(python3 -c 'import json; print(json.load(open(\"/root/.pinchtab/config.json\"))[\"server\"][\"token\"])')" http://127.0.0.1:9868/health

# Redis queue snapshot
redis-cli LLEN gmaps_bd_business:pending && redis-cli LLEN gmaps_bd_business:processing && redis-cli SCARD gmaps_bd_business:completed && redis-cli HLEN gmaps_bd_business:failed
redis-cli LLEN gmaps:pending && redis-cli LLEN gmaps:processing && redis-cli SCARD gmaps:completed && redis-cli HLEN gmaps:failed

# DB velocity (1h window) — must use unix socket (TCP 127.0.0.1 fails on WSL)
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "SELECT 'search_1h', count(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour' UNION ALL SELECT 'listings_1h', count(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour'"

# Firehose stats + recent logs
uv run python scripts/db_linkedin_firehose.py --stats
journalctl --user -u infinitecrawler-linkedin-firehose --no-pager --since "6 hours ago" | grep -E "start|done|status|family-summary"

# Restart all daemons
systemctl --user restart infinitecrawler-search infinitecrawler-listing

# Full monitor
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/monitor_pipeline.py --json
```
---
## REPOSITORY MEMORY

Stable Architecture

Known Bugs

Known Design Decisions

Historical Migrations

Known Performance Issues

Known Operational Risks

Known False Positives

- **listings_1h false-alarm post-restart**: `PostgreSQLListingDetailsUpsertStrategy._BATCH_SIZE=50` buffers writes in memory; first batch flush lands 13-40 min after daemon restart depending on per-URL latency. Use `MAX(updated_at)` age, not 1h COUNT, when daemon uptime < 1.5h.

Known Temporary Workarounds

Deprecated Components

---
## KNOWLEDGE UPDATES

Chronological log of facts added or corrected by audit runs. Newest first.

- **2026-08-07** — Phase 5: 6 ruff lint errors found; 5 auto-fixed (unused imports in `scripts/llm_classifier.py`, unused `daemons.common` import in tests), 1 manually fixed (missing `Dict` import in `api/models/models.py:8`). All 39 tests still pass.
- **2026-08-07** — Phase 3 false-alarm: `listings_1h=0` in first ~30 min after listing-daemon restart is normal (50-item in-memory batch flush). New drift trap added above + Phase 3 restructured: added 3a (`MAX(updated_at)`-based freshness) as the primary stall signal ahead of the 3b row-count secondary signal.
- **2026-08-07** — `scripts/assert_dead_code.sh` created (was missing since 2026-08-03). Guards 5 intentionally-removed paths. Phase 5c rewired to call it.
- **2026-08-07** — `[tool.ruff]` + `[tool.ruff.lint]` added to `pyproject.toml` (select `E4/E7/E9/F`, extend-exclude for shell/yaml/markdown). Stops ruff from emitting 41 bogus parse errors scanning `assert_dead_code.sh` and the new firehose config check.
- **2026-08-07** — `.agents/remediate.py` config-glob broadened from `gmaps_*.yaml` to `gmaps_*.yaml + linkedin_*.yaml` so it detects drift in `config/linkedin_firehose.yaml` too.
- **2026-08-07** — Phase 5e extended to verify `config/linkedin_firehose.yaml` parses (was previously only checking the two gmaps configs).

---

## UNKNOWNS

The following could not be verified.

- Backup strategy

- Failover

- Metrics retention

- Secret rotation

- Queue TTL

- Browser pool sizing

Unknowns must never become assumptions.

If later discovered,
move them into Repository Memory.

Whenever a new subsystem is discovered that is not covered by an existing audit phase:

Automatically create a new phase.

Each new phase must include:

Purpose

Checks

Expected state

Red flags

Repair actions

Knowledge updates

Quick-reference commands

Never require manual additions.

## PHASE X — PLAYBOOK REVIEW

Review THIS document.

Detect:

obsolete checks

duplicate checks

missing diagnostics

missing repairs

incorrect assumptions

commands that could be automated

sections that became too large

sections that should be split

commands that repeatedly fail

Then rewrite the affected sections.

If no improvements exist,
explicitly report:

"No playbook improvements discovered."

## CONTINUOUS SELF-EVOLUTION

This playbook is a living operational specification.

It must evolve together with the repository.

Whenever executing this playbook, also audit the playbook itself.

You MUST continuously improve:

- accuracy
- completeness
- maintainability
- diagnostics
- repair procedures
- organization
- command reliability
- readability
- automation
- self-healing capability

Repository reality always overrides documented assumptions.

Whenever reality differs from this playbook:

1. Verify the change.
2. Update the playbook.
3. Remove obsolete knowledge.
4. Record the change.
5. Improve future diagnostics so the same drift is detected automatically.

Do not merely append new information.

Continuously refactor this document exactly as you would refactor production code.

The objective is that every execution leaves the playbook smarter, shorter where possible, more accurate, more maintainable, and better able to detect, repair, and prevent future failures.