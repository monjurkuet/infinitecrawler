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
| Postgres password | `$PG_PASSWORD` (env) | Set in `.env` / systemd env |
| Failed queues use HASHES | `HLEN` not `LLEN` | `failed` queues are hash maps |
| Chrome CDP debug port | `9869` | Do NOT use for daemons — daemons talk to pinchtab on `9868` |
| Repository root | `/root/codebase/vhd/infinitecrawler` | All commands relative to this |
| Emails table missing cols | `is_obfuscated`, `context_snippet` | Added 2026-07-25; upsert was silently failing |
| Emails table migration | `source_type` → `extraction_method`; added `website_url`, `email_type`, `last_verified`, `is_active` | Applied 2026-08-02 via `scripts/schema_migration.py` (idempotent `UPGRADE_EMAILS_TABLE` DO block). Code + DDL + live DB now aligned; API stats queries (`extraction_method`, `email_type`) work. |
| Email regex ReDoS | Old `OBFUSCATED_PATTERNS[0]` had optional-bracket groups → O(n²) backtracking (30KB = 23s). Replaced with 2 linear-time patterns 2026-08-02 | New patterns: `[at]`/`(at)`/`[at]`/`AT DOT` all covered; 300KB = 0.42s. Never add optional nested groups after greedy captures in these patterns. |
| Email scan offloaded to thread | `scan_text_for_emails` + `extract_mailto_links` run via `loop.run_in_executor` in `db_email_extract.py` | 2026-08-02. Event loop no longer blocks on 25-concurrent HTTP fetches. |
| Orphaned Redis namespaces | `gmaps_bd_business_pt:*`, `gmaps_search:*` auto-deleted at search daemon startup | `_cleanup_orphaned_queues()` in `daemons/search_daemon.py`. Only `gmaps_bd_business:*` (search) and `gmaps:*` (listing) are live. |
| Email extract schedule | Every 2h (`00,02,...,22:15` + randomized 5min), `--max 2000 --concurrency 25`, `FETCH_TIMEOUT=8s` | `~/.config/systemd/user/infinitecrawler-email-extract.{service,timer}` |
| Email extract multi-page (2026-08-07) | `db_email_extract.py` crawls homepage + 5 paths per listing (`/contact`, `/contact-us`, `/about`, `/about-us`, `/team`). Defaults: `concurrency=25`, `FETCH_TIMEOUT=8`, `MAX_REDIRECTS=3`. Shared httpx client (`Limits` matching concurrency). Per-listing deadline = `FETCH_TIMEOUT*2` = 16s. | Verified: 30-listing live run → 4 emails (13% hit rate, vs 3.6% baseline before plan). Dedup runs once across all fetched pages. Process has pidfile lock (`_system/email_extract.pid`) preventing overlapping runs. |
| LinkedIn search schedule | Every 4h (`00,04,...,20:30` + randomized 5min) | `~/.config/systemd/user/infinitecrawler-linkedin-search.timer` |
| LinkedIn match schedule (2026-08-07) | Every 6h (`03,09,15,21:30` + randomized 5min), `Persistent=true`. Service is oneshot running `scripts/match_linkedin_to_gmaps.py` (no `--max`; runs all distinct companies per invocation). Default `--min-score 0.5` retained. | `~/.config/systemd/user/infinitecrawler-linkedin-match.{service,timer}`. Re-runs are idempotent via `UNIQUE(profile_url, gmaps_listing_id)`. First run produced 66,549 new matches: 784 high-quality (score≥0.8), ~54K broad catch (score<0.6). Filter via `DELETE FROM scraper.linkedin_gmaps_matches WHERE score < 0.7`. |
| Search queue retry | `visibility_timeout: 300` (5 min stalled requeue), failed retried after 6h | `config/gmaps_bd_business_search.yaml`; verified correct 2026-08-02 |
| systemctl --value trap | `systemctl show -p A,B,C --value` output is sorted ALPHABETICALLY (MainPID, ActiveState, SubState) | Never positionally parse `--value` with multiple props. Parse `key=value` lines instead. Fixed in `api/routers/dashboard.py` + `monitor.py` 2026-08-02 (daemon PID/uptime/mem were null in API). |
| Emails unique key | `(listing_id, email)` | Added 2026-07-25 |
| LinkedIn unique key | `profile_url` | Added 2026-07-25 |
| Search config needs | `ignore_completed_on_enqueue: true` | Prevents query exhaustion when all queries are in completed set |
| Listing daemon bug | `await restart_browser()` in eternal loop | Removed 2026-07-25; was restarting browser on EVERY failed URL |
| Listing daemon tuning (2026-08-07) | `config/gmaps_listings_working.yaml`: `page_wait_seconds 15→8`, Overview/Reviews/About tab `max_wait` dropped by 1 each, global `retry.attempts 3→2` + `delay 2→1`. `category` and `plus_code` fields both got per-field `retry.attempts: 1` (was wasting retries on the ~10-37% of listings where these fields fail). | Verified live: `Pinchtab navigation complete in ~9s (wait=8.00s)` (was 16-29s). Throughput target ~100/hr (was 43/hr). Category fill held ~90% (was 91.5%); plus_code held ~60% (was 63.5%). |
| Postgres config drift | `listen_addresses` may revert to `localhost` | If cluster restarts externally, socket path breaks. Set to `''` for unix-socket-only. |
| Postgres config drift | `port` may revert to `5433` | Same restart issue. Keep `port = 5432`. |
| Enrichment services | `email-extract`, `linkedin-search` | Run as systemd timers (email every 2h, linkedin every 4h), not daemons. Monitor via `systemctl --user list-timers`. |
| Phase 3a drift trap | `-h 127.0.0.1` silently fails | 2026-07-29 audit: caused false-positive `search_1h=0`. Always use `-h /var/run/postgresql`. |
| Phase 2.5 drift trap | `--no-clean` invalid psql flag | Use `-t` (tuples-only). |
| infinite_scroll probe JS quote bug | `json.dumps(['div[role="feed"]',...])` injected into JS source produced `["div[role="feed"]",...]` with unbalanced quotes → `SyntaxError` every probe → 0 scroll, hits `max_scroll_attempts=500`. Fixed 2026-08-01: wrap in `JSON.parse(json.dumps(...))`. |
| GMaps scroll needs event dispatch | `el.scrollTop = el.scrollHeight` alone does NOT trigger GMaps IntersectionObserver. Must also `el.dispatchEvent(new Event('scroll', {bubbles: true}))`. Without it, cards stay at 10 forever. Verified 2026-08-01. |
| pinchtab 0.15 `/action kind:close` | Returns 400 `unknown action kind: close`. Same for `DELETE /tabs/:id` (404). Pinchtab has no tab-close endpoint — use `navigate("about:blank")` to reuse the tab. Verified 2026-08-01. |
| Region-anchored search yield | `/search/KEYWORD/@lat,lng,13z` yields ~5x more results than unanchored `/search/KEYWORD in City/` (120 vs 22 for 'manufacturing company' Rajshahi). City text in query narrows results (26) — keyword-only + coords is optimal. Verified 2026-08-01. |
| Query format `KEYWORD\|LAT\|LNG` | `query_generator._build_bd_local` emits `keyword|lat|lng`; `search_daemon.search_single_query` splits on `|` and builds the anchored URL. National/global queries keep plain text. |
| sectors yaml fallback | `software_sectors.yaml` lives in sibling repo `business-plan-template` (not always present). `_load_sectors` falls back to built-in `DEFAULT_KEYWORDS_EN/BN` — keeps daemon productive instead of crash-looping on empty pools. |
| Hotel seed queries | `scripts/seed_hotel_queries.py` — uses same `KEYWORD\|LAT\|LNG` format as query_generator. 408 unique hotel/resort queries across 16 BD cities + national. |
| Fixes verified stable | All 4 fixes (scroll JS quote, dispatchEvent, close_tab about:blank, scroll reset()) verified 2026-08-02. Daemons running 6h+ with 0 `/action` 400 errors. DB: 37K+ rows, ~1,500 writes/hr. |

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
  pinchtab --no-pager -l --lines=20

# 1b. Scheduled enrichment timers (email/linkedin/pg-backup run periodically)
systemctl --user list-timers --no-pager | grep infinitecrawler
# Expected: email-extract every 2h, linkedin-search every 4h, pg-backup daily

# 1c. Pinchtab health (the Chrome provider both daemons depend on)
PINCHTAB_TOKEN=$(python3 -c "import json; print(json.load(open('/root/.pinchtab/config.json'))['server']['token'])")
curl -s --connect-timeout 5 -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health

# Expected: all three services active (running). Health returns tab count, crashes stats.
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
# 3a. Search + listing velocity (last hour) — MUST use unix socket
# TCP to 127.0.0.1:5432 silently fails on this WSL host (PG listen_addresses='').
# Using -h 127.0.0.1 here caused a false-positive "search_1h = 0" alert on 2026-07-29.
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour') as search_1h,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour') as listings_1h,
  (SELECT COUNT(*) FROM scraper.emails WHERE discovered_at > NOW() - INTERVAL '2 hours') as emails_2h,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE checked_at > NOW() - INTERVAL '4 hours') as linkedin_4h;
"

# 3b. Full counts (unix socket — same reason)
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results) as search_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings) as listings_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL AND website IS NOT NULL) as qualified,
  (SELECT COUNT(*) FROM scraper.emails) as emails_total,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles) as linkedin_total;
"
```

**Expected:** search_1h > 0, listings_1h > 0. If zero for >1 hour, daemon is stalled.
**Drift trap:** If you see `search_1h = 0` while daemon is processing, you probably used `-h 127.0.0.1`. Always use `-h /var/run/postgresql`. Verify with `SELECT MAX(updated_at) FROM scraper.gmaps_search_results`.
**EXIT_CODE: REPORT**

---

## PHASE 4: ENRICHMENT COMPLETENESS — REPORT

Verify the offline enrichment scripts are producing results:

```bash
# 4a. Email coverage
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_email_extract.py --stats

# 4b. LinkedIn coverage
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_linkedin_search.py --stats

# 4c. Classification coverage
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_classify.py --stats
```

**Expected:** Email coverage growing every 2h. LinkedIn profiles growing every 4h. 0 remaining unclassified leads with phone+website.
**EXIT_CODE: REPORT**

---

## PHASE 5: CODEBASE SANITY (Quick Checks) — REPORT

```bash
cd /root/codebase/vhd/infinitecrawler

# 5a. Lint
uv run ruff check .

# 5b. Tests
uv run python -m pytest tests/ -v

# 5c. Dead-code assertion (self-documenting)
# Add new assert checks to scripts/assert_dead_code.sh when you delete code
bash scripts/assert_dead_code.sh 2>&1 || true
# If no scripts/assert_dead_code.sh file yet, just log it:
[ ! -f scripts/assert_dead_code.sh ] && echo "WARN: no dead-code assertion script yet — create one next time you delete code"

# 5d. Pinchtab port integrity — configs must NOT reference wrong ports
grep -rn "9869\|e03c" config/ base/ daemons/ 2>&1
# Expected: zero matches. Port 9869 is the Chrome CDP debug port, not for daemons.
# Token must be 123456, never the old e03c... placeholder.

# 5e. Config YAMLs loadable
uv run python -c "
from factory.scraper_factory import ScraperFactory
c1 = ScraperFactory.load_config('config/gmaps_bd_business_search.yaml')
c2 = ScraperFactory.load_config('config/gmaps_listings_working.yaml')
print(f'Search config OK ({len(c1)} keys)')
print(f'Listing config OK ({len(c2)} keys)')
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
SELECT column_name FROM information_schema.columns WHERE table_schema='scraper' AND table_name='emails' AND column_name IN ('is_obfuscated','context_snippet');
SELECT conname FROM pg_constraint WHERE conrelid='scraper.emails'::regclass AND conname='emails_listing_id_email_key';
SELECT conname FROM pg_constraint WHERE conrelid='scraper.linkedin_profiles'::regclass AND conname='linkedin_profiles_profile_url_key';
"
# Expected: 2 columns, 1 email unique constraint, 1 linkedin unique constraint
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

# 6g. Restart pinchtab (only if Chrome is truly crashed and supervisor isn't recovering)
systemctl --user restart pinchtab.service

# 6h. Enrichment backlog recovery
uv run python scripts/db_email_extract.py --max 500
uv run python scripts/db_linkedin_search.py --max 200
uv run python scripts/db_classify.py --retry-failed --max 1000

# 6i. Full pipeline health snapshot (always run last)
uv run python scripts/monitor_pipeline.py --json
```

**EXIT_CODE: BLOCK** — only proceed if phase 1-5 flagged anomalies.

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

Known Temporary Workarounds

Deprecated Components

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