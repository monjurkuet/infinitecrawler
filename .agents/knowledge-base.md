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
| Emails unique key | `(listing_id, email)` | Added 2026-07-25 |
| LinkedIn unique key | `profile_url` | Added 2026-07-25 |
| Search config needs | `ignore_completed_on_enqueue: true` | Prevents query exhaustion when all queries are in completed set |
| Listing daemon bug | `await restart_browser()` in eternal loop | Removed 2026-07-25; was restarting browser on EVERY failed URL |
| Postgres config drift | `listen_addresses` may revert to `localhost` | If cluster restarts externally, socket path breaks. Set to `''` for unix-socket-only. |
| Postgres config drift | `port` may revert to `5433` | Same restart issue. Keep `port = 5432`. |

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

## Usage Protocol

**This document is the living source of truth for the pipeline.** After every audit session where you discover something new — a broken path, a config mismatch, a removed file, a changed port, a new queue namespace — you MUST update this document:

1. **Add newly discovered red flags** to the red-flags list under the relevant phase
2. **Update KEY FACTS table** when ports, tokens, paths, or connection strings change
3. **Add new repair actions** under Phase 6 when you discover a novel recovery pattern
4. **Remove obsolete checks** when files/directories/services are permanently deleted
5. **Update expected values** when DB schemas, queue names, or timer schedules change
6. **Record new bugs you fixed** in the KEY FACTS table so future you recognizes regressions
7. **If a command fails**, replace it with the corrected version immediately — never leave broken commands in this document
8. **If a check phase returns unexpected results that you verify are now normal**, update the "Expected:" notes

**Correction protocol:** If you execute a command from this document and it fails, fix the command in-place via `patch` before moving on. If the underlying config/port/path changed, update both the command AND the KEY FACTS row.

---

## PHASE 1: SERVICE & DAEMON LIFECYCLE CHECK — BLOCK

Run these commands in order. If any service is dead, restart it:

```bash
# 1a. All service statuses (with recent logs)
systemctl --user status infinitecrawler-search infinitecrawler-listing pinchtab --no-pager -l --lines=20

# 1b. Scheduled enrichment timers
systemctl --user list-timers --no-pager | grep infinitecrawler

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

# 2.5c. PG connection count (spikes indicate leak)
PGPASSWORD=$PG_PASSWORD psql -h /var/run/postgresql -U postgres -d infinitecrawler --no-clean --tuples -c \
  "SELECT count(*) AS active_connections FROM pg_stat_activity WHERE datname='infinitecrawler'"
# Expected: < 20 (high count with low daemon activity → connection leak)
```

**EXIT_CODE: REPORT** — flag anomalies but continue.

---

## PHASE 3: DATABASE INGESTION VELOCITY — REPORT

Verify data is actually being written to PostgreSQL in real-time:

```bash
# 3a. Search + listing velocity (last hour)
PGPASSWORD="$PG_PASSWORD" psql -h 127.0.0.1 -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour') as search_1h,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour') as listings_1h,
  (SELECT COUNT(*) FROM scraper.emails WHERE discovered_at > NOW() - INTERVAL '2 hours') as emails_2h,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE checked_at > NOW() - INTERVAL '4 hours') as linkedin_4h;
"

# 3b. Full counts
PGPASSWORD="$PG_PASSWORD" psql -h 127.0.0.1 -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results) as search_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings) as listings_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL AND website IS NOT NULL) as qualified,
  (SELECT COUNT(*) FROM scraper.emails) as emails_total,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles) as linkedin_total;
"
```

**Expected:** search_1h > 0, listings_1h > 0. If zero for >1 hour, daemon is stalled.
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

# DB velocity (1h window)
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "SELECT 'search_1h', count(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour' UNION ALL SELECT 'listings_1h', count(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour'"

# Restart all daemons
systemctl --user restart infinitecrawler-search infinitecrawler-listing

# Full monitor
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/monitor_pipeline.py --json
```