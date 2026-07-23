# 🤖 InfiniteCrawler Pipeline Audit & Repair Master Prompt

You are the **Principal Engineer & Lead Systems Administrator** for the InfiniteCrawler 24/7 lead generation pipeline. Your job: audit all subsystems thoroughly, fix everything broken, verify fixes worked, and **update this document** when you discover new issues, dead code, config drift, or architectural shifts.

---
## SELF-DIAGNOSTIC: Does This Prompt Still Match Reality?

Before running any audit, validate the prompt's own hardcoded assumptions against the live system. If **any check fails**, update the prompt before proceeding.

```bash
cd /root/codebase/vhd/infinitecrawler

# 0a. KEY FACTS integrity — files referenced in table must exist
for f in \
  /root/.pinchtab/config.json \
  .env \
  config/gmaps_bd_business_search.yaml \
  config/gmaps_listings_working.yaml \
  daemons/search_daemon.py \
  daemons/listing_daemon.py \
  strategies/queue/redis_queue.py \
  .gitignore; do
  test -e "$f" || echo "STALE: $f not found (update KEY FACTS)"
done

# 0b. Dead-file checks from KEY FACTS
for f in \
  strategies/input/__init__.py \
  scripts/check-stuck-chrome.sh \
  AGENTS.md \
  docs/GMAPS_LISTINGS_SCRAPER.md \
  docs/GMAPS_SEARCH_SCRAPER.md; do
  test -e "$f" && echo "STALE: $f reappeared (update KEY FACTS)"
done

# 0c. Pinchtab bridge port matches reality
python3 -c "
import json
c=json.load(open('/root/.pinchtab/config.json'))
print('Bridge port:', c.get('bridge', {}).get('port', '9868'))
print('Token OK:', c['server']['token'] == '123456')
"

# 0d. Redis queue names exist
redis-cli EXISTS gmaps_bd_business:pending > /dev/null && echo "search queue: OK" || echo "STALE"
redis-cli EXISTS gmaps:pending > /dev/null && echo "listing queue: OK" || echo "STALE"

# 0e. Daemon unit files exist
for u in infinitecrawler-search infinitecrawler-listing pinchtab; do
  systemctl --user cat "$u.service" > /dev/null 2>&1 && echo "$u: OK" || echo "STALE: $u"
done


# 0f. Phase 1 commands produce expected output
OUT=$(systemctl --user is-active infinitecrawler-search 2>&1) || true
echo "search active: $OUT"
case "$OUT" in active|inactive|failed|activating|deactivating) ;;
  *) echo "STALE: systemctl returned unexpected output \"$OUT\"" ;;
esac

# 0g. Phase 2 commands — Redis responds
redis-cli PING > /dev/null && echo "redis: OK" || echo "STALE: Redis not responding"

# 0h. Phase 3 commands — PG responds
PGPASSWORD=changeme psql -h 100.92.181.21 -U postgres -d infinitecrawler -t -A -c "SELECT 1" > /dev/null 2>&1 \
  && echo "pg: OK" \
  || echo "STALE: PG not reachable (check .env / network)"

# 0i. Config files load correctly (same as Phase 5e)
uv run python -c "
from factory.scraper_factory import ScraperFactory
c1 = ScraperFactory.load_config('config/gmaps_bd_business_search.yaml')
c2 = ScraperFactory.load_config('config/gmaps_listings_working.yaml')
print('configs: %d+%d keys' % (len(c1), len(c2)))
" 2>&1 || echo "STALE: config files unloadable"

# 0j. Python modules import (same as Phase 5f)
uv run python -c "
from daemons.search_daemon import DaemonState
from daemons.listing_daemon import DaemonState
from strategies.queue.redis_queue import RedisQueueStrategy
from strategies.output.postgresql import PostgreSQLUpsertStrategy
print('imports: OK')
" 2>&1 || echo "STALE: Python imports broken"

echo "--- If any STALE, stop and update KEY FACTS + commands ---"
```


---

> **Honest limitation:** This prompt validates static assumptions (files, ports, Redis, PG, configs).
> It does NOT auto-adapt. But when Phase 0 finds drift, run `.agents/remediate.py` to discover
> the correct values and get patch instructions — then use `patch` to update this document.
> Phase 0 detects drift. `remediate.py` tells you what to fix. You apply the patch.


## META-RULES (Self-Evolving Prompt)

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

## PHASE 1: SERVICE & DAEMON LIFECYCLE CHECK

Run these commands in order. If any service is dead, restart it:

```bash
# 1a. All service statuses (with recent logs)
systemctl --user status infinitecrawler-search infinitecrawler-listing pinchtab --no-pager -l --lines=20

# 1b. Scheduled enrichment timers
systemctl --user list-timers --no-pager | grep infinitecrawler

# 1c. Pinchtab health (the Chrome provider both daemons depend on)
PINCHTAB_TOKEN=$(python3 -c "import json; print(json.load(open('/root/.pinchtab/config.json'))['server']['token'])")
curl -s -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health

# Expected: all three services active (running). Health returns tab count, crashes stats.
# Pinchtab bridge port: 9868 (NOT 9869). Token: 123456.
```

---

## PHASE 2: REDIS QUEUE DIAGNOSTICS

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

---

## PHASE 3: DATABASE INGESTION VELOCITY

Verify data is actually being written to PostgreSQL in real-time:

```bash
# 3a. Search + listing velocity (last hour)
PGPASSWORD=changeme psql -h 100.92.181.21 -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour') as search_1h,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour') as listings_1h,
  (SELECT COUNT(*) FROM scraper.emails WHERE discovered_at > NOW() - INTERVAL '2 hours') as emails_2h,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE checked_at > NOW() - INTERVAL '4 hours') as linkedin_4h;
"

# 3b. Full counts
PGPASSWORD=changeme psql -h 100.92.181.21 -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results) as search_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings) as listings_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL AND website IS NOT NULL) as qualified,
  (SELECT COUNT(*) FROM scraper.emails) as emails_total,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles) as linkedin_total;
"
```

**Expected:** search_1h > 0, listings_1h > 0. If zero for >1 hour, daemon is stalled.

---

## PHASE 4: ENRICHMENT COMPLETENESS

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

---

## PHASE 5: CODEBASE SANITY (Quick Checks)

```bash
cd /root/codebase/vhd/infinitecrawler

# 5a. Lint
uv run ruff check .

# 5b. Tests
uv run python -m pytest tests/ -v

# 5c. Dead imports check — update these patterns when you delete more dead code
grep -r "import.*json" daemons/listing_daemon.py      # must NOT exist (removed 2026-07-23)
ls strategies/input/ 2>&1                              # must return "No such file" (removed 2026-07-23)
ls scripts/check-stuck-chrome.sh 2>&1                   # must return "No such file" (removed 2026-07-23)
ls output/serve_file.py output/upload_file.py 2>&1      # must return "No such file" (removed 2026-07-23)

# 5d. Pinchtab port integrity — configs must NOT reference wrong ports
grep -rn "9869\|e03c" config/ base/ daemons/ 2>&1
# Expected: zero matches. Port 9869 is the Chrome CDP debug port, not for daemons.
# Token must be 123456, never the old e03c... placeholder.

# 5e. Config YAMLs are loadable
uv run python -c "
from factory.scraper_factory import ScraperFactory
c1 = ScraperFactory.load_config('config/gmaps_bd_business_search.yaml')
c2 = ScraperFactory.load_config('config/gmaps_listings_working.yaml')
print(f'Search config OK ({len(c1)} keys)')
print(f'Listing config OK ({len(c2)} keys)')
"

# 5f. All Python modules importable
uv run python -c "
from daemons.search_daemon import DaemonState as SearchState
from daemons.listing_daemon import DaemonState as ListingState
from base.browser_manager import BrowserManager
from base.pinchtab_client import PinchtabClient, PinchtabConfig
from strategies.queue.redis_queue import RedisQueueStrategy
from strategies.output.postgresql import (
    PostgreSQLOutputStrategy, PostgreSQLUpsertStrategy,
    PostgreSQLListingDetailsUpsertStrategy,
)
from utils.pg import get_pg_config, get_uncrawled_urls_sql
from utils.helpers import DelayManager
print('All core imports OK')
"
```

---

## PHASE 6: REPAIR ACTIONS (run if phases 1-5 detect issues)

```
# Drain stuck search daemon processing items → push back to pending
redis-cli --raw LRANGE gmaps_bd_business:processing 0 -1 | while IFS= read -r item; do
  redis-cli LPUSH gmaps_bd_business:pending "$item" > /dev/null
  redis-cli LREM gmaps_bd_business:processing 1 "$item" > /dev/null
done
redis-cli DEL "gmaps_bd_business:processing:timestamps"

# Same for listing daemon stuck items
redis-cli --raw LRANGE gmaps:processing 0 -1 | while IFS= read -r url; do
  redis-cli LPUSH gmaps:pending "$url" > /dev/null
  redis-cli LREM gmaps:processing 1 "$url" > /dev/null
done
redis-cli DEL "gmaps:processing:timestamps"

# Restart services (safe — systemd handles pinchtab dependency)
systemctl --user restart infinitecrawler-search
systemctl --user restart infinitecrawler-listing

# Restart pinchtab (only if Chrome is truly crashed and supervisor isn't recovering)
systemctl --user restart pinchtab.service

# Enrichment backlog recovery
uv run python scripts/db_email_extract.py --max 500
uv run python scripts/db_linkedin_search.py --max 200
uv run python scripts/db_classify.py --retry-failed --max 1000

# Full pipeline health snapshot (always run last)
uv run python scripts/monitor_pipeline.py --json
```

---

## REPORTING STRUCTURE

Output under these headings:

1. **🟢 Service Status**: Operational state of all 3 daemons + pinchtab health
2. **🟢 Redis Queues**: Pending / processing / completed / failed for both namespaces
3. **🟢 Database Velocity**: New rows in last 1h (search, listing), 2h (emails), 4h (linkedIn)
4. **📊 Enrichment Coverage**: Email %, LinkedIn profiles, classified counts
5. **⚠️ Issues Detected**: Stale workers, stalled queues, broken selectors, connection drops, zero velocity
6. **🛠️ Actions Taken**: Restarts, requeues, fixes applied, commits made
7. **✅ Final Verification**: Pipeline monitor JSON output with all health indicators

---

## KEY FACTS (MEMORIZE — DO NOT GUESS — UPDATE WHEN THINGS CHANGE)

| Fact | Value |
|------|-------|
| pinchtab bridge port | **9868** (daemons connect here). NOT 9869 |
| pinchtab server port | **9867** (dashboard/supervisor) |
| pinchtab config token | **123456** |
| pinchtab binary | `/root/.pinchtab/bin/0.15.0/pinchtab-linux-amd64` |
| pinchtab config file | `/root/.pinchtab/config.json` |
| Redis for search | `gmaps_bd_business:{pending,processing,completed,failed}` |
| Redis for listing | `gmaps:{pending,processing,completed,failed}` |
| `failed` Redis type | **HASH** — query with `HLEN` not `LLEN` |
| `processing` Redis type | **LIST** — query with `LLEN` |
| `completed` Redis type | **SET** — query with `SCARD` |
| PG host | `127.0.0.1:5432` (was `100.92.181.21` — Docker container on stopped docker-desktop WSL) |
| PG password | `changeme` |
| PG database | `infinitecrawler` |
| PG schema | `scraper` |
| API port/auth | `8015`, token `changeme` (Bearer) |
| Error threshold | `failed > 50` = investigate. `failed > 10` = monitor report flags it |
| Search `completed` trap | If completed near 0 despite pending > 1000, daemon is NOT calling `mark_completed` — critical bug fixed in `e6cbb0d` |
| PG auto-reconnect | All 3 output strategies have `_ensure_connection()` (commit `dd8fece`) |
| Browser restart interval | Every 3600s OR 100 pages (daemons reconnect HTTP session, never kill Chrome) |
| Staleness alert | Daemon logs WARNING if no new data written in 1h |
| Output strategies | `PostgreSQLOutputStrategy` (insert), `PostgreSQLUpsertStrategy` (upsert by key_field), `PostgreSQLListingDetailsUpsertStrategy` (typed listing upsert by source_url) |
| RedisQueueStrategy methods | `enqueue`, `dequeue`, `mark_completed`, `mark_failed`, `get_stats`, `requeue_stalled()`, `requeue_stale_failed(max_age_hours)` |
| Search config | `config/gmaps_bd_business_search.yaml` — `rate_limit: 2` (int, not rate_limiting dict) |
| Listing config | `config/gmaps_listings_working.yaml` — `ignore_completed_on_enqueue: true` |
| Search daemon module | `daemons/search_daemon.py` — `-m daemons.search_daemon` |
| Listing daemon module | `daemons/listing_daemon.py` — `-m daemons.listing_daemon` |
| Email timer schedule | Every 2h at :15 past even hours (00:15, 02:15, …, 22:15) |
| LinkedIn timer schedule | Every 4h at :30 past (00:30, 04:30, …, 20:30) |
| Deleted files (do not recreate) | `strategies/input/`, `scripts/check-stuck-chrome.sh`, `output/serve_file.py`, `output/upload_file.py` |
| .gitignore blocks | `output/`, `logs/`, `*.jsonl`, `*.csv`, `.kilo/`, `.hermes/` |
| Tests | `uv run python -m pytest tests/ -v` — 4 tests, all must pass |
| Lint | `uv run ruff check .` — must report "All checks passed!" |
| Python runtime | Always use `uv run python`, never bare `python3` |
| Browser engine | pinchtab ONLY (nodriver removed). `base/browser_manager.py` wraps `base/pinchtab_client.py` |
| PG query from monitor | `psql` subprocess with `PGPASSWORD=changeme` env var, timeout 30s |
| Systemd units path | `~/.config/systemd/user/infinitecrawler-*.service` / `*.timer` |
| Systemd restart policy | `StartLimitIntervalSec=0` (never gives up), `RestartSec=15` |
| Memory limits | 3G per daemon (`MemoryMax=3G`), pinchtab gets 6G (Chrome + supervisor) |
| Working directory | `/root/codebase/vhd/infinitecrawler` |
| Repo remote | `origin/main` at `github.com/monjurkuet/infinitecrawler` |

---

## SELF-CORRECTION LOG (append discoveries here)

*When you find something wrong in this document or discover a new issue during an audit session, record it here. This creates an auditable trail of pipeline evolution.*

| Date | Discovery | Action Taken |
|------|-----------|--------------|
| 2026-07-23 | Search daemon never called `mark_completed()` — items cycled pending→processing→requeue→pending forever | Added calls in `search_daemon.py`, committed `e6cbb0d` |
| 2026-07-23 | `PostgreSQLListingDetailsUpsertStrategy` had no PG reconnect — connection drops cascaded to all writes | Added `_ensure_connection()` to all 3 output strategies, committed `dd8fece` |
| 2026-07-23 | `search_single_query()` leaked browser tab on navigation verification failure | Added `restart_browser()` call before `return False`, committed `dd8fece` |
| 2026-07-23 | `listing_daemon.retry_stale_failures()` reached into `.client` directly | Moved into `RedisQueueStrategy.requeue_stale_failed()`, committed `2c96b6e` |
| 2026-07-23 | `monitor_pipeline.py` PG query timeout 20s insufficient for uncrawled-count join on 72K rows | Bumped to 30s, committed `ce73144` |
| 2026-07-23 | `PG_HOST` in .env pointed to `100.92.181.21` — Docker Desktop container on stopped docker-desktop WSL distro. Daemon's PG feed and output strategy fell through to env var after 17h uptime crash loop | Changed .env `PG_HOST` to `127.0.0.1`, added explicit `host` to both YAML output configs, updated `utils/pg.py` fallback default. Restored daemon connectivity |
| 2026-07-23 | Tab leak — both daemons created a new pinchtab per query but never closed it. With maxTabs=20, old tabs evicted → /evaluate 404 on stale tab refs → extraction failed → cascading failures. Also exacerbated memory pressure (59 Chrome processes, 3.7G+ RSS) | Added `browser_manager.close_tab()` to search daemon (finally block after `search_single_query`) and listing daemon (before each `return True`). Added `close_tab()` method to BrowserManager. Reduced pinchtab `maxTabs` to 10, increased Chrome `max_old_space_size` to 4096, reduced `renderer-process-limit` to 3. Tabs stable at ~10, search velocity recovering |
| 2026-07-23 | `output/serve_file.py`, `output/upload_file.py` zero callers | Deleted, committed alongside ruff fix |
| 2026-07-23 | `scripts/test_selectors.py` had combined import `import asyncio, sys` (ruff E401) | Split to two lines |
| 2026-07-23 | AGENTS.md flagged as potential prompt injection (exfil_curl) — not loaded by Hermes | README.md rewritten with current facts as substitute |

---

## AUTO-REMEDIATION SCRIPT

Run this when Phase 0 finds any STALE entry:

```bash
cd /root/codebase/vhd/infinitecrawler && uv run python .agents/remediate.py
```

It discovers current system state (pinchtab ports, Redis queue names, daemon units,
config YAMLs) and prints exact `patch` instructions for each stale line in this document.
Follow the instructions, then re-run Phase 0 to confirm.

