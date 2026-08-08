# InfiniteCrawler — Agent Knowledge Base

> **Self-healing pipeline audit playbook.** Run top-to-bottom when: pipeline seems stuck, data stops flowing, or after any code change. Each phase reports findings — phases marked **BLOCK** halt the audit; phases marked **REPORT** continue regardless.

---

## KEY FACTS TABLE

| Fact | Value | Source / Why |
|------|-------|--------------|
| Pinchtab server port (ABM/dashboard) | `9867` | `/root/.pinchtab/config.json` → `server.port`. Process: `pinchtab-linux-amd64 server`. Dashboard UI + always-on supervisor. |
| Pinchtab bridge port (daemon target) | `9868` | `/root/.pinchtab/config.json` → `instanceDefaults.instancePortStart`. Process: `pinchtab-linux-amd64 bridge`. Daemons (`listing_daemon.py`, `search_daemon.py`) talk to **this** port for browser commands. |
| Pinchtab API token | `123456` | `/root/.pinchtab/config.json` → `server.token`. Required as `Authorization: Bearer 123456` on every request to ports 9867/9868. |
| Pinchtab instance port drift | Port may be 9869 instead of 9868 after a restart | If 9868 was bound by a leftover process at restart time, pinchtab bridge lands on 9869 and all daemons time out (the listing daemon flaps indefinitely in that state). Fix: `systemctl --user restart pinchtab` so it re-binds 9868. Verified 2026-08-07 (NRestarts=37 flap → 0 after pinchtab restart). |
| Chrome CDP debug port | `9869` (or higher) | Owned by Chrome process (pid ~190000+), not pinchtab. Do NOT use for daemons — daemons talk to pinchtab bridge on 9868. Returns HTTP 404 on `/health`. |
| Redis namespace (search) | `gmaps_bd_business:*` | Search daemon queue prefix |
| Redis namespace (listing) | `gmaps:*` | Listing daemon queue prefix |
| Postgres host (unix socket, primary) | `/var/run/postgresql` | `.env` uses socket path. `connect_timeout` short; `pg_service.py` omits port param when host contains `/` to avoid `psycopg` parsing port as hostname. |
| Postgres host (TCP 127.0.0.1, works) | `127.0.0.1:5432` | Verified working 2026-08-02 onward. PG `listen_addresses=127.0.0.1`. Both TCP and unix socket are valid; systemd services use the socket path per `.env`. Older claim "TCP fails on WSL" is RESOLVED. |
| Postgres db | `infinitecrawler` | `scraper.*` schema |
| Postgres user | `postgres` | Default |
| Postgres password | `$PG_PASSWORD` (env) | Set in `.env` / systemd env |
| Postgres schema inventory (10 tables) | `scraper.{api_tasks, discovered_profiles, emails, gmaps_listings, gmaps_search_results, linkedin_gmaps_matches, linkedin_profiles, luxury_contacts, luxury_targets, traffic_results}` | As of 2026-08-07. 7 are pipeline-active (`gmaps_*`, `emails`, `linkedin_*`); `api_tasks` is task tracking; `luxury_*` and `discovered_profiles` are staging; `traffic_results` is unused. |
| Postgres DB size | `~260 MB` | As of 2026-08-07. Backup dumps in `backups/*.dump.zst` (~5-50 MB each). |
| Failed queues use HASHES | `HLEN` not `LLEN` | `failed` queues are hash maps |
| Repository root | `/root/codebase/vhd/infinitecrawler` | All commands relative to this |
| Emails table missing cols | `is_obfuscated`, `context_snippet` | Added 2026-07-25; upsert was silently failing |
| Emails table migration | `source_type` → `extraction_method`; added `website_url`, `email_type`, `last_verified`, `is_active` | Applied 2026-08-02 via `scripts/schema_migration.py` (idempotent `UPGRADE_EMAILS_TABLE` DO block). Code + DDL + live DB now aligned; API stats queries (`extraction_method`, `email_type`) work. |
| Email regex ReDoS | Old `OBFUSCATED_PATTERNS[0]` had optional-bracket groups → O(n²) backtracking (30KB = 23s). Replaced with 2 linear-time patterns 2026-08-02 | New patterns: `[at]`/`(at)`/`[at]`/`AT DOT` all covered; 300KB = 0.42s. Never add optional nested groups after greedy captures in these patterns. |
| Email scan offloaded to thread | `scan_text_for_emails` + `extract_mailto_links` run via `loop.run_in_executor` in `db_email_extract.py` | 2026-08-02. Event loop no longer blocks on 25-concurrent HTTP fetches. |
| Orphaned Redis namespaces | `gmaps_bd_business_pt:*`, `gmaps_search:*` auto-deleted at search daemon startup | `_cleanup_orphaned_queues()` in `daemons/search_daemon.py`. Only `gmaps_bd_business:*` (search) and `gmaps:*` (listing) are live. |
| Email extract schedule | Every 2h (`00,02,...,22:15` + randomized 5min), `--max 200 --concurrency 25`, `FETCH_TIMEOUT=8s`. Use `--mode both` to enable browser fallback for zero-result listings; pass `--force-rescan` for one-shot re-scans of the full backlog. | `~/.config/systemd/user/infinitecrawler-email-extract.{service,timer}`. `--max 200` cap (2026-08-08 plan) so the timer doesn't fight the perpetual loop. |
| Email extract perpetual loop (2026-08-08) | New perpetual daemon `infinitecrawler-email-extract-loop.service` runs `db_email_extract.py --loop --loop-gap 30 --max 2000 --concurrency 25`, `Restart=always`. The in-script default `--max` was raised 1000→2000 in 2026-08-09 (T6) so each cycle drains more of the backlog. Replaces the 2h timer as primary driver; the 2h timer stays as safety net for long-tail/hard listings. | `~/.config/systemd/user/infinitecrawler-email-extract-loop.service`. Target ≥20 emails/hr vs ~4/hr baseline. Watchdog metric `emails_1h < 5` flags leakage. |
| Email extract multi-page (2026-08-07) | `db_email_extract.py` crawls homepage + 5 paths per listing (`/contact`, `/contact-us`, `/about`, `/about-us`, `/team`). Defaults: `concurrency=25`, `FETCH_TIMEOUT=8`, `MAX_REDIRECTS=3`. Shared httpx client (`Limits` matching concurrency). Per-listing deadline = `FETCH_TIMEOUT*2` = 16s. | Verified: 30-listing live run → 4 emails (13% hit rate, vs 3.6% baseline before plan). Dedup runs once across all fetched pages. Process has pidfile lock (`_system/email_extract.pid`) preventing overlapping runs. |
| Email extract browser fallback (2026-08-09, T2) | New `--mode {http,browser,both}` flag (default `http`). `both` runs HTTP first, then queues zero-result listings for a pinchtab browser pass using `extract_emails_from_page()` (rendered DOM + mailto hrefs). Concurrency 3, 0.2s nav delay, 60s page timeout via env (`BROWSER_CONCURRENCY`, `BROWSER_NAV_DELAY`, `BROWSER_PAGE_TIMEOUT`). Browser rows get `extraction_method='browser'`. Gracefully no-ops if pinchtab unreachable. | `scripts/db_email_extract.py`. Targets ≥10% email hit-rate vs 3.6% HTTP-only baseline. Single-page HTTP approach missed JS-obfuscated emails (Cloudflare decoder, [at]/[dot], runtime mailto). |
| Email extract force-rescan (2026-08-09, T6) | New `--force-rescan` flag drops `NOT EXISTS emails` predicate so one-shot re-scans can hit listings that already have a row (useful for new contact addresses or after browser-pass rollouts). Loop `--max` default raised 500→2000 so each cycle drains more of the backlog. New helper `utils.pg.get_all_listings_with_website(conn, limit)` powers the path. | `scripts/db_email_extract.py:454`, `utils/pg.py:get_all_listings_with_website`. |
| DDGS Bengali transliteration + 500 circuit breaker (2026-08-09, T1) | `utils/transliterate.bn_to_en()` transliterates Bengali script (U+0980–U+09FF) → Latin via `indic-transliteration` (added to `pyproject.toml`). `contains_bengali()` guards the gate. Brand-name safety: when transliteration yields 0 Latin letters, original query is dispatched unchanged. Circuit breaker: 3 consecutive HTTP 500s trip a global cooldown (default 300s) — both firehose and search scripts implement independently. | `utils/transliterate.py`, `scripts/db_linkedin_firehose.py:search_one`, `scripts/db_linkedin_search.py:search_linkedin`. Env: `DDGS_BACKOFF_S` (default 300), `DDGS_500_THRESHOLD` (default 3). Verified live: Bengali queries now return Latin-script results; cooldown log line is `DDGS cooldown started: N consecutive 500s, sleeping Ns`. |
| PG session hardening (2026-08-09, T3) | All psycopg connections (sync + async) now apply 3 libpq timeouts via the `options=` kwarg: `idle_in_transaction_session_timeout=30s`, `statement_timeout=120s`, `lock_timeout=10s`. Env-overridable via `PG_IDLE_TX_TIMEOUT` / `PG_STATEMENT_TIMEOUT` / `PG_LOCK_TIMEOUT`. Async pool uses `utils.pg.build_async_dsn()` (DSN string with `options='-c …'`); sync `get_pg_config()` packs timeouts into `options=`. Live-verified: backend shows the expected values via `SHOW idle_in_transaction_session_timeout`. | `utils/pg.py`, `api/services/pg_service.py:create_pool`. **Caught bug during audit**: psycopg3 rejects bare `key=value` GUC names as kwargs — must use the libpq-standard `options='-c k=v'` form. |
| Listing daemon T4 diagnostics (2026-08-09) | New `cycle_success{phone,website,plus_code,category}` counters + `cycle_retries` + `cycle_processed` on `DaemonState`. Per-cycle summary line every 300s: `cycle_summary processed=N success{phone:a, website:b, plus_code:c, category:d} retries{e→f}`. On final retry failure, `log.error` emits URL + last-failure-kind; when env `LOG_SAMPLE_HTML=1` is set, also emits first 500 chars of HTML for forensics (off by default — page bytes bloat logs). | `daemons/listing_daemon.py` (DaemonState + process_url + eternal_loop). |
| Listing partial-record early flush (2026-08-09, T5) | New env knobs `FLUSH_INTERVAL_SEC` (was hardcoded 5.0) and `FLUSH_ON_REQUIRED_FIELD` (off by default). When enabled, any `write_item` that lands a `phone` or `website` flushes the single-row batch immediately instead of waiting for the 50-row size trigger. Listing daemon opts in via env without code change. | `strategies/output/postgresql.py:_PostgreSQLOutputBase` (`_maybe_flush_partial`, `FLUSH_ON_REQUIRED_FIELD`, `REQUIRED_FLUSH_FIELDS`). Wiring: `PostgreSQLListingDetailsUpsertStrategy.write_item` calls `_maybe_flush_partial(item)` after append (skipped when batch is already ≥50 — size trigger wins). |
| Standardized daemon logging (2026-08-09, T7) | Listing + search daemons + firehose now use `%(asctime)s - %(name)s - %(levelname)s - %(message)s`. Each main() emits `started version=1 args=…` and `stopped reason={SIGTERM,SIGINT,exit,dry_run}` on shutdown (via `try/finally` around the eternal loop). grep `/var/log/infinitecrawler/*.log` for `started` / `stopped` markers to confirm lifecycle. | `daemons/listing_daemon.py:576,599`, `daemons/search_daemon.py:534,546`, `scripts/db_linkedin_firehose.py:504,515,554,556`. |
| `linkedin_gmaps_matches.is_verified` (2026-08-08) | New BOOLEAN column, `NOT NULL DEFAULT false`, partial index. Set to `(score >= 0.7)` on INSERT and ON CONFLICT UPDATE. Soft-delete marker for the ~63% noise matches below the 0.7 threshold — preserves data, allows re-scoring without DELETE. Backfilled in-place on 2026-08-08: 1093 verified / 69323 total. | `scripts/schema_migration.py` (idempotent). Writer: `scripts/match_linkedin_to_gmaps.py:225`. Monitor: `verified_matches`, `verified_matches_1h`. |
| Listing daemon flush trigger (2026-08-08) | `_PostgreSQLOutputBase.FLUSH_INTERVAL_SEC=5.0` — daemon loop calls `flush_if_due()` every iteration. Structured log: `listing.flush size=N age=Xs trigger=timer\|size`. Supersedes the 50-row size-only trigger; eliminates 13–40 min stall blindness after daemon restart. | `strategies/output/postgresql.py:42,52,76`. Wired in `daemons/listing_daemon.py:422` (loop step 0). Size trigger still fires at 50 rows. |
| File logging on user units (2026-08-08) | All 11 infinitecrawler-* + pinchtab user units get drop-in override `StandardOutput=append:/var/log/infinitecrawler/<unit>.log`. Drop-ins live in `~/.config/systemd/user/<unit>.service.d/override.conf`. Logrotate is out of scope (logs grow unbounded; monitor `df` in Phase 2.5). | Active files observed 2026-08-08: `infinitecrawler-{search,listing,email-extract-loop,linkedin-firehose-loop,watchdog,pinchtab}.log`. Timer-triggered units write on next activation. |
| Classify LLM key health gate (2026-08-08) | `db_classify.py:main()` checks `os.environ.get("LLM_API_KEY")` first; if empty, logs `classify.aborted reason=missing_llm_api_key` and exits with code 2 (distinct from 1 to prevent systemd Restart=on-failure tight loop). `LLM_API_KEY` is sourced from `.env` via systemd `EnvironmentFile`. | `scripts/db_classify.py:284`. Verify with `env -u LLM_API_KEY uv run python scripts/db_classify.py` → exit 2. |
| LinkedIn search schedule | Every 4h (`00,04,...,20:30` + randomized 5min) | `~/.config/systemd/user/infinitecrawler-linkedin-search.timer` |
| LinkedIn match schedule (2026-08-07) | Every 6h (`03,09,15,21:30` + randomized 5min), `Persistent=true`. Service is oneshot running `scripts/match_linkedin_to_gmaps.py` (no `--max`; runs all distinct companies per invocation). Default `--min-score 0.5` retained (2026-08-09: keep — preserves weak matches for future independent LinkedIn crawler). | `~/.config/systemd/user/infinitecrawler-linkedin-match.{service,timer}`. Re-runs are idempotent via `UNIQUE(profile_url, gmaps_listing_id)`. First run produced 66,549 new matches: 784 high-quality (score≥0.8), ~54K broad catch (score<0.6). Filter via `DELETE FROM scraper.linkedin_gmaps_matches WHERE score < 0.7`. |
| Match scorer distinct-overlap guard (2026-08-09) | `score_match` requires ≥1 non-noise word of length ≥4 in the intersection (`c_clean & l_clean`), otherwise returns 0.0. Effect: kills ~90% of recent Jaccard-path matches (the 0.50–0.60 noise cluster) for NEW rows. Substring paths (0.85/0.75) unaffected. Skipped `'co'`/`'pvt'` from NOISE_WORDS — too short, breaks real brand names. | `scripts/match_linkedin_to_gmaps.py:87`. Verified yield unchanged at ~0.08% (substring path dominates). Real bottleneck is input data quality (LinkedIn `company_name` is too generic), not scorer math. |
| Match NOISE_WORDS expansion (2026-08-09) | Added role tokens (manager/director/ceo/cto/founder/...), business-suffix tokens (group/company/international/global/world/...), LinkedIn-suffix tokens (official/page/profile/career/jobs/hiring/...). ~40 new words. | `scripts/match_linkedin_to_gmaps.py:54`. |
| Match verified-rate ceiling (2026-08-09) | Verified (score≥0.7) yield from new runs is bottlenecked by LinkedIn `company_name` quality, not scorer math. Recent 1,965-row run produced 1 verified (substring path only). Even with the distinct-overlap guard, Jaccard-path matches don't reach 0.7 — they max out around 0.45+0.45*j where j is small. | Verified by simulation against `matched_at > NOW() - INTERVAL '24 hours'` sample. Real fix requires tighter `parse_company` regex or matching on `gmaps_website` domain. Deferred to independent LinkedIn crawler plan. |
| LinkedIn future direction (2026-08-09) | Firehose is becoming the primary independent path. GMaps-matching is secondary. Future independent crawler will capture company size, employees, etc. Deferred to a separate plan. | Current firehose captures profile-level only (`full_name`, `profile_url`, `profile_title`, `company_name`, `search_query`, `confidence`, `snippet`). |
| Search queue retry | `visibility_timeout: 300` (5 min stalled requeue), failed retried after 6h | `config/gmaps_bd_business_search.yaml`; verified correct 2026-08-02 |
| systemctl --value trap | `systemctl show -p A,B,C --value` output is sorted ALPHABETICALLY (MainPID, ActiveState, SubState) | Never positionally parse `--value` with multiple props. Parse `key=value` lines instead. Fixed in `api/routers/dashboard.py` + `monitor.py` 2026-08-02 (daemon PID/uptime/mem were null in API). |
| Emails unique key | `(listing_id, email)` | Added 2026-07-25 |
| LinkedIn unique key | `profile_url` | Added 2026-07-25 |
| Search config needs | `ignore_completed_on_enqueue: true` | Prevents query exhaustion when all queries are in completed set |
| Listing daemon bug | `await restart_browser()` in eternal loop | Removed 2026-07-25; was restarting browser on EVERY failed URL |
| Listing daemon tuning (2026-08-07) | `config/gmaps_listings_working.yaml`: `page_wait_seconds 15→8`, Overview/Reviews/About tab `max_wait` dropped by 1 each, global `retry.attempts 3→2` + `delay 2→1`. `category` and `plus_code` fields both got per-field `retry.attempts: 1` (was wasting retries on the ~10-37% of listings where these fields fail). | Verified live: `Pinchtab navigation complete in ~9s (wait=8.00s)` (was 16-29s). Throughput target ~100/hr (was 43/hr). Category fill held ~90% (was 91.5%); plus_code held ~60% (was 63.5%). |
| Postgres config drift | `listen_addresses` may revert to `localhost` | If cluster restarts externally, socket path breaks. Set to `''` for unix-socket-only. |
| Postgres config drift | `port` may revert to `5433` | Same restart issue. Keep `port = 5432`. |
| Enrichment services (full inventory) | 6 timer-driven + 3 perpetual | All under `~/.config/systemd/user/infinitecrawler-*`.<br>• `email-extract.{service,timer}` — every 2h at `00,02,...,22:15` + 5min randomized (`--max 200 --concurrency 25`; safety net).<br>• `email-extract-loop.service` — **PERPETUAL** (added 2026-08-08). `--loop --loop-gap 30 --max 1000 --concurrency 25`, `Restart=always`. Primary email driver.<br>• `linkedin-search.{service,timer}` — every 4h at `00,04,08,12,16,20:30` + 5min randomized.<br>• `linkedin-match.{service,timer}` — every 6h at `03,09,15,21:30` + 5min randomized. (Added 2026-08-07.)<br>• `classify.{service,timer}` — daily at `03:00`. Requires `LLM_API_KEY` env (in `.env`, loaded via `EnvironmentFile`). Missing key → exit 2 (health gate, 2026-08-08).<br>• `pg-backup.{service,timer}` — daily (midnight-ish).<br>• `watchdog.{service,timer}` — every 15min, runs `monitor_pipeline.py --restart --quiet --json`.<br>• `linkedin-firehose-loop.service` — **PERPETUAL** (no timer). `--loop --loop-gap 60 --max-queries 8000 --concurrency 8`, `Restart=always`.<br>• `pinchtab.service` — **PERPETUAL** (`pinchtab-linux-amd64 server`). Watchdog depends on this being healthy. |
| Phase 3a drift trap (RESOLVED 2026-08-02) | `-h 127.0.0.1` works; the old false-alarm was PG not listening on TCP at that time | The 2026-07-29 audit's false-positive `search_1h=0` from `-h 127.0.0.1` was caused by PG temporarily not bound to TCP. Both `-h 127.0.0.1` and `-h /var/run/postgresql` are valid since 2026-08-02. Verified 2026-08-07 (psql works to both). |
| Phase 2.5 drift trap | `--no-clean` invalid psql flag | Use `-t` (tuples-only). |
| Listing daemon hardcoded knobs | `URL_FETCH_BATCH=100`, `QUEUE_LOW_THRESHOLD=20`, `URL_MAX_RETRIES=3` | Hardcoded in `daemons/listing_daemon.py` — NOT in YAML config. Plan's 2026-08-07 tuning was config-only; changing these would require daemon code changes. |
| Listing daemon write-batching | `PostgreSQLListingDetailsUpsertStrategy._BATCH_SIZE=50` + 5s timer-based flush via `flush_if_due()`. Log line `listing.flush size=N age=Xs trigger=timer\|size`. | 2026-08-08 plan replaced size-only flush with timer+size so first flush lands within 5s of first URL completion (was 13-40 min). |
| Listing daemon false-alarm (post-restart) | First `listing.flush trigger=timer` log line should appear within ~10-30s of daemon startup. If absent, suspect pinchtab port drift (9868→9869) or daemon flap. | 2026-08-08 plan. Verified live. |
| Phase 3 listings_1h healthy floor | ≥60/hr (post 2026-08-07 tuning) | Pre-tuning was ~43/hr. If `listings_1h` drops below 60 with the new config, suspect pinchtab port drift (9868→9869), daemon flap, or Redis queue starvation. |
| Email extract rate | ~50-100 emails/hr | 254 emails in last 7d before 2026-08-07 tuning; 451 emails from 177 listings in 30 min after tuning (multi-page crawl). Hit rate 8-15% on the easy backlog; ~1-3% on the residual hard backlog. |
| LinkedIn firehose throughput | ~225 profiles/hr | 5,414 profiles/day (2026-08-06 baseline); 8,000 queries/cycle with 60s gap. `source='firehose'` in `scraper.linkedin_profiles`. Concurrency=8 with thread-local DDGS instances. |
| Watchdog restart trigger | Only restarts a unit if `procs == 0` AND there's work remaining, OR email is >45min stale | `scripts/monitor_pipeline.py --restart`. Runs every 15min via `infinitecrawler-watchdog.timer`. Logged under `ic-watchdog` syslog identifier. |
| Plan kb doc marker | `.kilo/plans/1786058782715-pipeline-improvement-plan.md` | The pipeline-improvement plan that drove the 2026-08-07 changes. Plan's "Open Questions" section ended at "None" before implementation. |
| Git state drift trap | Leftover interactive rebase state | 2026-08-07. A prior session left `git rebase --merge` state, silently reverting working-tree edits. Diagnose with `git status` showing "interactive rebase in progress". Fix: `git rebase --abort`. Re-apply lost edits from plan output. |
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
# 1a. All service statuses (daemons + last 20 log lines each)
systemctl --user status \
  infinitecrawler-search \
  infinitecrawler-listing \
  infinitecrawler-linkedin-firehose-loop \
  pinchtab \
  --no-pager -l --lines=20

# 1b. All timers (one-shot scheduled units)
systemctl --user list-timers --all --no-pager | grep infinitecrawler
# Expected (current as of 2026-08-07):
#   - watchdog every 15min
#   - email-extract every 2h
#   - linkedin-search every 4h
#   - linkedin-match every 6h
#   - pg-backup daily
#   - classify daily at 03:00

# 1c. Pinchtab health (the Chrome provider both daemons depend on)
PINCHTAB_TOKEN=$(python3 -c "import json; print(json.load(open('/root/.pinchtab/config.json'))['server']['token'])")
# Bridge (daemon target): 9868. Returns {"status":"ok","tabs":N}.
curl -s --connect-timeout 5 -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9868/health
# Server (ABM dashboard): 9867. Returns larger JSON with defaultInstance status.
curl -s --connect-timeout 5 -H "Authorization: Bearer $PINCHTAB_TOKEN" http://127.0.0.1:9867/health

# 1d. Daemon port integrity (catch listing daemon flap)
ss -tlnp 2>&1 | grep -E ":98(67|68|69)" | head -5
# Expected: 9867 = pinchtab server, 9868 = pinchtab bridge (daemon target), 9869 = Chrome CDP (do NOT use).
# If bridge is on 9869 instead of 9868: `systemctl --user restart pinchtab`.

# Expected: all four services active (running). Pinchtab bridge health returns tab count, crashes stats.
# If listing daemon has high NRestarts (e.g. >5), it's flapping — almost always pinchtab port drift.
# Restore: `systemctl --user restart pinchtab && systemctl --user restart infinitecrawler-listing`.
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
# 3a. Search + listing velocity (last hour) — TCP 127.0.0.1 or unix socket both work (verified 2026-08-02)
PGPASSWORD="$PG_PASSWORD" psql -h 127.0.0.1 -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results WHERE updated_at > NOW() - INTERVAL '1 hour') as search_1h,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE updated_at > NOW() - INTERVAL '1 hour') as listings_1h,
  (SELECT COUNT(*) FROM scraper.emails WHERE discovered_at > NOW() - INTERVAL '2 hours') as emails_2h,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles WHERE checked_at > NOW() - INTERVAL '4 hours') as linkedin_4h;
"

# 3b. Full counts (both targets valid; unix socket matches systemd env)
PGPASSWORD="$PG_PASSWORD" psql -h /var/run/postgresql -U postgres -d infinitecrawler -c "
SELECT
  (SELECT COUNT(*) FROM scraper.gmaps_search_results) as search_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings) as listings_total,
  (SELECT COUNT(*) FROM scraper.gmaps_listings WHERE phone IS NOT NULL AND website IS NOT NULL) as qualified,
  (SELECT COUNT(*) FROM scraper.emails) as emails_total,
  (SELECT COUNT(*) FROM scraper.linkedin_profiles) as linkedin_total;
"
```

**Expected:** search_1h > 0, listings_1h > 60 (post 2026-08-07 tuning; was ≥0 before). With multi-page email crawl, emails_2h should be ≥50 (was 10-30). If `search_1h=0` while daemon is processing, verify with `SELECT MAX(updated_at) FROM scraper.gmaps_search_results` — drift is more likely a daemon stall or pinchtab port flap than a psql flag.
**Drift trap (RESOLVED 2026-08-02):** the 2026-07-29 false-positive `search_1h=0` was caused by PG not listening on TCP at that time. Both `-h 127.0.0.1` and `-h /var/run/postgresql` are now valid; the `.env` and systemd env use the unix socket path.
**EXIT_CODE: REPORT**

---

## PHASE 4: ENRICHMENT COMPLETENESS — REPORT

Verify the offline enrichment scripts are producing results:

```bash
# 4a. Email coverage (incl. emails_1h velocity)
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_email_extract.py --stats

# 4b. LinkedIn coverage
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_linkedin_search.py --stats

# 4c. Classification coverage
cd /root/codebase/vhd/infinitecrawler && uv run python scripts/db_classify.py --stats

# 4d. emails_1h velocity from monitor (added 2026-08-08 plan)
PGPASSWORD=$(grep ^PG_PASSWORD /root/codebase/vhd/infinitecrawler/.env | cut -d= -f2) \
  psql -h /var/run/postgresql -U postgres -d infinitecrawler -t \
  -c "SELECT count(*) FROM scraper.emails WHERE discovered_at > NOW() - INTERVAL '1 hour';"
# Warn if <5 (loop daemon target is ≥20/hr)

# 4e. LinkedIn match score-bucket histogram (added 2026-08-09 plan)
PGPASSWORD=$(grep ^PG_PASSWORD /root/codebase/vhd/infinitecrawler/.env | cut -d= -f2) \
  psql -h /var/run/postgresql -U postgres -d infinitecrawler -t -c "
SELECT width_bucket(score, 0.0, 1.01, 10) AS bucket, count(*)
FROM scraper.linkedin_gmaps_matches
WHERE matched_at > NOW() - INTERVAL '24 hours'
GROUP BY 1 ORDER BY 1;"
# Expected post-2026-08-09 plan: bucket 5+6 (≤0.600) should be near-zero; most new
# rows either reach substring path (0.85/0.75) or get killed (no row). Jaccard-path
# matches in bucket 7+ (≥0.611) are rare because input data is too generic.
```

# 4f. Email extraction by method (added 2026-08-09, T2)
PGPASSWORD=$(grep ^PG_PASSWORD /root/codebase/vhd/infinitecrawler/.env | cut -d= -f2) \
  psql -h /var/run/postgresql -U postgres -d infinitecrawler -t -c "
SELECT extraction_method, count(*) FROM scraper.emails
WHERE discovered_at > NOW() - INTERVAL '24 hours'
GROUP BY 1 ORDER BY 2 DESC;"
# Expected post-T2 (browser fallback on): both `http` and `browser` rows
# appear in the last 24h. Pre-T2 only `http` rows exist.
```

**Expected:**
- Email coverage growing every 2h (timer-driven; multi-page crawl since 2026-08-07).
- LinkedIn profiles growing every 4h (search timer) and continuously (~225/hr via firehose loop).
- LinkedIn↔GMaps matches written every 6h (match timer added 2026-08-07); high-quality (score≥0.8) match count growing.
- 0 remaining unclassified leads with phone+website (the `infinitecrawler-classify.timer` runs daily at 03:00; **requires `LLM_API_KEY` in systemd env** to actually classify — if missing, the timer exits early without writing sectors).

**Drift trap:** `--stats` commands that hit the LLM classifier need `LLM_API_KEY` in env. Run via `set -a; source .env; ...` or `systemctl --user show infinitecrawler-classify.service -p Environment | tr ' ' '\n' | grep LLM_API_KEY` to confirm.
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

# 5d.2 File-log freshness (2026-08-08 plan) — must show fresh activity
head -50 /var/log/infinitecrawler/infinitecrawler-listing.log
ls -la /var/log/infinitecrawler/*.log | wc -l
# Expect: first line should contain a `listing.flush` event within last ~30s of any active daemon;
#        ≥5 files for actively running units.

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
    from utils.pg import get_pg_config, get_uncrawled_urls_sql, build_async_dsn
except: print('WARN: utils.pg'), None

try:
    from utils.transliterate import bn_to_en, contains_bengali
except: print('WARN: utils.transliterate'), None

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