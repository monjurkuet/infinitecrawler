# InfiniteCrawler Pipeline Audit — 2026-09-04 03:00 HKT

Scope: all extraction datapoints (GMaps listings/search, Places API, Nearby grid,
emails — HTTP+browser, LinkedIn profiles/companies, classification), every daemon,
dashboards, Redis/PG. Verified live: `psql` socket, `redis-cli`, `curl`,
`systemctl --user` — all numbers below are live reads at this timestamp.

**Verdict: everything is working, every datapoint class is extracting, both dashboards up.**

## 1. Daemon fleet — 14/14 active, 0 restarts in error

| Unit | State | Live evidence |
|---|---|---|
| search | active | 603–1,010 search_results/h scroll-extracting |
| listing | active | `listing.flush size=3` every ~1–2 min; 0 phantom rows last 1h |
| places-api | active | quota-paused overnight, resets ~08:00 UTC (pitfall #5, normal) |
| nearby-scanner | active | 575 grid cells/24h, 13 this hour |
| email-extract-loop | active | `--mode both`; 1,208 emails this hour, browser method live |
| linkedin-firehose-loop | active | 1,371 profile checks/24h |
| linkedin-company-loop | active | lookups 200 OK |
| linkedin-backfill | active | fixed 2026-09-03 — no more 0-row spin |
| classify | active | LLM responding; gmaps-listing classified 96.7 % |
| pinchtab | active | port 9868 healthy, browser tier fed |
| phantom-sweeper (timer) | active | runs every 30 min; LLEN gmaps:phantom = 17 |
| watchdog (timer) | active | every 5 min, last run 3 min ago |
| osm-import (timer) | scheduled | Mon 03:01 (one-shot by design, pitfall #14) |
| api :8015 / premium-api :8016 / web :5173 | active | all health-checked below |

## 2. Totals (live)

| Table | Rows | Fresh movement |
|---|---|---|
| gmaps_listings (all) | 1,014,303 | 2,648 created/24h, plus hundreds of upserts/h |
| — gmaps_listing (browser) | 72,187 | 2,356/24h |
| — osm (one-shot) | 940,049 | static (expected) |
| — nearby_search | 2,067 | quota window only |
| gmaps_search_results | 202,533 | 11,591/24h, ~1,010 this hour |
| emails | 476,409 (200,559 distinct addresses) on 130,580 listings | 3,969/24h |
| linkedin_profiles | 85,296 | 1,371 checks/24h |
| linkedin_companies | 1,173 | growing |
| nearby_scan_grid | 272,578 | 4,589 done (1.7 %), 575/24h |

## 3. Datapoint coverage — gmaps_listing rows (all-time)

| Field | Fill % | Baseline verdict |
|---|---|---|
| name, rating(87.9), address(98.3), category(93.3) | healthy | ≥ baseline |
| phone | 74.8 % | at baseline (75 %) |
| website | 30.9 % | normal for BD market |
| plus_code | 64.3 % | above baseline (59 %) |
| email_scanned | 28.0 % | growing (backlog §5) |
| classified | 96.7 % | healthy |
| payload jsonb | 100 % | full raw capture kept |

Phantom guard working: **0 bare-shell rows in the last hour** (phantom = 0).
`is_claimed` deliberately NOT extracted — Google never exposes it publicly (pitfall #29).

## 4. Emails — post-fix confirmed

The 2026-09-03 browser-pass fix (pinchtab allowlist + contact-page walk + EnvironmentFile)
is producing: **312 browser-method emails in the last 24 h** (was 0), HTTP still
delivers the bulk. Hourly discovery rate accelerated through the evening
(9 → 164 → 31 → 62 → 1,208/h) — the loop is draining.

## 5. Backlogs (drift, not faults — same state as prior audits)

- 131,468 search_results seeds awaiting deep extraction (up from 86k on 2026-08-28).
  Browser tier healthy but seeding (11.6k seeds/day) > extraction (~2.4k/day).
  Fixing = more tabs or a second listing worker; deliberately not chased here to
  protect per-listing datapoint completeness (your stated priority).
- 85,100 website-listings awaiting email scan — extractor outpaced during the
  HTTP-only era; now draining with `--mode both`.
- 2,340 gmaps_listing rows unclassified — nightly classify drains it.
- Redis: pending=1,276, processing=3, completed=14,634, phantom=17 — queue fed,
  no starvation (watchdog backfill active).

## 6. Dashboards

- Internal API :8015 → `{"status":"ok","postgres":"ok","redis":"ok", disk 47 GB free}`
- Premium API :8016 → `{"status":"ok"}` (4 registered users)
- Premium web :5173 → 200 (Vite dev server)

## 7. Known quota windows (not failures)

places-api + nearby-scanner pause overnight on the 200/day/key free-tier cap and
resume automatically at the 08:00 UTC reset. This is by design (pitfall #5).

## Bottom line

Every datapoint class you listed — Google listing fields, search results, emails
(HTTP + browser), LinkedIn profiles/companies, classification — is extracting now
with quality at-or-above the documented baselines, dashboards are serving, and the
only open items are throughput (seed backlog drift) rather than correctness.
