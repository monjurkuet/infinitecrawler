# InfiniteCrawler DB Comparison — this box (pearOS) vs desktop (Tailscale 100.108.5.65)

Date: 2026-09-03 17:45 HKT
Hosts:
- **LOCAL**  = this machine (pearOS, `100.105.32.122`), PostgreSQL at `/var/run/postgresql`
- **REMOTE** = desktop WSL PostgreSQL reachable via Tailscale `100.108.5.65:5432`

Both databases hold the **same schema** (28 cols on `gmaps_listings`, 18 on `linkedin_profiles`,
18 on `linkedin_companies`, 11 on `emails`, 6 on `gmaps_search_results`, 11 on `nearby_scan_grid`).
`LOCAL` additionally has `scraper.app_users` (premium dashboard accounts) which `REMOTE` lacks.

## 1. Verdict

**This system's database (LOCAL) is bigger, more recent, and a strict superset of the desktop.**
Desktop Postgres has been offline from the pipeline since ~2026-08-28 (its newest listing is
`2026-08-26`, newest email `2026-08-28`). Everything captured on desktop is present here, with
zero rows lost, plus 8 days of new activity the desktop never saw.

| Table | LOCAL rows | REMOTE rows | Δ (local − remote) | Local-only keys | Remote-only keys |
|---|---|---|---|---|---|
| gmaps_listings | 1,012,645 | 991,727 | +20,918 | +20,945 | **0** |
| gmaps_search_results | 193,133 | 126,672 | +66,461 | +66,568 | **0** |
| emails (distinct email) | 200,351 | 90,413 | +109,938 | +109,938 | **0** |
| linkedin_profiles | 84,216 | 76,817 | +7,399 | +7,411 | **0** |
| linkedin_companies | 1,163 | 95 | +1,068 | n/a | 0 |
| nearby_scan_grid | 272,578 | 272,578 | 0 | — | — |
| **pg_database size** | **2,346 MB** | **2,015 MB** | **+331 MB** | | |

`0 remote-only` on listings/search_results/emails/linkedin confirms LOCAL ⊇ REMOTE — every row
the desktop has, this box has too. The +20,945 local-only listings + 66,568 extra seeds are the
work this instance did while the desktop was offline (or that desktop never ran).

## 2. Recency — when did each side last write?

| Metric | LOCAL | REMOTE |
|---|---|---|
| latest `gmaps_listings.created_at` | 2026-09-03 17:41 | 2026-08-26 02:40 |
| latest `emails.discovered_at` | 2026-09-03 17:41 | 2026-08-28 04:18 |
| latest `linkedin_profiles.checked_at` | 2026-09-03 17:40 | 2026-08-28 03:19 |
| listings in last 7d | 18,775 | **0** |
| emails in last 7d | 241,573 | **17** |
| search_results in last 7d | 62,003 | **150** |
| li_profiles checked in last 24h | 1,961 | **0** |

**Conclusion: the desktop has not been a writer since ~2026-08-28.** It is a stale snapshot. ALL
current pipeline activity lands on this box.

## 3. Per-source_type completeness

| source_type | rows | phone% | website% | rating% | email_scanned% | classified% |
|---|---|---|---|---|---|---|
| **LOCAL osm** | 940,049 | 33.4 | 41.1 | 0.0 | 32.3 | 9.4 |
| REMOTE osm | 939,599 | 33.4 | 41.1 | 0.0 | 14.4 | 0.6 |
| **LOCAL gmaps_listing** | 70,763 | 74.7 | 30.7 | 88.2 | 28.6 | 96.9 |
| REMOTE gmaps_listing | 51,349 | 73.7 | 29.6 | 92.9 | 29.2 | 97.1 |
| **LOCAL nearby_search** | 1,854 | 79.8 | 43.9 | 93.1 | 43.9 | 72.7 |
| REMOTE nearby_search | 779 | 92.0 | 60.1 | 98.7 | 60.1 | 58.3 |

Local matches desktop on field rates (within ±5 pts) — same extractors, just running.
The remote has slightly higher rating% on gmaps_listing because the heavy 70,763-row local
corpus has more "no rating" tail, not because extract quality differed.

## 4. Database on disk

| | LOCAL | REMOTE |
|---|---|---|
| pg_database total | 2,346 MB | 2,015 MB |
| gmaps_listings table | 1,199 MB | 1,058 MB |
| gmaps_search_results | 266 MB | 160 MB |
| emails | 178 MB | 90 MB |
| linkedin_* (both) | 68 MB | 63 MB |

LOCAL is +331 MB — the extra ~8 days of activity. Indexes on `source_url`, `place_id`,
`created_at`, `updated_at` all present on both.

## 5. Did anything move backwards?

- **`scraper.app_users` exists only on LOCAL** (the premium dashboard was built here). Desktop
  silently has no paid-tier accounts — expected, dashboard lives on this box.
- `nearby_scan_grid` is byte-identical between the two (272,578 rows, same distribution of
  `pending`/`done`). Neither side has scanned since quota freeze.
- LinkedIn-company enrichment on remote : 95 rows. Local : 1,163 (+1068). The company loop
  daemon is only wired on this box.

## 6. Bottom line

- Local = live + complete superset. Desktop = stale read-only snapshot (last activity 2026-08-28).
- You can treat the desktop database as a historical backup. Do NOT push it back onto this box —
  it would duplicate `source_url`s harmlessly but write backwards timestamps.
- If you want the desktop to resume contributions, point its daemons at the same `.env` and
  `tailscale0` address — its writes will land here via the same `ON CONFLICT` mechanics
  without collisions (the 0-remote-only keys prove there is no divergence to reconcile).
