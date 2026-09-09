# LinkedIn Jobs + Companies — Implementation Plan

> **Status: BUILT & VERIFIED end-to-end (2026-09-07).** All three daemons scrape real data through the proxy into Postgres. Remaining: systemd units + launch_daemons integration + monitor.
> Replaces/supersedes: nothing. Complements: `config/linkedin_firehose.yaml` (profile discovery).

## Verified results (2026-09-07, live through datasolved proxy)

- **Search**: `merchandiser × Dhaka` → 63 job IDs queued to Redis (page caps hit correctly at <10 results).
- **Job detail**: 63/63 jobs fetched, upserted into `scraper.linkedin_jobs`. 61 had company slug, all 63 had descriptions, city/seniority/employment extraction correct.
- **Company**: 9/9 companies scraped (name, employee_count, website, HQ) via the `-cf` proxy + fresh-connection pattern. E.g. Centric Brands (2629 emp), CAREER141, NDs9, nextjobz.
- **Lint**: `ruff check` clean on all new files.

## Schema (already applied to prod)

- `scraper.sectors` — 45 rows (29 BD-business + 16 software), seeded from YAML.
- `scraper.linkedin_jobs` — 25 cols, PK `job_id`, 6 indexes.
- `scraper.linkedin_query_state` — resume/pagination state.
- `scraper.linkedin_companies` — extended with `linkedin_slug` (UNIQUE), `jobs_count`, `last_job_seen_at`.

## 0. Consolidation question: yes, with a hard separation

Two streams currently discover things that look like "companies." They are **not same entity kind**, but they converge into one table.

| | firehose (already running) | jobs scraper (this plan) |
|---|---|---|
| discovers | **people** (profiles) | **job postings** |
| side-effect | pulls company *names* out of headlines ("X at Y") | pulls company *slug + page URL* from job topcard |
| gives you | `scraper.linkedin_profiles.company_name` (string, fuzzy) | `linkedin.com/company/{slug}` (canonical, dedup-safe) |

**Decision: consolidate on `scraper.linkedin_companies`, but keep provenance.**

The current `linkedin_companies` table is keyed on `company_name_norm` (normalized fuzzy name). That's fine for merging the firehose side, but it forces all jobs-side upserts through a fuzzy string match — lossy when two BD firms share a name ("Noor Trading", "Islam Enterprise").

### The fix (additive)

1. Add a new column to `scraper.linkedin_companies`:
   ```sql
   ALTER TABLE scraper.linkedin_companies
     ADD COLUMN IF NOT EXISTS linkedin_slug TEXT UNIQUE;
   ```
   (The `slug` column exists per the current UPSERT in `utils/pg.py:420` — but make it UNIQUE and treat *it*, not `company_name_norm`, as the primary identity when present.)

2. Upsert precedence, in order:
   - If `linkedin_slug` known → match on slug, fill in name/fields via COALESCE.
   - Else if `company_name_norm` matches → attach to that row, keep its slug=NULL pending.
   - Else insert a fresh row keyed by slug alone; a later firehose hit will attach by name.

3. Don't merge *in* the other direction (firehose profiles get a `company_slug` once known).

This gives one table, no double-crawl of the same company, and no false merges.

## 1. Schema

Additive only. All new tables live under `scrapers.linkedin_jobs*` + extend `linkedin_companies`.

```sql
-- in a new scripts/linkedin_schema.py (or appended to schema_migration.py)
CREATE TABLE IF NOT EXISTS scraper.linkedin_jobs (
    id                  BIGSERIAL PRIMARY KEY,
    job_id              BIGINT      NOT NULL UNIQUE,
    source_url          TEXT        NOT NULL,
    title               TEXT        NOT NULL,
    company_name        TEXT,
    company_linkedin_slug TEXT,
    company_linkedin_url  TEXT,
    location            TEXT,
    location_city       TEXT,           -- from our query (dhaka|chattogram|bd)
    listed_at           TIMESTAMPTZ,
    seniority_level     TEXT,
    employment_type     TEXT,
    job_function        TEXT,
    industries          TEXT,
    description_html    TEXT,
    description_text    TEXT,
    applicants_count    INTEGER,
    promoted            BOOLEAN,
    easy_apply          BOOLEAN,
    apply_url           TEXT,
    source_sector       TEXT,           -- which sectors.yaml block produced it
    source_keyword      TEXT,           -- which linkedin_jobs keyword
    sector_id           INTEGER REFERENCES scraper.sectors(id),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_company_slug
    ON scraper.linkedin_jobs(company_linkedin_slug);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_sector  ON scraper.linkedin_jobs(sector_id);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_listed  ON scraper.linkedin_jobs(listed_at DESC);
CREATE INDEX IF NOT EXISTS idx_linkedin_jobs_company ON scraper.linkedin_jobs(company_name);

ALTER TABLE scraper.linkedin_companies
    ADD COLUMN IF NOT EXISTS linkedin_slug TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS jobs_count   INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_job_seen_at TIMESTAMPTZ;
```

`linkedin_profiles` is untouched.

The `scraper.sectors` table will need rows for the 12 new sectors — handled by the same seeding script that already syncs the YAML into PG (`scripts/db_classify.py` does it lazily; check whether it uses `display_name` or key, run it once after the YAML change).

## 2. Three new daemons

All are `systemctl --user` units, follow the existing pattern (`After=network-online.target`, `EnvironmentFile=/etc/infinitecrawler/env` or wherever your current units source from). Each is a `uv run python -m daemons.<name>` entry.

### 2a. `linkedin_search_daemon`
- Reads `linkedin_jobs` block from `SECTORS_YAML_PATH` (the file we just extended).
- Builds Cartesian `keyword × location` query set (417 upfront today; grows when sectors added).
- Stores progress in a small PG table `scraper.linkedin_query_state (keyword, location, last_start, exhausted_at, last_run_at, PRIMARY KEY (keyword, location))` — additive.
- For each query: page `start=0,10,20,…,975` on `jobs-guest/jobs/api/seeMoreJobPostings/search`. Each response has 10 job cards. Stop when:
  - `< 10` cards (end of results), OR
  - `start > 975` (LinkedIn hard cap — mark `exhausted_at=NOW()` and split keywords via `+ subsegment` synonyms if we want more).
- Extracts job_ids from `data-entity-urn="urn:li:jobPosting:{id}"`.
- `SADD linkedin:jobs:pending <job_id>` for each new id.
- Sleeps 600s when Redis queue > watermark (default 50k). Rate limit: 1 req / 0.8s with jitter, using datasolved default residential proxy.

### 2b. `linkedin_job_detail_daemon`
- `SPOP linkedin:jobs:pending` in batches of 50.
- For each: GET `https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{id}`.
- Parse with BeautifulSoup:
  - `top-card-layout__title` → title
  - `topcard__org-name-link` → company name + `href` → company slug
  - `description__job-criteria-list` → 4 criteria `<li>`s
  - `show-more-less-html__markup` → description (keep both HTML and text)
  - `posted-time-ago__text` + relative-date parser → `listed_at`
  - `num-applicants` text → int when parseable
- Upsert into `scraper.linkedin_jobs` (`ON CONFLICT (job_id) DO UPDATE ...`).
- Extract `(slug, name)` pair → `SADD linkedin:companies:pending slug`.
- On 999/429 → push the ID to a 5-min delay queue and rotate proxy; on 404 (job expired) → mark `updated_at` with empty body but keep the row.

### 2c. `linkedin_company_daemon` (replaces the current firehose-side enrichment loop)
- `SPOP linkedin:companies:pending` in batches of 20.
- For each slug: GET `https://www.linkedin.com/company/{slug}` via proxy.
- Parse the `application/ld+json` `@graph` for the `Organization` node:
  - `name`, `url`, `description`,
  - `address.{streetAddress,addressLocality,addressRegion,postalCode,addressCountry}` → `headquarters`,
  - `numberOfEmployees.value` → `employee_count`,
  - `sameAs` → `website`,
  - `logo.contentUrl` → `logo_url`.
- Upsert into `scraper.linkedin_companies` using the **slug-first** rule from §0.
- Fallback when ld+json absent (some companies with no schema block): regex-extract `top-card-layout__title`, `top-card-layout__first-subline`, employee band, then store partial row with `notes="html_fallback"`.
- Merge with existing firehose rows: when a slugged row arrives and a same-normalized-name row exists with NULL slug → UPDATE that row, set slug, merge fields.

### 2d. (Existing) Firehose bridge — one-time + weekly
- Script `scripts/linkedin_slug_backfill.py`:
  - For every `scraper.linkedin_companies` row where `slug IS NULL`: search DDGS for `site:linkedin.com/company {company_name}`; take top hit's slug; UPDATE row. Bounded by `--max`.
  - Runs weekly to attach slugs to the long tail of old firehose rows.
- After 30 days of operation, `linkedin_companies.slug IS NULL` count should trend toward zero.

## 3. Proxy / HTTP client

- **Client**: `curl_cffi.requests.Session(impersonate="chrome120")` as a per-worker singleton. Falls back to plain `httpx` if curl_cffi is unavailable.
- **Proxy pool** via env: `DATASOLVED_PROXY` accepts comma-separated patterns and the daemon rotates round-robin on each batch:
  ```
  DATASOLVED_PROXY=http://datasolved:PASS@gw.proxy.datasolved.org:10044
  DATASOLVED_PROXY_CF=http://datasolved-cf:PASS@gw.proxy.datasolved.org:10044
  ```
- HTTP/2, `accept-language: en-US,en;q=0.9`, `sec-ch-ua` triple, fixed header order — matches the curl you pasted. Verified all four flavors return 200 on all three endpoints.
- Per-worker pacing: 0.8s mean delay + exponential backoff on 429 (`min(2^n, 30s)`); on 999 → burn that proxy for 10 min and switch.

## 4. Monitoring & ops

- Extend `scripts/monitor_pipeline.py` with a `linkedin_jobs` block: rows in `linkedin_jobs`, queue lengths, `linkedin_companies.slug IS NULL` count, daemon heartbeat timestamps.
- Extend `scripts/launch_daemons.sh` with `ensure_daemon linkedin-search linkedin-job-detail linkedin-company`.
- New systemd units in `systemd/` named symmetrically with the existing ones: `infinitecrawler-linkedin-search.service`, `infinitecrawler-linkedin-job-detail.service`, `infinitecrawler-linkedin-company.service`.

## 5. Build order (estimated 3 PRs)

1. **PR-A: Schema + config loader** (~2h)
   - `scripts/linkedin_schema_migration.py`
   - `utils/linkedin_config.py` (reads `linkedin_jobs` block from sectors.yaml)
   - `tests/test_linkedin_config.py`
2. **PR-B: Search + detail daemons** (~6h)
   - `daemons/linkedin_search_daemon.py`
   - `daemons/linkedin_job_detail_daemon.py`
   - `utils/pg.py`: add `upsert_linkedin_job`, `get_linkedin_query_state`, `bump_linkedin_query_state`, `mark_linkedin_query_exhausted`
   - One happy-path integration test that hits one keyword × Dhaka and asserts ≥1 row in `linkedin_jobs`
3. **PR-C: Company enrichment + firehose bridge** (~4h)
   - `daemons/linkedin_company_daemon.py`
   - `scripts/linkedin_slug_backfill.py`
   - `utils/pg.py`: rework upsert to be slug-first, plus `merge_linkedin_company_by_slug`
   - Wire `monitor_pipeline.py`, systemd units, `launch_daemons.sh`

## 6. Operations checklist (when merging)

1. `uv run ruff check .` and `uv run pytest` clean.
2. New systemd units installed, `lingering` enabled.
3. `DATASOLVED_PROXY` added to `.env`.
4. `SECTORS_YAML_PATH` set to the BPT sectors.yaml in `.env` (so this and the GMaps pipeline share one source).
5. Run `uv run python scripts/linkedin_schema_migration.py --apply` on prod.
6. `systemctl --user start` the three units, confirm `journalctl --user -u <unit>` shows first batch committed.

## 7. Risks and decisions to revisit later

- **975-result cap**: when a (kw, loc) pair exhausts at the cap, we lose the tail. Mitigate with sub-keyword expansion (e.g. "mechanical engineer" → "senior mechanical engineer", "junior mechanical engineer"). Flag for later.
- **Dedup across locations**: a job listed as "Dhaka" may also appear under location "Bangladesh"; dedupe by `job_id` (primary key) — that's already in the schema.
- **Slug-less firehose companies** with same normalized name as a slugged company from jobs (e.g. two distinct BD firms both called "Noor Trading"): keep as separate rows, never auto-merge by name alone — merge only explicitly by slug.
- **apply_url**: LinkedIn embeds `applyMethod` only for offsite applies in ~half of postings; expose the field but don't gating success on it.
