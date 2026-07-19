# Implementation Plan: Email Extraction + LinkedIn Employee Discovery

## Goal

Extend InfiniteCrawler to:
1. Extract email addresses from company websites during listing extraction
2. Discover LinkedIn employee profiles via DDGS web search API
3. Provide offline batch scripts for backfill and ongoing enrichment

## Architecture Decision

**Integrate into InfiniteCrawler** — not a separate codebase. Both features share the same PG database, connection config, and data model (listing_id FK). Separate codebase would duplicate infrastructure for no benefit.

**Pattern**: Hybrid — inline (in listing_daemon) for email extraction during the browser session + cron scripts for offline HTTP backfill and LinkedIn search (following the proven `db_classify.py` pattern).

## Files to Create

| # | File | Purpose |
|---|------|---------|
| 1 | `scripts/schema_migration.py` | Create `scraper.emails` and `scraper.linkedin_profiles` tables |
| 2 | `utils/email_extractor.py` | Email regex patterns, page scanning, normalization |
| 3 | `scripts/db_email_extract.py` | Offline HTTP-based email backfill (cron) |
| 4 | `scripts/db_linkedin_search.py` | DDGS-based LinkedIn profile discovery (cron) |

## Files to Modify

| # | File | Change |
|---|------|--------|
| 1 | `daemons/listing_daemon.py` | Add inline email extraction step after website extraction |
| 2 | `strategies/output/postgresql.py` | Add `EmailUpsertStrategy` for scraper.emails |
| 3 | `scripts/monitor_pipeline.py` | Add email + LinkedIn stats |
| 4 | `utils/pg.py` | Add `upsert_emails` / `upsert_linkedin_profiles` helpers |

## Phase 1: Foundation + Email Extraction

### 1a. Database Migration

New tables:

```sql
CREATE TABLE scraper.emails (
    id              BIGSERIAL PRIMARY KEY,
    listing_id      BIGINT REFERENCES scraper.gmaps_listings(id) ON DELETE CASCADE,
    website_url     TEXT NOT NULL,
    email           TEXT NOT NULL,
    email_type      TEXT DEFAULT 'general',
    extraction_method TEXT DEFAULT 'browser',  -- 'browser', 'http', 'mailto'
    is_obfuscated   BOOLEAN DEFAULT FALSE,
    context_snippet TEXT,
    discovered_at   TIMESTAMPTZ DEFAULT NOW(),
    last_verified   TIMESTAMPTZ,
    is_active       BOOLEAN DEFAULT TRUE,
    UNIQUE(listing_id, email)
);

CREATE TABLE scraper.linkedin_profiles (
    id              BIGSERIAL PRIMARY KEY,
    listing_id      BIGINT REFERENCES scraper.gmaps_listings(id) ON DELETE CASCADE,
    full_name       TEXT,
    profile_url     TEXT NOT NULL UNIQUE,
    profile_title   TEXT,
    company_name    TEXT,
    search_query    TEXT,
    confidence      REAL DEFAULT 0.5,
    snippet         TEXT,
    checked_at      TIMESTAMPTZ DEFAULT NOW(),
    last_updated    TIMESTAMPTZ,
    notes           TEXT
);

CREATE INDEX idx_emails_listing ON scraper.emails(listing_id);
CREATE INDEX idx_emails_email ON scraper.emails(email);
CREATE INDEX idx_linkedin_listing ON scraper.linkedin_profiles(listing_id);
CREATE INDEX idx_linkedin_company ON scraper.linkedin_profiles(company_name);
```

### 1b. Extract utility (`utils/email_extractor.py`)

Class/module with:
- `EMAIL_REGEX` — compiled pattern for standard emails
- `Obfuscated` patterns: `[at]`, `[@]`, `(at)`, `[dot]`, `(dot)`
- `scan_text_for_emails(text: str) -> list[dict]` — find all emails in text
- `extract_mailto_links(page) -> list[str]` — extract from mailto: hrefs
- `normalize_email(raw: str) -> str | None` — clean up obfuscated emails
- `filter_noise(emails: list) -> list` — reject noreply/careers/jobs/test/example

### 1c. Inline extraction in listing_daemon.py

In `process_url()`, after successful extraction and when `items[0].get('website')` exists:

```
if website_url := items[0].get('website'):
    emails = await extract_emails_from_website(browser, website_url)
    if emails:
        upsert_emails(pg_conn, listing_id, website_url, emails)
```

Function `extract_emails_from_website(browser, url, timeout=8)`:
1. Open new tab (or navigate same tab) to website URL
2. Wait for load (timeout 8s)
3. Get `document.documentElement.innerText`
4. Extract mailto: links from page
5. Scan text + mailto for emails
6. Normalize + filter
7. Return deduplicated list

Timeboxed: if navigation or extraction hangs, release and continue.

### 1d. Offline backfill (`scripts/db_email_extract.py`)

- Reads `SELECT id, website FROM scraper.gmaps_listings WHERE website IS NOT NULL AND id NOT IN (SELECT listing_id FROM scraper.emails)`
- Uses `httpx` (no browser) to fetch each website
- Scans response text for emails
- Upserts to `scraper.emails`
- Configurable batch size, rate limit
- Dry-run + stats modes (same pattern as `db_classify.py`)

### 1e. Email output strategy

Add `EmailUpsertStrategy` to `strategies/output/postgresql.py`:
- `write_item(item)` — upsert one email with listing_id FK

Or simpler: add `upsert_emails()` standalone function in `utils/pg.py`.

## Phase 2: LinkedIn Discovery

### 2a. DDGS search utility

`utils/linkedin_search.py`:
- `search_linkedin_profiles(company_name: str, sector: str | None, max_results=5) -> list[dict]`
- Constructs query: `site:linkedin.com/in/ "{company_name}"`
- If sector known, adds role-targeted keywords: `site:linkedin.com/in/ "{company_name}" "manager"`
- Calls `GET https://search.datasolved.org/search/text?query=...&max_results=...`
- Returns parsed results with confidence scores

Confidence scoring:
| Signal | Points |
|--------|--------|
| Company name in body text | +0.4 |
| URL path contains company name substring | +0.3 |
| Title contains role keyword | +0.2 |
| Body mentions connection count | +0.1 |
| Total capped at 1.0 | |

### 2b. Discovery script (`scripts/db_linkedin_search.py`)

- Reads listings with name, optionally filtered by sector priority
- For each: DDGS query → parse → score → dedup → upsert
- Skips listings searched within last 7 days
- Rate limit: 1 query per 2s (via DelayManager)
- Supports `--max`, `--sector`, `--dry-run`, `--stats` flags
- Dry-run shows what would be searched without writing

## Phase 3: Pipeline Verification

### 3a. Monitor integration

Update `scripts/monitor_pipeline.py`:
- "Emails extracted (last 24h): COUNT"
- "LinkedIn profiles found (total): COUNT"
- "Listings without email: COUNT"
- "Listings without LinkedIn search: COUNT"

### 3b. Verification cron (optional)

`scripts/db_verify_emails.py`: re-check previously found emails.
`scripts/db_linkedin_verify.py`: LLM check of low-confidence profiles.

## Test Plan

- Run `schema_migration.py` → verify tables exist with `\d scraper.emails` + `\d scraper.linkedin_profiles`
- Run `db_email_extract.py --dry-run` on a few listings → verify output format
- Run `db_linkedin_search.py --dry-run --max 5` → verify API output + confidence scores
- Verify listing_daemon still runs without errors on a URL (smoke test)
- Verify monitor_pipeline.py shows new stats

## Risks

| Risk | Mitigation |
|------|------------|
| Email extraction slows listing daemon | Independent 8s timeout per URL; skip if website is None; error doesn't crash process_url |
| DDGS API rate limits | 2s delay between queries; batch limit configurable |
| Low-quality email matches | Noise filter (noreply/careers/jobs/test/example) + min length check |
| Irrelevant LinkedIn profiles | Confidence threshold; only keep scores > 0.3 |
| PG migration conflicts | New tables only, no ALTER TABLE on existing |

## Open Questions

- Should inline extraction use a new browser tab (leaving the GMaps page loaded) or navigate the current tab? → **Navigate current tab, then navigate back to GMaps if needed.** The GMaps page is already fully extracted. However, navigating away may lose the GMaps session. **Alternative**: open a new tab for the website, scan, close it. This is safer but adds overhead. → **Use a new tab** to preserve the GMaps page.
