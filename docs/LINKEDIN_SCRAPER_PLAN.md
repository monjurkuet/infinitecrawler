# LinkedIn Jobs Scraper — Research & Implementation Plan

> Status: researched & verified 2026-09-06 against live endpoints.
> All endpoints below are **unauthenticated, publicly routable, and JS-optional** (server-rendered HTML). No cookies / Login required.

## 1. Endpoints the scraper will use (verified working)

| Purpose | URL pattern | Verified |
|---|---|---|
| Discover jobs by keyword+location, paginated | `https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={kw}&location={loc}&start={n}` | 200, 10 cards/page, **hard cap at `start=975`** (start=1000 → 400) |
| Job detail (title, company, criteria, full description) | `https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}` | 200, full HTML body |
| Company profile (org schema, address, size, website) | `https://www.linkedin.com/company/{slug}` (fallback `https://{cc}.linkedin.com/company/...`) | 200, ld+json Organization schema embedded |
| SEO/browse page (60-card variant, extra discovery) | `https://www.linkedin.com/jobs-guest/jobs/search?keywords=...&location=...` | 200, 60 cards/page |

All accept plain GET with a browser User-Agent. Mobile UA works too.

### Data fields available per job (from jobPosting HTML)
- `top-card-layout__title` → job title
- `topcard__org-name-link` → company name + company URL
- `description__job-criteria-list` → Seniority level, Employment type, Job function, Industries
- `show-more-less-html__markup` → full description text
- `posted-time-ago__text` + entity urn `data-entity-urn="urn:li:jobPosting:{id}"` → listed time + canonical id

### Data fields available per company (from ld+json `Organization`)
- `name`, `description`, `address` (street, city, region, postal, country)
- `numberOfEmployees` (int)
- `sameAs` → **official website**
- `logo.contentUrl`
- Posts feed in same graph (recent activity)

The HTML also exposes `follower counts` and industry text in `top-card-layout__entity-info` (fallback if numberOfEmployees missing).

## 2. Pagination strategy (the 975-item wall)

LinkedIn caps guest search to `start <= 975` per (keywords+location) combination — same as their logged-in limit. To harvest "all categories × all locations":

- Treat every (keyword, location) pair as its own crawl job.
- For keywords with >975 hits, **bisect by additional filters** until each sub-query returns <975: LinkedIn guest search only accepts a small subset of filters via query params — the practical splitter is **keyword enrichment** (synonyms/subcategories) + **narrower locations** (city level).
- Job categories come from your own taxonomy (sectors.yaml already lists 15 BDT sectors; expand with LinkedIn's 14 standard job functions: Engineering, Sales, Marketing, Operations, Finance, HR, IT, Legal, Healthcare, Education, Customer Service, Design, Admin, Other).

Rough ceiling: ~980 × N_subqueries per location. For Dhaka with ~150 subcategory keywords ≈ 20-50k unique jobs/month observed.

## 3. Proxy findings (your datasolved setup)

Tested all four on `ifconfig.me`, guest search API, job detail, company page:

| proxy | pattern | result |
|---|---|---|
| `datasolved-cf` | HTTP :10044 | ✅ all 200, 2-4s/req |
| `datasolved` (residential) | HTTP :10044 | ✅ all 200, 2-3s/req |
| `datasolved-country-us` | HTTP :10044 | ✅ all 200, 1-2s/req |
| `datasolved-sess-{id}` | HTTP :10044 | ✅ all 200, 2-5s/req |

All four CDN flavors returned exit IPs in their respective pools and **none triggered LinkedIn authwall/429** during tests. SOCKS5 (:10045) is also available for httpx-socks if HTTP proxying is undesirable.

**Recommendation**: use the rotating residential pool (`datasolved`) for scale, fall back to `-cf` if any batch hits 999/429. No need for the per-request sticky sessions for read-only crawling; sticky sessions matter only when we later log in.

**Rate observation**: 60 requests / ~7s from one residential IP all returned 200. Within the first 200-search-window the public endpoint tolerates far more than the "1 req/2s" folklore. Still, default pacing should be ~1 req/s per worker with jitter, and concurrency = N workers, not N pipelined requests.

## 4. Client design (httpx2 / curl_cffi)

No Playwright/Selenium needed.

- **HTTP client**: `httpx[http2]` or `curl_cffi.requests` (impersonate=chrome120). Either is fine; curl_cffi gives free TLS/JA3 fingerprint matching which makes the CF-less endpoints more durable.
- **Fingerprint**: fixed header order, `user-agent`, `accept`, `accept-language`, `sec-ch-ua*` triple, `upgrade-insecure-requests`. Mirror the browser cURLs the user pasted.
- **Session**: one `httpx.Client` per worker thread, with `http2=True` and `follow_redirects=True`.
- **Cookies**: none required to scrape anonymously. Sending a fresh `bcookie`/`lidc` per session is optional and only matters at login-attempt stage — skip for v1.
- **Retries**: exponential backoff (0.5×2^n, max 32s), retry on 429/5xx/timeout. Treat `start=400` as end-of-pagination, `403/999` as IP/user-agent throttled → rotate proxy and retry.

## 5. Work units & storage

Mirror the GMaps architecture:

- `jobs_search_queries` table — (keyword, location_slug, job_function, status, last_run, total_seen) populated from seeds (BPT sectors + LinkedIn's 14 functions × target cities).
- `linkedin_jobs` table — one row per job_id; columns: job_id PK, source_url, title, company_name, company_linkedin_url, company_id, location, listed_at, seniority_level, employment_type, job_function, industries, description_html, description_text, apply_url (when embedded), raw_json, inserted_at, updated_at. `ON CONFLICT (job_id) DO UPDATE` — mirrors `gmaps_listings`.
- `linkedin_companies` table — keyed by LinkedIn company slug/URL; holds the ld+json fields above + scraped_at.

Crawler shape:
1. `linkedin-search-daemon` — walks `jobs_search_queries`, hits the seeMoreJobPostings API page-by-page, extracts job_ids, pushes to Redis set `linkedin:jobs:pending` (dedup by job_id). Sleeps when <975 page is detected (query exhausted until refresh interval).
2. `linkedin-job-detail-daemon` — pops from `linkedin:jobs:pending`, fetches `jobPosting/{id}`, upserts into `linkedin_jobs`, extracts company slug, pushes to `linkedin:companies:pending`.
3. `linkedin-company-daemon` — pops company slugs, fetches `linkedin.com/company/{slug}`, upserts into `linkedin_companies`.

All daemons `systemctl --user` units, staggered with `After=network-online.target`, `EnvironmentFile=/run/media/growloop/codebase/infinitecrawler/.env`.

Because the public endpoints are fast (~2s/call) and 200/day caps **don't apply** (those were Places API caps — irrelevant here), even one worker at 1 rps can process ~86k requests/day. The realistic constraint is **query coverage**, not throughput.

## 6. What we deliberately rejected

- **Browser strategy** — unnecessary; all data is in server-rendered HTML.
- **Cookie-jar scraping** — the cURLs provided carry cookies but none are needed; we will not reuse those tokens (they're short-lived and tied to your session).
- **Infinite scroll / IntersectionObserver on /jobs/search page** — LinkedIn's logged-in page lazy-loads via the same `seeMoreJobPostings` call; calling it directly is strictly better.
- **GraphQL/voyager endpoints** — require login + CSRF token, fragile. Only useful if we ever need apply-links or competitor-of.

## 7. Build order (MVP → full)

1. **Spike** (1 day): single script, one (keyword, location), pagination loop, save job_ids + job details JSON to disk. Verify coverage vs manual browser.
2. **Pipeline**: add the three tables + three daemons; ship with `uv run python -m daemons.linkedin_search` etc.
3. **Enrichment**: company scraper, then optional cross-walk to your `gmaps_listings` for the lead-gen product (matching by name + city).
4. **Scaling**: add geo expansion (more BD cities + your 29 global cities), keyword thesaurus per sector, weekly re-crawl cron to catch new postings.

## 8. Open questions to answer before coding

- Target city list (just BD? + the 29 global cities?)
- Sector list: reuse `sectors.yaml` as seed keywords? Need a keyword→LinkedIn-job-function map.
- Expected volume / day target: decides worker count & proxy concurrency budget.
- Where in the product does a LinkedIn job signal surface? (pure B2C dataset, or join against gmaps_listings for the Bangladesh lead-gen corpus?)
