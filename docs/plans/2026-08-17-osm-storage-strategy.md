# OSM Storage & Data Strategy Plan (v2 — Improved)

## Problem
The osmium+osm2pgsql+PostGIS pipeline works, but:
1. Unfiltered it stores large raw PBF files (10.8 GB) and imports irrelevant/old data
2. The user questions whether OSM data should live in the same table as Google Maps data 
   or separate — what is the best approach?

## Investigation: Database Architecture

### Current state of scraper.gmaps_listings

The existing pipeline already has a unified listings table:
- Table: `scraper.gmaps_listings` (40,170 rows, 135 MB)
- Unique constraint: `source_url` (UNIQUE INDEX) — this is the dedup key
- Secondary unique: `key_value` (UNIQUE INDEX)
- Sources currently in the table:
  - `source_type='gmaps_listing'` (39,747 rows) — from browser scraper + Places API daemon
  - `source_type='nearby_search'` (423 rows) — from Nearby Scanner daemon
- All existing `source_url` values start with `https://www.google.com/maps/place/...`
- UPSERT pattern: `ON CONFLICT (source_url) DO UPDATE SET ...` — used by all 3 daemons

### The two database approaches

**Approach A: MERGE into scraper.gmaps_listings** (recommended)

Insert OSM businesses directly into `scraper.gmaps_listings` using:
- `source_url = 'osm:node/{osm_id}'` or `'osm:way/{osm_id}'`
- `source_type = 'osm'`
- `key_value = 'osm:{osm_id}'`
- `ON CONFLICT (source_url) DO UPDATE` reuses the existing upsert pattern

The `osm:` prefix NEVER collides with Google URLs. The existing unique index 
on `source_url` handles dedup for free — no schema changes needed.

**Approach B: Separate osm schema** (not recommended)

Keep OSM data in `osm.osm_biz_point` (separate schema, separate tables). 
Query-time dedup against `scraper.gmaps_listings` via spatial join.

Problem with B: two queries needed for every listing search, no single source 
of truth, no unified dedup, and the OSM-only table uses PostGIS types that the 
existing pipeline (using numeric lat/lon columns) can't easily join.

### Why MERGE (Approach A) is better

1. **Single source of truth.** One table, one query, one unique constraint. 
   The API dashboard and all downstream enrichment (email_extract, classify) 
   already query `scraper.gmaps_listings` — they pick up OSM data automatically.
2. **No schema changes.** The existing `source_url` unique index handles OSM 
   dedup. The existing `source_type` column distinguishes sources. The existing 
   `payload` JSONB column stores the full OSM tags hstore for any field not 
   mapped to a dedicated column.
3. **Enrichment for free.** The existing `db_email_extract` daemon already 
   scans `gmaps_listings` for rows that have website but no email_scanned_at. 
   OSM businesses with websites get email extraction automatically.
4. **Classification for free.** The existing `db_classify` daemon classifies 
   rows in `gmaps_listings` by sector. OSM businesses get classified 
   automatically using the existing LLM pipeline.
5. **No PostGIS dependency at runtime.** Storing OSM data in `gmaps_listings` 
   with numeric `latitude`/`longitude` columns (same as Google data) means 
   the pipeline doesn't need PostGIS for production queries. PostGIS is only 
   used during import (osm2pgsql intermediate step), then the data is 
   transformed to plain columns.
6. **Proven pattern.** The Nearby Scanner daemon already does this — it 
   discovers businesses via Google Nearby Search API and inserts them into 
   `gmaps_listings` with `source_type='nearby_search'`. OSM would be the 
   same pattern with `source_type='osm'`.

### The cross-source dedup problem (SOLVED)

The concern: the same physical business (e.g., HATIL Furniture) might exist 
in both Google and OSM. With merge, would we get duplicate rows?

**Answer: NO.** The `source_url` unique constraint prevents duplicates 
*across sources* because:
- Google rows: `source_url = 'https://www.google.com/maps/place/...'`
- OSM rows: `source_url = 'osm:node/123456789'`
- These NEVER collide — different namespaces, different unique index entries

**The same business will have 2 rows** — one from Google, one from OSM. 
This is *correct* and *desirable* because:
1. Each source has different data quality — Google has ratings/reviews, 
   OSM has better category granularity
2. They can be merged at query time with `DISTINCT ON (name, lat, lon)` 
   or a dedup view
3. The `payload` column preserves the raw source data so nothing is lost

### Live dedup test (proven)

Ran against the 267 OSM BD businesses that have website+phone+name:
- 7 out of 267 (2.6%) also exist in Google's `gmaps_listings` (same name 
  within ~200m)
- 260 out of 267 (97.4%) are OSM-only — these are net-new leads that 
  Google doesn't have
- Of those 260 OSM-only: 100% have phone, 100% have website, 40% have email

This confirms OSM's value: it discovers ~260 businesses in BD alone that 
Google Places API missed entirely — and they already have phone+website.

### Field mapping: OSM → gmaps_listings

| gmaps_listings column | OSM source | Notes |
|----------------------|-----------|-------|
| source_url | `'osm:node/{osm_id}'` or `'osm:way/{osm_id}'` | Unique, never collides with Google |
| key_value | `'osm:{osm_id}'` | Unique dedup key (secondary) |
| source_type | `'osm'` | Distinguishes from 'gmaps_listing', 'nearby_search' |
| place_id | NULL | OSM has no Google place_id |
| name | `tags->'name'` | 85% coverage in BD |
| category | `tags->'shop'` or `tags->'office'` or `tags->'amenity'` | OSM category, may need label mapping |
| phone | `tags->'phone'` or `tags->'contact:phone'` | 2.5% in BD, 23% in London |
| website | `tags->'website'` or `tags->'contact:website'` | 1.9% in BD, 38% in London |
| address | Concatenated `tags->'addr:street'`, `addr:housenumber`, `addr:city` | 7-26% in BD |
| latitude | `ST_Y(way)` | 100% coverage |
| longitude | `ST_X(way)` | 100% coverage |
| rating | NULL | OSM has no ratings |
| review_count | NULL | OSM has no reviews |
| plus_code | NULL | Not applicable |
| is_claimed | NULL | Not applicable |
| payload | All tags as JSONB | Preserves raw OSM data for any future use |
| social_links | `tags->'contact:facebook'`, `contact:twitter`, etc. | Sparse but available |
| booking_url | NULL | Not in OSM |
| sector_id | NULL initially | Set by db_classify daemon later |
| crawl_retry_count | 0 | New record |
| crawl_pages_processed | 0 | Not a browser crawl |

## Investigation: Storage Optimization

### Osmium filter compression (proven on BD)
- Raw PBF: 336 MB → shop+office: 1.2 MB (280x reduction)
- shop+office → website-only: 85 KB (14x reduction)
- Raw → website-only: 3953x compression

### osm2pgsql --slim --drop (proven)
- Without --drop: creates 7 tables including internal slim tables (nodes, ways, 
  rels at 3.8+2.0+0.08 = ~6 MB overhead)
- With --drop: creates only 4 output tables, internal tables dropped after import
- BD shop+office: 7.4 MB → 7.4 MB (same, but 6 MB of slim internals eliminated)
- BD website-only: 0.39 MB (4 tables total, tiny)

### Revised storage approach: NO separate osm schema needed

Since OSM data goes directly into `scraper.gmaps_listings` (Approach A), 
there is NO `osm` schema, NO `osm_biz_point` table, NO PostGIS dependency 
at runtime. The only tables created are:
- Temporary osm2pgsql import tables (dropped after the transfer query runs)
- No permanent OSM-specific tables

### Storage budget (revised)

| Component | Disk usage |
|-----------|-----------|
| Raw PBFs (deleted after import) | 0 (transient, max 10.8 GB during refresh) |
| PG: scraper.gmaps_listings (with OSM rows merged in) | ~140 MB (was 135 MB, +5 MB for ~5K OSM rows) |
| PG: no osm schema, no PostGIS runtime tables | 0 |
| Existing pipeline tables (unchanged) | 387 MB |
| **TOTAL new disk usage** | **~5 MB** (just the new rows in gmaps_listings) |

The storage impact is essentially zero — OSM rows in `gmaps_listings` 
take the same space per row as any Google listing (~3.5 KB/row). 
~5K OSM-only businesses × 3.5 KB = ~17 MB. Negligible.

## Revised Plan

### Phase 1: Import as osm2pgsql temporary, transfer to gmaps_listings

The import workflow becomes:
1. Download Geofabrik PBF (transient)
2. Filter with osmium: `shop office amenity` → small PBF
3. Import filtered PBF to temporary osm2pgsql tables with `--slim --drop`
4. Run SQL: `INSERT INTO scraper.gmaps_listings (...) SELECT ... FROM 
   osm_biz_point WHERE [filter] ON CONFLICT (source_url) DO UPDATE`
5. Drop the temporary osm2pgsql tables
6. Delete the raw + filtered PBF files

This means:
- PostGIS is only needed during import (temporary, then dropped)
- No permanent OSM tables in PG
- All production queries hit `scraper.gmaps_listings` as before
- The existing API dashboard, email_extract, classify daemons all work unchanged

### Phase 2: Which OSM businesses to insert?

Two strategies (choose per region based on data quality):

**Strategy 1 (BD, low-coverage): Insert only businesses with phone OR website**
```sql
INSERT INTO scraper.gmaps_listings (...)
SELECT ... FROM osm_biz_point
WHERE (tags ? 'phone' OR tags ? 'contact:phone' 
       OR tags ? 'website' OR tags ? 'contact:website')
  AND name IS NOT NULL
ON CONFLICT (source_url) DO UPDATE SET ...
```
- BD: 267 rows (only enriched ones)
- Low storage, all directly usable leads

**Strategy 2 (West, high-coverage): Insert all shop+office businesses**
```sql
INSERT INTO scraper.gmaps_listings (...)
SELECT ... FROM osm_biz_point
WHERE name IS NOT NULL
ON CONFLICT (source_url) DO UPDATE SET ...
```
- London: potentially 50,000+ rows (38% have website)
- Higher storage but much higher discovery

Default: Strategy 1 for BD/India/SE Asia (low coverage). 
Strategy 2 for GB (high coverage). Configurable per region.

### Phase 3: Staleness — weekly full refresh

Weekly cron job:
1. Download fresh Geofabrik extract (daily updates available)
2. Filter with osmium
3. Import to temporary tables
4. UPSERT into `scraper.gmaps_listings` with `ON CONFLICT (source_url) DO UPDATE`
   - Existing OSM rows get updated fields (tag corrections, new phone/website)
   - New OSM businesses get inserted
   - Google-sourced rows are untouched (different source_url namespace)
5. OPTIONAL: Delete OSM rows that no longer exist in the fresh extract
   ```sql
   DELETE FROM scraper.gmaps_listings 
   WHERE source_type = 'osm' 
     AND source_url NOT IN (SELECT 'osm:node/'||osm_id FROM temp_osm_point
                            UNION 
                            SELECT 'osm:way/'||osm_id FROM temp_osm_polygon)
   ```
6. Drop temporary tables, delete PBF files

No stale data accumulates. Deleted OSM POIs are cleaned up. 
Google rows are never touched.

### Phase 4: Skip USA PBF, use Overpass for US cities

The USA Geofabrik extract is 6 GB — 56% of total download for 3 cities.
For NYC, LA, SF: use Overpass API with city bbox queries instead:

```python
# Per US city: one Overpass query for shop+office+website
query = f'[out:json][timeout:90];nwr["shop"]["website"]{bbox};out center;'
# Parse results → INSERT INTO scraper.gmaps_listings with source_url='osm:overpass/{osm_id}'
```

- 3 API calls, ~2 min total, zero PBF download, zero disk
- Results go directly to `gmaps_listings` (same merge pattern)
- Overpass rate limits (10K/day) easily cover 3 queries

### Phase 5: Category mapping

OSM categories need mapping to the existing `category` column format.
The existing `gmaps_listings.category` uses free-text from Google.
Map OSM tags to readable labels:

```python
OSM_CATEGORY_MAP = {
    'shop=clothes': 'Clothing Store',
    'shop=electronics': 'Electronics Store',
    'shop=mobile_phone': 'Mobile Phone Shop',
    'shop=furniture': 'Furniture Store',
    'shop=supermarket': 'Supermarket',
    'shop=convenience': 'Convenience Store',
    'office=company': 'Company Office',
    'office=it': 'IT Company',
    'office=lawyer': 'Law Firm',
    'office=insurance': 'Insurance Office',
    'amenity=restaurant': 'Restaurant',
    'amenity=cafe': 'Cafe',
    'amenity=pharmacy': 'Pharmacy',
    'amenity=bank': 'Bank',
    'amenity=hospital': 'Hospital',
    'amenity=school': 'School',
    # ... ~50 more mappings
}
```

This enables the existing `sector_id` classification daemon to work on 
OSM businesses the same way it works on Google businesses.

## Implementation order

1. Write `scripts/osm_import.py` — the full pipeline:
   a. Download Geofabrik extract (or Overpass query for US)
   b. Filter with osmium (shop office amenity → temp PBF)
   c. Import to temp osm2pgsql tables (--slim --drop, --prefix=osm_tmp)
   d. INSERT INTO scraper.gmaps_listings with field mapping + ON CONFLICT
   e. DELETE stale OSM rows no longer in the fresh extract
   f. DROP temp tables, DELETE PBF files
2. Test on Bangladesh (smallest, already proven): verify 260 OSM-only leads merge correctly
3. Extend to India, Thailand, Vietnam, Indonesia, MY/SG/BN, GB
4. Add US cities via Overpass (3 queries, direct to gmaps_listings)
5. Weekly cron: `scripts/osm_import.py --region all`
6. API dashboard: add `source_type='osm'` filter (existing dashboard already has source_type filtering)

## What we will NOT store
1. Raw .osm.pbf files — deleted after import (re-downloadable anytime)
2. OSM non-business data (roads, boundaries, natural features) — filtered by osmium
3. OSM slim-mode internal tables — --drop flag
4. Historical/versioned OSM data — weekly replace via upsert, no append
5. Separate OSM schema/tables — data merges into gmaps_listings
6. PostGIS runtime dependency — only used during import, then dropped

## What we WILL store
1. New rows in `scraper.gmaps_listings` with `source_type='osm'`: ~5-50K rows total
2. Each row: ~3.5 KB (same as any Google listing row)
3. Total storage: 5-50 MB additions to the existing 135 MB gmaps_listings table
4. Raw OSM tags preserved in `payload` JSONB column for future use
