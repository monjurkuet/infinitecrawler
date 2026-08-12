"""utils/pg.py — PostgreSQL helper module for infinitecrawler.

Provides connection config, SQL query constants, and upsert helpers
for emails and LinkedIn profiles.
"""

import logging
import os
from typing import Optional

import psycopg


# ---------------------------------------------------------------------------
# Connection config
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
PG_DEFAULT_HOST = os.environ.get("PG_HOST", "")
PG_DEFAULT_PASSWORD = os.environ.get("PG_PASSWORD", "")
PG_DEFAULT_DB = os.environ.get("PG_DB", "infinitecrawler")


def get_pg_config(include_timeouts: bool = True) -> dict:
    """Return postgres connection kwargs from environment.

    When `include_timeouts=True` (default) the returned dict carries the
    T3 session-hardening options.  psycopg3 does NOT accept libpq GUC names
    as direct kwargs, so the timeouts are packed into a single `options=`
    string with `-c` flags — exactly what libpq expects.
    """
    cfg = {
        "host": PG_DEFAULT_HOST,
        "port": os.getenv("PG_PORT", "5432"),
        "dbname": PG_DEFAULT_DB,
        "user": os.getenv("PG_USER", "postgres"),
        "password": PG_DEFAULT_PASSWORD,
    }
    if include_timeouts:
        cfg["options"] = (
            f"-c idle_in_transaction_session_timeout={PG_IDLE_TX_TIMEOUT} "
            f"-c statement_timeout={PG_STATEMENT_TIMEOUT} "
            f"-c lock_timeout={PG_LOCK_TIMEOUT}"
        )
    return cfg


# ──────────────────────────────────────────────────────────────────────────
# Async DSN builder (T3 — PG session hardening)
# ──────────────────────────────────────────────────────────────────────────
#
# Defaults lifted from the rollout plan. Env overrides:
#   PG_IDLE_TX_TIMEOUT  (default 30s)
#   PG_STATEMENT_TIMEOUT (default 120s)
#   PG_LOCK_TIMEOUT     (default 10s)
#
# `idle_in_transaction_session_timeout` aborts any backend that sits in
# `idle in transaction` longer than the threshold — this was the root cause
# of the slow "idle in transaction" PG backends observed in the health report.
# `statement_timeout` and `lock_timeout` bound the worst-case latency a single
# runaway query can inflict on the pool.

PG_IDLE_TX_TIMEOUT = os.getenv("PG_IDLE_TX_TIMEOUT", "30s")
PG_STATEMENT_TIMEOUT = os.getenv("PG_STATEMENT_TIMEOUT", "120s")
PG_LOCK_TIMEOUT = os.getenv("PG_LOCK_TIMEOUT", "10s")


def build_async_dsn() -> str:
    """Return a libpq DSN string suitable for psycopg3 AsyncConnectionPool.

    Honors the unix-socket quirk documented in api.services.pg_service:
    when PG_HOST contains a `/` (unix socket), the port parameter is omitted
    so psycopg3 doesn't misparse it as a hostname.

    Note: psycopg3 ignores bare `key=value` pairs in DSN strings unless they
    are standard libpq options (host, port, dbname, user, password).  All
    session-level GUCs must be wrapped in `options=...`.  We use the
    `connect_timeout` kwarg path on the pool itself for connect-level limits.
    """
    cfg = get_pg_config()
    host = cfg["host"]
    port = cfg["port"]
    user = cfg["user"]
    password = cfg["password"]
    dbname = cfg["dbname"]

    if "/" in host:
        base = f"host={host} user={user} password={password} dbname={dbname}"
    else:
        base = f"host={host} port={port} user={user} password={password} dbname={dbname}"

    options = (
        f"-c idle_in_transaction_session_timeout={PG_IDLE_TX_TIMEOUT} "
        f"-c statement_timeout={PG_STATEMENT_TIMEOUT} "
        f"-c lock_timeout={PG_LOCK_TIMEOUT}"
    )
    return f"{base} options='{options}'"


# ──────────────────────────────────────────────────────────────────────────
# Queries for uncrawled / unprocessed listing rows
# ──────────────────────────────────────────────────────────────────────────


def get_uncrawled_count_sql() -> str:
    """SQL for counting uncrawled URLs (strips ORDER BY for efficiency).

    Used by get_uncrawled_count() and monitor_pipeline.py where we only need
    the count, not the sorted list.
    """
    return """
        SELECT COUNT(DISTINCT sr.payload->>'url')
        FROM scraper.gmaps_search_results sr
        LEFT JOIN scraper.gmaps_listings gl
          ON gl.source_url = sr.payload->>'url'
        WHERE sr.payload->>'url' IS NOT NULL
          AND gl.source_url IS NULL
    """


def get_uncrawled_count(conn) -> int:
    """Return the number of uncrawled URLs (search results not yet extracted)."""
    with conn.cursor() as cur:
        cur.execute(get_uncrawled_count_sql())
        return cur.fetchone()[0] or 0


def get_uncrawled_urls_sql(limit: int = 100) -> str:
    """Return SQL query string for listings without detail extraction."""
    return f"""
        SELECT sr.payload->>'url' as url, sr.id
        FROM scraper.gmaps_search_results sr
        LEFT JOIN scraper.gmaps_listings gl
          ON gl.source_url = sr.payload->>'url'
        WHERE sr.payload->>'url' IS NOT NULL
          AND gl.source_url IS NULL
        ORDER BY sr.updated_at DESC
        LIMIT {limit}
    """


# ──────────────────────────────────────────────────────────────────────────
# Email extraction
# ──────────────────────────────────────────────────────────────────────────

UPSERT_EMAIL_SQL = """
    INSERT INTO scraper.emails
        (listing_id, email, extraction_method, is_obfuscated, context_snippet)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (listing_id, email) DO UPDATE SET
        extraction_method   = EXCLUDED.extraction_method,
        context_snippet     = COALESCE(EXCLUDED.context_snippet, scraper.emails.context_snippet),
        discovered_at       = NOW()
"""


def upsert_emails(conn, emails: list[dict]) -> int:
    """Upsert email records into scraper.emails.

    Each dict must have keys: listing_id, email.
    Optional: extraction_method, is_obfuscated, context_snippet.
    Returns number of rows written.
    """
    if not emails:
        return 0
    written = 0
    with conn.cursor() as cur:
        for e in emails:
            try:
                cur.execute("SAVEPOINT sv_row")
                cur.execute(UPSERT_EMAIL_SQL, (
                    e["listing_id"],
                    e["email"],
                    e.get("extraction_method", "http"),
                    e.get("is_obfuscated", False),
                    e.get("context_snippet"),
                ))
                written += cur.rowcount or 1
                cur.execute("RELEASE SAVEPOINT sv_row")
            except Exception as exc:
                logger.error("Failed to upsert email %s for listing %s: %s", e.get("email"), e.get("listing_id"), exc)
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT sv_row")
                except Exception:
                    pass
    for attempt in range(3):
        try:
            conn.commit()
            return written
        except (psycopg.OperationalError, ConnectionError):
            # sync retry; caller is non-async
            if attempt < 2:
                import time
                time.sleep(0.5 * (attempt + 1))
                continue
            logger.error("upsert_emails: commit failed after 3 attempts")
            return 0


# ──────────────────────────────────────────────────────────────────────────
# LinkedIn profile extraction
# ──────────────────────────────────────────────────────────────────────────

UPSERT_LINKEDIN_SQL = """
    INSERT INTO scraper.linkedin_profiles
        (listing_id, full_name, profile_url, profile_title, company_name,
         search_query, confidence, snippet, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (profile_url) DO UPDATE SET
        listing_id    = EXCLUDED.listing_id,
        profile_title = COALESCE(EXCLUDED.profile_title, scraper.linkedin_profiles.profile_title),
        confidence    = GREATEST(scraper.linkedin_profiles.confidence, EXCLUDED.confidence),
        checked_at    = NOW(),
        last_updated  = NOW()
"""


FETCH_UNPROCESSED_EMAILS_SQL = r"""
    SELECT l.id, l.website
    FROM scraper.gmaps_listings l
    WHERE l.website IS NOT NULL
      AND l.website != ''
      AND (
            l.email_scanned_at IS NULL
         OR l.email_scanned_at < NOW() - INTERVAL '14 days'
      )
    ORDER BY COALESCE(l.email_scanned_at, '1970-01-01'::timestamptz) ASC,
             l.updated_at DESC
"""


def get_unprocessed_emails(conn, limit: int = 100) -> list[dict]:
    """Return listings needing an email re-scan.

    Listings with no prior scan (`email_scanned_at IS NULL`) or whose last scan
    is older than 14 days. A site is re-scanned periodically so newly published
    contact pages are caught; the 14-day window prevents infinite re-fetches of
    sites that have no email on any of the paths we crawl.
    """
    with conn.cursor() as cur:
        cur.execute(FETCH_UNPROCESSED_EMAILS_SQL + " LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [{"id": r[0], "website": r[1]} for r in rows]


MARK_EMAIL_SCANNED_SQL = """
    UPDATE scraper.gmaps_listings
       SET email_scanned_at = NOW()
     WHERE id = ANY(%s)
"""


def mark_listings_email_scanned(conn, listing_ids: list[int]) -> int:
    """Stamp `email_scanned_at = NOW()` on the given listings.

    Called by `db_email_extract` after each batch — even when no emails were
    found — so the perpetual loop does not re-fetch the same zero-email
    listings on the next 30s cycle. Without this, the loop refetches the same
    1000 listings forever.
    """
    if not listing_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(MARK_EMAIL_SCANNED_SQL, (listing_ids,))
        n = cur.rowcount
    conn.commit()
    return n


FETCH_ALL_LISTINGS_WITH_WEBSITE_SQL = r"""
    SELECT l.id, l.website
    FROM scraper.gmaps_listings l
    WHERE l.website IS NOT NULL
      AND l.website != ''
    ORDER BY l.updated_at DESC
"""


def get_all_listings_with_website(conn, limit: int = 100) -> list[dict]:
    """T6 — return every listing with a website, ignoring email history.

    Used by `--force-rescan` so the email backlog drainer can re-scan sites
    that already have a row in scraper.emails (and may have a new contact
    address since).
    """
    with conn.cursor() as cur:
        cur.execute(FETCH_ALL_LISTINGS_WITH_WEBSITE_SQL + " LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [{"id": r[0], "website": r[1]} for r in rows]


FETCH_UNPROCESSED_LINKEDIN_SQL = """
    SELECT l.id, l.name
    FROM scraper.gmaps_listings l
    WHERE l.name IS NOT NULL
      AND l.name != ''
      AND NOT EXISTS (
          SELECT 1 FROM scraper.linkedin_profiles p
          WHERE p.listing_id = l.id
            AND p.checked_at > NOW() - INTERVAL '7 days'
      )
    ORDER BY l.updated_at DESC
"""


def get_unprocessed_linkedin(conn, limit: int = 50) -> list[dict]:
    """Return listings not searched for LinkedIn in the last 7 days."""
    with conn.cursor() as cur:
        cur.execute(FETCH_UNPROCESSED_LINKEDIN_SQL + " LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [{"id": r[0], "name": r[1]} for r in rows]


def upsert_linkedin_profiles(conn, profiles: list[dict], source: str = "linkedin_search") -> int:
    """Upsert LinkedIn profile records into scraper.linkedin_profiles.

    Each dict must have: listing_id, profile_url, company_name, search_query.
    Optional: full_name, profile_title, confidence, snippet.
    source: value for the source column (default 'linkedin_search' for
            backward compatibility with db_linkedin_search.py).
    Returns number of rows written.
    """
    if not profiles:
        return 0
    written = 0
    with conn.cursor() as cur:
        for p in profiles:
            try:
                cur.execute("SAVEPOINT sv_row")
                cur.execute(UPSERT_LINKEDIN_SQL, (
                    p["listing_id"],
                    p.get("full_name"),
                    p["profile_url"],
                    p.get("profile_title"),
                    p["company_name"],
                    p["search_query"],
                    p.get("confidence", 0.5),
                    p.get("snippet"),
                    source,
                ))
                written += cur.rowcount or 1
                cur.execute("RELEASE SAVEPOINT sv_row")
            except Exception as exc:
                logger.error("Failed to upsert LinkedIn profile %s for listing %s: %s", p.get("profile_url"), p.get("listing_id"), exc)
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT sv_row")
                except Exception:
                    pass
    conn.commit()
    return written


# ---------------------------------------------------------------------------
# LinkedIn company enrichment (T1.2 / T1.3)
# ---------------------------------------------------------------------------

UPSERT_LINKEDIN_COMPANY_SQL = """
    INSERT INTO scraper.linkedin_companies
        (company_name_norm, company_name, slug, linkedin_url, industry,
         company_size, employee_count, followers, headquarters, website,
         founded, specialties, description, logo_url,
         last_checked_at, attempts, notes)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 1, %s)
    ON CONFLICT (company_name_norm) DO UPDATE SET
        slug              = COALESCE(EXCLUDED.slug,              scraper.linkedin_companies.slug),
        linkedin_url      = COALESCE(EXCLUDED.linkedin_url,      scraper.linkedin_companies.linkedin_url),
        industry          = COALESCE(EXCLUDED.industry,          scraper.linkedin_companies.industry),
        company_size      = COALESCE(EXCLUDED.company_size,      scraper.linkedin_companies.company_size),
        employee_count    = COALESCE(EXCLUDED.employee_count,    scraper.linkedin_companies.employee_count),
        followers         = COALESCE(EXCLUDED.followers,         scraper.linkedin_companies.followers),
        headquarters      = COALESCE(EXCLUDED.headquarters,      scraper.linkedin_companies.headquarters),
        website           = COALESCE(EXCLUDED.website,           scraper.linkedin_companies.website),
        founded           = COALESCE(EXCLUDED.founded,           scraper.linkedin_companies.founded),
        specialties       = COALESCE(EXCLUDED.specialties,       scraper.linkedin_companies.specialties),
        description       = COALESCE(EXCLUDED.description,       scraper.linkedin_companies.description),
        logo_url          = COALESCE(EXCLUDED.logo_url,          scraper.linkedin_companies.logo_url),
        last_checked_at   = NOW(),
        attempts          = scraper.linkedin_companies.attempts + 1
"""


def upsert_linkedin_company(conn, c: dict) -> int:
    """Upsert one row into scraper.linkedin_companies. Key = normalized company name."""
    cur = conn.cursor()
    cur.execute(UPSERT_LINKEDIN_COMPANY_SQL, (
        c["company_name_norm"],
        c["company_name"],
        c.get("slug"),
        c.get("linkedin_url"),
        c.get("industry"),
        c.get("company_size"),
        c.get("employee_count"),
        c.get("followers"),
        c.get("headquarters"),
        c.get("website"),
        c.get("founded"),
        c.get("specialties"),
        c.get("description"),
        c.get("logo_url"),
        c.get("notes"),
    ))
    written = cur.rowcount or 1
    conn.commit()
    return written


MARK_COMPANY_ATTEMPTED_SQL = """
    INSERT INTO scraper.linkedin_companies (company_name_norm, company_name, last_attempted_at, attempts)
    VALUES (%s, %s, NOW(), 1)
    ON CONFLICT (company_name_norm) DO UPDATE SET
        last_attempted_at = NOW(),
        attempts          = scraper.linkedin_companies.attempts + 1
"""


def mark_company_attempted(conn, company_name: str, company_name_norm: str) -> None:
    """Bump `last_attempted_at` even when the scraper returned nothing.

    Prevents the enrichment loop from hammering a slug that consistently 404s.
    """
    cur = conn.cursor()
    cur.execute(MARK_COMPANY_ATTEMPTED_SQL, (company_name_norm, company_name))
    conn.commit()


FETCH_COMPANIES_TO_ENRICH_SQL = r"""
    SELECT all_co.company_name AS company_name, COUNT(*) AS n
      FROM (
          SELECT DISTINCT company_name
            FROM scraper.linkedin_profiles
           WHERE company_name IS NOT NULL AND company_name != ''
          UNION
          SELECT DISTINCT name AS company_name
            FROM scraper.gmaps_listings
           WHERE name IS NOT NULL AND name != ''
      ) all_co
      LEFT JOIN scraper.linkedin_companies lc
        ON lc.company_name_norm = lower(regexp_replace(all_co.company_name, '\s+', ' ', 'g'))
     WHERE lc.company_name_norm IS NULL
        OR lc.last_checked_at  IS NULL
        OR lc.last_checked_at  < NOW() - INTERVAL '30 days'
     GROUP BY all_co.company_name
     ORDER BY n DESC
"""


def get_companies_to_enrich(conn, limit: int = 200) -> list[dict]:
    """Return distinct company names still needing (re-)enrichment.

    Sources are BOTH:
      - `linkedin_profiles.company_name` (companies we discovered employees at)
      - `gmaps_listings.name`            (businesses we crawled)

    A company is eligible if its `linkedin_companies` row is missing OR its
    `last_checked_at` is older than 30 days. Order = source frequency (most
    cited first), so we enrich the high-value targets before one-offs.
    """
    with conn.cursor() as cur:
        cur.execute(FETCH_COMPANIES_TO_ENRICH_SQL + " LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [{"company_name": r[0], "occurrences": r[1]} for r in rows]


# ---------------------------------------------------------------------------
# LinkedIn profile snippet backfill (T1.1)
# ---------------------------------------------------------------------------

FETCH_PROFILES_TO_BACKFILL_SQL = r"""
    SELECT profile_url, snippet, profile_title
      FROM scraper.linkedin_profiles
     WHERE (enriched_at IS NULL
            OR enriched_at < NOW() - INTERVAL '60 days')
       AND snippet IS NOT NULL AND snippet != ''
"""


def get_profiles_to_backfill(conn, limit: int = 500) -> list[dict]:
    """Profiles whose snippet can be re-parsed for location/connections/headline."""
    with conn.cursor() as cur:
        cur.execute(FETCH_PROFILES_TO_BACKFILL_SQL + " LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [{"profile_url": r[0], "snippet": r[1], "profile_title": r[2]} for r in rows]


BACKFILL_PROFILE_ENRICHMENT_SQL = """
    UPDATE scraper.linkedin_profiles
       SET profile_location = COALESCE(%s, profile_location),
           profile_country  = COALESCE(%s, profile_country),
           connections_count= COALESCE(%s, connections_count),
           headline         = COALESCE(%s, headline),
           enriched_at      = NOW()
     WHERE profile_url = %s
"""


def update_profile_enrichment(
    conn,
    profile_url: str,
    *,
    profile_location: Optional[str] = None,
    profile_country: Optional[str] = None,
    connections_count: Optional[str] = None,
    headline: Optional[str] = None,
) -> int:
    """Apply snippet-parsed enrichment fields to a single profile."""
    cur = conn.cursor()
    cur.execute(BACKFILL_PROFILE_ENRICHMENT_SQL, (
        profile_location, profile_country, connections_count, headline, profile_url,
    ))
    n = cur.rowcount or 0
    conn.commit()
    return n