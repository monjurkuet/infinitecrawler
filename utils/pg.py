"""Centralized PostgreSQL connection config — single source of truth.

All env-wrapped PG connection dicts should import ``get_pg_config()`` instead
of re-declaring the host/password defaults. Changing the defaults here updates
every caller at once.
"""

import os


def get_pg_config() -> dict:
    """Return a psycopg-compatible connection config dict from env vars."""
    return {
        "host": os.environ.get("POSTGRESQL_HOST", "100.92.181.21"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "user": os.environ.get("POSTGRES_USERNAME", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "changeme"),
        "dbname": os.environ.get("POSTGRES_DB", "infinitecrawler"),
    }

# Exported defaults so subprocess callers (monitor) don't duplicate them.
PG_DEFAULT_HOST = os.environ.get("POSTGRESQL_HOST", "100.92.181.21")
PG_DEFAULT_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")
PG_DEFAULT_DB = os.environ.get("POSTGRES_DB", "infinitecrawler")

# ── Queries ──────────────────────────────────────────────────────────

UNCRAWLED_URLS_SQL = """
    SELECT DISTINCT sr.payload->>'url' AS source_url
    FROM scraper.gmaps_search_results sr
    LEFT JOIN scraper.gmaps_listings gl
      ON gl.source_url = sr.payload->>'url'
    WHERE sr.payload->>'url' IS NOT NULL
      AND gl.source_url IS NULL
    ORDER BY source_url
"""


def get_uncrawled_urls_sql(limit: int | None = None) -> tuple[str, tuple]:
    """Return parameterized SQL + params for uncrawled listing URLs.

    The query finds search-result URLs that have not yet been deep-extracted
    into ``scraper.gmaps_listings``.  Passing ``limit`` appends ``LIMIT %s``.
    """
    sql = UNCRAWLED_URLS_SQL
    if limit is not None:
        sql += "\n    LIMIT %s"
        return sql, (limit,)
    return sql, ()


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


# ══════════════════════════════════════════════════════════════════════════════
# Email & LinkedIn enrichment helpers
# ══════════════════════════════════════════════════════════════════════════════

UPSERT_EMAIL_SQL = """
    INSERT INTO scraper.emails
        (listing_id, website_url, email, email_type, extraction_method,
         is_obfuscated, context_snippet)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (listing_id, email) DO UPDATE SET
        extraction_method = EXCLUDED.extraction_method,
        context_snippet   = COALESCE(EXCLUDED.context_snippet, scraper.emails.context_snippet),
        discovered_at     = NOW()
"""


def upsert_emails(conn, emails: list[dict]) -> int:
    """Upsert email records into scraper.emails.

    Each dict must have keys: listing_id, website_url, email.
    Optional: email_type, extraction_method, is_obfuscated, context_snippet.
    Returns number of rows written.
    """
    if not emails:
        return 0
    written = 0
    with conn.cursor() as cur:
        for e in emails:
            try:
                cur.execute(UPSERT_EMAIL_SQL, (
                    e["listing_id"],
                    e.get("website_url", ""),
                    e["email"],
                    e.get("email_type", "general"),
                    e.get("extraction_method", "browser"),
                    e.get("is_obfuscated", False),
                    e.get("context_snippet"),
                ))
                written += cur.rowcount or 1
            except Exception:
                pass
    conn.commit()
    return written


UPSERT_LINKEDIN_SQL = """
    INSERT INTO scraper.linkedin_profiles
        (listing_id, full_name, profile_url, profile_title, company_name,
         search_query, confidence, snippet)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (profile_url) DO UPDATE SET
        listing_id    = EXCLUDED.listing_id,
        profile_title = COALESCE(EXCLUDED.profile_title, scraper.linkedin_profiles.profile_title),
        confidence    = GREATEST(scraper.linkedin_profiles.confidence, EXCLUDED.confidence),
        last_updated  = NOW()
"""


def upsert_linkedin_profiles(conn, profiles: list[dict]) -> int:
    """Upsert LinkedIn profile records into scraper.linkedin_profiles.

    Each dict must have: listing_id, profile_url, company_name, search_query.
    Optional: full_name, profile_title, confidence, snippet.
    Returns number of rows written.
    """
    if not profiles:
        return 0
    written = 0
    with conn.cursor() as cur:
        for p in profiles:
            try:
                cur.execute(UPSERT_LINKEDIN_SQL, (
                    p["listing_id"],
                    p.get("full_name"),
                    p["profile_url"],
                    p.get("profile_title"),
                    p["company_name"],
                    p["search_query"],
                    p.get("confidence", 0.5),
                    p.get("snippet"),
                ))
                written += cur.rowcount or 1
            except Exception:
                pass
    conn.commit()
    return written


FETCH_UNPROCESSED_EMAILS_SQL = """
    SELECT l.id, l.website
    FROM scraper.gmaps_listings l
    WHERE l.website IS NOT NULL
      AND l.website != ''
      AND l.id NOT IN (SELECT listing_id FROM scraper.emails)
    ORDER BY l.updated_at DESC
"""


def get_unprocessed_emails(conn, limit: int = 100) -> list[dict]:
    """Return listings that have a website but no emails extracted yet."""
    with conn.cursor() as cur:
        cur.execute(FETCH_UNPROCESSED_EMAILS_SQL + " LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [{"id": r[0], "website": r[1]} for r in rows]


FETCH_UNPROCESSED_LINKEDIN_SQL = """
    SELECT l.id, l.name
    FROM scraper.gmaps_listings l
    WHERE l.name IS NOT NULL
      AND l.name != ''
      AND l.id NOT IN (
          SELECT listing_id FROM scraper.linkedin_profiles
          WHERE checked_at > NOW() - INTERVAL '7 days'
      )
    ORDER BY l.updated_at DESC
"""


def get_unprocessed_linkedin(conn, limit: int = 50) -> list[dict]:
    """Return listings not searched for LinkedIn in the last 7 days."""
    with conn.cursor() as cur:
        cur.execute(FETCH_UNPROCESSED_LINKEDIN_SQL + " LIMIT %s", (limit,))
        rows = cur.fetchall()
    return [{"id": r[0], "name": r[1]} for r in rows]