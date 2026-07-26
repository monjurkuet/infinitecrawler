"""utils/pg.py — PostgreSQL helper module for infinitecrawler.

Provides connection config, SQL query constants, and upsert helpers
for emails and LinkedIn profiles.
"""

import logging
import os


# ---------------------------------------------------------------------------
# Connection config
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
PG_DEFAULT_HOST = os.environ.get("PG_HOST", "")
PG_DEFAULT_PASSWORD = os.environ.get("PG_PASSWORD", "")
PG_DEFAULT_DB = os.environ.get("PG_DB", "infinitecrawler")


def get_pg_config() -> dict:
    """Return postgres connection kwargs from environment."""
    return {
        "host": PG_DEFAULT_HOST,
        "port": os.getenv("PG_PORT", "5432"),
        "dbname": PG_DEFAULT_DB,
        "user": os.getenv("PG_USER", "postgres"),
        "password": PG_DEFAULT_PASSWORD,
    }


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
        (listing_id, email, source_type, is_obfuscated, context_snippet)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (listing_id, email) DO UPDATE SET
        source_type       = EXCLUDED.source_type,
        context_snippet   = COALESCE(EXCLUDED.context_snippet, scraper.emails.context_snippet),
        discovered_at     = NOW()
"""


def upsert_emails(conn, emails: list[dict]) -> int:
    """Upsert email records into scraper.emails.

    Each dict must have keys: listing_id, email.
    Optional: source_type, is_obfuscated, context_snippet.
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
                    e["email"],
                    e.get("source_type", "http"),
                    e.get("is_obfuscated", False),
                    e.get("context_snippet"),
                ))
                written += cur.rowcount or 1
            except Exception as exc:
                logger.error("Failed to upsert email %s for listing %s: %s", e.get("email"), e.get("listing_id"), exc)
    conn.commit()
    return written


# ──────────────────────────────────────────────────────────────────────────
# LinkedIn profile extraction
# ──────────────────────────────────────────────────────────────────────────

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
            except Exception as exc:
                logger.error("Failed to upsert LinkedIn profile %s for listing %s: %s", p.get("profile_url"), p.get("listing_id"), exc)
    conn.commit()
    return written