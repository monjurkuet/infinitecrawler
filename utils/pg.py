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
