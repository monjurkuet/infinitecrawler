"""Input strategy for loading uncrawled Google Maps listing URLs from PostgreSQL."""

import logging
import os
from typing import Iterator, Optional

from psycopg import connect
from psycopg import sql

from base.strategies import InputStrategy


class PostgreSQLUncrawledInputStrategy(InputStrategy):
    """Load listing URLs that exist in search results but not in listings."""

    def __init__(self, config: dict):
        # config is the input section from the main config
        # e.g., {strategy: "postgresql_uncrawled_gmaps", config: {...}}
        self.config = config.get("config", {})
        self.schema = self.config.get("schema", "scraper")
        self.search_results_table = self.config.get(
            "search_results_table", "gmaps_search_results"
        )
        self.listings_table = self.config.get("listings_table", "gmaps_listings")
        self.source_url_field = self.config.get("source_url_field", "source_url")
        self.batch_size = self.config.get("batch_size", 1000)
        self.max_urls = self.config.get("max_urls")
        self.logger = logging.getLogger(self.__class__.__name__)
        self._total_count: Optional[int] = None
        self._connection = None
        self._connect()

    def _resolve_setting(
        self,
        config_key: str,
        env_keys: tuple[str, ...],
        default: Optional[str] = None,
    ) -> str:
        value = self.config.get(config_key)
        if value not in (None, ""):
            return value

        for env_key in env_keys:
            env_value = os.getenv(env_key)
            if env_value not in (None, ""):
                return env_value

        if default is not None:
            self.logger.info(f"No {config_key} configured; defaulting to {default}")
            return default

        return ""

    def _connect(self):
        try:
            host = self._resolve_setting(
                "host", ("POSTGRESQL_HOST", "POSTGRES_HOST"), "localhost"
            )
            port = int(self._resolve_setting("port", ("POSTGRES_PORT",), "5432"))
            user = self._resolve_setting(
                "user", ("POSTGRES_USERNAME", "POSTGRES_USER"), "postgres"
            )
            password = self._resolve_setting("password", ("POSTGRES_PASSWORD",), "")
            database = self._resolve_setting(
                "database",
                ("POSTGRES_DB", "POSTGRES_DATABASE"),
                "infinitecrawler",
            )

            self._connection = connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=database,
            )
            self._connection.autocommit = True
            self.logger.info(
                f"Connected to PostgreSQL for uncrawled URLs: {database}.{self.schema}"
            )
        except ImportError:
            self.logger.error("psycopg not installed. Run: uv sync")
            raise
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _source_url_query(self, count_only: bool = False):
        source_url_expr = sql.SQL("sr.payload->>") + sql.Literal(self.source_url_field)
        search_results = sql.Identifier(self.schema, self.search_results_table)
        listings = sql.Identifier(self.schema, self.listings_table)

        if count_only:
            return sql.SQL(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT {source_url_expr} AS source_url
                    FROM {search_results} sr
                    LEFT JOIN {listings} gl
                      ON gl.source_url = {source_url_expr}
                    WHERE {source_url_expr} IS NOT NULL
                      AND gl.source_url IS NULL
                ) uncrawled
                """
            ).format(
                source_url_expr=source_url_expr,
                search_results=search_results,
                listings=listings,
            )

        return sql.SQL(
            """
            SELECT DISTINCT {source_url_expr} AS source_url
            FROM {search_results} sr
            LEFT JOIN {listings} gl
              ON gl.source_url = {source_url_expr}
            WHERE {source_url_expr} IS NOT NULL
              AND gl.source_url IS NULL
            ORDER BY source_url
            """
        ).format(
            source_url_expr=source_url_expr,
            search_results=search_results,
            listings=listings,
        )

    def load_urls(self) -> Iterator[str]:
        if not self._connection:
            raise RuntimeError("PostgreSQL connection not initialized")

        self.logger.info(
            "Loading uncrawled listing URLs from PostgreSQL search results"
        )

        query = self._source_url_query(count_only=False)

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query)
                yielded = 0
                while True:
                    rows = cursor.fetchmany(self.batch_size)
                    if not rows:
                        break
                    for (url,) in rows:
                        if url:
                            yield url
                            yielded += 1
                            if self.max_urls and yielded >= self.max_urls:
                                return
        except Exception as e:
            self.logger.error(f"Error loading uncrawled URLs from PostgreSQL: {e}")
            raise

    def get_total_count(self) -> Optional[int]:
        if self._total_count is not None:
            return self._total_count

        if not self._connection:
            return None

        query = self._source_url_query(count_only=True)

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
                self._total_count = int(row[0]) if row and row[0] is not None else 0
                return self._total_count
        except Exception as e:
            self.logger.warning(f"Could not count uncrawled URLs in PostgreSQL: {e}")
            return None

    def cleanup(self):
        if self._connection:
            self._connection.close()
            self._connection = None
            self.logger.info("PostgreSQL input connection closed")
