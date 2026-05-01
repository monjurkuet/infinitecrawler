"""PostgreSQL output strategies for persisting scraped data"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from psycopg import connect
from psycopg.types.json import Jsonb
from psycopg import sql

from base.strategies import OutputStrategy


class _PostgreSQLOutputBase(OutputStrategy):
    """Shared PostgreSQL connection and table management."""

    default_table = "gmaps_listings"

    def __init__(self, config: dict):
        self.config = config.get("config", {})
        self.schema = self.config.get("schema", "scraper")
        self.table = self.config.get("table", self.default_table)
        self.source_type = self.config.get("source_type")
        self.key_field = self.config.get("key_field", "source_url")
        self.max_results = self.config.get("max_results", 100000)
        self.results_count = 0
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection = None
        self._connect()
        self._ensure_schema_and_table()

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
        """Connect to PostgreSQL."""
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

            self._ensure_database_exists(host, port, user, password, database)
            self._connection = connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=database,
            )
            self._connection.autocommit = True
            self.logger.info(
                f"Connected to PostgreSQL: {database}.{self.schema}.{self.table}"
            )
        except ImportError:
            self.logger.error("psycopg not installed. Run: uv sync")
            raise
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _ensure_database_exists(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ):
        """Create the target database if it does not already exist."""
        try:
            admin_connection = connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname="postgres",
            )
            admin_connection.autocommit = True

            with admin_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s", (database,)
                )
                exists = cursor.fetchone() is not None

                if not exists:
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
                    )
                    self.logger.info(f"Created PostgreSQL database: {database}")

            admin_connection.close()
        except Exception as e:
            message = str(e)
            if (
                "permission denied" in message.lower()
                and "create database" in message.lower()
            ):
                raise RuntimeError(
                    f"PostgreSQL database '{database}' does not exist and the configured user cannot create it. "
                    f"Create the database manually or grant CREATEDB to the role."
                ) from e
            raise

    def _ensure_schema_and_table(self):
        if not self._connection:
            raise RuntimeError("PostgreSQL connection not initialized")

        create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(self.schema)
        )
        create_table = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id BIGSERIAL PRIMARY KEY,
                key_value TEXT,
                source_type TEXT,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        ).format(sql.Identifier(self.schema), sql.Identifier(self.table))

        with self._connection.cursor() as cursor:
            cursor.execute(create_schema)
            cursor.execute(create_table)

    def _serialize_payload(self, item: Dict) -> Dict:
        """Convert item to a JSON-serializable payload."""

        def default(value):
            if isinstance(value, datetime):
                return value.isoformat()
            return str(value)

        return json.loads(json.dumps(item, default=default))

    def _extract_key_value(self, item: Dict) -> Optional[str]:
        if not self.key_field:
            return None

        value = item.get(self.key_field)
        if value in (None, ""):
            return None
        return str(value)

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip().replace(",", ""))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_float(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_numeric(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_bool(value: Any) -> Optional[bool]:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        normalized = str(value).strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False
        return None

    async def cleanup(self):
        """Close PostgreSQL connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            self.logger.info("PostgreSQL connection closed")


class PostgreSQLOutputStrategy(_PostgreSQLOutputBase):
    """Insert items into PostgreSQL."""

    default_table = "gmaps_listings"

    async def write_item(self, item: Dict):
        """Insert item into PostgreSQL."""
        try:
            if self.results_count >= self.max_results:
                self.logger.warning(f"Max results limit ({self.max_results}) reached")
                return

            if not self._connection:
                self.logger.error("PostgreSQL connection not initialized")
                return

            payload = self._serialize_payload(item)
            key_value = self._extract_key_value(item)
            source_type = self.source_type or item.get("source")

            insert_sql = sql.SQL(
                """
                INSERT INTO {}.{} (key_value, source_type, payload, created_at, updated_at)
                VALUES (%s, %s, %s, NOW(), NOW())
                """
            ).format(sql.Identifier(self.schema), sql.Identifier(self.table))

            with self._connection.cursor() as cursor:
                cursor.execute(
                    insert_sql,
                    (key_value, source_type, Jsonb(payload)),
                )

            self.results_count += 1
        except Exception as e:
            self.logger.error(f"Failed to write to PostgreSQL: {e}")
            raise

    def has_reached_limit(self) -> bool:
        """Check if max results limit reached"""
        return self.results_count >= self.max_results


class PostgreSQLUpsertStrategy(_PostgreSQLOutputBase):
    """Upsert items into PostgreSQL using a configured unique key field."""

    default_table = "gmaps_search_results"

    def _ensure_schema_and_table(self):
        super()._ensure_schema_and_table()
        if not self._connection:
            raise RuntimeError("PostgreSQL connection not initialized")

        create_index = sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (key_value)"
        ).format(
            sql.Identifier(f"{self.table}_key_value_uidx"),
            sql.Identifier(self.schema),
            sql.Identifier(self.table),
        )

        with self._connection.cursor() as cursor:
            cursor.execute(create_index)

    async def write_item(self, item: Dict):
        """Upsert item into PostgreSQL using configured key field."""
        try:
            if self.results_count >= self.max_results:
                self.logger.warning(f"Max results limit ({self.max_results}) reached")
                return

            if not self._connection:
                self.logger.error("PostgreSQL connection not initialized")
                return

            key_value = self._extract_key_value(item)
            if key_value is None:
                self.logger.error(
                    f"Key field '{self.key_field}' missing; skipping PostgreSQL upsert"
                )
                return

            payload = self._serialize_payload(item)
            source_type = self.source_type or item.get("source")

            upsert_sql = sql.SQL(
                """
                INSERT INTO {}.{} (key_value, source_type, payload, created_at, updated_at)
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (key_value) DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
                """
            ).format(sql.Identifier(self.schema), sql.Identifier(self.table))

            with self._connection.cursor() as cursor:
                cursor.execute(
                    upsert_sql,
                    (key_value, source_type, Jsonb(payload)),
                )

            self.results_count += 1
        except Exception as e:
            self.logger.error(f"Failed to upsert to PostgreSQL: {e}")
            raise

    def has_reached_limit(self) -> bool:
        """Check if max results limit reached"""
        return self.results_count >= self.max_results


class PostgreSQLListingDetailsUpsertStrategy(_PostgreSQLOutputBase):
    """Upsert Google Maps listing details into a typed PostgreSQL table."""

    default_table = "gmaps_listings"

    def __init__(self, config: dict):
        self._drop_and_recreate = config.get("config", {}).get("recreate_table", False)
        super().__init__(config)

    def _ensure_schema_and_table(self):
        if not self._connection:
            raise RuntimeError("PostgreSQL connection not initialized")

        create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(self.schema)
        )
        drop_table = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(self.schema), sql.Identifier(self.table)
        )
        create_table = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id BIGSERIAL PRIMARY KEY,
                place_id TEXT,
                source_url TEXT NOT NULL,
                key_value TEXT NOT NULL,
                source_type TEXT,
                name TEXT,
                category TEXT,
                rating NUMERIC(3,2),
                review_count INTEGER,
                address TEXT,
                phone TEXT,
                website TEXT,
                booking_url TEXT,
                plus_code TEXT,
                is_claimed BOOLEAN,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                crawl_retry_count INTEGER,
                crawl_pages_processed INTEGER,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        ).format(sql.Identifier(self.schema), sql.Identifier(self.table))

        unique_index = sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (key_value)"
        ).format(
            sql.Identifier(f"{self.table}_key_value_uidx"),
            sql.Identifier(self.schema),
            sql.Identifier(self.table),
        )
        indexes = [
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (place_id)").format(
                sql.Identifier(f"{self.table}_place_id_idx"),
                sql.Identifier(self.schema),
                sql.Identifier(self.table),
            ),
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (source_url)").format(
                sql.Identifier(f"{self.table}_source_url_idx"),
                sql.Identifier(self.schema),
                sql.Identifier(self.table),
            ),
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (name)").format(
                sql.Identifier(f"{self.table}_name_idx"),
                sql.Identifier(self.schema),
                sql.Identifier(self.table),
            ),
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (category)").format(
                sql.Identifier(f"{self.table}_category_idx"),
                sql.Identifier(self.schema),
                sql.Identifier(self.table),
            ),
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (updated_at DESC)").format(
                sql.Identifier(f"{self.table}_updated_at_idx"),
                sql.Identifier(self.schema),
                sql.Identifier(self.table),
            ),
        ]

        with self._connection.cursor() as cursor:
            cursor.execute(create_schema)
            if self._drop_and_recreate:
                cursor.execute(drop_table)
            cursor.execute(create_table)
            cursor.execute(unique_index)
            for index_sql in indexes:
                cursor.execute(index_sql)

    def _resolve_source_url(self, item: Dict) -> Optional[str]:
        crawl_meta = item.get("_crawl_meta") or {}
        source_url = crawl_meta.get("source_url") or item.get("source_url")
        return self._clean_text(source_url)

    def _resolve_key_value(self, item: Dict) -> Optional[str]:
        place_id = self._clean_text(item.get("place_id"))
        if place_id:
            return place_id
        return self._resolve_source_url(item)

    def _map_row(self, item: Dict) -> Optional[Dict[str, Any]]:
        source_url = self._resolve_source_url(item)
        if not source_url:
            return None

        crawl_meta = item.get("_crawl_meta") or {}
        key_value = self._resolve_key_value(item)
        if not key_value:
            return None

        return {
            "place_id": self._clean_text(item.get("place_id")),
            "source_url": source_url,
            "key_value": key_value,
            "source_type": self._clean_text(self.source_type or item.get("source")),
            "name": self._clean_text(item.get("name")),
            "category": self._clean_text(item.get("category")),
            "rating": self._parse_numeric(item.get("rating")),
            "review_count": self._parse_int(item.get("review_count")),
            "address": self._clean_text(item.get("address")),
            "phone": self._clean_text(item.get("phone")),
            "website": self._clean_text(item.get("website")),
            "booking_url": self._clean_text(item.get("booking_url")),
            "plus_code": self._clean_text(item.get("plus_code")),
            "is_claimed": self._parse_bool(item.get("is_claimed")),
            "latitude": self._parse_float(item.get("latitude")),
            "longitude": self._parse_float(item.get("longitude")),
            "crawl_retry_count": self._parse_int(crawl_meta.get("retry_count")),
            "crawl_pages_processed": self._parse_int(crawl_meta.get("pages_processed")),
            "payload": self._serialize_payload(item),
        }

    async def write_item(self, item: Dict):
        """Upsert listing details into PostgreSQL."""
        try:
            if self.results_count >= self.max_results:
                self.logger.warning(f"Max results limit ({self.max_results}) reached")
                return

            if not self._connection:
                self.logger.error("PostgreSQL connection not initialized")
                return

            row = self._map_row(item)
            if row is None:
                self.logger.warning(
                    "Skipping PostgreSQL listing upsert without source_url"
                )
                return

            upsert_sql = sql.SQL(
                """
                INSERT INTO {}.{} (
                    place_id, source_url, key_value, source_type, name, category,
                    rating, review_count, address, phone, website, booking_url,
                    plus_code, is_claimed, latitude, longitude, crawl_retry_count,
                    crawl_pages_processed, payload, created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, NOW(), NOW()
                )
                ON CONFLICT (key_value) DO UPDATE SET
                    place_id = EXCLUDED.place_id,
                    source_url = EXCLUDED.source_url,
                    source_type = EXCLUDED.source_type,
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    rating = EXCLUDED.rating,
                    review_count = EXCLUDED.review_count,
                    address = EXCLUDED.address,
                    phone = EXCLUDED.phone,
                    website = EXCLUDED.website,
                    booking_url = EXCLUDED.booking_url,
                    plus_code = EXCLUDED.plus_code,
                    is_claimed = EXCLUDED.is_claimed,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    crawl_retry_count = EXCLUDED.crawl_retry_count,
                    crawl_pages_processed = EXCLUDED.crawl_pages_processed,
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
                """
            ).format(sql.Identifier(self.schema), sql.Identifier(self.table))

            params = (
                row["place_id"],
                row["source_url"],
                row["key_value"],
                row["source_type"],
                row["name"],
                row["category"],
                row["rating"],
                row["review_count"],
                row["address"],
                row["phone"],
                row["website"],
                row["booking_url"],
                row["plus_code"],
                row["is_claimed"],
                row["latitude"],
                row["longitude"],
                row["crawl_retry_count"],
                row["crawl_pages_processed"],
                Jsonb(row["payload"]),
            )

            with self._connection.cursor() as cursor:
                cursor.execute(upsert_sql, params)

            self.results_count += 1
        except Exception as e:
            self.logger.error(f"Failed to upsert listing details to PostgreSQL: {e}")
            raise

    def has_reached_limit(self) -> bool:
        return self.results_count >= self.max_results
