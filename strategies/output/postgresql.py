"""strategies/output/postgresql.py — Plain insert strategy (no upsert)."""
from __future__ import annotations

from typing import Dict

from psycopg import sql
from psycopg.types.json import Jsonb

from strategies.output._base import _PostgreSQLOutputBase


class PostgreSQLOutputStrategy(_PostgreSQLOutputBase):
    """Insert items into PostgreSQL."""

    default_table = "gmaps_listings"

    async def write_item(self, item: Dict):
        """Insert item into PostgreSQL."""
        try:
            async with self._flush_lock:
                self._ensure_connection()
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
                self._batch_sql = insert_sql

                self._write_batch.append((key_value, source_type, Jsonb(payload)))
                if len(self._write_batch) >= self._BATCH_SIZE:
                    self._flush_write_batch_unlocked(insert_sql, trigger="size")
        except Exception as e:
            self.logger.error(f"Failed to write to PostgreSQL: {e}")
            raise

    def has_reached_limit(self) -> bool:
        """Check if max results limit reached"""
        return self.results_count >= self.max_results
