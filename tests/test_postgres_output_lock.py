"""Tests for strategies/output/upsert.py — write_item lock serialization.

Bug fixed in 2026-08-12: the previous batch was a bare list mutated under
`_flush_lock`, but `write_item` re-acquired the lock after partial flush
without the lock being reentrant, leading to double-flushes / lost rows.
The fix is an `_flush_lock` held across `write_item`'s batch mutation +
flush. These tests assert no double-flush under concurrent writes.
"""
import asyncio
import unittest
from unittest import mock

from strategies.output.upsert import PostgreSQLUpsertStrategy


class _CountingConn:
    """Minimal psycopg.Connection fake that counts executemany() calls."""

    def __init__(self) -> None:
        self.executemany_calls: int = 0
        self.autocommit = True

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def executemany(self, sql, batch):
        self.executemany_calls += 1

    def close(self):
        pass


def _make_strat(batch_size: int = 50) -> PostgreSQLUpsertStrategy:
    """Build a PostgreSQLUpsertStrategy with fake state (no real PG connect).

    Caller controls ``_BATCH_SIZE`` so the flush path can be exercised or
    sidestepped per-test.
    """
    cfg = {"config": {"key_field": "source_url", "table": "gmaps_search_results"}}
    strat = PostgreSQLUpsertStrategy.__new__(PostgreSQLUpsertStrategy)
    strat.config = cfg["config"]
    strat.schema = "scraper"
    strat.table = "gmaps_search_results"
    strat.source_type = None
    strat.key_field = "source_url"
    strat.max_results = 100000
    strat.results_count = 0
    strat.logger = mock.MagicMock()
    strat._connection = _CountingConn()
    strat._write_batch = []
    strat._BATCH_SIZE = batch_size
    strat._last_flush = 0.0
    strat._flush_lock = asyncio.Lock()
    strat._batch_sql = None  # set by write_item on first call

    # Stub internal methods so we don't touch the real PG schema path.
    strat._ensure_connection = lambda: None
    strat._ensure_schema_and_table = lambda: None
    return strat


class TestPostgresUpsertLock(unittest.TestCase):
    def test_concurrent_write_items_below_batch_size_no_flush(self):
        """10 items with BATCH_SIZE=50: no flush, all 10 rows remain in the batch."""
        strat = _make_strat(batch_size=50)

        async def go():
            items = [{"source_url": f"https://x/{i}"} for i in range(10)]
            await asyncio.gather(*(strat.write_item(it) for it in items))

        asyncio.run(go())
        self.assertEqual(len(strat._write_batch), 10)
        self.assertEqual(strat._connection.executemany_calls, 0)

    def test_concurrent_write_items_trigger_flush_exactly_once(self):
        """15 concurrent write_items with BATCH_SIZE=5 → exactly 3 flushes, 0 lost.

        This exercises the actual concurrency-critical path: the lock must
        serialize the batch-mutation+flush so two coroutines hitting the
        size threshold simultaneously don't double-flush the same rows or
        lose items between the append and the clear. We assert that the
        number of `executemany` calls equals ceil(15/5)=3 (no double-flush)
        and the in-batch remainder is 15 % 5 = 0 (no lost tail).
        """
        strat = _make_strat(batch_size=5)

        async def go():
            items = [{"source_url": f"https://x/{i}"} for i in range(15)]
            await asyncio.gather(*(strat.write_item(it) for it in items))

        asyncio.run(go())
        # flushes = floor(15 / 5) = 3 (one at size 5, 10, 15). Trailing
        # items that land on the exact threshold still trigger the flush,
        # so the batch should be empty after the run.
        self.assertEqual(strat._connection.executemany_calls, 3,
                         f"expected 3 flushes (15 items / BATCH_SIZE 5), "
                         f"got {strat._connection.executemany_calls}")
        self.assertEqual(len(strat._write_batch), 0,
                         "all items flushed since 15 % 5 == 0")
        # results_count must equal total items written (no double-counting
        # under concurrency — _flush_write_batch_unlocked adds n each call).
        self.assertEqual(strat.results_count, 15)

    def test_partial_batch_left_in_queue_after_concurrent_writes(self):
        """17 items with BATCH_SIZE=5 → 3 flushes, 2 left in batch (no loss)."""
        strat = _make_strat(batch_size=5)

        async def go():
            items = [{"source_url": f"https://x/{i}"} for i in range(17)]
            await asyncio.gather(*(strat.write_item(it) for it in items))

        asyncio.run(go())
        self.assertEqual(strat._connection.executemany_calls, 3)
        self.assertEqual(len(strat._write_batch), 2)
        self.assertEqual(strat.results_count, 15)


if __name__ == "__main__":
    unittest.main()
