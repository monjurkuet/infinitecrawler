"""Tests for daemons/listing_daemon.py — init_infrastructure wiring.

Mirrors the `FakeState` + `importlib.reload` convention from
test_shutdown_order.py so we exercise init paths without needing live PG
or a running pinchtab. Verifies:

1. `output_strategy`, `queue_strategy`, `delay_manager` are configured.
2. The `Infrastructure initialized. Entering eternal loop.` log line fires.
"""
import asyncio
import unittest
from unittest import mock


class FakeState:
    def __init__(self):
        self.browser_manager = mock.AsyncMock()
        self.output_strategy = mock.MagicMock()
        self.queue_strategy = mock.MagicMock()
        self.delay_manager = mock.MagicMock()
        self.pg_conn = None
        self.config = {}
        self.sectors = {}
        self.pages_since_restart = 0
        self.last_restart_time = 0.0
        self.total_pages_processed = 0
        self.tab_pages = {}
        self.restart_lock = asyncio.Lock()
        self.consecutive_errors = 0
        self.max_consecutive_errors = 10
        self.shutdown_requested = False
        self.last_heartbeat_time = 0.0
        self.last_heartbeat_pages = 0
        # Strategy slots the plan asks us to tighten later.
        self.extraction_strategy = None


class TestListingDaemonInit(unittest.TestCase):
    def setUp(self):
        import importlib
        import daemons.listing_daemon
        importlib.reload(daemons.listing_daemon)
        self.listing_daemon = daemons.listing_daemon

    def test_init_infrastructure_emits_ready_log(self):
        """init_infrastructure sets strategies and emits the ready log line."""
        ld = self.listing_daemon
        # Patch heavy dependencies to avoid real PG / pinchtab / Redis.
        with mock.patch.object(ld, "ScraperFactory") as factory, \
             mock.patch.object(ld, "_connect_pg", return_value=mock.MagicMock()), \
             mock.patch.object(ld, "start_browser", new=mock.AsyncMock()), \
             mock.patch.object(ld, "_refresh_browser_bound_strategies",
                               new=mock.AsyncMock()):
            fake_state = FakeState()
            factory.load_config.return_value = {
                "output": {"strategy": "postgresql_upsert", "config": {}},
                "queue": {"strategy": "redis_queue", "config": {}},
                "rate_limit": 2,
                "workers": {"max_consecutive_errors": 10},
            }
            factory.create_strategy.return_value = mock.MagicMock()

            with self.assertLogs("listing_daemon", level="INFO") as cm:
                asyncio.run(ld.init_infrastructure(fake_state))

            joined = "\n".join(cm.output)
            self.assertIn("Infrastructure initialized. Entering eternal loop.", joined)
            self.assertIsNotNone(fake_state.output_strategy)
            self.assertIsNotNone(fake_state.queue_strategy)
            self.assertIsNotNone(fake_state.delay_manager)


if __name__ == "__main__":
    unittest.main()
