"""Tests for daemons/common.py — flush-before-cleanup ordering."""

import unittest
from unittest import mock



class FakeState:
    """Minimal state with mocks for shutdown testing."""
    def __init__(self):
        self.browser_manager = mock.AsyncMock()
        self.output_strategy = mock.MagicMock()
        self.queue_strategy = mock.MagicMock()


class TestShutdownStrategiesOrder(unittest.TestCase):
    def setUp(self):
        import importlib
        import daemons.common
        importlib.reload(daemons.common)
        global common  # avoid stale reference across tests
        common = daemons.common

    def test_flush_before_cleanup(self):
        """flush_batch() must be called before cleanup() during shutdown."""
        import importlib
        import daemons.common
        importlib.reload(daemons.common)
        cm = daemons.common

        state = FakeState()
        state.output_strategy.configure_mock(
            **{
                "flush_batch.return_value": None,
                "cleanup.return_value": None,
            }
        )

        import asyncio
        asyncio.run(cm.shutdown_strategies(state))

        # Get all method calls in order (call[0] is the method name string)
        call_names = [call[0] for call in state.output_strategy.method_calls]

        flush_idx = call_names.index("flush_batch") if "flush_batch" in call_names else -1
        cleanup_idx = call_names.index("cleanup") if "cleanup" in call_names else -1

        self.assertGreaterEqual(flush_idx, 0, f"flush_batch was never called. Calls: {call_names}")
        self.assertGreaterEqual(cleanup_idx, 0, "cleanup was never called")
        self.assertLess(flush_idx, cleanup_idx,
                        "flush_batch must be called BEFORE cleanup")

    def test_browser_cleanup_called(self):
        """browser_manager.cleanup() is called during shutdown."""
        import importlib
        import daemons.common
        importlib.reload(daemons.common)
        cm = daemons.common

        state = FakeState()
        browser = state.browser_manager  # capture before shutdown sets it to None
        import asyncio
        asyncio.run(cm.shutdown_strategies(state))
        browser.cleanup.assert_called_once()

    def test_no_browser_manager(self):
        import importlib
        import daemons.common
        importlib.reload(daemons.common)
        cm = daemons.common
        state = FakeState()
        state.browser_manager = None
        import asyncio
        asyncio.run(cm.shutdown_strategies(state))

    def test_no_output_strategy(self):
        import importlib
        import daemons.common
        importlib.reload(daemons.common)
        cm = daemons.common
        state = FakeState()
        state.output_strategy = None
        import asyncio
        asyncio.run(cm.shutdown_strategies(state))

    def test_no_queue_strategy(self):
        import importlib
        import daemons.common
        importlib.reload(daemons.common)
        cm = daemons.common
        state = FakeState()
        state.queue_strategy = None
        import asyncio
        asyncio.run(cm.shutdown_strategies(state))


if __name__ == "__main__":
    unittest.main()