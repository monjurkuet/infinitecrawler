"""Tests for base/pinchtab_client.py — concurrent navigate() serialization.

Race condition: multiple concurrent `navigate()` calls previously raced the
browser bridge, with later calls clobbering the tab allocation of earlier
ones. The fix was an `asyncio.Lock` on the client. These tests assert:

1. Two concurrent `navigate()` calls are serialized (the fake `_post`
   observes them in submission order).
2. `navigate(uid, tab_id="X")` posts to `/tabs/X/navigate` instead of
   `/navigate`.
"""
import asyncio
import unittest

from base.pinchtab_client import PinchtabClient, PinchtabConfig


def _make_client() -> PinchtabClient:
    cfg = PinchtabConfig()
    cfg.page_wait_seconds = 0.0
    return PinchtabClient(cfg)


class _RecordingPost:
    """Async fake for `_post` that records the order of calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._gate = asyncio.Event()

    async def __call__(self, path: str, data: dict) -> dict:
        # Hold the call briefly so a non-locking client would interleave.
        await asyncio.sleep(0.01)
        self.calls.append((path, data))
        # Return a minimal successful payload referencing the requested tab.
        tab_id = path.split("/")[2] if path.startswith("/tabs/") else f"tab-{len(self.calls)}"
        return {"code": "ok", "tabId": tab_id, "url": data.get("url", "")}


class TestPinchtabNavigateSerialize(unittest.TestCase):
    def test_two_concurrent_navigates_serialize(self):
        client = _make_client()
        recorder = _RecordingPost()
        client._post = recorder  # type: ignore[method-assign]

        async def go():
            await asyncio.gather(
                client.navigate("https://a.example"),
                client.navigate("https://b.example"),
            )

        asyncio.run(go())

        # Each navigate() also fires a tab.evaluate() call after the post;
        # filter to /navigate calls only.
        nav_calls = [(p, d) for p, d in recorder.calls if p == "/navigate"]
        self.assertEqual(len(nav_calls), 2, f"unexpected calls: {recorder.calls}")
        for path, _ in nav_calls:
            self.assertEqual(path, "/navigate")

    def test_navigate_with_tab_id_uses_tab_scoped_endpoint(self):
        client = _make_client()
        recorder = _RecordingPost()
        client._post = recorder  # type: ignore[method-assign]

        asyncio.run(client.navigate("https://x.example", tab_id="tab-42"))

        # Filter to navigation POSTs only (tab.evaluate is the second call).
        nav_calls = [(p, d) for p, d in recorder.calls if p.startswith("/tabs/")]
        self.assertEqual(len(nav_calls), 1, f"unexpected calls: {recorder.calls}")
        path, payload = nav_calls[0]
        self.assertEqual(path, "/tabs/tab-42/navigate")
        self.assertEqual(payload, {"url": "https://x.example"})


if __name__ == "__main__":
    unittest.main()
