"""Pinchtab HTTP client + Tab adapter.

The crawler daemons talk to a running pinchtab browser server (default port
9868) over a tiny async HTTP client.  The Tab / Element shape mimics the small
subset of nodriver's interface that the daemon strategies actually use, so the
existing pagination/extraction code keeps working without changes:

    tab.evaluate(js)        → JS result value
    tab.select(selector)    → Element-like object (or None)
    tab.select_all(selector)→ [Element-like, …]
    tab.wait(seconds)       → async sleep
    element.attrs           → {attr: value}
    element.text            → text content
    element.html            → outerHTML

Wire format is plain JSON over HTTP.  The server + browser instance must be
started out-of-band — this module only issues commands.

Stability note: the chrome instance launched by pinchtab is configured with the
extraFlags override

    --max_old_space_size=2048 --renderer-process-limit=5

in `/root/.pinchtab/config.json`.  Without those Chrome would crash on Google
Maps every 1-3 navigations with `"context canceled"` errors.  See the
`pinchtab-chrome-stability` skill for details.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    import aiohttp
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "pinchtab engine needs aiohttp — install with `uv add aiohttp`"
    ) from exc


log = logging.getLogger("pinchtab_client")


# ── Element shim ────────────────────────────────────────────────────────────

class PinchtabElement:
    """Tab-output element shape consumed by the strategy code.

    Has just enough attributes for `generic_selector.py` /
    `multi_step.py` / `infinite_scroll.py` to work — `.attrs`, `.text`,
    `.html`, plus `__getattr__` forwarding dict lookups (so `el.href` works).
    """

    __slots__ = ("attrs", "text", "html", "_tag")

    def __init__(self, attrs: dict, text: str, html: str, tag: str = ""):
        self.attrs = attrs
        self.text = text
        self.html = html
        self._tag = tag

    @property
    def tag(self) -> str:
        return self._tag

    def __getattr__(self, item: str) -> Any:
        # Forward unknown attribute lookups into the attrs dict (legacy shim
        # for code that did `element.href` etc.).
        return self.attrs.get(item)


# ── Tab adapter ─────────────────────────────────────────────────────────────

class PinchtabTab:
    """Adapter exposing the few Tab methods the daemon strategies call.

    Talks to pinchtab's instance HTTP API (port 9868 by default).  Each method
    is `async` to match nodriver's interface so the strategies work unchanged.
    """

    def __init__(self, client: "PinchtabClient", tab_id: str, url: str = ""):
        self._client = client
        self._tab_id = tab_id
        self.url = url

    # The two extraction/pagination strategies use `tab` directly; keep `tab.url`
    # populated so listing-daemon multi_step.py (which reads
    # `self.browser_manager.tab.url`) does not crash on AttributeError.

    async def evaluate(self, expression: str, await_promise: bool = False) -> Any:
        """Run JS on the active tab and return the value.

        Pinchtab's /evaluate returns {"result": <value>} on success,
        {"code": "error", "error": "…"} on failure.

        Recovers from "context canceled" (Chrome tab crashed) by re-navigating
        to the last URL once and retrying — this works around pinchtab 0.15's
        tendency to crash the CDP context on heavy GMaps pages.
        """
        for attempt in (1, 2):
            result = await self._client._post("/evaluate", {
                "expression": expression,
                "tabId": self._tab_id,
            })
            if result.get("code") == "error":
                err = result.get("error", "")
                # Recover from transient tab/context failures by re-navigating
                # once.  Pinchtab 0.15's Chrome frequently tears down the CDP
                # context on heavy GMaps pages; the always-on supervisor
                # quickly restarts the instance and we re-acquire a fresh tab.
                recoverable = (
                    "context canceled" in err
                    or ("tab " in err and "not found" in err)
                    or "tab manager not initialized" in err
                    or "tab " in err and "not connected" in err
                )
                if attempt == 1 and recoverable:
                    self._client.logger.warning(
                        "pinchtab: tab context dropped (%s), re-navigating to %s",
                        err[:80], self.url or "(no URL)",
                    )
                    try:
                        new_tab = await self._client.navigate(self.url or "about:blank")
                        # adopt the new tab id so subsequent ops target the fresh page
                        self._tab_id = new_tab._tab_id
                    except RuntimeError:
                        pass  # navigate itself can fail if instance is mid-restart
                    continue  # retry once on the fresh tab
                raise RuntimeError(f"pinchtab evaluate failed: {err}")
            return result.get("result")
        # Should not reach here
        raise RuntimeError("pinchtab: evaluate exhausted retries")

    async def select(self, selector: str, timeout: float = 10) -> Optional[PinchtabElement]:
        """Find one element by CSS selector.  Returns None if not found."""
        elements = await self._query(selector, limit=1)
        return elements[0] if elements else None

    async def select_all(self, selector: str, timeout: float = 10, include_frames: bool = False) -> list:
        """Find all elements matching a CSS selector."""
        return await self._query(selector, limit=0)

    async def _query(self, selector: str, limit: int = 0) -> list[PinchtabElement]:
        # Use a single eval round-trip to extract attrs/text/html for all matches.
        # limit=0 means "all", limit=N means "first N".
        expr = """
        (() => {
            const els = [...document.querySelectorAll(%s)].slice(0, %d || 999999);
            return JSON.stringify(els.map(el => {
                const attrs = {};
                for (const a of el.attributes) attrs[a.name] = a.value;
                return {attrs, text: el.textContent, html: el.outerHTML, tag: el.tagName.toLowerCase()};
            }));
        })()
        """ % (json.dumps(selector), limit)
        raw = await self.evaluate(expr)
        if not raw:
            return []
        try:
            payload = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            return []
        return [
            PinchtabElement(p["attrs"], p.get("text", ""), p.get("html", ""), p.get("tag", ""))
            for p in payload
        ]

    async def wait(self, t: float = 0.5):
        await asyncio.sleep(t)

    async def find(self, text: str, best_match: bool = True, return_enclosing_element: bool = True, timeout: float = 10):
        """Approximate `tab.find('text')` — not heavily used by the daemons."""
        escaped = json.dumps(text)
        expr = """
        (() => {
            const xp = `//*[contains(text(), %s]`;
            const r = document.evaluate(xp, document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
            const el = r.snapshotItem(0);
            if (!el) return null;
            const node = el.nodeType === 3 ? el.parentElement : el;
            const attrs = {};
            for (const a of node.attributes) attrs[a.name] = a.value;
            return JSON.stringify({attrs, text: node.textContent, html: node.outerHTML, tag: node.tagName.toLowerCase()});
        })()
        """ % escaped
        raw = await self.evaluate(expr)
        if not raw:
            return None
        try:
            p = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            return None
        return PinchtabElement(p["attrs"], p.get("text", ""), p.get("html", ""), p.get("tag", ""))

    async def find_all(self, text: str, timeout: float = 10) -> list:
        """Approximate `tab.find_all('text')` — returns Element list."""
        escaped = json.dumps(text)
        expr = """
        (() => {
            const xp = `//*[contains(text(), %s]`;
            const r = document.evaluate(xp, document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
            const out = [];
            for (let i = 0; i < r.snapshotLength; i++) {
                const el = r.snapshotItem(i);
                const node = el.nodeType === 3 ? el.parentElement : el;
                const attrs = {};
                for (const a of node.attributes) attrs[a.name] = a.value;
                out.push({attrs, text: node.textContent, html: node.outerHTML, tag: node.tagName.toLowerCase()});
            }
            return JSON.stringify(out);
        })()
        """ % escaped
        raw = await self.evaluate(expr)
        if not raw:
            return []
        try:
            payload = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            return []
        return [
            PinchtabElement(p["attrs"], p.get("text", ""), p.get("html", ""), p.get("tag", ""))
            for p in payload
        ]


# ── HTTP client ─────────────────────────────────────────────────────────────

@dataclass
class PinchtabConfig:
    server_url: str = "http://127.0.0.1:9867"
    instance_url: str = "http://127.0.0.1:9868"  # direct browser instance
    token: str = ""
    page_wait_seconds: float = 1.0
    headless: bool = True
    navigate_timeout: float = 60.0

    @classmethod
    def from_env_and_config(cls, config: dict) -> "PinchtabConfig":
        # Allow config overrides, fallback to PINCHTAB_* env vars and defaults.
        pt = config.get("pinchtab", {})
        env = os.environ
        return cls(
            server_url=pt.get("server_url", env.get("PINCHTAB_URL", "http://127.0.0.1:9867")),
            instance_url=pt.get("instance_url", env.get("PINCHTAB_INSTANCE_URL", "http://127.0.0.1:9868")),
            token=pt.get("token", env.get("PINCHTAB_TOKEN", env.get("BRIDGE_TOKEN", ""))),
            page_wait_seconds=config.get("page_wait_seconds", 1.0),
            headless=config.get("headless", True),
            navigate_timeout=pt.get("navigate_timeout", 60.0),
        )


class PinchtabClient:
    """Async HTTP wrapper around a running pinchtab browser instance."""

    def __init__(self, config: PinchtabConfig):
        self.cfg = config
        self._session: Optional[aiohttp.ClientSession] = None
        self.tab: Optional[PinchtabTab] = None
        self.logger = logging.getLogger("pinchtab_client")

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {}
            if self.cfg.token:
                headers["Authorization"] = f"Bearer {self.cfg.token}"
            self._session = aiohttp.ClientSession(headers=headers)
        assert self._session is not None  # set above; for type-checker
        return self._session

    def _base(self) -> str:
        # Prefer the instance URL (direct, fewer hop failures) and fall back.
        return self.cfg.instance_url or self.cfg.server_url

    async def _post(self, path: str, data: dict) -> dict:
        session = await self._ensure_session()
        url = f"{self._base()}{path}"
        async with session.post(url, json=data) as r:
            text = await r.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                self.logger.warning("pinchtab: non-JSON %s → %s", path, text[:200])
                return {"code": "error", "error": f"non-JSON response: {text[:200]}"}

    async def _get(self, path: str) -> dict:
        session = await self._ensure_session()
        url = f"{self._base()}{path}"
        async with session.get(url) as r:
            text = await r.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"code": "error", "error": f"non-JSON response: {text[:200]}"}

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self):
        """Pinchtab is an external server — we only verify connectivity."""
        await self._ensure_session()
        # Verify the instance is alive.
        health = await self._get("/health")
        if health.get("code") == "error" and "context canceled" in (health.get("error") or "").lower():
            # The instance crashed since the server started; the server's always-on
            # policy will restart it.  Politely wait and retry once.
            self.logger.warning("pinchtab: instance not healthy, waiting for restart…")
            await asyncio.sleep(5)
            health = await self._get("/health")
        # The /health response shape is different on the dashboard vs instance:
        # - instance returns {crashes: …, crashes: …} but no status field; absence of
        #   {"code": "error"} is good enough.
        if health.get("code") == "error":
            raise RuntimeError(f"pinchtab: cannot reach browser instance: {health.get('error')}")
        self.logger.info("pinchtab: connected to %s", self._base())

    async def navigate(self, url: str) -> PinchtabTab:
        # Pinchtab's Chrome sometimes tears down the CDP context right after a
        # previous operation — re-navigating once after the always-on supervisor
        # has restarted the instance resolves it.
        for attempt in (1, 2):
            result = await self._post("/navigate", {"url": url})
            if result.get("code") == "error":
                err = result.get("error", "")
                if "context canceled" in err and attempt == 1:
                    self.logger.warning(
                        "pinchtab navigate: %s (will retry once after restart)",
                        err[:80],
                    )
                    await asyncio.sleep(3)  # give the supervisor time to restart
                    continue
                raise RuntimeError(f"pinchtab navigate failed: {err}")
            tab_id = result.get("tabId", "")
            self.tab = PinchtabTab(self, tab_id, url=url)
            if self.cfg.page_wait_seconds > 0:
                await asyncio.sleep(self.cfg.page_wait_seconds)
            return self.tab
        raise RuntimeError("pinchtab navigate: exhausted retries")

    async def close_tab(self):
        if self.tab:
            await self._post("/action", {"kind": "close", "tabId": self.tab._tab_id})
            self.tab = None

    async def cleanup(self):
        """Release HTTP session.  We intentionally do NOT kill the browser
        instance — pinchtab manages its own lifecycle and the always-on policy
        will restart it anyway.  Killing Chrome from outside pinchtab's
        knowledge can desync its dashboard."""
        if self.tab:
            try:
                await self.close_tab()
            except Exception:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self.tab = None
