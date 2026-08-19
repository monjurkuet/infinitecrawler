import asyncio
import logging
import os


class BrowserManager:
    """Manages the pinchtab-attached browser.

    The browser runs in an external `pinchtab server` process (bridge port 9868 by
    default, configurable via PINCHTAB_INSTANCE_URL).  We don't launch Chrome
    ourselves — the pinchtab `always-on` supervisor restarts crashed instances
    automatically.  Stability is greatly improved by setting
    `browser.extraFlags` in `/root/.pinchtab/config.json` so Chrome uses
    `--max_old_space_size=2048 --renderer-process-limit=5` (the pinchtab defaults
    OOM on Google Maps).

    `self.tab` continues to expose a single Tab for back-compat with code paths
    that don't pass a tab explicitly (e.g. search daemon).  The listing daemon
    uses the round-robin ``tabs`` pool (size = ``LISTING_TAB_POOL`` env, default
    3) so multiple URLs can be processed concurrently.
    """

    def __init__(
        self,
        headless: bool = True,
        page_wait_seconds: float = 1.0,
        pinchtab_config: dict | None = None,
    ):
        self.headless = headless
        self.page_wait_seconds = page_wait_seconds
        self.tab = None
        self.tabs: list = []            # tab pool (round-robin)
        self.tab_pool_size: int = int(os.environ.get("LISTING_TAB_POOL", "3"))
        self.tab_pool_lock = asyncio.Lock()
        self._next_idx: int = 0
        self.pinchtab_config = pinchtab_config or {}
        self._pinchtab = None
        self.logger = logging.getLogger(self.__class__.__name__)

    async def start(self):
        """Attach to a running pinchtab server."""
        from base.pinchtab_client import PinchtabClient, PinchtabConfig
        pt_cfg = PinchtabConfig.from_env_and_config({
            "pinchtab": self.pinchtab_config,
            "page_wait_seconds": self.page_wait_seconds,
            "headless": self.headless,
        })
        self._pinchtab = PinchtabClient(pt_cfg)
        self.logger.info(
            "Starting pinchtab session (instance=%s, token=***%s)",
            pt_cfg.instance_url, pt_cfg.token[-4:] if pt_cfg.token else "(none)",
        )
        await self._pinchtab.start()
        self.logger.info("Pinchtab browser attached")

    async def navigate(self, url: str):
        """Navigate single-slot tab (back-compat for search daemon)."""
        start = asyncio.get_running_loop().time()
        self.tab = await self._pinchtab.navigate(url)
        elapsed = asyncio.get_running_loop().time() - start
        self.logger.info(
            f"Pinchtab navigation complete in {elapsed:.2f}s (wait={self.page_wait_seconds:.2f}s)"
        )
        return self.tab

    async def acquire_tab(self):
        """Return a tab from the pool, creating one lazily up to
        ``tab_pool_size``.  New tabs land on ``about:blank``; the caller
        navigates to the real URL.  Reused tabs are returned round-robin
        without navigation (avoids double page-loads — the caller navigates
        in the same step).  New tabs are created with the unscoped
        ``/navigate`` endpoint — never reuse ``self.tab`` here, or every
        acquire would return the same tab.
        """
        async with self.tab_pool_lock:
            if len(self.tabs) < self.tab_pool_size:
                tab = await self._pinchtab.navigate("about:blank")
                self.tabs.append(tab)
                return tab
            tab = self.tabs[self._next_idx % len(self.tabs)]
            self._next_idx += 1
        return tab

    async def cleanup(self):
        """Single-tab cleanup (back-compat for search daemon / older callers)."""
        if self._pinchtab:
            try:
                await self._pinchtab.cleanup()
            except Exception as e:
                self.logger.warning("Pinchtab cleanup error: %s", e)
        self._pinchtab = None
        self.tab = None
        self.tabs.clear()
        self.logger.info("Pinchtab session cleaned up")

    async def cleanup_all(self):
        """Close every tab in the pool via the real pinchtab close endpoint,
        then tear down the pinchtab session.

        Called by the listing daemon's wall-clock browser restart.  Each
        pooled tab is closed with ``POST /tabs/{id}/close`` — the previous
        ``navigate("about:blank")`` reset actually *created* a new tab and
        dropped the reference, leaking the old pool tabs in Chrome across
        restarts (3 tabs per 1h restart — a real leak behind the OOM).
        """
        async with self.tab_pool_lock:
            for t in list(self.tabs):
                try:
                    await t._client.close_tab(tab=t)
                except RuntimeError:
                    pass
                except Exception as e:
                    self.logger.warning("tab close error: %s", e)
            self.tabs.clear()
            self._next_idx = 0
        if self._pinchtab:
            try:
                await self._pinchtab.cleanup()
            except Exception as e:
                self.logger.warning("Pinchtab cleanup error: %s", e)
        self._pinchtab = None
        self.tab = None
        self.logger.info("Pinchtab session cleaned up (all tabs)")

    async def close_tab(self):
        """Close the current single-slot tab to prevent tab buildup."""
        if self._pinchtab and self.tab:
            try:
                await self._pinchtab.close_tab()
            except Exception as e:
                self.logger.warning("close_tab error: %s", e)
        self.tab = None
