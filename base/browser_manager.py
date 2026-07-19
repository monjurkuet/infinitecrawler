import asyncio
import logging
from typing import Optional


class BrowserManager:
    """Manages the pinchtab-attached browser.

    The browser runs in an external `pinchtab server` process (port 9868 by
    default, configurable via PINCHTAB_INSTANCE_URL).  We don't launch Chrome
    ourselves — the pinchtab `always-on` supervisor restarts crashed instances
    automatically.  Stability is greatly improved by setting
    `browser.extraFlags` in `/root/.pinchtab/config.json` so Chrome uses
    `--max_old_space_size=2048 --renderer-process-limit=5` (the pinchtab defaults
    OOM on Google Maps).

    `self.tab` continues to expose a Tab interface so the existing
    pagination/extraction strategies work unchanged.
    """

    def __init__(
        self,
        engine: str = "pinchtab",
        headless: bool = True,
        page_wait_seconds: float = 1.0,
        pinchtab_config: dict | None = None,
    ):
        self.engine = engine
        self.headless = headless
        self.page_wait_seconds = page_wait_seconds
        self.tab = None
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
        """Navigate to URL and return a Tab adapter for the scraper."""
        start = asyncio.get_running_loop().time()
        self.tab = await self._pinchtab.navigate(url)
        elapsed = asyncio.get_running_loop().time() - start
        self.logger.info(
            f"Pinchtab navigation complete in {elapsed:.2f}s (wait={self.page_wait_seconds:.2f}s)"
        )
        return self.tab

    async def cleanup(self):
        """Release the HTTP session.  Never kill pinchtab's Chrome — the always-on
        supervisor manages that lifecycle.  Killing from outside desyncs the
        dashboard."""
        if self._pinchtab:
            try:
                await self._pinchtab.cleanup()
            except Exception as e:
                self.logger.warning("Pinchtab cleanup error: %s", e)
            self._pinchtab = None
        self.tab = None
        self.logger.info("Pinchtab session cleaned up")
