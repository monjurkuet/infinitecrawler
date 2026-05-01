import asyncio
import logging
from typing import Optional
import nodriver as uc


class BrowserManager:
    """Manages browser automation with different engines"""

    def __init__(
        self, engine: str = "nodriver", headless: bool = True, page_wait_seconds: float = 1.0
    ):
        self.engine = engine
        self.headless = headless
        self.page_wait_seconds = page_wait_seconds
        self.browser = None
        self.tab = None
        self.logger = logging.getLogger(self.__class__.__name__)

    async def start(self):
        """Start browser instance"""
        if self.engine == "nodriver":
            self.logger.info("Starting nodriver browser...")
            self.browser = await uc.start(headless=self.headless)
            self.logger.info("Browser started successfully")
        else:
            raise ValueError(f"Unsupported browser engine: {self.engine}")

    async def navigate(self, url: str):
        """Navigate to URL and return tab"""
        if self.engine == "nodriver":
            start = asyncio.get_running_loop().time()
            self.tab = await self.browser.get(url)
            if self.page_wait_seconds > 0:
                await self.tab.wait(self.page_wait_seconds)
            elapsed = asyncio.get_running_loop().time() - start
            self.logger.info(
                f"Navigation complete in {elapsed:.2f}s (wait={self.page_wait_seconds:.2f}s)"
            )
            return self.tab
        else:
            raise ValueError(f"Unsupported browser engine: {self.engine}")

    async def cleanup(self):
        """Clean up browser resources"""
        if self.browser:
            self.browser.stop()
            self.logger.info("Browser stopped successfully")
