import asyncio
import logging
from typing import Optional
import nodriver as uc


class BrowserManager:
    """Manages browser automation with different engines"""

    def __init__(self, engine: str = "nodriver", headless: bool = True):
        self.engine = engine
        self.browser = None
        self.tab = None
        self.logger = logging.getLogger(self.__class__.__name__)

    async def start(self):
        """Start browser instance"""
        if self.engine == "nodriver":
            self.logger.info("Starting nodriver browser...")
            self.browser = await uc.start(headless=True)
            self.logger.info("Browser started successfully")
        else:
            raise ValueError(f"Unsupported browser engine: {self.engine}")

    async def navigate(self, url: str):
        """Navigate to URL and return tab"""
        if self.engine == "nodriver":
            self.tab = await self.browser.get(url)
            await self.tab.wait(5)  # Wait for page to load
            return self.tab
        else:
            raise ValueError(f"Unsupported browser engine: {self.engine}")

    async def cleanup(self):
        """Clean up browser resources"""
        if self.browser:
            self.browser.stop()
            self.logger.info("Browser stopped successfully")
