from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import asyncio
import logging


class BaseScraper(ABC):
    """Abstract base class for all scrapers"""

    def __init__(self, config: Dict, **kwargs):
        self.config = config
        self.pagination_strategy = None
        self.extraction_strategy = None
        self.output_strategy = None
        self.browser = None
        self.tab = None
        self.seen_items = set()
        self.total_results = 0

        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def scrape(self, query: str):
        """Main scraping method to be implemented by subclasses"""
        pass

    @abstractmethod
    async def start_browser(self):
        """Start browser instance"""
        pass

    @abstractmethod
    async def navigate_to_search(self, url: str):
        """Navigate to search URL"""
        pass

    @abstractmethod
    async def cleanup(self):
        """Clean up browser resources"""
        pass
