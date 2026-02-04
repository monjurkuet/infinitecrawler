from abc import ABC, abstractmethod
from typing import Dict, Iterator, List, Optional


class PaginationStrategy(ABC):
    """Abstract base class for pagination strategies"""

    @abstractmethod
    async def has_more_results(self) -> bool:
        """Check if there are more results to load"""
        pass

    @abstractmethod
    async def load_more_results(self) -> bool:
        """Load more results and return True if successful"""
        pass


class ExtractionStrategy(ABC):
    """Abstract base class for extraction strategies"""

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def extract_items(self) -> List[Dict]:
        """Extract items from current page and return list of dictionaries"""
        pass


class OutputStrategy(ABC):
    """Abstract base class for output strategies"""

    @abstractmethod
    async def write_item(self, item: Dict):
        """Write a single item to output"""
        pass

    @abstractmethod
    def has_reached_limit(self) -> bool:
        """Check if the output has reached its maximum limit"""
        pass


class InputStrategy(ABC):
    """Abstract base class for input strategies - load URLs to process"""

    @abstractmethod
    def load_urls(self) -> Iterator[str]:
        """Yield URLs to process from the input source"""
        pass

    @abstractmethod
    def get_total_count(self) -> Optional[int]:
        """Return total URL count if known, None otherwise"""
        pass


class QueueStrategy(ABC):
    """Abstract base class for queue strategies - manage URL queue"""

    @abstractmethod
    def enqueue(self, urls: List[str]) -> int:
        """Add URLs to queue, return count actually added (after deduplication)"""
        pass

    @abstractmethod
    def dequeue(self, timeout: int = 5) -> Optional[str]:
        """Get next URL from queue with blocking wait, return None if empty"""
        pass

    @abstractmethod
    def mark_completed(self, url: str):
        """Mark URL as successfully processed"""
        pass

    @abstractmethod
    def mark_failed(self, url: str, error: str, retry_count: int = 0):
        """Mark URL as failed with error info and retry count"""
        pass

    @abstractmethod
    def get_stats(self) -> Dict:
        """Return queue statistics (pending, processing, completed, failed counts)"""
        pass


class NavigationStrategy(ABC):
    """Abstract base class for navigation strategies - navigate page sections"""

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def navigate_to_section(self, section_name: str) -> bool:
        """Navigate to named section, return True if successful"""
        pass

    @abstractmethod
    async def get_available_sections(self) -> List[str]:
        """Return list of available section names"""
        pass


import logging  # noqa: E402
