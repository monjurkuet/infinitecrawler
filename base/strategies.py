from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import logging


class PaginationStrategy(ABC):
    @abstractmethod
    async def has_more_results(self) -> bool:
        pass

    @abstractmethod
    async def load_more_results(self) -> bool:
        pass


class ExtractionStrategy(ABC):
    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def extract_items(self) -> List[Dict]:
        pass


class OutputStrategy(ABC):
    @abstractmethod
    async def write_item(self, item: Dict):
        pass

    @abstractmethod
    def has_reached_limit(self) -> bool:
        pass


class QueueStrategy(ABC):
    @abstractmethod
    def enqueue(self, urls: List[str]) -> int:
        pass

    @abstractmethod
    def dequeue(self, timeout: int = 5) -> Optional[str]:
        pass

    @abstractmethod
    def mark_completed(self, url: str):
        pass

    @abstractmethod
    def mark_failed(self, url: str, error: str, retry_count: int = 0):
        pass

    @abstractmethod
    def get_stats(self) -> Dict:
        pass


class NavigationStrategy(ABC):
    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def navigate_to_section(self, section_name: str) -> bool:
        pass

    @abstractmethod
    async def get_available_sections(self) -> List[str]:
        pass
