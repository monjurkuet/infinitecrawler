from abc import ABC, abstractmethod
from typing import Dict, List, Optional


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
