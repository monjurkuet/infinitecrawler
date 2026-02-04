"""Utility modules for the scraping framework"""

import asyncio
import random
import logging
from typing import Dict, List, Optional, Tuple, Union


class DelayManager:
    """
    Manage realistic, human-like delays between actions.
    Supports multiple delay distributions and context-aware delays.
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)

        # Default delay ranges (in seconds)
        self.default_delays: Dict[str, Tuple[float, float]] = {
            "page_load": (3.0, 6.0),
            "tab_switch": (1.0, 2.0),
            "scroll": (0.5, 1.5),
            "extract": (0.2, 0.8),
            "between_requests": (5.0, 10.0),
            "between_listings": (8.0, 15.0),
        }

    async def apply_delay(self, delay_type: str, custom_config: Optional[Dict] = None):
        """
        Apply a delay of the specified type.

        Args:
            delay_type: Type of delay (e.g., 'page_load', 'between_listings')
            custom_config: Optional custom delay configuration
        """
        config = custom_config or self.config

        # Get delay range from config or use defaults
        delay_range = config.get(
            delay_type, self.default_delays.get(delay_type, (1.0, 3.0))
        )

        # Parse delay range
        if isinstance(delay_range, (list, tuple)) and len(delay_range) == 2:
            min_delay = float(delay_range[0])
            max_delay = float(delay_range[1])
        elif isinstance(delay_range, (int, float)):
            min_delay = max_delay = float(delay_range)
        else:
            min_delay, max_delay = 1.0, 3.0

        # Calculate delay with human-like variation
        distribution = config.get("distribution", "random")

        if distribution == "random":
            delay = random.uniform(min_delay, max_delay)
        elif distribution == "normal":
            # Normal distribution centered between min and max
            mean = (min_delay + max_delay) / 2.0
            std = (max_delay - min_delay) / 4.0
            delay = random.gauss(mean, std)
            delay = max(min_delay, min(max_delay, delay))  # Clamp to range
        elif distribution == "fixed":
            delay = (min_delay + max_delay) / 2.0
        else:
            delay = random.uniform(min_delay, max_delay)

        self.logger.debug(f"Applying {delay_type} delay: {delay:.2f}s")
        await asyncio.sleep(delay)

    async def apply_human_like_pattern(self):
        """
        Apply a human-like pattern of delays with variable pauses.
        Useful for making scraping behavior less predictable.
        """
        # Random small scroll delay
        if random.random() < 0.3:  # 30% chance
            await self.apply_delay("scroll", {"scroll": (0.5, 2.0)})

        # Main delay
        await self.apply_delay("between_requests")

        # Occasional longer pause (10% chance)
        if random.random() < 0.1:
            extra_delay = random.uniform(2.0, 5.0)
            self.logger.debug(f"Adding extra human-like pause: {extra_delay:.2f}s")
            await asyncio.sleep(extra_delay)


class URLProcessor:
    """Utilities for processing and extracting data from URLs"""

    @staticmethod
    def extract_pattern(url: str, pattern: str) -> Optional[str]:
        """
        Extract value from URL using regex pattern.

        Args:
            url: URL to extract from
            pattern: Regex pattern with capture group

        Returns:
            Extracted value or None if no match
        """
        import re

        match = re.search(pattern, url)
        return match.group(1) if match else None

    @staticmethod
    def normalize_url(url: str) -> str:
        """
        Normalize URL by removing tracking parameters and fragments.

        Args:
            url: Original URL

        Returns:
            Normalized URL
        """
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

        parsed = urlparse(url)

        # Remove common tracking parameters
        tracking_params = [
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_term",
            "utm_content",
            "fbclid",
            "gclid",
            "ref",
            "source",
            "campaign",
            "medium",
        ]

        query_params = parse_qs(parsed.query)
        filtered_params = {
            k: v for k, v in query_params.items() if k.lower() not in tracking_params
        }

        new_query = urlencode(filtered_params, doseq=True)

        # Reconstruct URL without fragment
        normalized = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                "",  # Remove fragment
            )
        )

        return normalized

    @staticmethod
    def get_domain(url: str) -> str:
        """Extract domain from URL"""
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return parsed.netloc.lower()

    @staticmethod
    def is_same_domain(url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain"""
        return URLProcessor.get_domain(url1) == URLProcessor.get_domain(url2)
