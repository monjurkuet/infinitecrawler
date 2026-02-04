"""Input strategies for loading URLs to process"""

import logging
from typing import Iterator, Optional, Set

from base.strategies import InputStrategy


class FileInputStrategy(InputStrategy):
    """Load URLs from a text file, one URL per line"""

    def __init__(self, config: dict):
        # config is the input section from the main config
        # e.g., {strategy: "file_url_loader", config: {file_path: "..."}}
        self.config = config.get("config", {})
        self.file_path = self.config.get("file_path")
        self.deduplicate = self.config.get("deduplicate", True)
        self.batch_size = self.config.get("batch_size", 1000)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._seen_urls: Set[str] = set()
        self._total_count: Optional[int] = None

    def load_urls(self) -> Iterator[str]:
        """
        Yield URLs from file.
        Skips empty lines and lines starting with # (comments).
        Optionally deduplicates URLs.
        """
        if not self.file_path:
            raise ValueError("file_path is required for FileInputStrategy")

        self.logger.info(f"Loading URLs from {self.file_path}")

        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    url = line.strip()

                    # Skip empty lines and comments
                    if not url or url.startswith("#"):
                        continue

                    # Deduplicate if enabled
                    if self.deduplicate:
                        if url in self._seen_urls:
                            self.logger.debug(
                                f"Skipping duplicate URL at line {line_num}"
                            )
                            continue
                        self._seen_urls.add(url)

                    self.logger.debug(
                        f"Yielding URL from line {line_num}: {url[:80]}..."
                    )
                    yield url

        except FileNotFoundError:
            self.logger.error(f"Input file not found: {self.file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading input file: {e}")
            raise

    def get_total_count(self) -> Optional[int]:
        """
        Return total URL count if file can be read.
        Note: This reads the file twice, so use sparingly.
        """
        if self._total_count is not None:
            return self._total_count

        try:
            count = 0
            with open(self.file_path, "r", encoding="utf-8") as f:
                for line in f:
                    url = line.strip()
                    if url and not url.startswith("#"):
                        if not self.deduplicate or url not in self._seen_urls:
                            count += 1
            self._total_count = count
            return count
        except Exception as e:
            self.logger.warning(f"Could not count URLs in file: {e}")
            return None
