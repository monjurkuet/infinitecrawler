from base.strategies import ExtractionStrategy
import logging
from typing import Dict, List


class GenericSelectorExtractionStrategy(ExtractionStrategy):
    """Generic extraction strategy using CSS selectors from config"""

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    async def extract_items(self) -> List[Dict]:
        """Extract items using configured selectors"""
        try:
            tab = self.browser_manager.tab
            if not tab:
                self.logger.error("No tab available for extraction")
                return []

            # Get selectors from config
            selectors_config = self.config.get("selectors", {})
            items_selector = selectors_config.get("items", "a.hfpxzc")
            fields_config = selectors_config.get("fields", {})

            # Find all item elements
            item_elements = await tab.select_all(items_selector)
            self.logger.info(f"Found {len(item_elements)} item elements")

            items = []
            for element in item_elements:
                try:
                    item = {}

                    # Extract each field based on configuration
                    for field_name, selector_or_attr in fields_config.items():
                        if selector_or_attr == "text":
                            item[field_name] = element.text
                        elif selector_or_attr == "html":
                            item[field_name] = element.html
                        else:
                            # Assume it's an attribute name
                            item[field_name] = element.attrs.get(selector_or_attr, "")

                    # Only add items with at least one field
                    if any(item.values()):
                        items.append(item)

                except Exception as e:
                    self.logger.warning(f"Error extracting item data: {e}")
                    continue

            return items

        except Exception as e:
            self.logger.error(f"Error extracting items: {e}")
            return []
