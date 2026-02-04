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

            # Debug logging
            self.logger.debug(f"Config keys: {list(self.config.keys())}")
            self.logger.debug(f"Selectors config: {selectors_config}")
            self.logger.debug(f"Items selector: {items_selector}")
            self.logger.debug(f"Fields config: {fields_config}")

            # Find all item elements
            item_elements = await tab.select_all(items_selector)
            self.logger.info(f"Found {len(item_elements)} item elements")

            if not item_elements:
                self.logger.warning(
                    f"No elements found with selector: {items_selector}"
                )
                return []

            items = []
            for i, element in enumerate(item_elements):
                try:
                    item = {}

                    # Debug: show element attributes
                    if i < 3:  # Only log first 3 for debugging
                        self.logger.debug(f"Element {i} attrs: {dict(element.attrs)}")

                    # Extract each field based on configuration
                    for field_name, selector_or_attr in fields_config.items():
                        if selector_or_attr == "text":
                            value = element.text
                        elif selector_or_attr == "html":
                            value = element.html
                        else:
                            # Assume it's an attribute name
                            value = element.attrs.get(selector_or_attr, "")
                            if not value:
                                # Try getting from element directly
                                value = getattr(element, selector_or_attr, "")

                        item[field_name] = value

                    # Debug: show extracted item
                    if i < 3:
                        self.logger.debug(f"Extracted item {i}: {item}")

                    # Only add items with at least one field
                    if any(item.values()):
                        items.append(item)
                    else:
                        self.logger.debug(f"Skipping item {i} - no values: {item}")

                except Exception as e:
                    self.logger.warning(f"Error extracting item data: {e}")
                    continue

            self.logger.info(f"Returning {len(items)} extracted items")
            return items

        except Exception as e:
            self.logger.error(f"Error extracting items: {e}")
            return []
