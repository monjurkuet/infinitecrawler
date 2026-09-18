from base.strategies import ExtractionStrategy
import logging
from typing import Dict, List


class GenericSelectorExtractionStrategy(ExtractionStrategy):
    """Generic extraction strategy using CSS selectors from config."""

    def __init__(self, browser_manager, config: dict):
        self.browser_manager = browser_manager
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    async def _count_via_js(self, tab, selector: str) -> int:
        """Count elements via JS when nodriver's select_all fails."""
        try:
            # Escape single quotes in selector for JS injection
            escaped = selector.replace("'", "\\'")
            count = await tab.evaluate(
                f"document.querySelectorAll('{escaped}').length"
            )
            return int(count) if count else 0
        except Exception as e:
            self.logger.debug(f"JS element count failed: {e}")
            return 0

    async def _extract_via_js(self, tab, selector: str, fields_config: dict) -> List[Dict]:
        """
        Fallback extraction using JavaScript when nodriver's select_all
        returns 0 elements (e.g. deep/shadow DOM traversal failures).
        """
        try:
            escaped = selector.replace("'", "\\'")
            items = []
            count = await self._count_via_js(tab, selector)
            self.logger.info(f"JS fallback: found {count} elements for '{selector}'")

            for i in range(count):
                item = {}
                for field_name, selector_or_attr in fields_config.items():
                    try:
                        if selector_or_attr == "text":
                            js = (
                                f"document.querySelectorAll('{escaped}')[{i}]"
                                f".innerText"
                            )
                        elif selector_or_attr == "html":
                            js = (
                                f"document.querySelectorAll('{escaped}')[{i}]"
                                f".innerHTML"
                            )
                        else:
                            # It's an attribute name
                            js = (
                                f"document.querySelectorAll('{escaped}')[{i}]"
                                f".getAttribute('{selector_or_attr}')"
                            )
                        value = await tab.evaluate(js)
                        if value:
                            item[field_name] = value
                    except Exception as e:
                        self.logger.debug(
                            f"JS fallback: failed to extract {field_name} "
                            f"for item {i}: {e}"
                        )

                if any(item.values()):
                    items.append(item)

            return items
        except Exception as e:
            self.logger.warning(f"JS fallback extraction failed: {e}")
            return []

    async def extract_items(self) -> List[Dict]:
        """Extract items using configured selectors."""
        try:
            tab = self.browser_manager.tab
            if not tab:
                self.logger.error("No tab available for extraction")
                return []

            # Get selectors from config
            selectors_config = self.config.get("selectors", {})
            items_selector = selectors_config.get("items", "a.hfpxzc")
            fields_config = selectors_config.get("fields", {})

            self.logger.debug(f"Items selector: {items_selector}")
            self.logger.debug(f"Fields config: {fields_config}")

            # Find all item elements using nodriver's DOM traversal
            item_elements = await tab.select_all(items_selector)
            self.logger.info(f"Found {len(item_elements)} item elements via select_all")

            # Fallback: if select_all returned 0, try JS-based extraction
            if not item_elements:
                js_count = await self._count_via_js(tab, items_selector)
                if js_count > 0:
                    self.logger.info(
                        f"select_all returned 0 but JS found {js_count} elements. "
                        f"Using JS fallback extraction."
                    )
                    return await self._extract_via_js(
                        tab, items_selector, fields_config
                    )
                self.logger.warning(
                    f"No elements found with selector: {items_selector}"
                )
                return []

            items = []
            for i, element in enumerate(item_elements):
                try:
                    item = {}

                    # Debug: show element attributes
                    if i < 3:
                        self.logger.debug(
                            f"Element {i} attrs: {dict(element.attrs)}"
                        )

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
                        self.logger.debug(
                            f"Skipping item {i} - no values: {item}"
                        )

                except Exception as e:
                    self.logger.warning(f"Error extracting item data: {e}")
                    continue

            self.logger.info(f"Returning {len(items)} extracted items")
            return items

        except Exception as e:
            self.logger.error(f"Error extracting items: {e}")
            return []
