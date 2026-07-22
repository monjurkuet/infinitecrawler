import logging
from typing import List

from base.strategies import NavigationStrategy


class TabNavigationStrategy(NavigationStrategy):
    def __init__(self, browser_manager, config: dict):
        super().__init__(browser_manager, config)
        self.tabs_config = (
            self.config.get("navigation", {}).get("config", {}).get("tabs", [])
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    async def navigate_to_section(self, section_name: str) -> bool:
        tab_config = self._get_tab_config(section_name)
        if not tab_config:
            self.logger.warning(f"No configuration found for tab: {section_name}")
            return False

        tab = self.browser_manager.tab
        selector = tab_config.get("selector")
        if not selector:
            self.logger.warning(f"No selector configured for tab: {section_name}")
            return False

        try:
            tab_element = await tab.select(selector, timeout=tab_config.get("timeout", 5))
            if not tab_element:
                self.logger.warning(f"Tab element not found: {selector}")
                return False

            aria_selected = tab_element.attrs.get("aria-selected")
            if aria_selected == "true":
                return True

            await tab_element.click()

            wait_selector = tab_config.get("wait_for_selector")
            max_wait = tab_config.get("max_wait", 5)
            if wait_selector:
                try:
                    await tab.wait_for(wait_selector, timeout=max_wait)
                except Exception as e:
                    self.logger.warning(f"Timeout waiting for content in tab '{section_name}': {e}")

            return True
        except Exception as e:
            self.logger.error(f"Error navigating to tab '{section_name}': {e}")
            return False

    async def get_available_sections(self) -> List[str]:
        return [tab.get("name") for tab in self.tabs_config if tab.get("name")]

    def _get_tab_config(self, section_name: str) -> dict:
        normalized_section = section_name.strip().lower()
        for tab in self.tabs_config:
            tab_name = (tab.get("name") or "").strip().lower()
            if tab_name == normalized_section:
                return tab
        return {}
