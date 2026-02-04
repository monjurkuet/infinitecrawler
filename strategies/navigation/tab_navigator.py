"""Navigation strategies for navigating page sections"""

import logging
from typing import List

from base.strategies import NavigationStrategy


class TabNavigationStrategy(NavigationStrategy):
    """
    Navigate between tabs on a page.
    Useful for sites like Google Maps, LinkedIn, etc.
    """

    def __init__(self, browser_manager, config: dict):
        super().__init__(browser_manager, config)
        self.tabs_config = (
            self.config.get("navigation", {}).get("config", {}).get("tabs", [])
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    async def navigate_to_section(self, section_name: str) -> bool:
        """
        Navigate to a tab by name.
        Returns True if successful, False if tab not found or navigation failed.
        """
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
            self.logger.debug(
                f"Navigating to tab '{section_name}' with selector: {selector}"
            )

            # Find tab element
            tab_element = await tab.select(
                selector, timeout=tab_config.get("timeout", 5)
            )
            if not tab_element:
                self.logger.warning(f"Tab element not found: {selector}")
                return False

            # Check if already active
            aria_selected = tab_element.attrs.get("aria-selected")
            if aria_selected == "true":
                self.logger.debug(f"Tab '{section_name}' already active")
                return True

            # Click tab
            await tab_element.click()
            self.logger.debug(f"Clicked tab '{section_name}'")

            # Wait for content to load
            wait_selector = tab_config.get("wait_for_selector")
            max_wait = tab_config.get("max_wait", 5)

            if wait_selector:
                try:
                    await tab.wait_for(wait_selector, timeout=max_wait)
                    self.logger.debug(f"Content loaded for tab '{section_name}'")
                except Exception as e:
                    self.logger.warning(
                        f"Timeout waiting for content in tab '{section_name}': {e}"
                    )
                    # Still return True since we clicked the tab

            return True

        except Exception as e:
            self.logger.error(f"Error navigating to tab '{section_name}': {e}")
            return False

    async def get_available_sections(self) -> List[str]:
        """Return list of configured tab names"""
        return [tab.get("name") for tab in self.tabs_config if tab.get("name")]

    def _get_tab_config(self, section_name: str) -> dict:
        """Get configuration for a specific tab"""
        for tab in self.tabs_config:
            if tab.get("name") == section_name:
                return tab
        return {}


class AccordionNavigationStrategy(NavigationStrategy):
    """
    Navigate accordion/collapsible sections.
    Useful for expanding sections to extract hidden content.
    """

    def __init__(self, browser_manager, config: dict):
        super().__init__(browser_manager, config)
        self.sections_config = (
            self.config.get("navigation", {}).get("config", {}).get("sections", [])
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    async def navigate_to_section(self, section_name: str) -> bool:
        """Expand an accordion section by name"""
        section_config = self._get_section_config(section_name)
        if not section_config:
            return False

        tab = self.browser_manager.tab
        selector = section_config.get("selector")
        expand_selector = section_config.get("expand_selector", selector)

        try:
            # Check if already expanded
            content_selector = section_config.get("content_selector")
            if content_selector:
                existing = await tab.select(content_selector, timeout=1)
                if existing:
                    return True

            # Click to expand
            element = await tab.select(expand_selector, timeout=5)
            if element:
                await element.click()

                # Wait for content
                if content_selector:
                    await tab.wait_for(
                        content_selector, timeout=section_config.get("max_wait", 3)
                    )

                return True

            return False

        except Exception as e:
            self.logger.error(f"Error expanding section '{section_name}': {e}")
            return False

    async def get_available_sections(self) -> List[str]:
        """Return list of configured section names"""
        return [
            section.get("name")
            for section in self.sections_config
            if section.get("name")
        ]

    def _get_section_config(self, section_name: str) -> dict:
        """Get configuration for a specific section"""
        for section in self.sections_config:
            if section.get("name") == section_name:
                return section
        return {}


class ModalNavigationStrategy(NavigationStrategy):
    """
    Open modals/dialogs to extract content.
    Useful for popups, lightboxes, etc.
    """

    def __init__(self, browser_manager, config: dict):
        super().__init__(browser_manager, config)
        self.modals_config = (
            self.config.get("navigation", {}).get("config", {}).get("modals", [])
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    async def navigate_to_section(self, section_name: str) -> bool:
        """Open a modal by name"""
        modal_config = self._get_modal_config(section_name)
        if not modal_config:
            return False

        tab = self.browser_manager.tab
        open_selector = modal_config.get("open_selector")

        try:
            # Click to open modal
            element = await tab.select(open_selector, timeout=5)
            if element:
                await element.click()

                # Wait for modal content
                content_selector = modal_config.get("content_selector")
                if content_selector:
                    await tab.wait_for(
                        content_selector, timeout=modal_config.get("max_wait", 5)
                    )

                return True

            return False

        except Exception as e:
            self.logger.error(f"Error opening modal '{section_name}': {e}")
            return False

    async def close_modal(self, section_name: str) -> bool:
        """Close a modal by name"""
        modal_config = self._get_modal_config(section_name)
        if not modal_config:
            return False

        close_selector = modal_config.get("close_selector")
        if not close_selector:
            # Try common close buttons
            close_selectors = [
                "button[aria-label='Close']",
                "button.close",
                ".modal-close",
                "[data-dismiss='modal']",
            ]

            tab = self.browser_manager.tab
            for selector in close_selectors:
                try:
                    element = await tab.select(selector, timeout=1)
                    if element:
                        await element.click()
                        return True
                except:
                    continue

            return False

        try:
            tab = self.browser_manager.tab
            element = await tab.select(close_selector, timeout=3)
            if element:
                await element.click()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error closing modal '{section_name}': {e}")
            return False

    async def get_available_sections(self) -> List[str]:
        """Return list of configured modal names"""
        return [modal.get("name") for modal in self.modals_config if modal.get("name")]

    def _get_modal_config(self, section_name: str) -> dict:
        """Get configuration for a specific modal"""
        for modal in self.modals_config:
            if modal.get("name") == section_name:
                return modal
        return {}
