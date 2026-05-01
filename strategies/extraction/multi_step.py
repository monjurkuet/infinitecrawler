"""Multi-step extraction strategy for complex data extraction pipelines"""

import logging
import re
import asyncio
from typing import Any, Dict, List, Optional

from base.strategies import ExtractionStrategy


class MultiStepExtractionStrategy(ExtractionStrategy):
    """
    Execute a pipeline of extraction steps.
    Supports: extract, navigate, extract_url, transform, conditional actions
    Features: retry logic, fallback selectors, exponential backoff
    """

    def __init__(self, browser_manager, config: dict):
        super().__init__(browser_manager, config)
        self.steps = (
            self.config.get("extraction", {}).get("config", {}).get("steps", [])
        )
        self.logger = logging.getLogger(self.__class__.__name__)

        # Retry configuration
        self.retry_config = self.config.get("extraction", {}).get("retry", {})
        self.max_retries = self.retry_config.get("attempts", 3)
        self.base_delay = self.retry_config.get("delay", 2)
        self.backoff = self.retry_config.get(
            "backoff", "exponential"
        )  # exponential or linear

        # Timeout configuration
        self.timeout_config = self.config.get("extraction", {}).get("timeouts", {})
        self.page_load_timeout = self.timeout_config.get("page_load", 10)
        self.element_timeout = self.timeout_config.get("element", 5)

        self.navigation_strategy = None
        self._init_navigation_strategy()

    def _init_navigation_strategy(self):
        """Initialize navigation strategy if configured"""
        nav_config = self.config.get("navigation")
        if nav_config:
            from factory.scraper_factory import ScraperFactory

            nav_strategy_name = nav_config.get("strategy", "tab_navigator")
            self.navigation_strategy = ScraperFactory.create_strategy(
                "navigation", nav_strategy_name, self.browser_manager, self.config
            )

    async def extract_items(self) -> List[Dict]:
        """
        Execute extraction pipeline.
        Returns list with single dictionary containing all extracted data.
        """
        data = {}
        context = {
            "url": self.browser_manager.tab.url if self.browser_manager.tab else None,
            "current_section": None,
        }

        step_num = 0
        for step_num, step in enumerate(self.steps, 1):
            action = step.get("action")
            self.logger.debug(f"Executing step {step_num}: {action}")

            try:
                if action == "extract":
                    extracted = await self._execute_extract_step(step, context)
                    data.update(extracted)

                elif action == "navigate":
                    success = await self._execute_navigate_step(step, context)
                    if not success and step.get("required", False):
                        self.logger.warning(
                            f"Required navigation failed at step {step_num}, stopping pipeline"
                        )
                        break

                elif action == "extract_url":
                    url_data = self._execute_extract_url_step(step, context)
                    data.update(url_data)

                elif action == "transform":
                    data = self._execute_transform_step(step, data)

                elif action == "conditional":
                    should_continue = await self._execute_conditional_step(step, data)
                    if not should_continue:
                        self.logger.info(
                            f"Conditional at step {step_num} triggered skip"
                        )
                        return []

                else:
                    self.logger.warning(f"Unknown action type: {action}")

            except Exception as e:
                self.logger.error(f"Error in step {step_num} ({action}): {e}")
                # Continue with next step unless configured otherwise
                if step.get("stop_on_error", False):
                    break

        # Add extraction metadata
        data["_extraction_meta"] = {
            "url": context.get("url"),
            "steps_executed": step_num,
        }

        return [data]

    async def _extract_field_with_retry(
        self, tab, field_config: dict, field_name: str
    ) -> Any:
        """
        Extract a single field with retry logic and fallback selectors.
        """
        selectors = field_config.get("selectors", [field_config.get("selector")])
        if not selectors[0]:
            raise ValueError(f"No selector specified for field '{field_name}'")

        retry_config = field_config.get("retry")
        if retry_config is False:
            return await self._extract_field_once(
                tab, selectors, field_config, field_name
            )

        if isinstance(retry_config, dict):
            field_retry_enabled = retry_config.get("enabled", True)
            field_attempts = retry_config.get("attempts", self.max_retries)
            field_delay = retry_config.get("delay", self.base_delay)
            field_backoff = retry_config.get("backoff", self.backoff)
        else:
            field_retry_enabled = self.retry_config.get("enabled", True)
            field_attempts = self.max_retries
            field_delay = self.base_delay
            field_backoff = self.backoff

        if not field_retry_enabled:
            return await self._extract_field_once(
                tab, selectors, field_config, field_name
            )

        extract_type = field_config.get("type", "text")
        last_error = None

        for attempt in range(field_attempts):
            for selector in selectors:
                if not selector:
                    continue

                try:
                    value = await self._extract_single_field(
                        tab, selector, extract_type, field_config, field_name
                    )

                    if value is not None:
                        self.logger.debug(
                            f"✓ Extracted '{field_name}' (attempt {attempt + 1})"
                        )
                        return value

                except Exception as e:
                    last_error = e
                    self.logger.debug(
                        f"✗ Failed attempt {attempt + 1} for '{field_name}' with selector '{selector}': {e}"
                    )

            # Wait before retry with exponential backoff
            if attempt < field_attempts - 1:
                delay = (
                    field_delay * (2**attempt)
                    if field_backoff == "exponential"
                    else field_delay * (attempt + 1)
                )
                self.logger.debug(
                    f"Waiting {delay}s before retry for '{field_name}'..."
                )
                await asyncio.sleep(delay)

        # All retries failed
        self.logger.warning(
            f"⚠ All {field_attempts} retries failed for '{field_name}': {last_error}"
        )
        return None

    async def _extract_field_once(
        self, tab, selectors: List[str], field_config: dict, field_name: str
    ) -> Any:
        """Try each selector once without retry backoff."""
        extract_type = field_config.get("type", "text")
        last_error = None

        for selector in selectors:
            if not selector:
                continue

            try:
                value = await self._extract_single_field(
                    tab, selector, extract_type, field_config, field_name
                )
                if value is not None:
                    return value
            except Exception as e:
                last_error = e
                self.logger.debug(
                    f"Failed single-pass extraction for '{field_name}' with selector '{selector}': {e}"
                )

        self.logger.debug(f"No selector matched for '{field_name}': {last_error}")
        return None

    async def _extract_single_field(
        self, tab, selector: str, extract_type: str, field_config: dict, field_name: str
    ) -> Any:
        """Extract a single field with a single selector"""
        timeout = field_config.get("timeout", self.element_timeout)

        if extract_type == "text":
            element = await tab.select(selector, timeout=timeout)
            return element.text if element else None

        elif extract_type == "attribute":
            element = await tab.select(selector, timeout=timeout)
            if not element:
                return None

            attr_name = field_config.get("attribute")
            if not attr_name:
                raise ValueError(f"No attribute specified for field '{field_name}'")

            value = element.attrs.get(attr_name, "")

            # Apply regex if specified
            regex_pattern = field_config.get("regex")
            if regex_pattern and value:
                match = re.search(regex_pattern, value)
                return match.group(1) if match else None

            return value

        elif extract_type == "list":
            sibling_selector = field_config.get("sibling_selector")
            if sibling_selector:
                parent = await tab.select(selector, timeout=timeout)
                if not parent:
                    return []
                elements = await parent.select_all(sibling_selector)
            else:
                elements = await tab.select_all(selector, timeout=timeout)

            return [e.text for e in elements if e.text]

        elif extract_type == "count":
            elements = await tab.select_all(selector, timeout=timeout)
            return len(elements)

        elif extract_type == "exists":
            element = await tab.select(selector, timeout=timeout)
            return element is not None

        else:
            raise ValueError(f"Unknown extract type: {extract_type}")

    async def _execute_extract_step(self, step: dict, context: dict) -> Dict:
        """Extract fields using selectors with retry logic"""
        extracted = {}
        fields = step.get("fields", {})
        tab = self.browser_manager.tab

        # Check if retry is enabled for this step
        use_retry = step.get("retry", self.retry_config.get("enabled", True))

        for field_name, field_config in fields.items():
            try:
                if use_retry:
                    value = await self._extract_field_with_retry(
                        tab, field_config, field_name
                    )
                else:
                    value = await self._extract_single_field(
                        tab,
                        field_config.get("selector", ""),
                        field_config.get("type", "text"),
                        field_config,
                        field_name,
                    )
                extracted[field_name] = value

            except Exception as e:
                # Skip field on error but log detailed error
                extracted[field_name] = None
                extracted[f"_{field_name}_error"] = str(e)
                self.logger.warning(f"Failed to extract field '{field_name}': {e}")

        return extracted

    async def _extract_field(self, tab, field_config: dict, field_name: str) -> Any:
        """Extract a single field based on configuration"""
        selector = field_config.get("selector")
        extract_type = field_config.get("type", "text")

        if not selector:
            raise ValueError(f"No selector specified for field '{field_name}'")

        if extract_type == "text":
            element = await tab.select(selector, timeout=field_config.get("timeout", 2))
            return element.text if element else None

        elif extract_type == "attribute":
            element = await tab.select(selector, timeout=field_config.get("timeout", 2))
            if not element:
                return None

            attr_name = field_config.get("attribute")
            if not attr_name:
                raise ValueError(f"No attribute specified for field '{field_name}'")

            value = element.attrs.get(attr_name, "")

            # Apply regex if specified
            regex_pattern = field_config.get("regex")
            if regex_pattern and value:
                match = re.search(regex_pattern, value)
                return match.group(1) if match else None

            return value

        elif extract_type == "list":
            sibling_selector = field_config.get("sibling_selector")
            if sibling_selector:
                # Extract from sibling elements
                parent = await tab.select(
                    selector, timeout=field_config.get("timeout", 2)
                )
                if not parent:
                    return []
                elements = await parent.select_all(sibling_selector)
            else:
                elements = await tab.select_all(
                    selector, timeout=field_config.get("timeout", 2)
                )

            return [e.text for e in elements if e.text]

        elif extract_type == "count":
            elements = await tab.select_all(
                selector, timeout=field_config.get("timeout", 2)
            )
            return len(elements)

        elif extract_type == "exists":
            element = await tab.select(selector, timeout=field_config.get("timeout", 1))
            return element is not None

        else:
            raise ValueError(f"Unknown extract type: {extract_type}")

    async def _execute_navigate_step(self, step: dict, context: dict) -> bool:
        """Navigate to a page section"""
        if not self.navigation_strategy:
            self.logger.warning("Navigation step but no navigation strategy configured")
            return False

        section_name = step.get("section")
        if not section_name:
            self.logger.warning("Navigation step missing 'section' name")
            return False

        success = await self.navigation_strategy.navigate_to_section(section_name)
        if success:
            context["current_section"] = section_name

        return success

    def _execute_extract_url_step(self, step: dict, context: dict) -> Dict:
        """Extract data from URL using regex patterns"""
        url = context.get("url", "")
        if not url:
            self.logger.warning("Cannot extract from URL: no URL in context")
            return {}

        extracted = {}
        fields = step.get("fields", {})

        for field_name, field_config in fields.items():
            try:
                pattern = field_config.get("pattern")
                if not pattern:
                    self.logger.warning(
                        f"No pattern specified for URL field '{field_name}'"
                    )
                    extracted[field_name] = None
                    continue

                match = re.search(pattern, url)
                extracted[field_name] = match.group(1) if match else None

            except Exception as e:
                extracted[field_name] = None
                extracted[f"_{field_name}_error"] = str(e)
                self.logger.warning(f"Failed to extract URL field '{field_name}': {e}")

        return extracted

    def _execute_transform_step(self, step: dict, data: Dict) -> Dict:
        """Apply transformations to extracted data"""
        field = step.get("field")
        operation = step.get("operation")

        if not field or not operation:
            self.logger.warning("Transform step missing 'field' or 'operation'")
            return data

        value = data.get(field)

        try:
            if operation == "normalize_phone":
                data[field] = self._normalize_phone(value)
            elif operation == "strip":
                data[field] = value.strip() if value else value
            elif operation == "lowercase":
                data[field] = value.lower() if value else value
            elif operation == "uppercase":
                data[field] = value.upper() if value else value
            elif operation == "remove_commas":
                data[field] = value.replace(",", "") if value else value
            elif operation == "extract_number":
                if value:
                    match = re.search(r"[\d,]+", str(value))
                    data[field] = match.group(0).replace(",", "") if match else None
            else:
                self.logger.warning(f"Unknown transform operation: {operation}")

        except Exception as e:
            self.logger.error(f"Transform failed for field '{field}': {e}")

        return data

    async def _execute_conditional_step(self, step: dict, data: Dict) -> bool:
        """
        Evaluate conditional expression.
        Returns False if pipeline should stop (e.g., skip this URL).
        """
        condition = step.get("if")
        if not condition:
            return True

        try:
            # Simple condition evaluation
            # Support: field > value, field < value, field == value, field exists
            result = self._evaluate_condition(condition, data)

            if result:
                action = step.get("then", "continue")
                if action == "skip":
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Condition evaluation failed: {e}")
            return True  # Continue on error

    def _evaluate_condition(self, condition: str, data: Dict) -> bool:
        """Evaluate a simple condition expression"""
        # Parse condition like "rating < 3.0" or "category == 'Restaurant'"

        # Support 'exists' check
        if " exists" in condition:
            field = condition.replace(" exists", "").strip()
            return field in data and data[field] is not None

        # Comparison operators
        for op in ["<=", ">=", "==", "!=", "<", ">"]:
            if op in condition:
                parts = condition.split(op)
                if len(parts) == 2:
                    field = parts[0].strip()
                    value_str = parts[1].strip()

                    # Get field value
                    field_value = data.get(field)
                    if field_value is None:
                        return False

                    # Parse comparison value and perform comparison
                    try:
                        # Try numeric comparison
                        numeric_value = float(value_str)
                        numeric_field_value = float(field_value)

                        # Numeric comparison
                        if op == "<=":
                            return numeric_field_value <= numeric_value
                        elif op == ">=":
                            return numeric_field_value >= numeric_value
                        elif op == "==":
                            return numeric_field_value == numeric_value
                        elif op == "!=":
                            return numeric_field_value != numeric_value
                        elif op == "<":
                            return numeric_field_value < numeric_value
                        elif op == ">":
                            return numeric_field_value > numeric_value
                    except (ValueError, TypeError):
                        # String comparison
                        str_value = value_str.strip("'\"")
                        str_field_value = str(field_value)

                        if op == "<=":
                            return str_field_value <= str_value
                        elif op == ">=":
                            return str_field_value >= str_value
                        elif op == "==":
                            return str_field_value == str_value
                        elif op == "!=":
                            return str_field_value != str_value
                        elif op == "<":
                            return str_field_value < str_value
                        elif op == ">":
                            return str_field_value > str_value

        return False

    def _normalize_phone(self, phone: Optional[str]) -> Optional[str]:
        """Normalize phone number to E.164 format"""
        if not phone:
            return None

        # Remove all non-numeric characters
        digits = re.sub(r"\D", "", phone)

        # Add country code if missing (assume US/Canada)
        if len(digits) == 10:
            digits = "1" + digits

        return digits if digits else None
