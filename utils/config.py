"""Configuration normalization and validation helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, Mapping, Set


class ConfigError(ValueError):
    """Raised when configuration is invalid."""


def _wrap_section(section: Any) -> Dict[str, Any]:
    if not isinstance(section, dict):
        return {}
    return deepcopy(section)


def _normalize_strategy_section(section: Any) -> Dict[str, Any]:
    if not isinstance(section, dict):
        return {}

    normalized = deepcopy(section)

    if "strategies" in normalized:
        return normalized

    if (
        "strategy" in normalized
        and "config" in normalized
        and isinstance(normalized.get("config"), dict)
    ):
        return normalized

    if "strategy" in normalized:
        config = {key: value for key, value in normalized.items() if key != "strategy"}
        return {"strategy": normalized["strategy"], "config": config}

    return normalized


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize legacy and canonical config shapes into a single internal form."""
    normalized = deepcopy(config or {})

    browser = _wrap_section(normalized.get("browser"))
    if "browser_automation" in normalized and "automation" not in browser:
        browser["automation"] = normalized["browser_automation"]
    if "headless" in normalized and "headless" not in browser:
        browser["headless"] = normalized["headless"]
    if "browser_executable_path" in normalized and "executable_path" not in browser:
        browser["executable_path"] = normalized["browser_executable_path"]
    if browser:
        normalized["browser"] = browser

    if "browser" in normalized:
        normalized["browser_automation"] = browser.get("automation", "nodriver")
        normalized["headless"] = browser.get("headless", True)
        normalized["browser_executable_path"] = browser.get("executable_path")

    for section_name in (
        "input",
        "queue",
        "output",
        "secondary_output",
        "navigation",
        "extraction",
    ):
        section = normalized.get(section_name)
        if (
            section_name == "output"
            and isinstance(section, dict)
            and "strategies" in section
        ):
            normalized[section_name] = deepcopy(section)
            continue
        if section_name in normalized:
            normalized[section_name] = _normalize_strategy_section(section)

    if "output_strategy" in normalized:
        output = _wrap_section(normalized.get("output"))
        if "strategies" in output:
            if "strategy" not in output:
                output["strategy"] = normalized.get("output_strategy", "composite")
            normalized["output"] = output
        else:
            if "strategy" not in output:
                output["strategy"] = normalized["output_strategy"]
            if "config" not in output:
                output["config"] = {
                    key: value for key, value in output.items() if key != "strategy"
                }
                for key in list(output.keys()):
                    if key not in {"strategy", "config"}:
                        del output[key]
            normalized["output"] = output
    elif isinstance(normalized.get("output"), dict) and "strategies" in normalized["output"]:
        output = deepcopy(normalized["output"])
        if "strategy" not in output:
            output["strategy"] = "composite"
        normalized["output"] = output

    if "secondary_output_strategy" in normalized:
        secondary_output = _wrap_section(normalized.get("secondary_output"))
        if "strategy" not in secondary_output:
            secondary_output["strategy"] = normalized["secondary_output_strategy"]
        if "config" not in secondary_output:
            secondary_output["config"] = {
                key: value
                for key, value in secondary_output.items()
                if key != "strategy"
            }
            for key in list(secondary_output.keys()):
                if key not in {"strategy", "config"}:
                    del secondary_output[key]
        normalized["secondary_output"] = secondary_output

    return normalized


def _ensure_dict_section(
    config: Dict[str, Any], section_name: str, required: bool = False
) -> Dict[str, Any]:
    section = config.get(section_name)
    if section is None:
        if required:
            raise ConfigError(f"Missing required '{section_name}' section")
        return {}
    if not isinstance(section, dict):
        raise ConfigError(f"'{section_name}' must be a mapping")
    return section


def _catalog_values(
    strategy_catalog: Mapping[str, Iterable[str]], strategy_type: str
) -> Set[str]:
    values = strategy_catalog.get(strategy_type)
    if values is None:
        return set()
    return set(values)


def _validate_section_strategy(
    section_name: str, section: Dict[str, Any], allowed: Set[str]
) -> None:
    if not section:
        return

    strategy_name = section.get("strategy")
    if not strategy_name:
        raise ConfigError(
            f"'{section_name}.strategy' is required when '{section_name}' section is present"
        )
    if strategy_name not in allowed:
        raise ConfigError(f"Unknown {section_name} strategy '{strategy_name}'")


def validate_config(
    config: Dict[str, Any], strategy_catalog: Mapping[str, Iterable[str]]
) -> None:
    """Validate normalized configuration and raise actionable errors."""
    if not isinstance(config, dict):
        raise ConfigError("Configuration must be a mapping")

    content_type = config.get("content_type", "dynamic")
    if content_type not in {"dynamic", "listing_crawler"}:
        raise ConfigError(
            f"Unsupported content_type '{content_type}'. Expected 'dynamic' or 'listing_crawler'."
        )

    for section_name in (
        "input",
        "queue",
        "output",
        "secondary_output",
        "navigation",
        "extraction",
    ):
        section = config.get(section_name)
        if section is not None and not isinstance(section, dict):
            raise ConfigError(f"'{section_name}' must be a mapping")

    output_allowed = _catalog_values(strategy_catalog, "output")
    input_allowed = _catalog_values(strategy_catalog, "input")
    queue_allowed = _catalog_values(strategy_catalog, "queue")
    navigation_allowed = _catalog_values(strategy_catalog, "navigation")
    extraction_allowed = _catalog_values(strategy_catalog, "extraction")
    pagination_allowed = _catalog_values(strategy_catalog, "pagination")

    output = _ensure_dict_section(config, "output")
    if output and "strategies" in output:
        strategies = output.get("strategies", [])
        if not isinstance(strategies, list):
            raise ConfigError("output.strategies must be a list")
        for index, item in enumerate(strategies, 1):
            if not isinstance(item, dict):
                raise ConfigError(f"output.strategies[{index}] must be a mapping")
            strategy_name = item.get("strategy")
            if not strategy_name:
                raise ConfigError(f"output.strategies[{index}] is missing 'strategy'")
            if strategy_name not in output_allowed:
                raise ConfigError(
                    f"Unknown output strategy '{strategy_name}' in output.strategies[{index}]"
                )
    elif output:
        _validate_section_strategy("output", output, output_allowed)

    _validate_section_strategy(
        "secondary_output",
        _ensure_dict_section(config, "secondary_output"),
        output_allowed,
    )
    _validate_section_strategy("input", _ensure_dict_section(config, "input"), input_allowed)
    _validate_section_strategy("queue", _ensure_dict_section(config, "queue"), queue_allowed)
    _validate_section_strategy(
        "navigation",
        _ensure_dict_section(config, "navigation"),
        navigation_allowed,
    )

    extraction_section = _ensure_dict_section(config, "extraction")
    if extraction_section and "strategy" in extraction_section:
        _validate_section_strategy("extraction", extraction_section, extraction_allowed)

    extraction_strategy = config.get("extraction_strategy")
    if extraction_strategy and extraction_strategy not in extraction_allowed:
        raise ConfigError(f"Unknown extraction strategy '{extraction_strategy}'")

    pagination_strategy = config.get("pagination_strategy")
    if pagination_strategy and pagination_strategy not in pagination_allowed:
        raise ConfigError(f"Unknown pagination strategy '{pagination_strategy}'")

    if content_type == "listing_crawler":
        browser = config.get("browser")
        if not isinstance(browser, dict) or "automation" not in browser:
            raise ConfigError(
                "'browser.automation' is required for listing_crawler configs"
            )
