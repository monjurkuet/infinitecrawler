"""Configuration normalization and validation helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, Tuple


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
    if browser:
        normalized["browser"] = browser

    if "browser" in normalized:
        normalized["browser_automation"] = browser.get("automation", "nodriver")
        normalized["headless"] = browser.get("headless", True)

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


def validate_config(config: Dict[str, Any], strategy_names: Iterable[str]) -> None:
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

    output = _ensure_dict_section(config, "output")
    if output and "strategies" in output:
        for index, item in enumerate(output.get("strategies", []), 1):
            if not isinstance(item, dict):
                raise ConfigError(f"output.strategies[{index}] must be a mapping")
            strategy_name = item.get("strategy")
            if not strategy_name:
                raise ConfigError(f"output.strategies[{index}] is missing 'strategy'")
            if strategy_name not in strategy_names:
                raise ConfigError(
                    f"Unknown output strategy '{strategy_name}' in output.strategies[{index}]"
                )
    elif output:
        strategy_name = output.get("strategy", "jsonl_file")
        if strategy_name not in strategy_names:
            raise ConfigError(f"Unknown output strategy '{strategy_name}'")

    for section_name in ("input", "queue", "secondary_output"):
        section = config.get(section_name)
        if not section:
            continue
        strategy_name = section.get("strategy")
        if strategy_name and strategy_name not in strategy_names:
            raise ConfigError(f"Unknown {section_name} strategy '{strategy_name}'")

    if content_type == "listing_crawler":
        browser = config.get("browser")
        if not isinstance(browser, dict) or "automation" not in browser:
            raise ConfigError(
                "'browser.automation' is required for listing_crawler configs"
            )
