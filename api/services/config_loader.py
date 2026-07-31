"""YAML config file management."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"

_VALID_NAME_RE = re.compile(r"^[-_a-zA-Z0-9]+\.yaml$")


def _validate_name(name: str) -> None:
    if not _VALID_NAME_RE.match(name) or ".." in name:
        raise ValueError(f"Invalid config name: {name!r}")


def _validate_content(content: dict) -> None:
    dumped = yaml.dump(content, default_flow_style=False, allow_unicode=True)
    reloaded = yaml.safe_load(dumped)
    if not isinstance(reloaded, dict):
        raise ValueError("Config must be a YAML mapping")


def list_configs() -> list[dict]:
    items = []
    for f in sorted(CONFIG_DIR.glob("*.yaml")):
        stat = f.stat()
        items.append({
            "name": f.name,
            "size_bytes": stat.st_size,
            "modified": stat.st_mtime,
        })
    return items


def get_config(name: str) -> Optional[dict]:
    _validate_name(name)
    path = CONFIG_DIR / name
    if not path.exists() or not path.is_file():
        return None
    return yaml.safe_load(path.read_text())


def write_config(name: str, content: dict) -> bool:
    _validate_name(name)
    _validate_content(content)
    path = CONFIG_DIR / name
    if path.exists():
        return False  # use update for existing
    path.write_text(yaml.dump(content, default_flow_style=False, allow_unicode=True))
    return True


def update_config(name: str, content: dict) -> bool:
    _validate_name(name)
    _validate_content(content)
    path = CONFIG_DIR / name
    if not path.exists():
        return False
    path.write_text(yaml.dump(content, default_flow_style=False, allow_unicode=True))
    return True


def delete_config(name: str) -> bool:
    _validate_name(name)
    path = CONFIG_DIR / name
    if not path.exists():
        return False
    path.unlink()
    return True


def get_log_lines(tail: int = 100, filter_text: Optional[str] = None) -> list[str]:
    log_dir = REPO_ROOT / "logs"
    lines = []
    for logfile in sorted(log_dir.glob("*.log")):
        with open(logfile, errors="replace") as f:
            file_lines = f.readlines()
            for line in file_lines[-tail:]:
                if filter_text is None or filter_text.lower() in line.lower():
                    lines.append(f"[{logfile.name}] {line.rstrip()}")
    return lines[-tail:]


def get_crawler_log(crawler_name: str, tail: int = 100) -> Optional[str]:
    log_path = REPO_ROOT / "logs" / f"{crawler_name}.log"
    if not log_path.exists():
        return None
    with open(log_path, errors="replace") as f:
        lines = f.readlines()
    return "".join(lines[-tail:])