#!/usr/bin/env python3
"""Provision and verify Chrome/Chromium on Ubuntu hosts."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Sequence

SUPPORTED_BROWSER_BINARIES = (
    "google-chrome",
    "google-chrome-stable",
    "chromium-browser",
    "chromium",
    "chrome",
)
AUTO_INSTALL_PACKAGES = ("chromium-browser", "chromium")


@dataclass
class ProvisionResult:
    success: bool
    executable_path: Optional[str]
    message: str


def detect_browser_executable(
    which_fn: Callable[[str], Optional[str]] = shutil.which,
    candidates: Iterable[str] = SUPPORTED_BROWSER_BINARIES,
) -> Optional[str]:
    for candidate in candidates:
        resolved = which_fn(candidate)
        if resolved:
            return resolved
    return None


def resolve_package_order(browser_package: str) -> list[str]:
    if browser_package == "auto":
        return list(AUTO_INSTALL_PACKAGES)
    return [browser_package]


def _is_root() -> bool:
    if hasattr(os, "geteuid"):
        return os.geteuid() == 0
    return False


def _install_package(
    package: str,
    run_fn: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> tuple[bool, str]:
    update = run_fn(
        ["apt-get", "update"],
        capture_output=True,
        text=True,
        check=False,
    )
    if update.returncode != 0:
        details = (update.stderr or update.stdout or "").strip()
        return False, f"`apt-get update` failed: {details}"

    install = run_fn(
        ["apt-get", "install", "-y", package],
        capture_output=True,
        text=True,
        check=False,
    )
    if install.returncode != 0:
        details = (install.stderr or install.stdout or "").strip()
        return False, f"`apt-get install -y {package}` failed: {details}"
    return True, f"Installed package: {package}"


def provision_browser(
    *,
    check_only: bool,
    browser_package: str,
    which_fn: Callable[[str], Optional[str]] = shutil.which,
    run_fn: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    is_root_fn: Callable[[], bool] = _is_root,
) -> ProvisionResult:
    current = detect_browser_executable(which_fn=which_fn)
    if current:
        return ProvisionResult(
            success=True,
            executable_path=current,
            message=f"Detected browser executable: {current}",
        )

    if check_only:
        return ProvisionResult(
            success=False,
            executable_path=None,
            message=(
                "No supported browser executable found. "
                "Install one or run this bootstrap without `--check-only`."
            ),
        )

    if not which_fn("apt-get"):
        return ProvisionResult(
            success=False,
            executable_path=None,
            message=(
                "No supported browser executable found and `apt-get` is unavailable. "
                "Install Chrome/Chromium manually and then provide "
                "`--browser-executable-path /path/to/browser` when running the scraper."
            ),
        )

    if not is_root_fn():
        return ProvisionResult(
            success=False,
            executable_path=None,
            message=(
                "No supported browser executable found and package installation requires root. "
                "Re-run this command with sudo, or provide "
                "`--browser-executable-path /path/to/browser`."
            ),
        )

    attempted = []
    for package in resolve_package_order(browser_package):
        ok, details = _install_package(package, run_fn=run_fn)
        attempted.append(f"{package}: {details}")
        if not ok:
            continue

        installed = detect_browser_executable(which_fn=which_fn)
        if installed:
            return ProvisionResult(
                success=True,
                executable_path=installed,
                message=f"Installed and detected browser executable: {installed}",
            )

    attempts_text = "; ".join(attempted) if attempted else "no packages attempted"
    return ProvisionResult(
        success=False,
        executable_path=None,
        message=(
            "Browser installation attempt completed but no supported executable was found. "
            f"Attempts: {attempts_text}. "
            "Install manually or run scraper with `--browser-executable-path /path/to/browser`."
        ),
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap Chrome/Chromium for nodriver on Ubuntu"
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only verify whether a browser executable is currently installed",
    )
    parser.add_argument(
        "--browser-package",
        choices=("auto", "chromium-browser", "chromium"),
        default="auto",
        help="Package to install when browser is missing (default: auto)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    result = provision_browser(
        check_only=args.check_only,
        browser_package=args.browser_package,
    )

    if result.success:
        print(result.message)
        print(f"Resolved browser executable path: {result.executable_path}")
        return 0

    print(result.message, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
