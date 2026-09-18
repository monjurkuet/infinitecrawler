import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import nodriver as uc

BROWSER_BOOTSTRAP_COMMAND = "uv run python scripts/bootstrap_ubuntu.py"


class BrowserStartupError(RuntimeError):
    """Raised when browser startup configuration is invalid or launch fails."""


def _non_empty(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def resolve_browser_launch_settings(
    config: Dict[str, Any],
    *,
    cli_headless: Optional[bool] = None,
    cli_browser_executable_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Resolve browser settings with precedence: CLI > env > config > autodiscovery."""
    browser_config = config.get("browser", {})
    if not isinstance(browser_config, dict):
        browser_config = {}

    engine = browser_config.get(
        "automation", config.get("browser_automation", "nodriver")
    )
    headless = (
        browser_config.get("headless", config.get("headless", True))
        if cli_headless is None
        else cli_headless
    )
    page_wait_seconds = browser_config.get("page_wait_seconds", 1.0)
    ready_selector = browser_config.get("ready_selector", "")

    cli_path = _non_empty(cli_browser_executable_path)
    env_path = _non_empty(os.getenv("BROWSER_EXECUTABLE_PATH"))
    config_path = _non_empty(browser_config.get("executable_path"))

    if cli_path:
        browser_executable_path = cli_path
        browser_executable_source = "cli"
    elif env_path:
        browser_executable_path = env_path
        browser_executable_source = "env"
    elif config_path:
        browser_executable_path = config_path
        browser_executable_source = "config"
    else:
        browser_executable_path = None
        browser_executable_source = "autodiscovery"

    return {
        "engine": engine,
        "headless": bool(headless),
        "page_wait_seconds": float(page_wait_seconds),
        "ready_selector": str(ready_selector),
        "browser_executable_path": browser_executable_path,
        "browser_executable_source": browser_executable_source,
        "browser_path_checks": {
            "cli": cli_path,
            "env": env_path,
            "config": config_path,
        },
    }


class BrowserManager:
    """Manages browser automation with different engines."""

    def __init__(
        self,
        engine: str = "nodriver",
        headless: bool = True,
        page_wait_seconds: float = 1.0,
        ready_selector: str = "",
        browser_executable_path: Optional[str] = None,
        browser_executable_source: str = "autodiscovery",
        browser_path_checks: Optional[Dict[str, Optional[str]]] = None,
    ):
        self.engine = engine
        self.headless = headless
        self.page_wait_seconds = page_wait_seconds
        self.ready_selector = ready_selector
        self.browser_executable_path = browser_executable_path
        self.browser_executable_source = browser_executable_source
        self.browser_path_checks = browser_path_checks or {}
        self.browser = None
        self.tab = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def _normalize_and_validate_browser_path(self) -> Optional[str]:
        if not self.browser_executable_path:
            return None

        resolved = Path(self.browser_executable_path).expanduser().resolve()
        if not resolved.exists() or not resolved.is_file():
            raise BrowserStartupError(
                "Configured browser executable path is invalid: "
                f"'{self.browser_executable_path}' ({self.browser_executable_source}). "
                f"Install Chromium/Chrome with `{BROWSER_BOOTSTRAP_COMMAND}` or pass a valid "
                "path via `--browser-executable-path`."
            )
        if not os.access(str(resolved), os.X_OK):
            raise BrowserStartupError(
                f"Configured browser executable is not executable: '{resolved}'. "
                "Update permissions or provide another binary with `--browser-executable-path`."
            )
        return str(resolved)

    def _missing_browser_message(self, details: str) -> str:
        checks = self.browser_path_checks
        checked_cli = checks.get("cli") or "<not set>"
        checked_env = checks.get("env") or "<not set>"
        checked_cfg = checks.get("config") or "<not set>"
        return (
            "No Chrome/Chromium browser executable could be started.\n"
            f"- CLI (--browser-executable-path): {checked_cli}\n"
            f"- ENV (BROWSER_EXECUTABLE_PATH): {checked_env}\n"
            f"- Config (browser.executable_path): {checked_cfg}\n"
            "- nodriver autodiscovery: failed\n"
            f"Install a supported browser with `{BROWSER_BOOTSTRAP_COMMAND}` "
            "or pass a valid path with `--browser-executable-path`.\n"
            f"Original error: {details}"
        )

    async def start(self):
        """Start browser instance."""
        if self.engine == "nodriver":
            self.logger.info("Starting nodriver browser...")
            # Always disable sandbox — required when running as root and
            # harmless for non-root users.
            launch_kwargs = {"headless": self.headless, "sandbox": False}

            resolved_path = self._normalize_and_validate_browser_path()
            if resolved_path:
                launch_kwargs["browser_executable_path"] = resolved_path
                self.logger.info(
                    "Using browser executable from %s: %s",
                    self.browser_executable_source,
                    resolved_path,
                )

            # Override user-agent to avoid HeadlessChrome detection.
            # Google Maps and similar sites serve blank pages when they
            # detect headless browsers via the UA string.
            if self.headless:
                launch_kwargs["browser_args"] = [
                    "--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/137.0.0.0 Safari/537.36",
                    "--disable-gpu",
                    "--window-size=1920,1080",
                ]

            try:
                self.browser = await uc.start(**launch_kwargs)
            except FileNotFoundError as exc:
                raise BrowserStartupError(
                    self._missing_browser_message(str(exc))
                ) from exc
            self.logger.info("Browser started successfully")
        else:
            raise ValueError(f"Unsupported browser engine: {self.engine}")

    async def navigate(self, url: str):
        """Navigate to URL and return tab.

        Uses ``main_tab`` + CDP ``Page.navigate`` instead of
        ``browser.get()`` to avoid intermittent WebSocket handshake
        timeouts when connecting to new CDP targets.

        If ``ready_selector`` is configured, polls for the selector
        to appear in the DOM (up to ``page_wait_seconds``) instead of
        sleeping for the full duration. This makes navigation faster
        on pages that render quickly and reliable on heavy SPAs that
        take a long time.
        """
        if self.engine == "nodriver":
            start = asyncio.get_running_loop().time()

            # Use main_tab + CDP Page.navigate to avoid
            # "timed out during opening handshake" errors that
            # browser.get() causes when creating new CDP targets.
            tab = self.browser.main_tab
            if tab is None:
                # Fallback: try browser.get() if no main_tab
                self.logger.warning("No main_tab found, falling back to browser.get()")
                self.tab = await self.browser.get(url)
            else:
                await tab.send(uc.cdp.page.navigate(url=url))
                self.tab = tab

            if self.ready_selector:
                # Smart poll: check every 2s for the selector
                elapsed = asyncio.get_running_loop().time() - start
                deadline = self.page_wait_seconds
                poll_interval = 2.0
                found = False

                while elapsed < deadline:
                    await asyncio.sleep(poll_interval)
                    elapsed = asyncio.get_running_loop().time() - start
                    try:
                        escaped = self.ready_selector.replace("'", "\\'")
                        count = await self.tab.evaluate(
                            f"document.querySelectorAll('{escaped}').length"
                        )
                        if count and int(count) > 0:
                            self.logger.info(
                                "Ready selector '%s' found after %.1fs (%d elements)",
                                self.ready_selector,
                                elapsed,
                                int(count),
                            )
                            found = True
                            # Brief additional wait for content to settle
                            await asyncio.sleep(2)
                            break
                    except Exception:
                        pass

                if not found:
                    self.logger.warning(
                        "Ready selector '%s' not found after %.1fs; "
                        "proceeding anyway (page may be partially loaded)",
                        self.ready_selector,
                        deadline,
                    )
            elif self.page_wait_seconds > 0:
                # Fallback: fixed sleep
                await asyncio.sleep(self.page_wait_seconds)

            elapsed = asyncio.get_running_loop().time() - start
            self.logger.info(
                "Navigation complete in %.2fs (config_wait=%.2fs)",
                elapsed,
                self.page_wait_seconds,
            )
            return self.tab
        raise ValueError(f"Unsupported browser engine: {self.engine}")

    async def cleanup(self):
        """Clean up browser resources."""
        if self.browser:
            self.browser.stop()
            self.logger.info("Browser stopped successfully")
