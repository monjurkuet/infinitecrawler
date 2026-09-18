import os
import unittest
from unittest.mock import AsyncMock, patch

from base.browser_manager import (
    BrowserManager,
    BrowserStartupError,
    resolve_browser_launch_settings,
)


class BrowserSettingsResolutionTests(unittest.TestCase):
    @patch.dict(os.environ, {"BROWSER_EXECUTABLE_PATH": "/from-env"}, clear=True)
    def test_resolve_browser_settings_precedence_cli_over_env_over_config(self):
        settings = resolve_browser_launch_settings(
            {
                "browser": {
                    "automation": "nodriver",
                    "headless": False,
                    "page_wait_seconds": 2.5,
                    "executable_path": "/from-config",
                }
            },
            cli_headless=True,
            cli_browser_executable_path="/from-cli",
        )

        self.assertEqual(settings["browser_executable_path"], "/from-cli")
        self.assertEqual(settings["browser_executable_source"], "cli")
        self.assertTrue(settings["headless"])
        self.assertEqual(settings["page_wait_seconds"], 2.5)

    @patch.dict(os.environ, {}, clear=True)
    def test_resolve_browser_settings_falls_back_to_autodiscovery(self):
        settings = resolve_browser_launch_settings(
            {"browser": {"automation": "nodriver", "headless": True}}
        )
        self.assertIsNone(settings["browser_executable_path"])
        self.assertEqual(settings["browser_executable_source"], "autodiscovery")


class BrowserManagerStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_browser_manager_raises_actionable_message_for_missing_browser(self):
        manager = BrowserManager(
            engine="nodriver",
            headless=True,
            browser_path_checks={"cli": None, "env": None, "config": None},
        )

        with patch(
            "base.browser_manager.uc.start",
            new=AsyncMock(side_effect=FileNotFoundError("not found")),
        ):
            with self.assertRaises(BrowserStartupError) as ctx:
                await manager.start()

        message = str(ctx.exception)
        self.assertIn("scripts/bootstrap_ubuntu.py", message)
        self.assertIn("BROWSER_EXECUTABLE_PATH", message)
        self.assertIn("--browser-executable-path", message)

    async def test_browser_manager_validates_explicit_browser_path(self):
        manager = BrowserManager(
            engine="nodriver",
            headless=True,
            browser_executable_path="/definitely/missing/browser",
            browser_executable_source="cli",
        )

        with self.assertRaises(BrowserStartupError) as ctx:
            await manager.start()

        self.assertIn("invalid", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
