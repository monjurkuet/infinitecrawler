import subprocess
import unittest

from scripts.bootstrap_ubuntu import provision_browser, resolve_package_order


class BootstrapUbuntuTests(unittest.TestCase):
    def test_resolve_package_order(self):
        self.assertEqual(resolve_package_order("auto"), ["chromium-browser", "chromium"])
        self.assertEqual(resolve_package_order("chromium"), ["chromium"])

    def test_check_only_missing_browser_does_not_attempt_install(self):
        calls = []

        def run_fn(*args, **kwargs):
            calls.append(args[0])
            return subprocess.CompletedProcess(args[0], 0, "", "")

        def which_fn(name: str):
            return None

        result = provision_browser(
            check_only=True,
            browser_package="auto",
            which_fn=which_fn,
            run_fn=run_fn,
            is_root_fn=lambda: True,
        )

        self.assertFalse(result.success)
        self.assertEqual(calls, [])

    def test_auto_package_order_is_attempted_in_sequence(self):
        install_commands = []

        def run_fn(cmd, capture_output, text, check):
            if cmd[1] == "install":
                install_commands.append(cmd[3])
                return subprocess.CompletedProcess(cmd, 1, "", "install failed")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        def which_fn(name: str):
            if name == "apt-get":
                return "/usr/bin/apt-get"
            return None

        result = provision_browser(
            check_only=False,
            browser_package="auto",
            which_fn=which_fn,
            run_fn=run_fn,
            is_root_fn=lambda: True,
        )

        self.assertFalse(result.success)
        self.assertEqual(install_commands, ["chromium-browser", "chromium"])


if __name__ == "__main__":
    unittest.main()
