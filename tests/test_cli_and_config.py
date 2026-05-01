import argparse
import unittest

from factory.scraper_factory import ScraperFactory
from utils.config import ConfigError, normalize_config, validate_config


class CliAndConfigTests(unittest.TestCase):
    def test_boolean_optional_action_supports_headless_flags(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--headless", action=argparse.BooleanOptionalAction, default=True
        )

        self.assertTrue(parser.parse_args([]).headless)
        self.assertFalse(parser.parse_args(["--no-headless"]).headless)
        self.assertTrue(parser.parse_args(["--headless"]).headless)

    def test_normalize_legacy_browser_and_output_config(self):
        config = normalize_config(
            {
                "content_type": "dynamic",
                "browser_automation": "nodriver",
                "headless": False,
                "output_strategy": "jsonl_file",
                "output": {"file_path": "output/data_{query}.jsonl", "max_results": 10},
            }
        )

        self.assertEqual(config["browser"]["automation"], "nodriver")
        self.assertFalse(config["browser"]["headless"])
        self.assertEqual(config["output"]["strategy"], "jsonl_file")
        self.assertEqual(
            config["output"]["config"]["file_path"], "output/data_{query}.jsonl"
        )

    def test_normalize_preserves_composite_output(self):
        config = normalize_config(
            {
                "content_type": "dynamic",
                "output_strategy": "composite",
                "output": {
                    "strategies": [
                        {
                            "strategy": "jsonl_file",
                            "config": {"file_path": "output/data.jsonl"},
                        }
                    ]
                },
            }
        )

        self.assertIn("strategies", config["output"])
        self.assertEqual(config["output"]["strategies"][0]["strategy"], "jsonl_file")
        self.assertNotIn("config", config["output"])

    def test_validate_config_rejects_unknown_output_strategy(self):
        config = normalize_config(
            {"content_type": "dynamic", "output": {"strategy": "bogus"}}
        )

        with self.assertRaisesRegex(ConfigError, "Unknown output strategy"):
            validate_config(config, ScraperFactory.get_strategy_map().keys())

    def test_validate_config_requires_browser_for_listing_crawler(self):
        config = normalize_config({"content_type": "listing_crawler"})

        with self.assertRaisesRegex(ConfigError, "browser.automation"):
            validate_config(config, ScraperFactory.get_strategy_map().keys())


if __name__ == "__main__":
    unittest.main()
