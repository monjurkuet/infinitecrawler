import argparse
import unittest

from factory.scraper_factory import ScraperFactory
from main import validate_runtime_mode
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
                "browser_executable_path": "/usr/bin/chromium",
                "output_strategy": "jsonl_file",
                "output": {"file_path": "output/data_{query}.jsonl", "max_results": 10},
            }
        )

        self.assertEqual(config["browser"]["automation"], "nodriver")
        self.assertFalse(config["browser"]["headless"])
        self.assertEqual(config["browser"]["executable_path"], "/usr/bin/chromium")
        self.assertEqual(config["output"]["strategy"], "jsonl_file")
        self.assertEqual(
            config["output"]["config"]["file_path"], "output/data_{query}.jsonl"
        )

    def test_normalize_sets_composite_strategy_for_legacy_shape(self):
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
        self.assertEqual(config["output"]["strategy"], "composite")

        strategy = ScraperFactory.create_strategy(
            "output",
            config["output"]["strategy"],
            config["output"],
        )
        self.assertEqual(strategy.__class__.__name__, "CompositeOutputStrategy")

    def test_validate_config_rejects_unknown_output_strategy(self):
        config = normalize_config(
            {"content_type": "dynamic", "output": {"strategy": "bogus"}}
        )

        with self.assertRaisesRegex(ConfigError, "Unknown output strategy"):
            validate_config(config, ScraperFactory.get_strategy_catalog())

    def test_validate_config_rejects_cross_section_strategy(self):
        config = normalize_config(
            {
                "content_type": "dynamic",
                "input": {"strategy": "postgresql_upsert", "config": {}},
                "output": {"strategy": "jsonl_file", "config": {}},
            }
        )

        with self.assertRaisesRegex(ConfigError, "Unknown input strategy"):
            validate_config(config, ScraperFactory.get_strategy_catalog())

    def test_validate_config_requires_browser_for_listing_crawler(self):
        config = normalize_config({"content_type": "listing_crawler"})

        with self.assertRaisesRegex(ConfigError, "browser.automation"):
            validate_config(config, ScraperFactory.get_strategy_catalog())

    def test_validate_runtime_mode_requires_input_and_queue_in_batch_mode(self):
        config = normalize_config({"content_type": "dynamic"})

        with self.assertRaisesRegex(ConfigError, "requires both 'input' and 'queue'"):
            validate_runtime_mode(config, query=None)

    def test_validate_runtime_mode_allows_single_query_without_queue(self):
        config = normalize_config({"content_type": "dynamic"})
        validate_runtime_mode(config, query="pizza in chicago")


if __name__ == "__main__":
    unittest.main()
