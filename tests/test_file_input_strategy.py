import tempfile
import unittest
import os

from strategies.input.file_url_loader import FileInputStrategy


class FileInputStrategyTests(unittest.TestCase):
    def test_get_total_count_stable_before_and_after_iteration(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("# comment\n")
            handle.write("alpha\n")
            handle.write("beta\n")
            handle.write("alpha\n")
            file_path = handle.name

        strategy = FileInputStrategy(
            {"config": {"file_path": file_path, "deduplicate": True}}
        )

        try:
            count_before = strategy.get_total_count()
            loaded = list(strategy.load_urls())
            count_after = strategy.get_total_count()

            self.assertEqual(count_before, 2)
            self.assertEqual(loaded, ["alpha", "beta"])
            self.assertEqual(count_after, 2)
        finally:
            os.unlink(file_path)


if __name__ == "__main__":
    unittest.main()
