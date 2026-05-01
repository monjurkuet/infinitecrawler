import unittest

from strategies.output.null_output import NullOutputStrategy


class NullOutputTests(unittest.TestCase):
    def test_null_output_never_reaches_limit(self):
        strategy = NullOutputStrategy({})
        self.assertFalse(strategy.has_reached_limit())


if __name__ == "__main__":
    unittest.main()
