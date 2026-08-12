"""Tests for utils/transliterate.py — fallback behavior when sanscript is None.

The firehose relies on `bn_to_en` returning *something* (never raising).
When `indic_transliteration` is unavailable, the module substitutes a
no-op lambda — but the `bn_to_en` safety contract still applies: if the
no-op output contains no Latin letters, the original BN query is returned.
"""
import unittest


class TestTransliterateFallback(unittest.TestCase):
    def setUp(self):
        import utils.transliterate as t
        # Force the missing-deps branch by clearing _sanscript.
        original = t._sanscript
        t._sanscript = None
        # lru_cache must be cleared between test runs so the cache hit
        # doesn't shadow our override.
        t._transliterate_cached.cache_clear()
        self._t = t
        self._original_sanscript = original

    def tearDown(self):
        self._t._sanscript = self._original_sanscript
        self._t._transliterate_cached.cache_clear()

    def test_bn_to_en_returns_original_when_no_latin_yields(self):
        """With _sanscript=None, the no-op lambda is used; safety net triggers."""
        from utils.transliterate import bn_to_en
        bangla_k = "\u0995"
        self.assertEqual(bn_to_en(bangla_k), bangla_k)


if __name__ == "__main__":
    unittest.main()
