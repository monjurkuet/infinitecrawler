"""Tests for email extractor — scan_text_for_emails, filter_noise, deduplicate_emails.

Also guards against ReDoS on large input.
"""

import unittest

from utils.email_extractor import (
    scan_text_for_emails,
    extract_mailto_links,
    filter_noise,
    deduplicate_emails,
    normalize_email,
)


class TestNormalizeEmail(unittest.TestCase):
    def test_valid_email(self):
        self.assertEqual(normalize_email("Foo@Realsite.com"), "foo@realsite.com")

    def test_trailing_dots(self):
        self.assertEqual(normalize_email("foo@realsite.com."), "foo@realsite.com")

    def test_noise_noreply(self):
        self.assertIsNone(normalize_email("noreply@realsite.com"))

    def test_noise_admin(self):
        self.assertIsNone(normalize_email("admin@example.com"))

    def test_noise_example_domain(self):
        self.assertIsNone(normalize_email("hello@example.com"))

    def test_no_at_sign(self):
        self.assertIsNone(normalize_email("notanemail"))

    def test_file_tld(self):
        self.assertIsNone(normalize_email("foo@bar.css"))

    def test_numeric_domain_part(self):
        self.assertIsNone(normalize_email("foo@123.com"))

    def test_too_short(self):
        self.assertIsNone(normalize_email("a@b.cd"))

    def test_non_string(self):
        self.assertIsNone(normalize_email(None))
        self.assertIsNone(normalize_email(""))


class TestScanTextForEmails(unittest.TestCase):
    def test_finds_standard_email(self):
        text = "Contact us at info@realsite.com for support."
        results = scan_text_for_emails(text)
        emails = [r["email"] for r in results]
        self.assertIn("info@realsite.com", emails)

    def test_finds_multiple_emails(self):
        text = "sales@realsite.com and support@realsite.com are both valid."
        results = scan_text_for_emails(text)
        self.assertEqual(len(results), 2)

    def test_finds_obfuscated_at_dot(self):
        text = "email: info [at] realsite [dot] com"
        results = scan_text_for_emails(text)
        emails = [r["email"] for r in results]
        self.assertIn("info@realsite.com", emails)

    def test_finds_obfuscated_parentheses(self):
        text = "contact info(at)realsite(dot)org"
        results = scan_text_for_emails(text)
        emails = [r["email"] for r in results]
        self.assertIn("info@realsite.org", emails)

    def test_finds_obfuscated_bracket(self):
        text = "mail: info[at]realsite[dot]net"
        results = scan_text_for_emails(text)
        emails = [r["email"] for r in results]
        self.assertIn("info@realsite.net", emails)

    def test_empty_text(self):
        self.assertEqual(scan_text_for_emails(""), [])
        self.assertEqual(scan_text_for_emails(None), [])

    def test_large_input_no_crash(self):
        """Large text must complete fast — 2MB cap, regex bounded, no backtracking."""
        large = ("a" * 200 + " info@realsite.com " + "b" * 200) * 500  # ~200K
        import time
        start = time.time()
        results = scan_text_for_emails(large)
        elapsed = time.time() - start
        emails = [r["email"] for r in results]
        self.assertIn("info@realsite.com", emails)
        self.assertLess(elapsed, 3, "Large input scan took too long")

    def test_large_random_input_no_crash(self):
        """Large text with no natural @ patterns — must finish without backtracking stall."""
        chars = "a" * 20000 + " info@realsite.com " + "z" * 20000  # spaces to isolate email
        import time
        start = time.time()
        results = scan_text_for_emails(chars)
        elapsed = time.time() - start
        emails = [r["email"] for r in results]
        self.assertIn("info@realsite.com", emails)
        self.assertLess(elapsed, 3)


class TestExtractMailto(unittest.TestCase):
    def test_extract_mailto(self):
        html = '<a href="mailto:hello@world.com">Email</a>'
        self.assertEqual(extract_mailto_links(html), ["hello@world.com"])

    def test_multiple_mailto(self):
        html = '<a href="mailto:sales@realsite.com">A</a> <a href="mailto:support@realsite.com">C</a>'
        self.assertEqual(set(extract_mailto_links(html)), {"sales@realsite.com", "support@realsite.com"})

    def test_empty_html(self):
        self.assertEqual(extract_mailto_links(""), [])


class TestFilterNoise(unittest.TestCase):
    def test_filters_noreply(self):
        emails = [{"email": "noreply@realsite.com", "is_obfuscated": False}]
        self.assertEqual(filter_noise(emails), [])

    def test_keeps_valid(self):
        emails = [{"email": "ceo@realsite.com", "is_obfuscated": False}]
        self.assertEqual(len(filter_noise(emails)), 1)

    def test_mixed(self):
        emails = [
            {"email": "ceo@realsite.com", "is_obfuscated": False},
            {"email": "noreply@realsite.com", "is_obfuscated": False},
            {"email": "info@realsite.com", "is_obfuscated": True},
        ]
        filtered = filter_noise(emails)
        self.assertEqual(len(filtered), 2)


class TestDeduplicateEmails(unittest.TestCase):
    def test_dedup(self):
        emails = [
            {"email": "a@b.com"},
            {"email": "a@b.com"},
            {"email": "c@d.com"},
        ]
        deduped = deduplicate_emails(emails)
        self.assertEqual(len(deduped), 2)

    def test_case_insensitive(self):
        emails = [
            {"email": "A@B.com"},
            {"email": "a@b.com"},
        ]
        deduped = deduplicate_emails(emails)
        self.assertEqual(len(deduped), 1)


if __name__ == "__main__":
    unittest.main()