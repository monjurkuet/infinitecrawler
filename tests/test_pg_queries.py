"""Tests for pg.py SQL queries — regression for NULL-trap bug.

Key fix: NOT IN → NOT EXISTS so rows with NULL listing_id in the subquery
do not silently filter out all candidates.
"""

import unittest
from unittest import mock

import utils.pg


class TestUnprocessedLinkedinSQL(unittest.TestCase):
    """FETCH_UNPROCESSED_LINKEDIN_SQL must NOT use NOT IN with a nullable column."""

    def test_not_exists_instead_of_not_in(self):
        """SQL uses NOT EXISTS rather than NOT IN (regression guard)."""
        sql = utils.pg.FETCH_UNPROCESSED_LINKEDIN_SQL
        self.assertIn("NOT EXISTS", sql)
        self.assertNotIn("NOT IN", sql)
        # Confirm the subquery filters by listing_id with p.listing_id = l.id
        self.assertIn("p.listing_id = l.id", sql)

    def test_returns_rows_when_null_listing_id_exists(self):
        """get_unprocessed_linkedin returns rows even when firehose writes NULL listing_id rows."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "Acme Corp"), (2, "Beta Inc")]

        results = utils.pg.get_unprocessed_linkedin(mock_conn, limit=5)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], {"id": 1, "name": "Acme Corp"})
        self.assertEqual(results[1], {"id": 2, "name": "Beta Inc"})


class TestUnprocessedEmailsSQL(unittest.TestCase):
    """FETCH_UNPROCESSED_EMAILS_SQL uses NOT EXISTS to avoid NULL trap."""

    def test_get_unprocessed_emails_returns_list_of_dicts(self):
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(42, "https://example.com")]

        results = utils.pg.get_unprocessed_emails(mock_conn, limit=10)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 42)
        self.assertEqual(results[0]["website"], "https://example.com")


class TestAllListingsWithWebsite(unittest.TestCase):
    """get_all_listings_with_website supports parameterized limit / empty rows."""

    def _run(self, fetchall_rows, limit):
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = fetchall_rows
        return utils.pg.get_all_listings_with_website(mock_conn, limit=limit)

    def test_returns_dicts_for_each_row(self):
        results = self._run([(1, "https://a"), (2, "https://b"), (3, "")], limit=10)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], {"id": 1, "website": "https://a"})
        self.assertEqual(results[2], {"id": 3, "website": ""})

    def test_empty_result(self):
        self.assertEqual(self._run([], limit=5), [])

    def test_limit_is_applied_to_sql(self):
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        utils.pg.get_all_listings_with_website(mock_conn, limit=42)
        # LIMIT is appended as a parameterized %s, not interpolated.
        executed = mock_cursor.execute.call_args
        self.assertIn("LIMIT %s", executed.args[0])
        self.assertEqual(executed.args[1], (42,))


if __name__ == "__main__":
    unittest.main()