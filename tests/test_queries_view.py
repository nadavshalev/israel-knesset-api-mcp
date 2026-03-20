"""Tests for the unified queries tool.

Integration tests use the real PostgreSQL database with known historical data.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.queries.queries_view import queries
from origins.queries.queries_models import QueryResult, QueriesResults


class TestSearchMode(unittest.TestCase):
    """Basic query searches (partial results)."""

    def test_knesset_20_search(self):
        results = queries(knesset_num=20, name_query="תקציב")
        self.assertIsInstance(results, QueriesResults)
        self.assertGreater(len(results.items), 0)
        for q in results.items:
            self.assertIsInstance(q, QueryResult)
            self.assertEqual(q.knesset_num, 20)

    def test_no_full_detail_in_partial(self):
        """Partial results should NOT include documents."""
        results = queries(knesset_num=20, name_query="תקציב")
        for q in results.items:
            self.assertIsNone(q.documents)

    def test_output_structure(self):
        results = queries(knesset_num=20, name_query="תקציב")
        self.assertGreater(len(results.items), 0)
        for q in results.items:
            self.assertIsNotNone(q.query_id)
            self.assertIsNotNone(q.name)
            self.assertIsNotNone(q.knesset_num)

    def test_name_no_match(self):
        results = queries(name_query="xxxNOTEXISTxxx", knesset_num=20)
        self.assertEqual(len(results.items), 0)


class TestStatusFilter(unittest.TestCase):
    def test_status_filter(self):
        results = queries(knesset_num=20, status="נקבע תאריך תשובה")
        self.assertGreater(len(results.items), 0)


class TestTypeFilter(unittest.TestCase):
    def test_type_filter(self):
        results = queries(knesset_num=20, type="דחופה", name_query="ביטחון")
        self.assertGreater(len(results.items), 0)


class TestQueryIdAutoFullDetails(unittest.TestCase):
    """query_id auto-enables full_details."""

    def test_single_query(self):
        results = queries(query_id=563908)
        self.assertEqual(len(results.items), 1)
        q = results.items[0]
        self.assertEqual(q.query_id, 563908)
        self.assertEqual(q.knesset_num, 20)
        # Full detail: submit_date should be present
        self.assertIsNotNone(q.submit_date)

    def test_nonexistent_query_returns_empty(self):
        results = queries(query_id=999999999)
        self.assertEqual(len(results.items), 0)

    def test_stages_present(self):
        """Full detail should include session stages."""
        results = queries(query_id=563908)
        q = results.items[0]
        if q.stages:
            for s in q.stages:
                has_session = s.plenum_session is not None or s.committee_session is not None
                self.assertTrue(has_session)


class TestLastUpdateDate(unittest.TestCase):
    def test_last_update_date_in_partial(self):
        results = queries(knesset_num=20, name_query="תקציב")
        self.assertGreater(len(results.items), 0)
        has_date = any(q.last_update_date for q in results.items)
        self.assertTrue(has_date)


class TestDateFilter(unittest.TestCase):
    def test_session_date_filter(self):
        """Date range should find queries discussed in sessions."""
        results = queries(knesset_num=20, from_date="2015-01-01", to_date="2015-06-30", name_query="תקציב")
        self.assertGreater(len(results.items), 0)


if __name__ == "__main__":
    unittest.main()
