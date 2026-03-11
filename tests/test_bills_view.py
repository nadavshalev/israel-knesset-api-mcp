"""Tests for views/bills_view.py (list view — summary, no stages)

Integration tests use the real PostgreSQL database with known historical
data.  Bill data from older Knessets (19, 20) is stable.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from views.bills_view import search_bills


class TestBasicSearch(unittest.TestCase):
    """Basic bill list searches."""

    def test_knesset_20_basic_laws(self):
        results = search_bills(knesset_num=20, name="חוק-יסוד")
        self.assertGreater(len(results), 0)
        for b in results:
            self.assertEqual(b["knesset_num"], 20)

    def test_no_stages_in_output(self):
        """List view should NOT include stages."""
        results = search_bills(knesset_num=20, name="חוק-יסוד")
        for b in results:
            self.assertNotIn("stages", b)

    def test_output_structure(self):
        results = search_bills(knesset_num=20, name="חוק-יסוד")
        self.assertGreater(len(results), 0)
        expected_keys = {
            "bill_id", "name", "knesset_num", "sub_type", "status",
            "committee", "publication_date", "publication_series", "summary",
        }
        for b in results:
            self.assertTrue(expected_keys.issubset(b.keys()),
                            f"Missing keys: {expected_keys - b.keys()}")

    def test_name_no_match(self):
        results = search_bills(name="xxxNOTEXISTxxx", knesset_num=20)
        self.assertEqual(len(results), 0)


class TestNameFilter(unittest.TestCase):
    def test_name_search(self):
        results = search_bills(name="מדינת הלאום", knesset_num=20)
        self.assertGreater(len(results), 0)
        for b in results:
            self.assertIn("לאום", b["name"])


class TestSubTypeFilter(unittest.TestCase):
    def test_government_bills(self):
        results = search_bills(sub_type="ממשלתית", knesset_num=20)
        self.assertGreater(len(results), 0)
        for b in results:
            self.assertIn("ממשלתית", b["sub_type"])


class TestStatusFilter(unittest.TestCase):
    def test_passed_third_reading(self):
        results = search_bills(status="קריאה שלישית", knesset_num=20)
        self.assertGreater(len(results), 0)
        for b in results:
            self.assertIn("קריאה שלישית", b["status"])


class TestDateFilter(unittest.TestCase):
    def test_date_filter_returns_bills(self):
        """Bills that appeared in plenum on a specific date."""
        results = search_bills(date="2015-05-04", knesset_num=20)
        self.assertGreater(len(results), 0)


class TestSortOrder(unittest.TestCase):
    def test_sorted_by_publication_date_desc(self):
        """Results should be sorted by publication_date DESC."""
        results = search_bills(name="חוק-יסוד", knesset_num=20)
        dates = [r["publication_date"] for r in results if r["publication_date"]]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                f"Not sorted DESC: {dates[i-1]} < {dates[i]}",
            )


if __name__ == "__main__":
    unittest.main()
