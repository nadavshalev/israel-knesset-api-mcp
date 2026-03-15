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

from origins.bills.search_bills_view import search_bills
from origins.bills.search_bills_models import BillSearchResults, BillSummary


class TestBasicSearch(unittest.TestCase):
    """Basic bill list searches."""

    def test_knesset_20_basic_laws(self):
        results = search_bills(knesset_num=20, name="חוק-יסוד")
        self.assertIsInstance(results, BillSearchResults)
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIsInstance(b, BillSummary)
            self.assertEqual(b.knesset_num, 20)

    def test_no_stages_in_output(self):
        """List view should NOT include stages."""
        results = search_bills(knesset_num=20, name="חוק-יסוד")
        for b in results.items:
            self.assertFalse(hasattr(b, "stages"))

    def test_output_structure(self):
        results = search_bills(knesset_num=20, name="חוק-יסוד")
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIsInstance(b, BillSummary)
            self.assertIsNotNone(b.bill_id)
            self.assertIsNotNone(b.name)
            self.assertIsNotNone(b.knesset_num)

    def test_name_no_match(self):
        results = search_bills(name="xxxNOTEXISTxxx", knesset_num=20)
        self.assertEqual(len(results.items), 0)


class TestNameFilter(unittest.TestCase):
    def test_name_search(self):
        results = search_bills(name="מדינת הלאום", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIn("לאום", b.name)


class TestSubTypeFilter(unittest.TestCase):
    def test_government_bills(self):
        results = search_bills(sub_type="ממשלתית", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIn("ממשלתית", b.sub_type)


class TestStatusFilter(unittest.TestCase):
    def test_passed_third_reading(self):
        results = search_bills(status="קריאה שלישית", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIn("קריאה שלישית", b.status)


class TestDateFilter(unittest.TestCase):
    def test_date_filter_returns_bills(self):
        """Bills that appeared in plenum on a specific date."""
        results = search_bills(date="2015-05-04", knesset_num=20)
        self.assertGreater(len(results.items), 0)


class TestSortOrder(unittest.TestCase):
    def test_sorted_by_publication_date_desc(self):
        """Results should be sorted by publication_date DESC."""
        results = search_bills(name="חוק-יסוד", knesset_num=20)
        dates = [b.publication_date for b in results.items if b.publication_date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                f"Not sorted DESC: {dates[i-1]} < {dates[i]}",
            )


if __name__ == "__main__":
    unittest.main()
