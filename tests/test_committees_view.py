"""Tests for views/committees_view.py

Integration tests use the real PostgreSQL database with known historical
data from older Knessets (19, 20) which are stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.committees.search_committees_view import search_committees
from origins.committees.search_committees_models import CommitteeSearchResults, CommitteeSummary


# ===================================================================
# Integration tests — use real PostgreSQL database
# ===================================================================


class TestKnessetFilter(unittest.TestCase):
    """Filter committees by Knesset number."""

    def test_knesset_20_total_count(self):
        """Knesset 20 had 136 committees (all types)."""
        results = search_committees(knesset_num=20)
        self.assertEqual(len(results.items), 136)

    def test_knesset_20_main_committees(self):
        """Knesset 20 had 12 main committees (ועדה ראשית)."""
        results = search_committees(knesset_num=20, committee_type="ועדה ראשית")
        self.assertEqual(len(results.items), 12)

    def test_knesset_20_subcommittees(self):
        """Knesset 20 had 56 sub-committees (ועדת משנה)."""
        results = search_committees(knesset_num=20, committee_type="ועדת משנה")
        self.assertEqual(len(results.items), 56)


class TestNameFilter(unittest.TestCase):
    """Filter committees by name."""

    def test_name_search_knesset_20(self):
        """Searching for 'כספים' returns committees with that name."""
        results = search_committees(knesset_num=20, name="כספים")
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertIn("כספים", r.name)

    def test_specific_committee_by_name(self):
        """Search for the Labor, Welfare and Health committee."""
        results = search_committees(knesset_num=20, name="עבודה")
        names = [r.name for r in results.items]
        self.assertTrue(
            any("עבודה" in n for n in names),
            f"Expected a committee with 'עבודה' in name, got: {names}",
        )


class TestTypeFilter(unittest.TestCase):
    """Filter committees by type."""

    def test_special_committees_knesset_20(self):
        """Knesset 20 had 14 special committees (ועדה מיוחדת)."""
        results = search_committees(knesset_num=20, committee_type="ועדה מיוחדת")
        self.assertEqual(len(results.items), 14)

    def test_joint_committees_knesset_20(self):
        """Knesset 20 had 53 joint committees (ועדה משותפת)."""
        results = search_committees(knesset_num=20, committee_type="ועדה משותפת")
        self.assertEqual(len(results.items), 53)

    def test_knesset_committee_knesset_20(self):
        """Knesset 20 had 1 Knesset committee (ועדת הכנסת)."""
        results = search_committees(knesset_num=20, committee_type="ועדת הכנסת")
        self.assertEqual(len(results.items), 1)


class TestIsCurrentFilter(unittest.TestCase):
    """Filter by active/inactive status."""

    def test_current_committees_exist(self):
        """There should be current committees in the database."""
        results = search_committees(is_current=True)
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertTrue(r.is_current)

    def test_inactive_committees_exist(self):
        """There should be inactive committees in the database."""
        results = search_committees(is_current=False)
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertFalse(r.is_current)

    def test_knesset_20_all_inactive(self):
        """Knesset 20 committees should all be inactive."""
        results = search_committees(knesset_num=20)
        for r in results.items:
            self.assertFalse(r.is_current, f"Committee {r.name} should be inactive")


class TestOutputStructure(unittest.TestCase):
    """Verify the output model structure."""

    def test_returns_pydantic_model(self):
        """search_committees returns a CommitteeSearchResults model."""
        results = search_committees(knesset_num=20, committee_type="ועדה ראשית")
        self.assertIsInstance(results, CommitteeSearchResults)
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertIsInstance(r, CommitteeSummary)

    def test_summary_fields(self):
        """Each committee summary has expected fields."""
        results = search_committees(knesset_num=20, committee_type="ועדה ראשית")
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertIsNotNone(r.committee_id)
            self.assertIsNotNone(r.name)
            self.assertIsNotNone(r.knesset_num)
            self.assertIsNotNone(r.type)
            self.assertIsNotNone(r.category)
            self.assertIsNotNone(r.start_date)

    def test_no_detail_fields(self):
        """Summary view should not include sessions, members, bills, documents."""
        results = search_committees(knesset_num=20, committee_type="ועדה ראשית")
        for r in results.items:
            self.assertFalse(hasattr(r, "sessions"))
            self.assertFalse(hasattr(r, "members"))
            self.assertFalse(hasattr(r, "bills"))
            self.assertFalse(hasattr(r, "documents"))

    def test_known_committee_928(self):
        """Committee 928 should appear in Knesset 20 results."""
        results = search_committees(knesset_num=20, name="עבודה")
        ids = {r.committee_id for r in results.items}
        self.assertIn(928, ids)

        c = next(r for r in results.items if r.committee_id == 928)
        self.assertEqual(c.name, "ועדת העבודה, הרווחה והבריאות")
        self.assertEqual(c.type, "ועדה ראשית")
        self.assertEqual(c.knesset_num, 20)
        self.assertFalse(c.is_current)


class TestSorting(unittest.TestCase):
    """Results are sorted by start_date DESC."""

    def test_sorted_by_start_date_desc(self):
        """Results should be sorted newest first by start_date."""
        results = search_committees(name="כספים")
        self.assertGreater(len(results.items), 1)
        dates = [r.start_date for r in results.items if r.start_date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                f"Not sorted DESC: {dates[i-1]} < {dates[i]}",
            )


class TestDateFormatting(unittest.TestCase):
    """Dates should be YYYY-MM-DD without time components."""

    def test_dates_no_time(self):
        """start_date and end_date should not contain time components."""
        results = search_committees(knesset_num=20, committee_type="ועדה ראשית")
        for r in results.items:
            if r.start_date:
                self.assertNotIn("T", r.start_date)
                self.assertNotIn(" ", r.start_date)
            if r.end_date:
                self.assertNotIn("T", r.end_date)
                self.assertNotIn(" ", r.end_date)


class TestNoFilters(unittest.TestCase):
    def test_no_filters_returns_results(self):
        """With no filters, we should get results across all Knessets."""
        results = search_committees()
        self.assertGreater(len(results.items), 0)


class TestCombinedFilters(unittest.TestCase):
    """Multiple filters are ANDed."""

    def test_name_and_type(self):
        """Search for main committees with 'חוקה' in name in Knesset 20."""
        results = search_committees(
            knesset_num=20, name="חוקה", committee_type="ועדה ראשית"
        )
        self.assertEqual(len(results.items), 1)
        self.assertIn("חוקה", results.items[0].name)


if __name__ == "__main__":
    unittest.main()
