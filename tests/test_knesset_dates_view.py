"""Tests for views/knesset_dates_view.py

Integration tests use the real PostgreSQL database with known historical
data which is stable and won't change for past Knesset terms.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from views.knesset_dates_view import get_knesset_dates


# ===================================================================
# Integration tests — use real PostgreSQL database
# ===================================================================


class TestNoFilters(unittest.TestCase):
    """Get all knesset terms."""

    def test_returns_all_knessets(self):
        """Should return all distinct knesset numbers."""
        results = get_knesset_dates()
        # Knessets 0 (provisional council) through 25
        self.assertEqual(len(results), 26)

    def test_sorted_by_knesset_num(self):
        """Results should be sorted ascending by knesset_num."""
        results = get_knesset_dates()
        nums = [r["knesset_num"] for r in results]
        self.assertEqual(nums, sorted(nums))

    def test_first_is_provisional_council(self):
        """First entry should be knesset 0 (provisional council)."""
        results = get_knesset_dates()
        self.assertEqual(results[0]["knesset_num"], 0)
        self.assertEqual(results[0]["name"], "מועצת המדינה הזמנית")

    def test_last_is_current(self):
        """Last entry should be the current knesset."""
        results = get_knesset_dates()
        self.assertTrue(results[-1]["is_current"])


class TestKnessetNumFilter(unittest.TestCase):
    """Filter by specific knesset number."""

    def test_single_knesset(self):
        """Filtering by knesset_num returns exactly one entry."""
        results = get_knesset_dates(knesset_num=20)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["knesset_num"], 20)

    def test_nonexistent_knesset(self):
        """Filtering by a knesset that doesn't exist returns empty."""
        results = get_knesset_dates(knesset_num=99)
        self.assertEqual(len(results), 0)

    def test_knesset_0(self):
        """Knesset 0 (provisional council) should have 1 period."""
        results = get_knesset_dates(knesset_num=0)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["periods"]), 1)
        self.assertEqual(results[0]["name"], "מועצת המדינה הזמנית")


class TestOutputStructure(unittest.TestCase):
    """Verify the output dict structure."""

    def test_top_level_keys(self):
        """Each knesset entry has expected keys."""
        results = get_knesset_dates(knesset_num=20)
        self.assertEqual(len(results), 1)
        expected_keys = {"knesset_num", "name", "is_current", "periods"}
        self.assertEqual(set(results[0].keys()), expected_keys)

    def test_period_keys(self):
        """Each period entry has expected keys."""
        results = get_knesset_dates(knesset_num=20)
        self.assertGreater(len(results[0]["periods"]), 0)
        expected_keys = {"id", "assembly", "plenum", "start_date", "finish_date", "is_current"}
        for period in results[0]["periods"]:
            self.assertEqual(set(period.keys()), expected_keys)

    def test_periods_are_list(self):
        """periods field should be a list."""
        results = get_knesset_dates(knesset_num=20)
        self.assertIsInstance(results[0]["periods"], list)


class TestGrouping(unittest.TestCase):
    """Verify rows are grouped by knesset number."""

    def test_knesset_1_has_multiple_periods(self):
        """Knesset 1 should have multiple assembly/plenum periods."""
        results = get_knesset_dates(knesset_num=1)
        self.assertEqual(len(results), 1)
        periods = results[0]["periods"]
        self.assertGreater(len(periods), 1)

    def test_periods_sorted_by_assembly_plenum(self):
        """Periods should be sorted by (assembly, plenum)."""
        results = get_knesset_dates(knesset_num=1)
        periods = results[0]["periods"]
        for i in range(1, len(periods)):
            prev = (periods[i - 1]["assembly"], periods[i - 1]["plenum"])
            curr = (periods[i]["assembly"], periods[i]["plenum"])
            self.assertLessEqual(
                prev, curr,
                f"Not sorted: assembly={prev} > assembly={curr}",
            )

    def test_knesset_25_periods(self):
        """Knesset 25 should have 7 periods (4 plenums * 2 assemblies - 1)."""
        results = get_knesset_dates(knesset_num=25)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["periods"]), 7)


class TestCurrentKnesset(unittest.TestCase):
    """Verify current knesset handling."""

    def test_current_knesset_is_25(self):
        """The current knesset should be 25."""
        results = get_knesset_dates()
        current = [r for r in results if r["is_current"]]
        self.assertEqual(len(current), 1)
        self.assertEqual(current[0]["knesset_num"], 25)

    def test_current_knesset_has_current_period(self):
        """The current knesset should have at least one current period."""
        results = get_knesset_dates(knesset_num=25)
        current_periods = [p for p in results[0]["periods"] if p["is_current"]]
        self.assertEqual(len(current_periods), 1)

    def test_current_period_has_no_finish_date(self):
        """The current period should have no finish date."""
        results = get_knesset_dates(knesset_num=25)
        current_period = [p for p in results[0]["periods"] if p["is_current"]][0]
        self.assertEqual(current_period["finish_date"], "")

    def test_past_knesset_not_current(self):
        """Knesset 20 should not be current."""
        results = get_knesset_dates(knesset_num=20)
        self.assertFalse(results[0]["is_current"])


class TestDateFormatting(unittest.TestCase):
    """Dates should be YYYY-MM-DD without time components."""

    def test_dates_no_time(self):
        """start_date and finish_date should not contain time or timezone."""
        results = get_knesset_dates(knesset_num=1)
        for period in results[0]["periods"]:
            if period["start_date"]:
                self.assertNotIn("T", period["start_date"])
                self.assertNotIn("+", period["start_date"])
            if period["finish_date"]:
                self.assertNotIn("T", period["finish_date"])
                self.assertNotIn("+", period["finish_date"])

    def test_known_dates_knesset_25_plenum_1(self):
        """Knesset 25 assembly 1 plenum 1 started 2022-11-15."""
        results = get_knesset_dates(knesset_num=25)
        p1 = [p for p in results[0]["periods"]
              if p["assembly"] == 1 and p["plenum"] == 1][0]
        self.assertEqual(p1["start_date"], "2022-11-15")
        self.assertEqual(p1["finish_date"], "2023-07-30")


class TestKnownData(unittest.TestCase):
    """Verify specific known data points."""

    def test_knesset_20_name(self):
        """Knesset 20 should be named 'העשרים'."""
        results = get_knesset_dates(knesset_num=20)
        self.assertEqual(results[0]["name"], "העשרים")

    def test_knesset_1_name(self):
        """Knesset 1 should be named 'הראשונה'."""
        results = get_knesset_dates(knesset_num=1)
        self.assertEqual(results[0]["name"], "הראשונה")


if __name__ == "__main__":
    unittest.main()
