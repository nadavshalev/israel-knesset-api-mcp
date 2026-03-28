"""Tests for the unified laws tool.

Integration tests use the real PostgreSQL database with known historical
data.  Law data from older Knessets (1-10) is stable.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.laws.laws_view import laws
from origins.laws.laws_models import LawResultPartial, LawResultFull, LawsResults, LawChange


class TestSearchMode(unittest.TestCase):
    """Basic law searches (partial results)."""

    def test_knesset_10_returns_results(self):
        results = laws(knesset_num=10)
        self.assertIsInstance(results, LawsResults)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIsInstance(law, LawResultPartial)
            self.assertEqual(law.knesset_num, 10)

    def test_no_full_detail_in_search(self):
        results = laws(knesset_num=10)
        for law in results.items:
            self.assertNotIsInstance(law, LawResultFull)

    def test_output_fields_present(self):
        results = laws(knesset_num=10)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIsNotNone(law.law_id)
            self.assertIsNotNone(law.name)

    def test_no_match(self):
        results = laws(name_query="xxxNOTEXISTxxx", knesset_num=1)
        self.assertEqual(len(results.items), 0)


class TestNameFilter(unittest.TestCase):
    def test_name_search(self):
        results = laws(name_query="התקשורת", knesset_num=10)
        self.assertGreater(len(results.items), 0)
        # Fuzzy search: at least one result should contain the root word
        self.assertTrue(
            any("תקשורת" in law.name for law in results.items),
            "Expected at least one result containing 'תקשורת'",
        )


class TestLawTypeFilter(unittest.TestCase):
    def test_basic_laws(self):
        results = laws(is_basic_law=True)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIsNotNone(law.law_types)
            self.assertIn("חוק יסוד", law.law_types)

    def test_basic_law_knesset_3(self):
        """חוק-יסוד: הכנסת was enacted in Knesset 3."""
        results = laws(is_basic_law=True, knesset_num=3)
        self.assertGreater(len(results.items), 0)
        ids = [r.law_id for r in results.items]
        self.assertIn(2000037, ids)

    def test_multi_type_is_or(self):
        """Selecting basic + favorite returns union (OR), more than either alone."""
        basic = laws(is_basic_law=True)
        favorite = laws(is_favorite_law=True)
        both = laws(is_basic_law=True, is_favorite_law=True)
        self.assertGreaterEqual(len(both.items), len(basic.items))
        self.assertGreaterEqual(len(both.items), len(favorite.items))

    def test_favorite_laws(self):
        results = laws(is_favorite_law=True)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIn("חוק מועדף", law.law_types)


class TestLawValidityFilter(unittest.TestCase):
    def test_valid_laws(self):
        results = laws(law_validity="תקף", knesset_num=10)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIn("תקף", law.law_validity)

    def test_invalid_laws(self):
        results = laws(law_validity="בטל", knesset_num=10)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIn("בטל", law.law_validity)


class TestDateFilter(unittest.TestCase):
    def test_date_range_returns_results(self):
        results = laws(from_date="2018-01-01", to_date="2018-12-31", knesset_num=20)
        self.assertGreater(len(results.items), 0)

    def test_from_date_filters_out_older(self):
        results_all = laws(knesset_num=20)
        results_recent = laws(knesset_num=20, from_date="2018-01-01")
        self.assertLessEqual(len(results_recent.items), len(results_all.items))


class TestLawIdAutoFullDetails(unittest.TestCase):
    """law_id with full_details=True."""

    def test_bekesset_law(self):
        """Law 2000002: חוק התקשורת (Knesset 10)."""
        results = laws(law_id=2000002, full_details=True)
        self.assertEqual(len(results.items), 1)
        law = results.items[0]
        self.assertEqual(law.law_id, 2000002)
        self.assertEqual(law.knesset_num, 10)
        self.assertIn("התקשורת", law.name)
        self.assertIsInstance(law, LawResultFull)

    def test_nonexistent_law_returns_empty(self):
        results = laws(law_id=999999999)
        self.assertEqual(len(results.items), 0)

    def test_law_id_with_name_query_still_works(self):
        """law_id + name_query: both ANDed — no crash."""
        results_by_id = laws(law_id=2001386, full_details=True)
        results_filtered = laws(law_id=2001386, name_query="ביטחון", full_details=True)
        self.assertGreaterEqual(len(results_by_id.items), len(results_filtered.items))

    def test_full_details_flag(self):
        results = laws(knesset_num=10, name_query="התקשורת", full_details=True)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIsInstance(law, LawResultFull)


class TestFullDetailFields(unittest.TestCase):
    """Verify detail fields on law 2000002 (חוק התקשורת)."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2000002, full_details=True)
        cls.law = result.items[0]

    def test_is_full_result(self):
        self.assertIsInstance(self.law, LawResultFull)

    def test_has_classifications(self):
        self.assertIsNotNone(self.law.classifications)
        self.assertGreater(len(self.law.classifications), 0)
        for c in self.law.classifications:
            self.assertIsInstance(c, str)

    def test_has_changes(self):
        self.assertIsNotNone(self.law.changes)
        self.assertGreater(len(self.law.changes), 0)

    def test_changes_have_bill(self):
        for change in self.law.changes:
            self.assertIsInstance(change, LawChange)
            self.assertIsNotNone(change.bill)
            self.assertIsNotNone(change.bill.bill_id)
            self.assertIsNotNone(change.bill.name)


class TestChanges(unittest.TestCase):
    """Law 2001386 (חוק שירות ביטחון) has changes (amendments and corrections)."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2001386, full_details=True)
        cls.law = result.items[0]

    def test_has_changes(self):
        self.assertIsNotNone(self.law.changes)
        self.assertGreater(len(self.law.changes), 0)

    def test_each_change_has_bill(self):
        for change in self.law.changes:
            self.assertIsNotNone(change.bill)
            self.assertIsNotNone(change.bill.bill_id)
            self.assertIsNotNone(change.bill.name)

    def test_has_corrections_in_changes(self):
        """At least one change has corrections."""
        with_corrections = [c for c in self.law.changes if c.corrections]
        self.assertGreater(len(with_corrections), 0)

    def test_correction_fields(self):
        """Corrections have correction_type and status (no bill_id/bill_name)."""
        for change in self.law.changes:
            if change.corrections:
                c = change.corrections[0]
                self.assertIsNotNone(c.correction_type)
                self.assertIsNotNone(c.status)
                self.assertFalse(hasattr(c, "bill_id"))
                self.assertFalse(hasattr(c, "bill_name"))
                return
        self.fail("No corrections found in any change")

    def test_has_amendments_in_changes(self):
        """At least one change has amendments."""
        with_amendments = [c for c in self.law.changes if c.amendments]
        self.assertGreater(len(with_amendments), 0)

    def test_amendment_has_no_bill_fields(self):
        """Amendments have no bill_id/bill_name (bill is on LawChange)."""
        for change in self.law.changes:
            if change.amendments:
                a = change.amendments[0]
                self.assertFalse(hasattr(a, "bill_id"))
                self.assertFalse(hasattr(a, "bill_name"))
                return
        self.fail("No amendments found in any change")


class TestReplacedLaws(unittest.TestCase):
    """Law 2001386 replaces law 2001385."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2001386, full_details=True)
        cls.law = result.items[0]

    def test_has_replaced_laws(self):
        self.assertIsNotNone(self.law.replaced_laws)
        self.assertGreater(len(self.law.replaced_laws), 0)

    def test_replaced_law_is_partial(self):
        """Replaced law has partial info (not just ID/name)."""
        rl = self.law.replaced_laws[0]
        self.assertIsNotNone(rl.replaced_law)
        self.assertIsInstance(rl.replaced_law, LawResultPartial)
        self.assertIsNotNone(rl.replaced_law.law_id)
        self.assertIsNotNone(rl.replaced_law.name)

    def test_replacing_bill_is_partial(self):
        """Bill that performed the replacement has partial info."""
        rl = self.law.replaced_laws[0]
        self.assertIsNotNone(rl.bill)
        self.assertIsNotNone(rl.bill.bill_id)
        self.assertIsNotNone(rl.bill.name)

    def test_no_old_fields(self):
        """Old israel_law_bindings field should not exist."""
        self.assertFalse(hasattr(self.law, "israel_law_bindings"))


class TestChangesStructure(unittest.TestCase):
    """Verify changes structure for law 2001386."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2001386, full_details=True)
        cls.law = result.items[0]

    def test_changes_bill_has_id_and_name(self):
        for change in self.law.changes:
            self.assertIsNotNone(change.bill.bill_id)
            self.assertIsNotNone(change.bill.name)

    def test_amendments_have_date(self):
        """Amendments include date."""
        for change in self.law.changes:
            if change.amendments:
                dated = [a for a in change.amendments if a.date is not None]
                if dated:
                    return
        self.fail("No amendments with date found")

    def test_no_old_bindings_field(self):
        """Old bindings/corrections/bills fields should not exist."""
        self.assertFalse(hasattr(self.law, "bindings"))
        self.assertFalse(hasattr(self.law, "corrections"))
        self.assertFalse(hasattr(self.law, "bills"))


class TestOriginalBill(unittest.TestCase):
    """original_bill: resolved from 'החוק המקורי' binding or common ParentLawID."""

    def test_original_law_binding_type_takes_priority(self):
        """Law 2000002 has a 'החוק המקורי' binding — use its LawID."""
        result = laws(law_id=2000002, full_details=True)
        law = result.items[0]
        self.assertIsNotNone(law.original_bill)
        self.assertEqual(law.original_bill.bill_id, 147159)  # חוק הבזק

    def test_common_parent_fallback(self):
        """Law 2000037 has no 'החוק המקורי' but all bindings share one parent."""
        result = laws(law_id=2000037, full_details=True)
        law = result.items[0]
        self.assertIsNotNone(law.original_bill)
        self.assertIsNotNone(law.original_bill.bill_id)
        self.assertIsNotNone(law.original_bill.name)

    def test_multiple_parents_no_original_bill(self):
        """Law 2001386 has multiple parents and no 'החוק המקורי' — None."""
        result = laws(law_id=2001386, full_details=True)
        law = result.items[0]
        self.assertIsNone(law.original_bill)


class TestBasicLawDetail(unittest.TestCase):
    """חוק-יסוד: הכנסת (ID 2000037, Knesset 3)."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2000037, full_details=True)
        cls.law = result.items[0]

    def test_is_basic_law(self):
        self.assertIsNotNone(self.law.law_types)
        self.assertIn("חוק יסוד", self.law.law_types)

    def test_name_contains_knesset(self):
        self.assertIn("כנסת", self.law.name)


class TestSortOrder(unittest.TestCase):
    def test_sorted_by_publication_date_desc(self):
        results = laws(knesset_num=10)
        dates = [r.publication_date for r in results.items if r.publication_date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                f"Not sorted DESC: {dates[i-1]} < {dates[i]}",
            )


if __name__ == "__main__":
    unittest.main()
