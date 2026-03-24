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
from origins.laws.laws_models import LawResultPartial, LawResultFull, LawsResults


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
        for law in results.items:
            self.assertIn("התקשורת", law.name)


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
    """law_id auto-enables full_details."""

    def test_bekesset_law(self):
        """Law 2000002: חוק התקשורת (Knesset 10)."""
        results = laws(law_id=2000002)
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
        results_by_id = laws(law_id=2001386)
        results_filtered = laws(law_id=2001386, name_query="ביטחון")
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
        result = laws(law_id=2000002)
        cls.law = result.items[0]

    def test_is_full_result(self):
        self.assertIsInstance(self.law, LawResultFull)

    def test_has_classifications(self):
        self.assertIsNotNone(self.law.classifications)
        self.assertGreater(len(self.law.classifications), 0)
        for c in self.law.classifications:
            self.assertIsInstance(c, str)

    def test_has_connected_bills(self):
        self.assertIsNotNone(self.law.bills)
        self.assertGreater(len(self.law.bills), 0)

    def test_bills_have_id_and_name(self):
        for bill in self.law.bills:
            self.assertIsNotNone(bill.bill_id)
            self.assertIsNotNone(bill.name)


class TestCorrections(unittest.TestCase):
    """Law 2001386 (חוק שירות ביטחון) has corrections."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2001386)
        cls.law = result.items[0]

    def test_has_corrections(self):
        self.assertIsNotNone(self.law.corrections)
        self.assertGreater(len(self.law.corrections), 0)

    def test_correction_fields(self):
        c = self.law.corrections[0]
        self.assertIsNotNone(c.correction_type)
        self.assertIsNotNone(c.status)

    def test_correction_has_bill_link(self):
        """At least one correction links to a bill."""
        bills = [c for c in self.law.corrections if c.bill_id is not None]
        self.assertGreater(len(bills), 0)


class TestReplacedLaws(unittest.TestCase):
    """Law 2001386 replaces law 2001385."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2001386)
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


class TestBindings(unittest.TestCase):
    """Law bindings for law 2001386."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2001386)
        cls.law = result.items[0]

    def test_has_bindings(self):
        self.assertIsNotNone(self.law.bindings)
        self.assertGreater(len(self.law.bindings), 0)

    def test_binding_has_bill_name(self):
        """Bindings include bill name."""
        named = [b for b in self.law.bindings if b.bill_name is not None]
        self.assertGreater(len(named), 0)

    def test_binding_has_date(self):
        """Bindings include date."""
        dated = [b for b in self.law.bindings if b.date is not None]
        self.assertGreater(len(dated), 0)

    def test_no_parent_law_field(self):
        """parent_law_id removed from binding model."""
        for b in self.law.bindings:
            data = b.model_dump(exclude_none=True)
            self.assertNotIn("parent_law_id", data)

    def test_no_israel_law_fields(self):
        """israel_law_id/name not in binding model."""
        for b in self.law.bindings:
            data = b.model_dump(exclude_none=True)
            self.assertNotIn("israel_law_id", data)
            self.assertNotIn("israel_law_name", data)


class TestOriginalBill(unittest.TestCase):
    """original_bill: resolved from 'החוק המקורי' binding or common ParentLawID."""

    def test_original_law_binding_type_takes_priority(self):
        """Law 2000002 has a 'החוק המקורי' binding — use its LawID."""
        result = laws(law_id=2000002)
        law = result.items[0]
        self.assertIsNotNone(law.original_bill)
        self.assertEqual(law.original_bill.bill_id, 147159)  # חוק הבזק

    def test_common_parent_fallback(self):
        """Law 2000037 has no 'החוק המקורי' but all bindings share one parent."""
        result = laws(law_id=2000037)
        law = result.items[0]
        self.assertIsNotNone(law.original_bill)
        self.assertIsNotNone(law.original_bill.bill_id)
        self.assertIsNotNone(law.original_bill.name)

    def test_multiple_parents_no_original_bill(self):
        """Law 2001386 has multiple parents and no 'החוק המקורי' — None."""
        result = laws(law_id=2001386)
        law = result.items[0]
        self.assertIsNone(law.original_bill)


class TestBasicLawDetail(unittest.TestCase):
    """חוק-יסוד: הכנסת (ID 2000037, Knesset 3)."""

    @classmethod
    def setUpClass(cls):
        result = laws(law_id=2000037)
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
