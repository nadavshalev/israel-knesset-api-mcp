"""Tests for the unified secondary_laws tool.

Integration tests use the real PostgreSQL database with known historical data.
Secondary legislation from older Knessets is stable.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.secondary_laws.secondary_laws_view import secondary_laws
from origins.secondary_laws.secondary_laws_models import (
    SecondaryLawResultPartial, SecondaryLawResultFull, SecondaryLawsResults,
    SecLawRegulator, SecLawBinding,
)
from origins.laws.laws_models import LawResultPartial
from core.session_models import SessionDocument


class TestSearchMode(unittest.TestCase):
    """Basic secondary law searches (partial results)."""

    def test_knesset_1_returns_results(self):
        results = secondary_laws(knesset_num=1)
        self.assertIsInstance(results, SecondaryLawsResults)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIsInstance(law, SecondaryLawResultPartial)
            self.assertEqual(law.knesset_num, 1)

    def test_no_full_detail_in_search(self):
        results = secondary_laws(knesset_num=1)
        for law in results.items:
            self.assertNotIsInstance(law, SecondaryLawResultFull)

    def test_output_fields_present(self):
        results = secondary_laws(knesset_num=1)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIsNotNone(law.secondary_law_id)
            self.assertIsNotNone(law.name)

    def test_no_match(self):
        results = secondary_laws(name_query="xxxNOTEXISTxxx", knesset_num=1)
        self.assertEqual(len(results.items), 0)


class TestNameFilter(unittest.TestCase):
    def test_name_search(self):
        results = secondary_laws(name_query="חירום", knesset_num=10)
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIn("חירום", law.name)


class TestTypeFilter(unittest.TestCase):
    def test_type_filter(self):
        """Types are 'חקיקת משנה', 'דיווח על פי חוק', 'פעולה אחרת על פי חוק'."""
        results = secondary_laws(knesset_num=20, type="חקיקת משנה",
                                 from_date="2016-01-01", to_date="2016-06-30")
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIn("חקיקת משנה", law.type)


class TestIsCurrentFilter(unittest.TestCase):
    def test_is_current_true(self):
        """IsCurrent=1 only exists for knesset 25."""
        results = secondary_laws(knesset_num=25, is_current=True,
                                 type="חקיקת משנה")
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertTrue(law.is_current)

    def test_is_current_false(self):
        results = secondary_laws(knesset_num=20, is_current=False,
                                 from_date="2016-01-01", to_date="2016-06-30")
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertFalse(law.is_current)


class TestDateFilter(unittest.TestCase):
    def test_date_range(self):
        results = secondary_laws(
            knesset_num=20, from_date="2016-01-01", to_date="2016-06-30"
        )
        self.assertGreater(len(results.items), 0)

    def test_from_date_filters_out_older(self):
        results_all = secondary_laws(knesset_num=1)
        results_recent = secondary_laws(
            knesset_num=1, from_date="1950-01-01"
        )
        self.assertLessEqual(len(results_recent.items), len(results_all.items))


class TestLawIdAutoFullDetails(unittest.TestCase):
    """secondary_law_id auto-enables full_details."""

    def test_detail_by_id(self):
        """Law 2067535 (תקנות הנכים, Knesset 20) — has regulators, auth laws, docs."""
        results = secondary_laws(secondary_law_id=2067535)
        self.assertEqual(len(results.items), 1)
        law = results.items[0]
        self.assertEqual(law.secondary_law_id, 2067535)
        self.assertIsInstance(law, SecondaryLawResultFull)

    def test_nonexistent_returns_empty(self):
        results = secondary_laws(secondary_law_id=999999999)
        self.assertEqual(len(results.items), 0)

    def test_full_details_flag(self):
        results = secondary_laws(
            knesset_num=10, name_query="חירום", full_details=True
        )
        self.assertGreater(len(results.items), 0)
        for law in results.items:
            self.assertIsInstance(law, SecondaryLawResultFull)


class TestFullDetailFields(unittest.TestCase):
    """Verify detail fields on law 2067535 (תקנות הנכים, Knesset 20)."""

    @classmethod
    def setUpClass(cls):
        result = secondary_laws(secondary_law_id=2067535)
        cls.law = result.items[0]

    def test_is_full_result(self):
        self.assertIsInstance(self.law, SecondaryLawResultFull)

    def test_has_regulators(self):
        self.assertIsNotNone(self.law.regulators)
        self.assertGreater(len(self.law.regulators), 0)
        for r in self.law.regulators:
            self.assertIsInstance(r, SecLawRegulator)

    def test_regulator_fields(self):
        r = self.law.regulators[0]
        self.assertTrue(r.regulator_type or r.regulator_name)

    def test_has_authorizing_laws(self):
        self.assertIsNotNone(self.law.authorizing_laws)
        self.assertGreater(len(self.law.authorizing_laws), 0)
        for al in self.law.authorizing_laws:
            self.assertIsInstance(al, LawResultPartial)
            self.assertIsNotNone(al.law_id)
            self.assertIsNotNone(al.name)

    def test_no_duplicate_authorizing_laws(self):
        """Authorizing laws should be deduplicated."""
        if self.law.authorizing_laws:
            ids = [al.law_id for al in self.law.authorizing_laws]
            self.assertEqual(len(ids), len(set(ids)))

    def test_has_documents(self):
        self.assertIsNotNone(self.law.documents)
        self.assertGreater(len(self.law.documents), 0)
        for d in self.law.documents:
            self.assertIsInstance(d, SessionDocument)

    def test_partial_fields_still_present(self):
        self.assertIsNotNone(self.law.secondary_law_id)
        self.assertIsNotNone(self.law.name)
        self.assertIsNotNone(self.law.knesset_num)

    def test_committee_name_resolved(self):
        """committee_name should be resolved from JOIN (not just ID)."""
        if self.law.committee_id:
            self.assertIsNotNone(self.law.committee_name)


class TestBindings(unittest.TestCase):
    """Test sec-to-sec bindings on law 2088561 (has 4 bindings as parent)."""

    @classmethod
    def setUpClass(cls):
        result = secondary_laws(secondary_law_id=2088561)
        cls.law = result.items[0] if result.items else None

    def test_law_found(self):
        self.assertIsNotNone(self.law)

    def test_has_bindings(self):
        if self.law is None:
            self.skipTest("Law not found")
        self.assertIsNotNone(self.law.bindings)
        self.assertGreater(len(self.law.bindings), 0)

    def test_binding_structure(self):
        if self.law is None or not self.law.bindings:
            self.skipTest("No bindings")
        for b in self.law.bindings:
            self.assertIsInstance(b, SecLawBinding)
            self.assertIsInstance(b.related_law, SecondaryLawResultPartial)
            self.assertIsNotNone(b.related_law.secondary_law_id)
            self.assertIn(b.related_role, ("child", "parent", "main"))
            self.assertIn(b.current_role, ("child", "parent", "main"))

    def test_binding_related_law_is_not_self(self):
        """The related law should not be the same as the current law."""
        if self.law is None or not self.law.bindings:
            self.skipTest("No bindings")
        for b in self.law.bindings:
            self.assertNotEqual(
                b.related_law.secondary_law_id, self.law.secondary_law_id
            )


class TestSortOrder(unittest.TestCase):
    def test_sorted_by_publication_date_desc(self):
        results = secondary_laws(knesset_num=1)
        dates = [r.publication_date for r in results.items if r.publication_date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                f"Not sorted DESC: {dates[i-1]} < {dates[i]}",
            )


class TestAuthorizingLawFilter(unittest.TestCase):
    """Filter by authorizing_law_id (uses sec_law_authorizing_law_raw JOIN)."""

    def test_filter_by_authorizing_law(self):
        # Law 2000111 authorizes secondary law 2067015 in knesset 20
        results = secondary_laws(authorizing_law_id=2000111, knesset_num=20)
        self.assertGreater(len(results.items), 0)
        ids = [r.secondary_law_id for r in results.items]
        self.assertIn(2067015, ids)


if __name__ == "__main__":
    unittest.main()
