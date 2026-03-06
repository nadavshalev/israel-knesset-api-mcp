"""Tests for views/members_view.py

Integration tests use the real PostgreSQL database with known historical
data from older Knessets (19, 20) which are stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from views.members_view import search_members


# ===================================================================
# Integration tests — use real PostgreSQL database
# ===================================================================


class TestSearchByPersonId(unittest.TestCase):
    """Query by person_id for known historical figures — summary view."""

    def test_netanyahu_knesset_20_summary(self):
        """Netanyahu (965) in Knesset 20: summary includes role_types."""
        results = search_members(person_id=965, knesset_num=20)
        self.assertEqual(len(results), 1)

        m = results[0]
        self.assertEqual(m["member_id"], 965)
        self.assertEqual(m["name"], "בנימין נתניהו")
        self.assertEqual(m["gender"], "זכר")
        self.assertEqual(m["knesset_num"], 20)
        self.assertEqual(m["faction"], ["הליכוד"])
        self.assertIn("role_types", m)
        self.assertIsInstance(m["role_types"], list)
        self.assertGreater(len(m["role_types"]), 0)
        # Should NOT have detailed roles sub-dict
        self.assertNotIn("roles", m)

    def test_lapid_knesset_20_summary_structure(self):
        """Lapid (23594) in Knesset 20: verify summary keys."""
        results = search_members(person_id=23594, knesset_num=20)
        self.assertEqual(len(results), 1)

        m = results[0]
        for key in ("member_id", "name", "gender", "knesset_num", "faction", "role_types"):
            self.assertIn(key, m)
        self.assertNotIn("roles", m)
        self.assertEqual(m["faction"], ["יש עתיד"])


class TestKnessetFilter(unittest.TestCase):
    def test_knesset_20_total_count(self):
        """Knesset 20 had 154 distinct members."""
        results = search_members(knesset_num=20)
        self.assertEqual(len(results), 154)


class TestCrossCategoryFilters(unittest.TestCase):
    """Filters that span different row categories (e.g. role + party)."""

    def test_ministers_from_likud_knesset_20(self):
        """שר + ליכוד in Knesset 20: 19 ministers."""
        results = search_members(
            role_type="שר", party="ליכוד", knesset_num=20
        )
        self.assertEqual(len(results), 19)
        actual_ids = {m["member_id"] for m in results}
        expected_ids = {
            467, 468, 475, 477, 479, 556, 965, 1025, 1037, 2178,
            4397, 12937, 12944, 12948, 12951, 12959, 12963, 30056, 30097,
        }
        self.assertEqual(actual_ids, expected_ids)

    def test_deputy_minister_bayit_yehudi_knesset_19(self):
        """סגן שר + הבית היהודי in Knesset 19: 2 deputy ministers."""
        results = search_members(
            role_type="סגן שר", party="הבית היהודי", knesset_num=19
        )
        self.assertEqual(len(results), 2)
        actual_ids = {m["member_id"] for m in results}
        self.assertEqual(actual_ids, {23531, 23533})

    def test_minister_meretz_knesset_20_empty(self):
        """שר + מרצ in Knesset 20: Meretz was in opposition, 0 results."""
        results = search_members(
            role_type="שר", party="מרצ", knesset_num=20
        )
        self.assertEqual(len(results), 0)


class TestPartyFilter(unittest.TestCase):
    def test_yesh_atid_knesset_19(self):
        """Yesh Atid had 19 members in Knesset 19."""
        results = search_members(
            party="יש עתיד", knesset_num=19
        )
        self.assertEqual(len(results), 19)
        actual_ids = {m["member_id"] for m in results}
        expected_ids = {
            23594, 23595, 23596, 23597, 23598, 23599, 23600, 23601,
            23602, 23631, 23632, 23633, 23634, 23635, 23636, 23637,
            23638, 23639, 23640,
        }
        self.assertEqual(actual_ids, expected_ids)


class TestNameFilters(unittest.TestCase):
    def test_first_name(self):
        results = search_members(first_name="אביגדור", knesset_num=20)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["member_id"], 427)
        self.assertIn("ליברמן", results[0]["name"])

    def test_last_name(self):
        results = search_members(last_name="לפיד", knesset_num=20)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["member_id"], 23594)

    def test_combined_first_and_last(self):
        results = search_members(
            first_name="יאיר", last_name="לפיד", knesset_num=20
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["member_id"], 23594)


class TestRoleQuery(unittest.TestCase):
    def test_education_role_query_knesset_20(self):
        """Free text search for 'חינוך' across duties, ministries, committees."""
        results = search_members(role="חינוך", knesset_num=20)
        self.assertEqual(len(results), 45)


class TestRoleType(unittest.TestCase):
    def test_nonexistent_role_type_returns_empty(self):
        """A role type that doesn't exist in position_raw returns []."""
        results = search_members(role_type="שגריר")
        self.assertEqual(results, [])

    def test_pm_role_type_knesset_20(self):
        """ראש הממשלה matches PM positions; Netanyahu was PM in Knesset 20."""
        results = search_members(
            role_type="ראש הממשלה", knesset_num=20
        )
        ids = {m["member_id"] for m in results}
        self.assertIn(965, ids)


class TestNoFilters(unittest.TestCase):
    def test_no_filters_returns_results(self):
        """With no filters, we should get results across all Knessets."""
        results = search_members()
        self.assertGreater(len(results), 0)


class TestRoleTypesField(unittest.TestCase):
    """Test the role_types list in the summary output."""

    def test_netanyahu_has_minister_role_type(self):
        """Netanyahu should have שר in his role_types."""
        results = search_members(person_id=965, knesset_num=20)
        self.assertEqual(len(results), 1)
        role_types = results[0]["role_types"]
        # Should contain minister-like roles
        has_minister = any("שר" in rt for rt in role_types)
        self.assertTrue(has_minister, f"Expected שר in role_types: {role_types}")

    def test_lapid_has_member_role_type(self):
        """Lapid should have חבר כנסת in his role_types."""
        results = search_members(person_id=23594, knesset_num=20)
        self.assertEqual(len(results), 1)
        role_types = results[0]["role_types"]
        has_mk = any("חבר" in rt for rt in role_types)
        self.assertTrue(has_mk, f"Expected חבר in role_types: {role_types}")


if __name__ == "__main__":
    unittest.main()
