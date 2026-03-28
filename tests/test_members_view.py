"""Tests for origins/members/members_view.py

Integration tests use the real PostgreSQL database with known historical
data from older Knessets (19, 20) which are stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.members.members_view import members
from origins.members.members_models import MemberResultPartial, MemberResultFull, MembersResults


# ===================================================================
# Integration tests — use real PostgreSQL database
# ===================================================================


class TestSearchByMemberId(unittest.TestCase):
    """Query by member_id with full_details=True."""

    def test_netanyahu_knesset_20_full_detail(self):
        """Netanyahu (965) in Knesset 20: full detail includes roles."""
        results = members(member_id=965, knesset_num=20, full_details=True)
        self.assertEqual(len(results.items), 1)

        m = results.items[0]
        self.assertEqual(m.member_id, 965)
        self.assertEqual(m.name, "בנימין נתניהו")
        self.assertEqual(m.gender, "זכר")
        self.assertEqual(m.knesset_num, 20)
        self.assertEqual(m.faction, ["הליכוד"])
        self.assertIsInstance(m.role_types, list)
        self.assertGreater(len(m.role_types), 0)
        # Should have full roles
        self.assertIsNotNone(m.roles)

    def test_member_id_all_terms(self):
        """member_id without knesset_num returns all terms."""
        results = members(member_id=965, full_details=True)
        self.assertGreater(len(results.items), 1)
        knesset_nums = [m.knesset_num for m in results.items]
        self.assertIn(20, knesset_nums)
        for m in results.items:
            self.assertIsNotNone(m.roles)

    def test_lapid_knesset_20_structure(self):
        """Lapid (23594) in Knesset 20: verify keys."""
        results = members(member_id=23594, knesset_num=20, full_details=True)
        self.assertEqual(len(results.items), 1)
        m = results.items[0]
        for attr in ("member_id", "name", "gender", "knesset_num", "faction", "role_types", "roles"):
            self.assertTrue(hasattr(m, attr))
        self.assertEqual(m.faction, ["יש עתיד"])


class TestKnessetFilter(unittest.TestCase):
    def test_knesset_20_narrowed_count(self):
        """Ministers in Knesset 20 (role_type narrows below MAX_SEARCH_RESULTS)."""
        results = members(knesset_num=20, role_type="שר")
        self.assertGreater(len(results.items), 0)
        for m in results.items:
            self.assertEqual(m.knesset_num, 20)
            self.assertNotIsInstance(m, MemberResultFull)  # partial mode


class TestCrossCategoryFilters(unittest.TestCase):
    """Filters that span different row categories (e.g. role + party)."""

    def test_ministers_from_likud_knesset_20(self):
        """שר + ליכוד in Knesset 20: 19 ministers."""
        results = members(role_type="שר", party="ליכוד", knesset_num=20)
        self.assertEqual(len(results.items), 19)
        actual_ids = {m.member_id for m in results.items}
        expected_ids = {
            467, 468, 475, 477, 479, 556, 965, 1025, 1037, 2178,
            4397, 12937, 12944, 12948, 12951, 12959, 12963, 30056, 30097,
        }
        self.assertEqual(actual_ids, expected_ids)

    def test_deputy_minister_bayit_yehudi_knesset_19(self):
        """סגן שר + הבית היהודי in Knesset 19: 2 deputy ministers."""
        results = members(role_type="סגן שר", party="הבית היהודי", knesset_num=19)
        self.assertEqual(len(results.items), 2)
        actual_ids = {m.member_id for m in results.items}
        self.assertEqual(actual_ids, {23531, 23533})

    def test_minister_meretz_knesset_20_empty(self):
        """שר + מרצ in Knesset 20: Meretz was in opposition, 0 results."""
        results = members(role_type="שר", party="מרצ", knesset_num=20)
        self.assertEqual(len(results.items), 0)


class TestPartyFilter(unittest.TestCase):
    def test_yesh_atid_knesset_19(self):
        """Yesh Atid had 19 members in Knesset 19."""
        results = members(party="יש עתיד", knesset_num=19)
        self.assertEqual(len(results.items), 19)
        actual_ids = {m.member_id for m in results.items}
        expected_ids = {
            23594, 23595, 23596, 23597, 23598, 23599, 23600, 23601,
            23602, 23631, 23632, 23633, 23634, 23635, 23636, 23637,
            23638, 23639, 23640,
        }
        self.assertEqual(actual_ids, expected_ids)


class TestNameFilters(unittest.TestCase):
    def test_first_name(self):
        results = members(first_name="אביגדור", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        ids = [m.member_id for m in results.items]
        self.assertIn(427, ids)  # Lieberman

    def test_last_name(self):
        results = members(last_name="לפיד", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        ids = [m.member_id for m in results.items]
        self.assertIn(23594, ids)

    def test_combined_first_and_last(self):
        results = members(first_name="יאיר", last_name="לפיד", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        ids = [m.member_id for m in results.items]
        self.assertIn(23594, ids)


class TestRoleQuery(unittest.TestCase):
    def test_education_role_query_knesset_20(self):
        """Free text search for 'חינוך' across duties, ministries, committees."""
        results = members(role="חינוך", knesset_num=20)
        self.assertEqual(len(results.items), 45)


class TestRoleType(unittest.TestCase):
    def test_nonexistent_role_type_returns_empty(self):
        """A role type that doesn't exist in position_raw returns []."""
        results = members(role_type="שגריר")
        self.assertEqual(len(results.items), 0)

    def test_pm_role_type_knesset_20(self):
        """ראש הממשלה matches PM positions; Netanyahu was PM in Knesset 20."""
        results = members(role_type="ראש הממשלה", knesset_num=20)
        ids = {m.member_id for m in results.items}
        self.assertIn(965, ids)


class TestNoFilters(unittest.TestCase):
    def test_no_filters_returns_paginated(self):
        """With no filters, pagination limits results instead of raising."""
        result = members()
        self.assertIsInstance(result, MembersResults)
        self.assertGreater(result.total_count, 0)
        self.assertLessEqual(len(result.items), 50)  # DEFAULT_PAGE_SIZE


class TestRoleTypesField(unittest.TestCase):
    """Test the role_types list in the summary output."""

    def test_netanyahu_has_minister_role_type(self):
        """Netanyahu should have שר in his role_types."""
        results = members(member_id=965, knesset_num=20, full_details=True)
        role_types = results.items[0].role_types
        has_minister = any("שר" in rt for rt in role_types)
        self.assertTrue(has_minister, f"Expected שר in role_types: {role_types}")

    def test_lapid_has_member_role_type(self):
        """Lapid should have חבר כנסת in his role_types."""
        results = members(member_id=23594, knesset_num=20, full_details=True)
        role_types = results.items[0].role_types
        has_mk = any("חבר" in rt for rt in role_types)
        self.assertTrue(has_mk, f"Expected חבר in role_types: {role_types}")


class TestFullDetailRoles(unittest.TestCase):
    """Test full detail roles structure."""

    def test_netanyahu_has_government_roles(self):
        """Netanyahu in Knesset 20 was PM — should have government roles."""
        results = members(member_id=965, knesset_num=20, full_details=True)
        m = results.items[0]
        self.assertIsNotNone(m.roles)
        self.assertGreater(len(m.roles.government), 0)

    def test_lapid_has_parliamentary_roles(self):
        """Lapid in Knesset 20 was MK — should have parliamentary roles."""
        results = members(member_id=23594, knesset_num=20, full_details=True)
        m = results.items[0]
        self.assertIsNotNone(m.roles)
        self.assertGreater(len(m.roles.parliamentary), 0)

    def test_partial_mode_has_no_roles(self):
        """Search mode (no member_id, no full_details) should be MemberResultPartial."""
        results = members(last_name="לפיד", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        for m in results.items:
            self.assertNotIsInstance(m, MemberResultFull)

    def test_full_details_flag(self):
        """full_details=True without member_id should populate roles."""
        results = members(last_name="לפיד", knesset_num=20, full_details=True)
        self.assertGreater(len(results.items), 0)
        for m in results.items:
            self.assertIsNotNone(m.roles)


if __name__ == "__main__":
    unittest.main()
