"""Tests for views/member_view.py

Integration tests use the real PostgreSQL database with known historical
data from older Knessets (19, 20) which are stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.helpers import simple_date
from origins.members.get_member_models import MemberDetail, MemberDetailList, MemberRoles
from origins.members.get_member_view import (
    _is_transition_gov,
    _row_category,
    get_member,
)


# ===================================================================
# Unit tests for helper functions
# ===================================================================


class TestRowCategory(unittest.TestCase):
    """_row_category classifies rows by which fields are populated."""

    def _make_row(self, faction="", gov_ministry="", committee_id=None):
        return {
            "FactionName": faction,
            "GovMinistryName": gov_ministry,
            "CommitteeID": committee_id,
        }

    def test_faction(self):
        row = self._make_row(faction="הליכוד")
        self.assertEqual(_row_category(row), "faction")

    def test_government(self):
        row = self._make_row(gov_ministry="משרד הביטחון")
        self.assertEqual(_row_category(row), "government")

    def test_committee(self):
        row = self._make_row(committee_id=929)
        self.assertEqual(_row_category(row), "committee")

    def test_parliamentary(self):
        row = self._make_row()
        self.assertEqual(_row_category(row), "parliamentary")

    def test_empty_strings_are_parliamentary(self):
        row = self._make_row(faction="", gov_ministry="", committee_id=0)
        self.assertEqual(_row_category(row), "parliamentary")


class TestSimpleDate(unittest.TestCase):
    def test_valid_datetime(self):
        self.assertEqual(simple_date("2015-03-31T00:00:00"), "2015-03-31")

    def test_date_only(self):
        self.assertEqual(simple_date("2015-03-31"), "2015-03-31")

    def test_empty_string(self):
        self.assertIsNone(simple_date(""))

    def test_none(self):
        self.assertIsNone(simple_date(None))


class TestIsTransitionGov(unittest.TestCase):
    def test_knesset_20_gov33_is_transition(self):
        # Gov 33 < primary 34 for Knesset 20
        self.assertTrue(_is_transition_gov(20, 33))

    def test_knesset_20_gov34_not_transition(self):
        self.assertFalse(_is_transition_gov(20, 34))

    def test_knesset_20_gov0_not_transition(self):
        self.assertFalse(_is_transition_gov(20, 0))

    def test_knesset_below_20_never_transition(self):
        self.assertFalse(_is_transition_gov(15, 28))

    def test_knesset_19_gov32_is_transition(self):
        # Gov 32 < primary 33 for Knesset 19
        self.assertTrue(_is_transition_gov(19, 32))

    def test_knesset_19_gov33_not_transition(self):
        self.assertFalse(_is_transition_gov(19, 33))

    def test_none_gov_num(self):
        self.assertFalse(_is_transition_gov(20, None))


# ===================================================================
# Integration tests — use real PostgreSQL database
# ===================================================================


class TestGetMemberSingleKnesset(unittest.TestCase):
    """get_member with a specific knesset_num returns a MemberDetailList with one item."""

    def test_netanyahu_knesset_20(self):
        """Netanyahu (965) in Knesset 20: known PM with many portfolios."""
        result = get_member(965, knesset_num=20)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, MemberDetailList)
        self.assertEqual(len(result.items), 1)
        m = result.items[0]
        self.assertIsInstance(m, MemberDetail)

        self.assertEqual(m.member_id, 965)
        self.assertEqual(m.name, "בנימין נתניהו")
        self.assertEqual(m.gender, "זכר")
        self.assertEqual(m.knesset_num, 20)
        self.assertEqual(m.faction, ["הליכוד"])
        self.assertEqual(len(m.roles.government), 24)
        self.assertEqual(len(m.roles.committees), 0)
        self.assertEqual(len(m.roles.parliamentary), 1)

    def test_lapid_knesset_20_output_structure(self):
        """Lapid (23594) in Knesset 20: opposition MK with committees."""
        result = get_member(23594, knesset_num=20)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, MemberDetailList)
        self.assertEqual(len(result.items), 1)
        m = result.items[0]
        self.assertIsInstance(m, MemberDetail)

        # Verify top-level attributes
        for key in ("member_id", "name", "gender", "knesset_num", "faction", "roles"):
            self.assertTrue(hasattr(m, key))
        # Verify roles sub-attributes — always includes committees
        for key in ("government", "parliamentary", "committees"):
            self.assertTrue(hasattr(m.roles, key))

        self.assertEqual(m.faction, ["יש עתיד"])
        self.assertEqual(len(m.roles.government), 0)  # opposition
        self.assertEqual(len(m.roles.parliamentary), 1)
        self.assertEqual(len(m.roles.committees), 3)

        # Verify committee model shape
        c = m.roles.committees[0]
        for key in ("id", "name", "role", "start", "end"):
            self.assertTrue(hasattr(c, key))

        # Verify parliamentary model shape
        p = m.roles.parliamentary[0]
        for key in ("name", "role", "start", "end"):
            self.assertTrue(hasattr(p, key))


class TestGetMemberAllKnessets(unittest.TestCase):
    """get_member without knesset_num returns a MemberDetailList."""

    def test_netanyahu_all_terms(self):
        """Netanyahu served in multiple Knessets."""
        result = get_member(965)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, MemberDetailList)
        self.assertGreater(len(result.items), 1)
        # Each entry should have a different knesset_num
        knesset_nums = [m.knesset_num for m in result.items]
        self.assertEqual(len(knesset_nums), len(set(knesset_nums)))

    def test_nonexistent_member_returns_none(self):
        """A non-existent member ID should return None."""
        result = get_member(999999999)
        self.assertIsNone(result)


class TestTransitionGovernment(unittest.TestCase):
    def test_netanyahu_knesset_20_transition_flags(self):
        """Gov 33 roles should be marked as transition; gov 34 should not."""
        result = get_member(965, knesset_num=20)
        self.assertIsNotNone(result)
        m = result.items[0]
        gov_roles = m.roles.government
        self.assertGreater(len(gov_roles), 0)

        # Find at least one transition and one non-transition
        transition_found = any(r.is_transition for r in gov_roles)
        non_transition_found = any(not r.is_transition for r in gov_roles)
        self.assertTrue(transition_found, "Expected at least one transition gov role")
        self.assertTrue(non_transition_found, "Expected at least one non-transition gov role")


class TestDateFormatting(unittest.TestCase):
    def test_dates_have_no_time_component(self):
        """Dates in output should be YYYY-MM-DD, not datetime strings."""
        result = get_member(23594, knesset_num=20)
        self.assertIsNotNone(result)
        m = result.items[0]
        parl = m.roles.parliamentary[0]
        self.assertEqual(parl.start, "2015-03-31")
        self.assertEqual(parl.end, "2019-04-30")
        self.assertNotIn("T", parl.start)
        self.assertNotIn("T", parl.end)


class TestCommitteesAlwaysIncluded(unittest.TestCase):
    """In the detail view, committees are always included."""

    def test_committees_always_present(self):
        """get_member always includes committees in roles."""
        result = get_member(23594, knesset_num=20)
        self.assertIsNotNone(result)
        m = result.items[0]
        self.assertTrue(hasattr(m.roles, "committees"))
        self.assertEqual(len(m.roles.committees), 3)

    def test_committees_empty_when_member_has_none(self):
        """Netanyahu (965) has no committee roles; committees list should be empty."""
        result = get_member(965, knesset_num=20)
        self.assertIsNotNone(result)
        m = result.items[0]
        self.assertTrue(hasattr(m.roles, "committees"))
        self.assertEqual(len(m.roles.committees), 0)

    def test_detail_roles_keys(self):
        """Detail view roles should have government, parliamentary, and committees."""
        result = get_member(23594, knesset_num=20)
        self.assertIsNotNone(result)
        self.assertEqual(set(MemberRoles.model_fields.keys()), {"government", "parliamentary", "committees"})


if __name__ == "__main__":
    unittest.main()
