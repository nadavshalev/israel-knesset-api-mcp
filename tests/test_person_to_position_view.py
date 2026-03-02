"""Tests for views/person_to_position_view.py

Integration tests use the real data.sqlite database with known historical
data from older Knessets (19, 20) which are stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from views.person_to_position_view import (
    _is_transition_gov,
    _row_category,
    _simple_date,
    search_knesset_members,
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
        self.assertEqual(_simple_date("2015-03-31T00:00:00"), "2015-03-31")

    def test_date_only(self):
        self.assertEqual(_simple_date("2015-03-31"), "2015-03-31")

    def test_empty_string(self):
        self.assertEqual(_simple_date(""), "")

    def test_none(self):
        self.assertEqual(_simple_date(None), "")


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
# Integration tests — use real data.sqlite
# ===================================================================


class TestSearchByPersonId(unittest.TestCase):
    """Query by person_id for known historical figures."""

    def test_netanyahu_knesset_20(self):
        """Netanyahu (965) in Knesset 20: known PM with many portfolios."""
        results = search_knesset_members(person_id=965, knesset_num=20, show_committees=True)
        self.assertEqual(len(results), 1)

        m = results[0]
        self.assertEqual(m["member_id"], 965)
        self.assertEqual(m["name"], "בנימין נתניהו")
        self.assertEqual(m["gender"], "זכר")
        self.assertEqual(m["knesset_num"], 20)
        self.assertEqual(m["faction"], ["הליכוד"])
        self.assertEqual(len(m["roles"]["government"]), 24)
        self.assertEqual(len(m["roles"]["committees"]), 0)
        self.assertEqual(len(m["roles"]["parliamentary"]), 1)

    def test_lapid_knesset_20_output_structure(self):
        """Lapid (23594) in Knesset 20: opposition MK with committees."""
        results = search_knesset_members(person_id=23594, knesset_num=20, show_committees=True)
        self.assertEqual(len(results), 1)

        m = results[0]
        # Verify top-level keys
        for key in ("member_id", "name", "gender", "knesset_num", "faction", "roles"):
            self.assertIn(key, m)
        # Verify roles sub-keys
        for key in ("government", "parliamentary", "committees"):
            self.assertIn(key, m["roles"])

        self.assertEqual(m["faction"], ["יש עתיד"])
        self.assertEqual(len(m["roles"]["government"]), 0)  # opposition
        self.assertEqual(len(m["roles"]["parliamentary"]), 1)
        self.assertEqual(len(m["roles"]["committees"]), 3)

        # Verify committee dict shape
        c = m["roles"]["committees"][0]
        for key in ("id", "name", "role", "start", "end"):
            self.assertIn(key, c)

        # Verify parliamentary dict shape
        p = m["roles"]["parliamentary"][0]
        for key in ("name", "role", "start", "end"):
            self.assertIn(key, p)


class TestKnessetFilter(unittest.TestCase):
    def test_knesset_20_total_count(self):
        """Knesset 20 had 154 distinct members."""
        results = search_knesset_members(knesset_num=20)
        self.assertEqual(len(results), 154)


class TestCrossCategoryFilters(unittest.TestCase):
    """Filters that span different row categories (e.g. role + party)."""

    def test_ministers_from_likud_knesset_20(self):
        """שר + ליכוד in Knesset 20: 19 ministers."""
        results = search_knesset_members(
            role_type="שר", faction_query="ליכוד", knesset_num=20
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
        results = search_knesset_members(
            role_type="סגן שר", faction_query="הבית היהודי", knesset_num=19
        )
        self.assertEqual(len(results), 2)
        actual_ids = {m["member_id"] for m in results}
        self.assertEqual(actual_ids, {23531, 23533})

    def test_minister_meretz_knesset_20_empty(self):
        """שר + מרצ in Knesset 20: Meretz was in opposition, 0 results."""
        results = search_knesset_members(
            role_type="שר", faction_query="מרצ", knesset_num=20
        )
        self.assertEqual(len(results), 0)


class TestPartyFilter(unittest.TestCase):
    def test_yesh_atid_knesset_19(self):
        """Yesh Atid had 19 members in Knesset 19."""
        results = search_knesset_members(
            faction_query="יש עתיד", knesset_num=19
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
        results = search_knesset_members(first_name="אביגדור", knesset_num=20)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["member_id"], 427)
        self.assertIn("ליברמן", results[0]["name"])

    def test_last_name(self):
        results = search_knesset_members(last_name="לפיד", knesset_num=20)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["member_id"], 23594)

    def test_combined_first_and_last(self):
        results = search_knesset_members(
            first_name="יאיר", last_name="לפיד", knesset_num=20
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["member_id"], 23594)


class TestRoleQuery(unittest.TestCase):
    def test_education_role_query_knesset_20(self):
        """Free text search for 'חינוך' across duties, ministries, committees."""
        results = search_knesset_members(role_query="חינוך", knesset_num=20)
        self.assertEqual(len(results), 45)


class TestRoleType(unittest.TestCase):
    def test_nonexistent_role_type_returns_empty(self):
        """A role type that doesn't exist in position_raw returns []."""
        results = search_knesset_members(role_type="שגריר")
        self.assertEqual(results, [])

    def test_pm_role_type_knesset_20(self):
        """ראש הממשלה matches PM positions; Netanyahu was PM in Knesset 20."""
        results = search_knesset_members(
            role_type="ראש הממשלה", knesset_num=20
        )
        ids = {m["member_id"] for m in results}
        self.assertIn(965, ids)


class TestTransitionGovernment(unittest.TestCase):
    def test_netanyahu_knesset_20_transition_flags(self):
        """Gov 33 roles should be marked as transition; gov 34 should not."""
        results = search_knesset_members(person_id=965, knesset_num=20)
        self.assertEqual(len(results), 1)
        gov_roles = results[0]["roles"]["government"]
        self.assertGreater(len(gov_roles), 0)

        # Find at least one transition and one non-transition
        transition_found = any(r["is_transition"] for r in gov_roles)
        non_transition_found = any(not r["is_transition"] for r in gov_roles)
        self.assertTrue(transition_found, "Expected at least one transition gov role")
        self.assertTrue(non_transition_found, "Expected at least one non-transition gov role")


class TestDateFormatting(unittest.TestCase):
    def test_dates_have_no_time_component(self):
        """Dates in output should be YYYY-MM-DD, not datetime strings."""
        results = search_knesset_members(person_id=23594, knesset_num=20)
        self.assertEqual(len(results), 1)
        m = results[0]
        parl = m["roles"]["parliamentary"][0]
        self.assertEqual(parl["start"], "2015-03-31")
        self.assertEqual(parl["end"], "2019-04-30")
        self.assertNotIn("T", parl["start"])
        self.assertNotIn("T", parl["end"])


class TestNoFilters(unittest.TestCase):
    def test_no_filters_returns_results(self):
        """With no filters, we should get results across all Knessets."""
        results = search_knesset_members()
        self.assertGreater(len(results), 0)


class TestCommitteesFlag(unittest.TestCase):
    """Test the show_committees parameter."""

    def test_no_committees_by_default(self):
        """Without show_committees, roles should not have 'committees' key."""
        results = search_knesset_members(person_id=23594, knesset_num=20)
        self.assertEqual(len(results), 1)
        m = results[0]
        self.assertNotIn("committees", m["roles"])

    def test_committees_included_when_flag_set(self):
        """With show_committees=True, roles should have 'committees' key."""
        results = search_knesset_members(person_id=23594, knesset_num=20, show_committees=True)
        self.assertEqual(len(results), 1)
        m = results[0]
        self.assertIn("committees", m["roles"])
        self.assertEqual(len(m["roles"]["committees"]), 3)

    def test_committees_empty_when_member_has_none(self):
        """Netanyahu (965) has no committee roles; committees list should be empty."""
        results = search_knesset_members(person_id=965, knesset_num=20, show_committees=True)
        self.assertEqual(len(results), 1)
        m = results[0]
        self.assertIn("committees", m["roles"])
        self.assertEqual(len(m["roles"]["committees"]), 0)

    def test_default_roles_keys(self):
        """Without show_committees, roles should only have government and parliamentary."""
        results = search_knesset_members(person_id=23594, knesset_num=20)
        self.assertEqual(len(results), 1)
        m = results[0]
        self.assertEqual(set(m["roles"].keys()), {"government", "parliamentary"})


if __name__ == "__main__":
    unittest.main()
