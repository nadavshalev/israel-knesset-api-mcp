"""Tests for the unified votes tool.

Integration tests use the real PostgreSQL database with known historical data.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.helpers import simple_date, simple_time
from origins.votes.votes_view import votes
from origins.votes.votes_models import VotesResults, VoteResultPartial, VoteResultFull


# ===================================================================
# Search mode (partial)
# ===================================================================


class TestSearchMode(unittest.TestCase):

    def test_knesset_20_opening_day(self):
        results = votes(knesset_num=20, date="2015-03-31")
        self.assertIsInstance(results, VotesResults)
        self.assertEqual(len(results.items), 2)
        for v in results.items:
            self.assertIsInstance(v, VoteResultPartial)
            self.assertEqual(v.knesset_num, 20)
            self.assertEqual(v.date, "2015-03-31")

    def test_partial_has_no_members(self):
        """Partial results should be VoteResultPartial, not VoteResultFull."""
        results = votes(knesset_num=20, date="2015-03-31")
        for v in results.items:
            self.assertNotIsInstance(v, VoteResultFull)

    def test_output_fields(self):
        results = votes(knesset_num=20, date="2015-03-31")
        for v in results.items:
            self.assertIsNotNone(v.vote_id)
            self.assertIsNotNone(v.knesset_num)
            self.assertIsNotNone(v.session_id)
            self.assertIsNotNone(v.title)
            self.assertIsNotNone(v.date)
            self.assertIsNotNone(v.time)
            self.assertIsNotNone(v.is_accepted)
            self.assertIsNotNone(v.total_for)
            self.assertIsNotNone(v.total_against)
            self.assertIsNotNone(v.total_abstain)


class TestDateFilters(unittest.TestCase):

    def test_date_range(self):
        results = votes(date="2015-03-31", date_to="2015-04-07", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        for v in results.items:
            self.assertGreaterEqual(v.date, "2015-03-31")
            self.assertLessEqual(v.date, "2015-04-07")


class TestNameFilter(unittest.TestCase):

    def test_title_search(self):
        results = votes(name="בחירת יושב-ראש", knesset_num=20)
        self.assertGreater(len(results.items), 0)

    def test_subject_search(self):
        """Use bill_id + name to find reservation votes (bypasses count guard)."""
        results = votes(bill_id=565913, name="הסתייגות")
        self.assertGreater(len(results.items), 0)


class TestAcceptedFilter(unittest.TestCase):

    def test_accepted_only(self):
        results = votes(knesset_num=20, date="2015-03-31", accepted=True)
        for v in results.items:
            self.assertTrue(v.is_accepted)

    def test_rejected_only(self):
        """Use bill_id (bypasses count guard) to test accepted=False filter."""
        results = votes(bill_id=565913, accepted=False)
        self.assertGreater(len(results.items), 0)
        for v in results.items:
            self.assertFalse(v.is_accepted)

    def test_accepted_plus_rejected_equals_total(self):
        """bill_id bypasses count guard — verify partition sums to total."""
        all_v = votes(bill_id=565913)
        acc = votes(bill_id=565913, accepted=True)
        rej = votes(bill_id=565913, accepted=False)
        self.assertEqual(len(acc.items) + len(rej.items), len(all_v.items))


class TestBillIdFilter(unittest.TestCase):

    def test_filter_by_bill_id(self):
        results = votes(bill_id=565913)
        self.assertEqual(len(results.items), 202)
        for v in results.items:
            self.assertEqual(v.bill_id, 565913)

    def test_nonexistent_bill_id(self):
        results = votes(bill_id=999999999)
        self.assertEqual(len(results.items), 0)


class TestSortOrder(unittest.TestCase):

    def test_sorted_by_date_time_desc(self):
        results = votes(knesset_num=20, date="2015-03-31")
        for i in range(1, len(results.items)):
            prev = results.items[i - 1]
            curr = results.items[i]
            prev_key = (prev.date, prev.time, prev.vote_id)
            curr_key = (curr.date, curr.time, curr.vote_id)
            self.assertGreaterEqual(prev_key, curr_key)


# ===================================================================
# Full detail (vote_id auto-enables)
# ===================================================================


class TestVoteIdFullDetail(unittest.TestCase):

    def test_vote_exists(self):
        results = votes(vote_id=21825)
        self.assertEqual(len(results.items), 1)
        v = results.items[0]
        self.assertEqual(v.vote_id, 21825)
        self.assertEqual(v.knesset_num, 20)
        self.assertEqual(v.date, "2015-03-31")
        self.assertEqual(v.time, "18:36")
        self.assertTrue(v.is_accepted)
        self.assertEqual(v.total_for, 103)
        self.assertEqual(v.total_against, 1)
        self.assertEqual(v.total_abstain, 7)

    def test_nonexistent_vote_returns_empty(self):
        results = votes(vote_id=999999999)
        self.assertEqual(len(results.items), 0)

    def test_bill_id_for_bill_vote(self):
        results = votes(vote_id=26916)
        self.assertEqual(results.items[0].bill_id, 565913)

    def test_bill_id_null_for_non_bill_vote(self):
        results = votes(vote_id=21825)
        self.assertIsNone(results.items[0].bill_id)


# ===================================================================
# Members (full detail)
# ===================================================================


class TestMembers(unittest.TestCase):

    def test_members_present_in_full_detail(self):
        results = votes(vote_id=21825)
        v = results.items[0]
        self.assertIsNotNone(v.members)
        self.assertIsInstance(v.members, list)

    def test_members_count(self):
        results = votes(vote_id=21825)
        self.assertEqual(len(results.items[0].members), 111)

    def test_member_structure(self):
        results = votes(vote_id=21825)
        m = results.items[0].members[0]
        self.assertTrue(hasattr(m, "member_id"))
        self.assertTrue(hasattr(m, "name"))
        self.assertTrue(hasattr(m, "party"))
        self.assertTrue(hasattr(m, "result"))

    def test_member_has_party(self):
        """Members in a vote should have party populated."""
        results = votes(vote_id=26916)
        members_with_party = [m for m in results.items[0].members if m.party]
        self.assertGreater(len(members_with_party), 0)

    def test_member_results_are_hebrew(self):
        results = votes(vote_id=21825)
        valid_results = {"בעד", "נגד", "נמנע", "נוכח", "לא נכח/ אינו נוכח", "הצביע"}
        for m in results.items[0].members:
            self.assertIn(m.result, valid_results)

    def test_full_details_flag_without_vote_id(self):
        """full_details=True without vote_id also fetches members."""
        results = votes(knesset_num=20, date="2015-03-31", full_details=True)
        self.assertGreater(len(results.items), 0)
        for v in results.items:
            self.assertIsNotNone(v.members)


# ===================================================================
# Related votes (full detail)
# ===================================================================


class TestRelatedVotes(unittest.TestCase):

    def test_related_votes_present(self):
        results = votes(vote_id=21825)
        self.assertIsNotNone(results.items[0].related_votes)
        self.assertIsInstance(results.items[0].related_votes, list)

    def test_standalone_has_no_related(self):
        results = votes(vote_id=21825)
        self.assertEqual(len(results.items[0].related_votes), 0)

    def test_terror_law_has_related(self):
        results = votes(vote_id=29054)
        self.assertEqual(len(results.items[0].related_votes), 2)

    def test_related_exclude_self(self):
        results = votes(vote_id=29054)
        ids = {rv.vote_id for rv in results.items[0].related_votes}
        self.assertNotIn(29054, ids)

    def test_related_sorted_by_ordinal(self):
        results = votes(vote_id=29054)
        ids = [rv.vote_id for rv in results.items[0].related_votes]
        self.assertEqual(ids, [29036, 29053])

    def test_odata_vote_related(self):
        results = votes(vote_id=45274)
        self.assertEqual(len(results.items[0].related_votes), 9)


# ===================================================================
# OData votes — inferred totals and is_accepted
# ===================================================================


class TestODataVotes(unittest.TestCase):

    def test_odata_is_accepted_inferred(self):
        results = votes(knesset_num=25, date="2022-12-19")
        for v in results.items:
            self.assertIsNotNone(v.is_accepted, f"vote {v.vote_id} has is_accepted=None")

    def test_odata_totals_computed(self):
        results = votes(knesset_num=25, date="2022-12-19")
        for v in results.items:
            self.assertIsNotNone(v.total_for, f"vote {v.vote_id} has total_for=None")

    def test_specific_accepted(self):
        results = votes(knesset_num=25, date="2022-12-13", accepted=True)
        ids = {v.vote_id for v in results.items}
        self.assertIn(37683, ids)
        v = next(r for r in results.items if r.vote_id == 37683)
        self.assertEqual(v.total_for, 61)
        self.assertTrue(v.is_accepted)

    def test_specific_rejected(self):
        results = votes(knesset_num=25, date="2022-12-19", accepted=False)
        ids = {v.vote_id for v in results.items}
        self.assertIn(37692, ids)
        v = next(r for r in results.items if r.vote_id == 37692)
        self.assertEqual(v.total_for, 51)
        self.assertFalse(v.is_accepted)


# ===================================================================
# Helper function unit tests
# ===================================================================


class TestSimpleDate(unittest.TestCase):
    def test_datetime_with_time(self):
        self.assertEqual(simple_date("2015-03-31T18:33:00"), "2015-03-31")

    def test_datetime_with_tz(self):
        self.assertEqual(simple_date("2021-07-13T03:40:21+03:00"), "2021-07-13")

    def test_date_only(self):
        self.assertEqual(simple_date("2015-03-31"), "2015-03-31")

    def test_empty(self):
        self.assertIsNone(simple_date(""))

    def test_none(self):
        self.assertIsNone(simple_date(None))


class TestSimpleTime(unittest.TestCase):
    def test_datetime(self):
        self.assertEqual(simple_time("2015-03-31T18:33:00"), "18:33")

    def test_no_t(self):
        self.assertIsNone(simple_time("2015-03-31"))

    def test_none(self):
        self.assertIsNone(simple_time(None))


if __name__ == "__main__":
    unittest.main()
