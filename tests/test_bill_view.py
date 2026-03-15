"""Tests for views/bill_view.py (single bill detail — with stages/votes)

Integration tests use the real PostgreSQL database with known historical
data.  Bill data from older Knessets (19, 20) is stable.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.bills.get_bill_view import get_bill
from origins.bills.get_bill_models import BillDetail, BillStage, StageVote


class TestGetBill(unittest.TestCase):
    """Query specific bills by ID."""

    def test_nation_state_bill(self):
        """Bill 565913: Basic Law: Nation State (Knesset 20)."""
        b = get_bill(565913)
        self.assertIsNotNone(b)
        self.assertIsInstance(b, BillDetail)
        self.assertEqual(b.bill_id, 565913)
        self.assertEqual(b.knesset_num, 20)
        self.assertIn("לאום", b.name)
        self.assertEqual(b.sub_type, "פרטית")
        self.assertIn("קריאה שלישית", b.status)

    def test_nonexistent_bill_returns_none(self):
        result = get_bill(999999999)
        self.assertIsNone(result)

    def test_output_structure(self):
        b = get_bill(565913)
        self.assertIsNotNone(b)
        self.assertIsInstance(b, BillDetail)
        # Always-present fields
        self.assertIsNotNone(b.bill_id)
        self.assertIsNotNone(b.name)
        self.assertIsNotNone(b.knesset_num)
        self.assertIsNotNone(b.stages)
        # These specific bill does have these fields populated
        self.assertIsNotNone(b.sub_type)
        self.assertIsNotNone(b.status)


class TestStages(unittest.TestCase):
    """Verify stages are populated for known bills."""

    def test_nation_state_has_multiple_stages(self):
        b = get_bill(565913)
        self.assertEqual(len(b.stages), 7)

    def test_stages_are_sorted_by_date(self):
        b = get_bill(565913)
        dates = [s.date for s in b.stages]
        self.assertEqual(dates, sorted(dates))

    def test_stage_structure(self):
        b = get_bill(565913)
        for s in b.stages:
            self.assertIsInstance(s, BillStage)
            self.assertIsNotNone(s.date)
            self.assertIsNotNone(s.status)
            self.assertIsNotNone(s.session_id)


class TestStageVotes(unittest.TestCase):
    """Verify the final vote attached only to the last sub-stage per session."""

    def test_stage_with_vote_has_vote_key(self):
        """Session 2016214 has only 1 stage, so it gets the vote (26916)."""
        b = get_bill(565913)
        early_debate = [s for s in b.stages if s.session_id == 2016214]
        self.assertEqual(len(early_debate), 1)
        self.assertIsNotNone(early_debate[0].vote)
        self.assertEqual(early_debate[0].vote.vote_id, 26916)

    def test_stage_without_vote_no_vote_key(self):
        """The 2015-07-29 tabling stage (sole stage in that session, no votes)."""
        b = get_bill(565913)
        tabling = [s for s in b.stages if s.session_id == 568294]
        self.assertEqual(len(tabling), 1)
        self.assertIsNone(tabling[0].vote)

    def test_vote_summary_structure(self):
        b = get_bill(565913)
        stages_with_vote = [s for s in b.stages if s.vote is not None]
        self.assertGreater(len(stages_with_vote), 0)

        vote = stages_with_vote[0].vote
        self.assertIsInstance(vote, StageVote)
        self.assertIsNotNone(vote.vote_id)
        # These fields exist on the model (may be None for some votes)
        self.assertTrue(hasattr(vote, "title"))
        self.assertTrue(hasattr(vote, "date"))
        self.assertTrue(hasattr(vote, "is_accepted"))
        self.assertTrue(hasattr(vote, "total_for"))
        self.assertTrue(hasattr(vote, "total_against"))
        self.assertTrue(hasattr(vote, "total_abstain"))

    def test_vote_only_on_last_substage_in_session(self):
        """Session 2073133 has 3 sub-stages for bill 565913.
        The vote (31013) should only appear on the last sub-stage."""
        b = get_bill(565913)
        final_stages = [s for s in b.stages if s.session_id == 2073133]
        self.assertEqual(len(final_stages), 3)
        self.assertIsNone(final_stages[0].vote)
        self.assertIsNone(final_stages[1].vote)
        self.assertIsNotNone(final_stages[2].vote)
        self.assertEqual(final_stages[2].vote.vote_id, 31013)

    def test_is_accepted_inferred_from_totals(self):
        """OData-origin votes with no stored IsAccepted should infer from totals."""
        b = get_bill(2234330)
        self.assertIsNotNone(b)
        stages_with_vote = [s for s in b.stages if s.vote is not None]
        self.assertGreater(len(stages_with_vote), 0)
        final = [s for s in stages_with_vote if s.vote.vote_id == 45274]
        self.assertEqual(len(final), 1)
        self.assertTrue(final[0].vote.is_accepted)


class TestSimpleBillWithVotes(unittest.TestCase):
    def test_bill_482355_final_votes(self):
        b = get_bill(482355)
        self.assertIsNotNone(b)
        vote_ids = set()
        for s in b.stages:
            if s.vote is not None:
                vote_ids.add(s.vote.vote_id)
        self.assertEqual(vote_ids, {19882, 21742, 22283, 24183})

    def test_bill_482355_vote_on_last_substage_only(self):
        b = get_bill(482355)
        stages_576670 = [s for s in b.stages if s.session_id == 576670]
        self.assertEqual(len(stages_576670), 2)
        self.assertIsNone(stages_576670[0].vote)
        self.assertIsNotNone(stages_576670[1].vote)
        self.assertEqual(stages_576670[1].vote.vote_id, 24183)


if __name__ == "__main__":
    unittest.main()
