"""Tests for the unified bills tool.

Integration tests use the real PostgreSQL database with known historical
data.  Bill data from older Knessets (19, 20) is stable.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.bills.bills_view import bills
from origins.bills.bills_models import BillResultPartial, BillResultFull, BillsResults


class TestSearchMode(unittest.TestCase):
    """Basic bill searches (partial results)."""

    def test_knesset_20_basic_laws(self):
        results = bills(knesset_num=20, name_query="חוק-יסוד: כבוד")
        self.assertIsInstance(results, BillsResults)
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIsInstance(b, BillResultPartial)
            self.assertEqual(b.knesset_num, 20)

    def test_no_stages_in_partial(self):
        """Partial results should be BillResultPartial, not BillResultFull."""
        results = bills(knesset_num=20, name_query="חוק-יסוד: כבוד")
        for b in results.items:
            self.assertNotIsInstance(b, BillResultFull)

    def test_output_structure(self):
        results = bills(knesset_num=20, name_query="חוק-יסוד: כבוד")
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIsNotNone(b.bill_id)
            self.assertIsNotNone(b.name)
            self.assertIsNotNone(b.knesset_num)

    def test_name_no_match(self):
        results = bills(name_query="xxxNOTEXISTxxx", knesset_num=20)
        self.assertEqual(len(results.items), 0)


class TestNameFilter(unittest.TestCase):
    def test_name_search(self):
        results = bills(name_query="מדינת הלאום", knesset_num=20)
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIn("לאום", b.name)


class TestTypeFilter(unittest.TestCase):
    def test_government_bills(self):
        results = bills(type="ממשלתית", knesset_num=20, name_query="חינוך")
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIn("ממשלתית", b.type)


class TestStatusFilter(unittest.TestCase):
    def test_passed_third_reading(self):
        results = bills(status="התקבלה בקריאה שלישית", knesset_num=20, name_query="חינוך")
        self.assertGreater(len(results.items), 0)
        for b in results.items:
            self.assertIn("קריאה שלישית", b.status)


class TestDateFilter(unittest.TestCase):
    def test_date_filter_returns_bills(self):
        results = bills(from_date="2016-06-01", knesset_num=20)
        self.assertGreater(len(results.items), 0)


class TestInitiatorFilter(unittest.TestCase):
    def test_initiator_id_filter(self):
        # Person ID 427 is known to have initiated bills in Knesset 20
        results = bills(initiator_id=427, knesset_num=20)
        self.assertGreater(len(results.items), 0)


class TestBillIdAutoFullDetails(unittest.TestCase):
    """bill_id auto-enables full_details."""

    def test_nation_state_bill(self):
        """Bill 565913: Basic Law: Nation State (Knesset 20)."""
        results = bills(bill_id=565913)
        self.assertEqual(len(results.items), 1)
        b = results.items[0]
        self.assertEqual(b.bill_id, 565913)
        self.assertEqual(b.knesset_num, 20)
        self.assertIn("לאום", b.name)
        self.assertEqual(b.type, "פרטית")
        self.assertIn("קריאה שלישית", b.status)
        # Full detail fields should be present
        self.assertIsNotNone(b.stages)

    def test_nonexistent_bill_returns_empty(self):
        results = bills(bill_id=999999999)
        self.assertEqual(len(results.items), 0)


class TestFullDetailStages(unittest.TestCase):
    """Verify unified stages (plenum + committee) in full detail mode."""

    def test_nation_state_has_stages(self):
        results = bills(bill_id=565913)
        b = results.items[0]
        self.assertIsNotNone(b.stages)
        self.assertGreater(len(b.stages), 0)

    def test_stages_have_session_info(self):
        results = bills(bill_id=565913)
        b = results.items[0]
        for s in b.stages:
            # Each stage has either a plenum or committee session
            has_session = s.plenum_session is not None or s.committee_session is not None
            self.assertTrue(has_session, f"Stage {s} has no session info")

    def test_stages_sorted_by_date(self):
        results = bills(bill_id=565913)
        b = results.items[0]
        dates = []
        for s in b.stages:
            session = s.plenum_session or s.committee_session
            if session and session.date:
                dates.append(session.date)
        self.assertEqual(dates, sorted(dates))


class TestFullDetailStagesWithVotes(unittest.TestCase):
    """Verify votes are attached to plenum stages (via stage.plenum_session.vote)."""

    def test_no_duplicate_sessions(self):
        """Each session should appear at most once in stages (DISTINCT ON deduplication)."""
        results = bills(bill_id=565913)
        b = results.items[0]
        session_ids = [
            s.plenum_session.session_id
            for s in b.stages
            if s.plenum_session
        ]
        self.assertEqual(len(session_ids), len(set(session_ids)), "Duplicate plenum sessions in stages")

    def test_stage_with_vote(self):
        """Session 2016214 should have vote 26916 attached."""
        results = bills(bill_id=565913)
        b = results.items[0]
        match = [s for s in b.stages if s.plenum_session and s.plenum_session.session_id == 2016214]
        self.assertEqual(len(match), 1)
        self.assertIsNotNone(match[0].plenum_session.vote)
        self.assertEqual(match[0].plenum_session.vote.vote_id, 26916)

    def test_stage_without_vote(self):
        """Tabling session 568294 has no associated vote."""
        results = bills(bill_id=565913)
        b = results.items[0]
        match = [s for s in b.stages if s.plenum_session and s.plenum_session.session_id == 568294]
        self.assertEqual(len(match), 1)
        self.assertIsNone(match[0].plenum_session.vote)

    def test_deduplication_for_bill_2234071(self):
        """Bill 2234071 should have exactly 3 unique session stages, not duplicates."""
        results = bills(bill_id=2234071)
        if not results.items:
            self.skipTest("Bill 2234071 not in DB")
        b = results.items[0]
        session_ids = [
            s.plenum_session.session_id
            for s in b.stages
            if s.plenum_session
        ]
        self.assertEqual(len(session_ids), len(set(session_ids)), "Duplicate plenum sessions in stages")


class TestFullDetailInitiators(unittest.TestCase):
    def test_initiators_present(self):
        results = bills(bill_id=565913)
        b = results.items[0]
        self.assertIsNotNone(b.initiators)
        self.assertIsNotNone(b.initiators.primary)
        self.assertGreater(len(b.initiators.primary), 0)


class TestSortOrder(unittest.TestCase):
    def test_sorted_by_publication_date_desc(self):
        results = bills(name_query="חוק-יסוד: כבוד", knesset_num=20)
        dates = [b.publication_date for b in results.items if b.publication_date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                f"Not sorted DESC: {dates[i-1]} < {dates[i]}",
            )


if __name__ == "__main__":
    unittest.main()
