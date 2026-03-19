"""Tests for committee_sessions unified tool.

Integration tests use the real PostgreSQL database with known historical
data from Knesset 20 which is stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.committees.committee_sessions_view import committee_sessions
from origins.committees.committee_sessions_models import CmtSessionsResults, CmtSessionResult
from core.session_models import SessionItem, SessionDocument, ItemVote


# ===================================================================
# Validation
# ===================================================================

class TestValidation(unittest.TestCase):
    """Input validation rules."""

    def test_to_date_without_from_date_raises(self):
        with self.assertRaises(ValueError):
            committee_sessions(to_date="2016-01-31")

    def test_no_session_id_and_no_from_date_raises(self):
        with self.assertRaises(ValueError):
            committee_sessions(knesset_num=20)

    def test_from_date_alone_does_not_raise(self):
        """from_date without to_date should not raise (defaults to today)."""
        results = committee_sessions(
            committee_id=922, from_date="2016-01-01", to_date="2016-01-15"
        )
        self.assertIsInstance(results, CmtSessionsResults)
        self.assertGreater(len(results.items), 0)


# ===================================================================
# Search (partial output)
# ===================================================================

class TestSearchPartial(unittest.TestCase):
    """Search returns summaries without items/docs."""

    def test_date_range(self):
        results = committee_sessions(
            knesset_num=20,
            from_date="2016-01-04",
            to_date="2016-01-10",
        )
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertGreaterEqual(r.date, "2016-01-04")
            self.assertLessEqual(r.date, "2016-01-10")

    def test_single_committee(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-02-15",
        )
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertEqual(r.committee_id, 922)

    def test_no_items_or_docs_when_partial(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-01-15",
        )
        for r in results.items:
            self.assertIsNone(r.items)
            self.assertIsNone(r.documents)

    def test_item_count_present(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-01-15",
        )
        for r in results.items:
            self.assertGreaterEqual(r.item_count, 0)

    def test_committee_name_in_results(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-01-15",
        )
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertIsNotNone(r.committee_name)
            self.assertIn("כספים", r.committee_name)

    def test_sort_order_newest_first(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-02-15",
        )
        dates = [r.date for r in results.items if r.date]
        self.assertGreater(len(dates), 1)
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                "Sessions should be sorted newest first",
            )


# ===================================================================
# Search filters
# ===================================================================

class TestSearchFilters(unittest.TestCase):
    """Various filter combinations."""

    def test_committee_name_query(self):
        results = committee_sessions(
            committee_name_query="כספים",
            from_date="2016-01-01",
            to_date="2016-01-15",
        )
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertIn("כספים", r.committee_name)

    def test_session_type_filter(self):
        results = committee_sessions(
            knesset_num=20,
            from_date="2016-01-04",
            to_date="2016-01-10",
            session_type="פתוחה",
        )
        self.assertGreater(len(results.items), 0)

    def test_query_items_filter(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-03-31",
            query_items="תקציב",
        )
        self.assertGreater(len(results.items), 0)


# ===================================================================
# Full details
# ===================================================================

class TestFullDetails(unittest.TestCase):
    """full_details=True returns items and documents."""

    def test_full_details_flag(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-04",
            to_date="2016-01-04",
            full_details=True,
        )
        self.assertGreater(len(results.items), 0)
        r = results.items[0]
        self.assertIsNotNone(r.items)
        self.assertIsNotNone(r.documents)

    def test_full_details_metadata(self):
        """Full details includes extra metadata fields."""
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-04",
            to_date="2016-01-04",
            full_details=True,
        )
        r = results.items[0]
        self.assertIsNotNone(r.type)

    def test_items_structure(self):
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-04",
            to_date="2016-01-04",
            full_details=True,
        )
        r = results.items[0]
        self.assertGreater(len(r.items), 0)
        item = r.items[0]
        self.assertIsInstance(item, SessionItem)
        self.assertIsNotNone(item.item_id)


# ===================================================================
# Session ID lookup
# ===================================================================

class TestSessionId(unittest.TestCase):
    """session_id auto-enables full_details."""

    def test_session_id_auto_full_details(self):
        # First find a valid session ID
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-01-15",
        )
        self.assertGreater(len(results.items), 0)
        sid = results.items[0].session_id

        detail = committee_sessions(session_id=sid)
        self.assertEqual(len(detail.items), 1)
        r = detail.items[0]
        self.assertEqual(r.session_id, sid)
        self.assertIsNotNone(r.items)
        self.assertIsNotNone(r.documents)

    def test_nonexistent_session_returns_empty(self):
        result = committee_sessions(session_id=999999999)
        self.assertEqual(len(result.items), 0)


# ===================================================================
# Bill votes
# ===================================================================

class TestBillVotes(unittest.TestCase):
    """Verify votes are included for bill items in full detail."""

    def test_bill_item_has_votes(self):
        """At least some bill items in finance committee should have votes."""
        results = committee_sessions(
            committee_id=922,
            from_date="2016-01-01",
            to_date="2016-02-15",
        )
        sessions_with_items = [r for r in results.items if r.item_count > 0]
        self.assertGreater(len(sessions_with_items), 0)

        found_votes = False
        for r in sessions_with_items[:20]:
            detail = committee_sessions(session_id=r.session_id)
            s = detail.items[0]
            bill_items = [i for i in s.items if i.bill_id and i.votes]
            if bill_items:
                found_votes = True
                vote = bill_items[0].votes[0]
                self.assertIsInstance(vote, ItemVote)
                self.assertIsNotNone(vote.vote_id)
                break
        self.assertTrue(found_votes, "Expected at least one bill item with votes")


if __name__ == "__main__":
    unittest.main()
