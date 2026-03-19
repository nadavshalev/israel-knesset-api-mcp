"""Tests for committee session views (search_cmt_sessions, get_cmt_session).

Integration tests use the real PostgreSQL database with known historical
data from Knesset 20 which is stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.committees.search_cmt_sessions_view import search_cmt_sessions
from origins.committees.search_cmt_sessions_models import CmtSessionSearchResults, CmtSessionSummary
from origins.committees.get_cmt_session_view import get_cmt_session
from origins.committees.get_cmt_session_models import CmtSessionDetail, CmtSessionItem, CmtSessionDocument, ItemVote


# ===================================================================
# search_cmt_sessions tests
# ===================================================================


class TestSearchByCommitteeId(unittest.TestCase):
    """Filter sessions by committee_id."""

    def test_committee_922_has_sessions(self):
        """Committee 922 (ועדת הכספים) had many sessions in Knesset 20."""
        results = search_cmt_sessions(committee_id=922)
        self.assertIsInstance(results, CmtSessionSearchResults)
        self.assertGreater(len(results.items), 100)
        for r in results.items:
            self.assertEqual(r.committee_id, 922)


class TestSearchByKnessetNum(unittest.TestCase):
    """Filter sessions by knesset_num."""

    def test_knesset_20_sessions(self):
        """Knesset 20 should have many committee sessions."""
        results = search_cmt_sessions(knesset_num=20)
        self.assertGreater(len(results.items), 1000)
        for r in results.items:
            self.assertEqual(r.knesset_num, 20)


class TestSearchByDateRange(unittest.TestCase):
    """Filter sessions by date range."""

    def test_january_2016(self):
        """Sessions in January 2016 (Knesset 20)."""
        results = search_cmt_sessions(
            knesset_num=20, date="2016-01-01", date_to="2016-01-31"
        )
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertGreaterEqual(r.date, "2016-01-01")
            self.assertLessEqual(r.date, "2016-01-31")

    def test_single_date(self):
        """Sessions on a specific date."""
        results = search_cmt_sessions(date="2016-01-04")
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertEqual(r.date, "2016-01-04")


class TestSearchByQuery(unittest.TestCase):
    """Filter sessions by query text (committee name or item name)."""

    def test_query_committee_name(self):
        """Search for sessions by committee name."""
        results = search_cmt_sessions(knesset_num=20, query="כספים")
        self.assertGreater(len(results.items), 0)
        # At least some should have כספים in committee name
        has_match = any("כספים" in (r.committee_name or "") for r in results.items)
        self.assertTrue(has_match)


class TestSearchBySessionType(unittest.TestCase):
    """Filter sessions by session type."""

    def test_open_sessions(self):
        """Filter to open sessions (פתוחה)."""
        results = search_cmt_sessions(knesset_num=20, session_type="פתוחה")
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertIn("פתוחה", r.type)


class TestSearchOutputStructure(unittest.TestCase):
    """Verify output model structure."""

    def test_returns_pydantic_model(self):
        """search_cmt_sessions returns CmtSessionSearchResults."""
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        self.assertIsInstance(results, CmtSessionSearchResults)
        self.assertGreater(len(results.items), 0)
        for r in results.items:
            self.assertIsInstance(r, CmtSessionSummary)

    def test_summary_fields(self):
        """Each session summary has expected fields."""
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        self.assertGreater(len(results.items), 0)
        r = results.items[0]
        self.assertIsNotNone(r.session_id)
        self.assertIsNotNone(r.committee_id)
        self.assertEqual(r.committee_id, 922)
        self.assertIsNotNone(r.knesset_num)

    def test_item_count_present(self):
        """Sessions should have item_count >= 0."""
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        for r in results.items:
            self.assertGreaterEqual(r.item_count, 0)


class TestSearchSorting(unittest.TestCase):
    """Results are sorted by date DESC."""

    def test_sorted_by_date_desc(self):
        """Results should be sorted newest first."""
        results = search_cmt_sessions(committee_id=922)
        dates = [r.date for r in results.items if r.date]
        self.assertGreater(len(dates), 1)
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                f"Not sorted DESC: {dates[i-1]} < {dates[i]}",
            )


# ===================================================================
# get_cmt_session tests
# ===================================================================


class TestGetCmtSessionMetadata(unittest.TestCase):
    """Get a known session and verify metadata."""

    def test_known_session(self):
        """Get a known Knesset 20 finance committee session."""
        # First find a session ID from committee 922
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        self.assertGreater(len(results.items), 0)
        session_id = results.items[0].session_id

        detail = get_cmt_session(session_id=session_id)
        self.assertIsNotNone(detail)
        self.assertIsInstance(detail, CmtSessionDetail)
        self.assertEqual(detail.session_id, session_id)
        self.assertEqual(detail.committee_id, 922)
        self.assertIsNotNone(detail.committee_name)
        self.assertIn("כספים", detail.committee_name)

    def test_nonexistent_session(self):
        """Non-existent session ID returns None."""
        result = get_cmt_session(session_id=999999999)
        self.assertIsNone(result)


class TestGetCmtSessionItems(unittest.TestCase):
    """Verify items list is populated."""

    def test_session_has_items(self):
        """A finance committee session should have agenda items."""
        # Find a session with items
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        sessions_with_items = [r for r in results.items if r.item_count > 0]
        self.assertGreater(len(sessions_with_items), 0)

        session_id = sessions_with_items[0].session_id
        detail = get_cmt_session(session_id=session_id)
        self.assertIsNotNone(detail)
        self.assertGreater(len(detail.items), 0)

        item = detail.items[0]
        self.assertIsInstance(item, CmtSessionItem)
        self.assertIsNotNone(item.item_id)


class TestGetCmtSessionDocuments(unittest.TestCase):
    """Verify documents list is populated."""

    def test_session_has_documents(self):
        """At least some sessions should have documents."""
        # Get several sessions and find one with documents
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        found_docs = False
        for r in results.items[:20]:
            detail = get_cmt_session(session_id=r.session_id)
            if detail and len(detail.documents) > 0:
                found_docs = True
                doc = detail.documents[0]
                self.assertIsInstance(doc, CmtSessionDocument)
                self.assertIsNotNone(doc.document_id)
                break
        self.assertTrue(found_docs, "Expected at least one session with documents")


class TestGetCmtSessionVotes(unittest.TestCase):
    """Verify votes are included for bill items."""

    def test_bill_item_has_votes(self):
        """At least some bill items should have plenum votes."""
        # Committee 922 (finance) discusses many bills that get voted on
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        sessions_with_items = [r for r in results.items if r.item_count > 0]

        found_votes = False
        for r in sessions_with_items[:30]:
            detail = get_cmt_session(session_id=r.session_id)
            if not detail:
                continue
            bill_items = [i for i in detail.items if i.item_type_id == 2 and i.votes]
            if bill_items:
                found_votes = True
                vote = bill_items[0].votes[0]
                self.assertIsInstance(vote, ItemVote)
                self.assertIsNotNone(vote.vote_id)
                self.assertIsNotNone(vote.date)
                break
        self.assertTrue(found_votes, "Expected at least one bill item with votes")

    def test_non_bill_items_have_no_votes(self):
        """Non-bill items (ItemTypeID != 2) should not have votes."""
        results = search_cmt_sessions(committee_id=922, knesset_num=20)
        for r in results.items[:10]:
            detail = get_cmt_session(session_id=r.session_id)
            if not detail:
                continue
            non_bill_items = [i for i in detail.items if i.item_type_id != 2]
            for item in non_bill_items:
                self.assertIsNone(item.votes)


if __name__ == "__main__":
    unittest.main()
