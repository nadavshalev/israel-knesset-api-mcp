"""Tests for views/plenum_sessions_view.py and views/plenum_session_view.py

Integration tests use the real PostgreSQL database with known historical
data.  Plenum data from older Knessets (19, 20) is stable.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.plenums.search_plenums_view import search_sessions
from origins.plenums.get_plenum_view import get_session


# ===================================================================
# List view (plenum_sessions_view)
# ===================================================================

class TestSearchSessions(unittest.TestCase):
    """Search plenum sessions — summary, no items/docs."""

    def test_knesset_20_sessions(self):
        results = search_sessions(knesset_num=20, date="2015-03-31")
        self.assertGreater(len(results.items), 0)
        for s in results.items:
            self.assertEqual(s.knesset_num, 20)

    def test_no_items_or_docs_in_list(self):
        """List view returns SessionSummary objects which lack items/documents."""
        results = search_sessions(knesset_num=20, date="2015-03-31")
        for s in results.items:
            self.assertFalse(hasattr(s, "documents"))
            # SessionSummary has no 'items' or 'documents' attributes

    def test_output_structure(self):
        results = search_sessions(knesset_num=20, date="2015-03-31")
        self.assertGreater(len(results.items), 0)
        expected_fields = {"session_id", "knesset_num", "name", "date"}
        from origins.plenums.search_plenums_models import SessionSummary
        self.assertEqual(set(SessionSummary.model_fields.keys()), expected_fields)

    def test_name_filter(self):
        results = search_sessions(knesset_num=20, name="חוק-יסוד")
        self.assertGreater(len(results.items), 0)

    def test_no_match(self):
        results = search_sessions(knesset_num=20, name="xxxNOTEXISTxxx")
        self.assertEqual(len(results.items), 0)

    def test_date_range(self):
        results = search_sessions(
            knesset_num=20,
            date="2015-03-31",
            date_to="2015-04-01",
        )
        self.assertGreater(len(results.items), 0)
        for s in results.items:
            self.assertGreaterEqual(s.date, "2015-03-31")
            self.assertLessEqual(s.date, "2015-04-01")

    def test_sort_order(self):
        """Results should be sorted by date DESC."""
        results = search_sessions(knesset_num=20, date="2015-03-31", date_to="2015-05-10")
        for i in range(1, len(results.items)):
            self.assertGreaterEqual(
                results.items[i - 1].date, results.items[i].date,
                "Sessions should be sorted newest first",
            )


# ===================================================================
# Single view (plenum_session_view)
# ===================================================================

class TestGetSession(unittest.TestCase):
    """Get full detail for a single plenum session."""

    def test_session_exists(self):
        result = get_session(568294)
        self.assertIsNotNone(result)
        self.assertEqual(result.session_id, 568294)

    def test_nonexistent_session_returns_none(self):
        result = get_session(999999999)
        self.assertIsNone(result)

    def test_output_has_items(self):
        result = get_session(568294)
        self.assertTrue(hasattr(result, "items"))
        self.assertGreater(len(result.items), 0)

    def test_output_has_documents(self):
        result = get_session(568294)
        self.assertTrue(hasattr(result, "documents"))
        self.assertGreater(len(result.documents), 0)

    def test_output_structure(self):
        result = get_session(568294)
        expected_fields = {"session_id", "knesset_num", "name", "date", "items", "documents"}
        from origins.plenums.get_plenum_models import SessionDetail
        self.assertTrue(expected_fields.issubset(SessionDetail.model_fields.keys()))

    def test_item_structure(self):
        result = get_session(568294)
        for item in result.items:
            self.assertIsNotNone(item.item_id)
            self.assertIsNotNone(item.name)
            self.assertIsNotNone(item.type)
            # "status" may be None for some items

    def test_document_structure(self):
        result = get_session(568294)
        for doc in result.documents:
            self.assertIsNotNone(doc.group_type)
            self.assertIsNotNone(doc.application)
            self.assertIsNotNone(doc.file_path)


if __name__ == "__main__":
    unittest.main()
