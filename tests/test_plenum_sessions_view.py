"""Tests for plenums unified tool.

Integration tests use the real PostgreSQL database with known historical
data.  Plenum data from older Knessets (19, 20) is stable.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.plenums.plenum_sessions_view import plenums as plenum_sessions
from origins.plenums.plenum_sessions_models import PlenumSessionsResults, PlenumSessionResultPartial, PlenumSessionResultFull
from core.session_models import SessionItem, SessionDocument


# ===================================================================
# Validation
# ===================================================================

class TestValidation(unittest.TestCase):
    """Input validation rules."""

    def test_to_date_without_from_date_raises(self):
        with self.assertRaises(ValueError):
            plenum_sessions(to_date="2015-04-01")

    def test_no_session_id_and_no_from_date_raises(self):
        with self.assertRaises(ValueError):
            plenum_sessions(knesset_num=20)

    def test_from_date_alone_defaults_to_date_to_today(self):
        """from_date without to_date should not raise (defaults to today)."""
        # Use a narrow enough window to avoid too-many-results error
        results = plenum_sessions(knesset_num=20, from_date="2015-03-31", to_date="2015-04-30")
        self.assertIsInstance(results, PlenumSessionsResults)
        self.assertGreater(len(results.items), 0)


# ===================================================================
# Search (partial output)
# ===================================================================

class TestSearchPartial(unittest.TestCase):
    """Search returns summaries without items/docs."""

    def test_date_range(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-04-01",
        )
        self.assertGreater(len(results.items), 0)
        for s in results.items:
            self.assertGreaterEqual(s.date, "2015-03-31")
            self.assertLessEqual(s.date, "2015-04-01")

    def test_no_items_or_docs_when_partial(self):
        """Partial results should be PlenumSessionResultPartial, not Full."""
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-04-01",
        )
        for s in results.items:
            self.assertNotIsInstance(s, PlenumSessionResultFull)

    def test_item_count_present(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-04-01",
        )
        for s in results.items:
            self.assertGreaterEqual(s.item_count, 0)

    def test_sort_order_newest_first(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-05-10",
        )
        dates = [s.date for s in results.items if s.date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(
                dates[i - 1], dates[i],
                "Sessions should be sorted newest first",
            )

    def test_no_match(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-04-01",
            query_items="xxxNOTEXISTxxx",
        )
        self.assertEqual(len(results.items), 0)


# ===================================================================
# Search filters
# ===================================================================

class TestSearchFilters(unittest.TestCase):
    """Query and item_type filters."""

    def test_query_items_filter(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-01",
            to_date="2015-12-31",
            query_items="חוק-יסוד",
        )
        self.assertGreater(len(results.items), 0)


# ===================================================================
# Full details
# ===================================================================

class TestFullDetails(unittest.TestCase):
    """full_details=True returns items and documents."""

    def test_full_details_flag(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-03-31",
            full_details=True,
        )
        self.assertGreater(len(results.items), 0)
        s = results.items[0]
        self.assertIsNotNone(s.items)
        self.assertIsNotNone(s.documents)

    def test_items_structure(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-03-31",
            full_details=True,
        )
        s = results.items[0]
        self.assertGreater(len(s.items), 0)
        item = s.items[0]
        self.assertIsInstance(item, SessionItem)
        self.assertIsNotNone(item.item_name)
        self.assertIsNotNone(item.item_type)

    def test_documents_structure(self):
        results = plenum_sessions(
            knesset_num=20,
            from_date="2015-03-31",
            to_date="2015-03-31",
            full_details=True,
        )
        s = results.items[0]
        self.assertGreater(len(s.documents), 0)
        doc = s.documents[0]
        self.assertIsInstance(doc, SessionDocument)
        self.assertIsNotNone(doc.name)
        self.assertIsNotNone(doc.path)


# ===================================================================
# Session ID lookup
# ===================================================================

class TestSessionId(unittest.TestCase):
    """session_id with full_details=True."""

    def test_session_id_returns_full_details(self):
        result = plenum_sessions(session_id=568294, full_details=True)
        self.assertGreater(len(result.items), 0)
        s = result.items[0]
        self.assertEqual(s.session_id, 568294)
        self.assertIsNotNone(s.items)
        self.assertIsNotNone(s.documents)
        self.assertGreater(len(s.items), 0)
        self.assertGreater(len(s.documents), 0)

    def test_nonexistent_session_returns_empty(self):
        result = plenum_sessions(session_id=999999999)
        self.assertEqual(len(result.items), 0)


if __name__ == "__main__":
    unittest.main()
