"""Tests for views/committee_view.py

Integration tests use the real PostgreSQL database with known historical
data from older Knessets (19, 20) which are stable and won't change.

Known test data for committee 928 (ועדת העבודה, הרווחה והבריאות, Knesset 20):
  All data:       1084 sessions, 29 members, 212 bills, 2465 documents
  H1 2016:        170 sessions, 20 members, 44 bills, 370 documents
  (H1 2016 = date='2016-01-01', date_to='2016-06-30')
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.helpers import simple_date, simple_time
from origins.committees.get_committee_view import (
    get_committee,
)


# ===================================================================
# Unit tests for helper functions
# ===================================================================


class TestSimpleDate(unittest.TestCase):
    def test_iso_datetime(self):
        self.assertEqual(simple_date("2019-01-30T10:00:00"), "2019-01-30")

    def test_space_datetime(self):
        self.assertEqual(simple_date("2019-01-30 10:00:00"), "2019-01-30")

    def test_date_only(self):
        self.assertEqual(simple_date("2019-01-30"), "2019-01-30")

    def test_empty_string(self):
        self.assertIsNone(simple_date(""))

    def test_none(self):
        self.assertIsNone(simple_date(None))


class TestSimpleTime(unittest.TestCase):
    def test_iso_datetime(self):
        self.assertEqual(simple_time("2019-01-30T10:00:00"), "10:00")

    def test_space_datetime(self):
        self.assertEqual(simple_time("2019-01-30 10:30:00"), "10:30")

    def test_empty_string(self):
        self.assertIsNone(simple_time(""))

    def test_none(self):
        self.assertIsNone(simple_time(None))

    def test_with_timezone(self):
        self.assertEqual(simple_time("2019-01-30T10:00:00+02:00"), "10:00")


# ===================================================================
# Integration tests — use real PostgreSQL database
# ===================================================================


class TestGetCommitteeMetadata(unittest.TestCase):
    """Test committee metadata for known committees."""

    def test_committee_928_metadata(self):
        """Committee 928: ועדת העבודה, הרווחה והבריאות (Knesset 20)."""
        c = get_committee(928)
        self.assertIsNotNone(c)
        self.assertEqual(c.committee_id, 928)
        self.assertEqual(c.name, "ועדת העבודה, הרווחה והבריאות")
        self.assertEqual(c.knesset_num, 20)
        self.assertEqual(c.type, "ועדה ראשית")
        self.assertEqual(c.category, "ועדת העבודה והרווחה")
        self.assertFalse(c.is_current)

    def test_committee_922_metadata(self):
        """Committee 922: ועדת הכספים (Knesset 20)."""
        c = get_committee(922)
        self.assertIsNotNone(c)
        self.assertEqual(c.committee_id, 922)
        self.assertEqual(c.name, "ועדת הכספים")
        self.assertEqual(c.knesset_num, 20)
        self.assertEqual(c.type, "ועדה ראשית")


class TestNotFound(unittest.TestCase):
    def test_nonexistent_committee(self):
        """A non-existent committee ID should return None."""
        result = get_committee(999999999)
        self.assertIsNone(result)


class TestMetadataOnlyByDefault(unittest.TestCase):
    """Without opt-in flags, only metadata keys are present."""

    def test_metadata_keys_only(self):
        """Default call returns only metadata — no sessions/members/bills/documents."""
        c = get_committee(928)
        self.assertIsNotNone(c)
        # Always-present attributes on the Pydantic model
        always_keys = {
            "committee_id", "name", "knesset_num", "type", "category",
            "is_current", "start_date",
        }
        for key in always_keys:
            self.assertTrue(hasattr(c, key), f"Missing attribute: {key}")

    def test_no_sessions_by_default(self):
        c = get_committee(928)
        self.assertIsNone(c.sessions)
        self.assertIsNone(c.session_count)

    def test_no_members_by_default(self):
        c = get_committee(928)
        self.assertIsNone(c.members)
        self.assertIsNone(c.member_count)

    def test_no_bills_by_default(self):
        c = get_committee(928)
        self.assertIsNone(c.bills)
        self.assertIsNone(c.bill_count)

    def test_no_documents_by_default(self):
        c = get_committee(928)
        self.assertIsNone(c.documents)
        self.assertIsNone(c.document_count)


class TestOutputStructureWithFlags(unittest.TestCase):
    """Opt-in flags add the expected keys."""

    def test_all_flags(self):
        """With all flags set, all sections are present."""
        c = get_committee(928, include_sessions=True, include_members=True,
                          include_bills=True, include_documents=True)
        self.assertIsNotNone(c)
        self.assertIsNotNone(c.sessions)
        self.assertIsNotNone(c.session_count)
        self.assertIsNotNone(c.members)
        self.assertIsNotNone(c.member_count)
        self.assertIsNotNone(c.bills)
        self.assertIsNotNone(c.bill_count)
        self.assertIsNotNone(c.documents)
        self.assertIsNotNone(c.document_count)

    def test_sessions_flag_only(self):
        """Only sessions keys appear when only include_sessions is set."""
        c = get_committee(928, include_sessions=True)
        self.assertIsNotNone(c.sessions)
        self.assertIsNotNone(c.session_count)
        self.assertIsNone(c.members)
        self.assertIsNone(c.bills)
        self.assertIsNone(c.documents)

    def test_members_flag_only(self):
        c = get_committee(928, include_members=True)
        self.assertIsNotNone(c.members)
        self.assertIsNotNone(c.member_count)
        self.assertIsNone(c.sessions)
        self.assertIsNone(c.bills)
        self.assertIsNone(c.documents)

    def test_bills_flag_only(self):
        c = get_committee(928, include_bills=True)
        self.assertIsNotNone(c.bills)
        self.assertIsNotNone(c.bill_count)
        self.assertIsNone(c.sessions)
        self.assertIsNone(c.members)
        self.assertIsNone(c.documents)

    def test_documents_flag_only(self):
        c = get_committee(928, include_documents=True)
        self.assertIsNotNone(c.documents)
        self.assertIsNotNone(c.document_count)
        self.assertIsNone(c.sessions)
        self.assertIsNone(c.members)
        self.assertIsNone(c.bills)


# -------------------------------------------------------------------
# Sessions
# -------------------------------------------------------------------

class TestSessions(unittest.TestCase):
    """Test committee sessions (opt-in)."""

    def test_session_count_928_all(self):
        """Committee 928 had 1084 sessions total."""
        c = get_committee(928, include_sessions=True)
        self.assertEqual(c.session_count, 1084)

    def test_session_count_922_all(self):
        """Committee 922 had 1372 sessions total."""
        c = get_committee(922, include_sessions=True)
        self.assertEqual(c.session_count, 1372)

    def test_session_dict_keys(self):
        """Each session has expected attributes."""
        c = get_committee(928, include_sessions=True)
        s = c.sessions[0]
        # Always-present attributes; optional attributes (end_time, status,
        # location, url, broadcast_url) may be None.
        always_keys = {
            "session_id", "number", "date", "start_time", "type",
        }
        for key in always_keys:
            self.assertTrue(hasattr(s, key), f"Missing attribute: {key}")

    def test_sessions_newest_first(self):
        """Sessions are returned newest first."""
        c = get_committee(928, include_sessions=True)
        dates = [s.date for s in c.sessions if s.date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(dates[i - 1], dates[i],
                                    "Sessions should be newest first")

    def test_session_date_no_time(self):
        """Session dates should be YYYY-MM-DD without time component."""
        c = get_committee(928, include_sessions=True)
        for s in c.sessions[:20]:
            if s.date:
                self.assertNotIn("T", s.date)
                self.assertNotIn(" ", s.date)
                self.assertRegex(s.date, r"^\d{4}-\d{2}-\d{2}$")


# -------------------------------------------------------------------
# Date filtering
# -------------------------------------------------------------------

class TestDateFiltering(unittest.TestCase):
    """Test date-based filtering on sessions."""

    def test_sessions_h1_2016(self):
        """Committee 928 had 170 sessions in H1 2016."""
        c = get_committee(928, date="2016-01-01", date_to="2016-06-30",
                          include_sessions=True)
        self.assertEqual(c.session_count, 170)

    def test_date_shortcut(self):
        """The ``date`` param alone filters a single day."""
        c = get_committee(928, date="2016-03-07", include_sessions=True)
        self.assertIsNotNone(c)
        for s in c.sessions:
            self.assertEqual(s.date, "2016-03-07")

    def test_sessions_filtered_are_within_range(self):
        """All returned sessions fall within the requested date range."""
        c = get_committee(928, date="2016-01-01", date_to="2016-06-30",
                          include_sessions=True)
        for s in c.sessions:
            if s.date:
                self.assertGreaterEqual(s.date, "2016-01-01")
                self.assertLessEqual(s.date, "2016-06-30")

    def test_date_only(self):
        """Using only date (without date_to) filters to that single day."""
        c = get_committee(928, date="2019-01-01", include_sessions=True)
        self.assertIsNotNone(c)
        # Committee 928 ran until 2019-01-30, so there should be some sessions on this day
        self.assertGreater(c.session_count, 0)
        self.assertLess(c.session_count, 1084)
        for s in c.sessions:
            if s.date:
                self.assertEqual(s.date, "2019-01-01")

    def test_date_range_up_to(self):
        """Using date + date_to filters up to that date."""
        c = get_committee(928, date="2015-01-01", date_to="2015-12-31",
                          include_sessions=True)
        self.assertIsNotNone(c)
        self.assertGreater(c.session_count, 0)
        self.assertLess(c.session_count, 1084)
        for s in c.sessions:
            if s.date:
                self.assertGreaterEqual(s.date, "2015-01-01")
                self.assertLessEqual(s.date, "2015-12-31")


# -------------------------------------------------------------------
# Members
# -------------------------------------------------------------------

class TestMembers(unittest.TestCase):
    """Test committee member data (opt-in)."""

    def test_member_count_928_all(self):
        """Committee 928 had 29 member assignments total."""
        c = get_committee(928, include_members=True)
        self.assertEqual(c.member_count, 29)

    def test_member_count_928_h1_2016(self):
        """Committee 928 had 20 overlapping member assignments in H1 2016."""
        c = get_committee(928, date="2016-01-01", date_to="2016-06-30",
                          include_members=True)
        self.assertEqual(c.member_count, 20)

    def test_member_dict_keys(self):
        """Each member has expected attributes."""
        c = get_committee(928, include_members=True)
        m = c.members[0]
        expected_keys = {"member_id", "name", "knesset_num", "role", "start", "end"}
        for key in expected_keys:
            self.assertTrue(hasattr(m, key), f"Missing attribute: {key}")

    def test_known_members_928(self):
        """Known members should appear in committee 928."""
        c = get_committee(928, include_members=True)
        member_ids = {m.member_id for m in c.members}
        # Eli Alaluf (30078) was chair; Itzik Shmuli (23568) was a member
        self.assertIn(30078, member_ids)
        self.assertIn(23568, member_ids)

    def test_member_date_formatting(self):
        """Member dates should be YYYY-MM-DD."""
        c = get_committee(928, include_members=True)
        for m in c.members:
            if m.start:
                self.assertNotIn("T", m.start)
                self.assertNotIn(" ", m.start)


# -------------------------------------------------------------------
# Bills
# -------------------------------------------------------------------

class TestBills(unittest.TestCase):
    """Test bills discussed in committee (opt-in)."""

    def test_bill_count_928_all(self):
        """Committee 928 discussed 212 distinct bills total."""
        c = get_committee(928, include_bills=True)
        self.assertEqual(c.bill_count, 212)

    def test_bill_count_928_h1_2016(self):
        """Committee 928 discussed 44 distinct bills in H1 2016 sessions."""
        c = get_committee(928, date="2016-01-01", date_to="2016-06-30",
                          include_bills=True)
        self.assertEqual(c.bill_count, 44)

    def test_bill_dict_keys(self):
        """Each bill has expected attributes."""
        c = get_committee(928, include_bills=True)
        b = c.bills[0]
        expected_keys = {"bill_id", "name", "knesset_num", "sub_type", "status"}
        for key in expected_keys:
            self.assertTrue(hasattr(b, key), f"Missing attribute: {key}")

    def test_known_bill_in_928(self):
        """Bill 482355 (חוק אומנה לילדים) was discussed in committee 928."""
        c = get_committee(928, include_bills=True)
        bill_ids = {b.bill_id for b in c.bills}
        self.assertIn(482355, bill_ids)


# -------------------------------------------------------------------
# Documents
# -------------------------------------------------------------------

class TestDocuments(unittest.TestCase):
    """Test committee session documents (opt-in)."""

    def test_document_count_928_all(self):
        """Committee 928 has 2465 documents total."""
        c = get_committee(928, include_documents=True)
        self.assertEqual(c.document_count, 2465)

    def test_document_count_928_h1_2016(self):
        """Committee 928 has 370 documents from H1 2016 sessions."""
        c = get_committee(928, date="2016-01-01", date_to="2016-06-30",
                          include_documents=True)
        self.assertEqual(c.document_count, 370)

    def test_document_dict_keys(self):
        """Each document has expected attributes."""
        c = get_committee(928, include_documents=True)
        d = c.documents[0]
        # Always-present attributes; optional attribute (name) may be None.
        always_keys = {
            "document_id", "type", "format",
            "file_path", "session_id", "session_date",
        }
        for key in always_keys:
            self.assertTrue(hasattr(d, key), f"Missing attribute: {key}")

    def test_document_date_formatting(self):
        """Document session dates should be YYYY-MM-DD."""
        c = get_committee(928, include_documents=True)
        for d in c.documents[:20]:
            if d.session_date:
                self.assertNotIn("T", d.session_date)
                self.assertNotIn(" ", d.session_date)

    def test_documents_newest_first(self):
        """Documents are returned newest session first."""
        c = get_committee(928, include_documents=True)
        dates = [d.session_date for d in c.documents if d.session_date]
        for i in range(1, len(dates)):
            self.assertGreaterEqual(dates[i - 1], dates[i],
                                    "Documents should be newest first")


# -------------------------------------------------------------------
# Combined flags with date filtering
# -------------------------------------------------------------------

class TestCombinedFlagsWithDates(unittest.TestCase):
    """Test multiple flags with date filtering together."""

    def test_all_flags_h1_2016(self):
        """All flags with H1 2016 dates return correct counts."""
        c = get_committee(928, date="2016-01-01", date_to="2016-06-30",
                          include_sessions=True, include_members=True,
                          include_bills=True, include_documents=True)
        self.assertEqual(c.session_count, 170)
        self.assertEqual(c.member_count, 20)
        self.assertEqual(c.bill_count, 44)
        self.assertEqual(c.document_count, 370)

    def test_some_flags_h1_2016(self):
        """Only requested sections appear — unrequested are absent."""
        c = get_committee(928, date="2016-01-01", date_to="2016-06-30",
                          include_sessions=True, include_bills=True)
        self.assertIsNotNone(c.sessions)
        self.assertIsNotNone(c.bills)
        self.assertIsNone(c.members)
        self.assertIsNone(c.documents)


# -------------------------------------------------------------------
# Date fields
# -------------------------------------------------------------------

class TestDateFields(unittest.TestCase):
    """Committee-level date fields."""

    def test_start_date_no_time(self):
        """Committee start_date should be YYYY-MM-DD."""
        c = get_committee(928)
        if c.start_date:
            self.assertNotIn("T", c.start_date)
            self.assertNotIn(" ", c.start_date)


if __name__ == "__main__":
    unittest.main()
