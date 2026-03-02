"""Tests for views/votes_view.py (list view)

Integration tests use the real data.sqlite database with known historical
data.  Vote data from older Knessets (16-20) is stable via CSV-origin rows.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from views.votes_view import (
    _simple_date,
    _simple_time,
    search_votes,
)


# ===================================================================
# Unit tests for helper functions
# ===================================================================


class TestSimpleDate(unittest.TestCase):
    def test_datetime_with_time(self):
        self.assertEqual(_simple_date("2015-03-31T18:33:00"), "2015-03-31")

    def test_datetime_with_tz(self):
        self.assertEqual(_simple_date("2021-07-13T03:40:21+03:00"), "2021-07-13")

    def test_date_only(self):
        self.assertEqual(_simple_date("2015-03-31"), "2015-03-31")

    def test_empty(self):
        self.assertEqual(_simple_date(""), "")

    def test_none(self):
        self.assertEqual(_simple_date(None), "")


class TestSimpleTime(unittest.TestCase):
    def test_datetime(self):
        self.assertEqual(_simple_time("2015-03-31T18:33:00"), "18:33")

    def test_datetime_with_tz(self):
        self.assertEqual(_simple_time("2021-07-13T03:40:21+03:00"), "03:40")

    def test_no_t(self):
        self.assertEqual(_simple_time("2015-03-31"), "")

    def test_empty(self):
        self.assertEqual(_simple_time(""), "")

    def test_none(self):
        self.assertEqual(_simple_time(None), "")


# ===================================================================
# Integration tests — use real data.sqlite
# ===================================================================


class TestKnessetFilter(unittest.TestCase):
    """Filter votes by Knesset number."""

    def test_knesset_20_vote_count(self):
        """Knesset 20 had 7690 votes."""
        results = search_votes(knesset_num=20)
        self.assertEqual(len(results), 7690)

    def test_knesset_19_vote_count(self):
        """Knesset 19 had 2630 votes."""
        results = search_votes(knesset_num=19)
        self.assertEqual(len(results), 2630)

    def test_all_knesset_20_have_knesset_num(self):
        """All results for knesset=20 should have knesset_num=20."""
        results = search_votes(knesset_num=20, date="2015-03-31")
        for v in results:
            self.assertEqual(v["knesset_num"], 20)


class TestDateFilters(unittest.TestCase):
    """Filter by date and date range."""

    def test_exact_date(self):
        """Knesset 20 opening day: 2015-03-31."""
        results = search_votes(date="2015-03-31", knesset_num=20)
        self.assertEqual(len(results), 2)
        for v in results:
            self.assertEqual(v["date"], "2015-03-31")

    def test_from_date(self):
        """Votes from 2015-03-31 onward in Knesset 20."""
        results = search_votes(from_date="2015-03-31", knesset_num=20)
        self.assertGreater(len(results), 0)
        for v in results:
            self.assertGreaterEqual(v["date"], "2015-03-31")

    def test_date_range(self):
        """Votes in first week of Knesset 20."""
        results = search_votes(
            from_date="2015-03-31", to_date="2015-04-07", knesset_num=20
        )
        self.assertGreater(len(results), 0)
        for v in results:
            self.assertGreaterEqual(v["date"], "2015-03-31")
            self.assertLessEqual(v["date"], "2015-04-07")


class TestNameFilter(unittest.TestCase):
    """Filter by vote title or subject."""

    def test_title_search(self):
        results = search_votes(name="בחירת יושב-ראש", knesset_num=20)
        self.assertGreater(len(results), 0)
        for v in results:
            title_or_subject = (v["title"] or "") + (v["subject"] or "")
            self.assertIn("יושב-ראש", title_or_subject)

    def test_subject_search(self):
        """Search by VoteSubject (e.g. 'הסתייגות')."""
        results = search_votes(name="הסתייגות", knesset_num=20)
        self.assertGreater(len(results), 0)


class TestAcceptedFilter(unittest.TestCase):
    """Filter by acceptance status."""

    def test_accepted_only(self):
        results = search_votes(knesset_num=20, date="2015-03-31", accepted=True)
        for v in results:
            self.assertTrue(v["is_accepted"])

    def test_rejected_only(self):
        """Find rejected votes in a range with known rejections."""
        results = search_votes(knesset_num=20, accepted=False)
        self.assertGreater(len(results), 0)
        for v in results:
            self.assertFalse(v["is_accepted"])


class TestSortOrder(unittest.TestCase):
    """Results should be sorted by (date, time, vote_id)."""

    def test_sorted_by_date_time(self):
        results = search_votes(knesset_num=20, date="2015-03-31")
        for i in range(1, len(results)):
            prev = results[i - 1]
            curr = results[i]
            prev_key = (prev["date"], prev["time"], prev["vote_id"])
            curr_key = (curr["date"], curr["time"], curr["vote_id"])
            self.assertLessEqual(prev_key, curr_key)


class TestOutputStructure(unittest.TestCase):
    """Verify list output has expected keys and no detail keys."""

    def test_output_keys(self):
        results = search_votes(knesset_num=20, date="2015-03-31")
        self.assertGreater(len(results), 0)
        expected_keys = {
            "vote_id", "bill_id", "knesset_num", "session_id", "title",
            "subject", "date", "time", "is_accepted", "total_for",
            "total_against", "total_abstain", "for_option",
            "against_option", "vote_method",
        }
        for v in results:
            self.assertTrue(expected_keys.issubset(v.keys()),
                            f"Missing keys: {expected_keys - v.keys()}")

    def test_no_members_in_list(self):
        """List view should NOT include members."""
        results = search_votes(knesset_num=20, date="2015-03-31")
        for v in results:
            self.assertNotIn("members", v)

    def test_no_related_votes_in_list(self):
        """List view should NOT include related_votes."""
        results = search_votes(knesset_num=20, date="2015-03-31")
        for v in results:
            self.assertNotIn("related_votes", v)


class TestBillIdField(unittest.TestCase):
    """Verify bill_id field in vote output."""

    def test_bill_id_present_in_output(self):
        """Vote output should always include 'bill_id' key."""
        results = search_votes(knesset_num=20, date="2015-03-31")
        self.assertGreater(len(results), 0)
        self.assertIn("bill_id", results[0])

    def test_bill_vote_has_bill_id(self):
        """Vote 26916 links to bill 565913 (Basic Law: Nation State)."""
        results = search_votes(knesset_num=20, name="חוק-יסוד: ישראל")
        bill_ids = {v["bill_id"] for v in results if v["bill_id"]}
        self.assertIn(565913, bill_ids)


class TestBillIdFilter(unittest.TestCase):
    """Filter votes by bill_id."""

    def test_filter_by_bill_id(self):
        """Bill 565913 (Nation State) had 202 votes."""
        results = search_votes(bill_id=565913)
        self.assertEqual(len(results), 202)
        for v in results:
            self.assertEqual(v["bill_id"], 565913)

    def test_filter_by_bill_id_with_accepted(self):
        """Combine bill_id with accepted filter."""
        all_votes = search_votes(bill_id=565913)
        accepted = search_votes(bill_id=565913, accepted=True)
        rejected = search_votes(bill_id=565913, accepted=False)
        self.assertEqual(len(accepted) + len(rejected), len(all_votes))
        for v in accepted:
            self.assertTrue(v["is_accepted"])
        for v in rejected:
            self.assertFalse(v["is_accepted"])

    def test_filter_by_nonexistent_bill_id(self):
        """Bill that doesn't exist should return empty."""
        results = search_votes(bill_id=999999999)
        self.assertEqual(len(results), 0)

    def test_filter_by_bill_id_and_knesset(self):
        """Bill 565913 is from Knesset 20."""
        results = search_votes(bill_id=565913, knesset_num=20)
        self.assertEqual(len(results), 202)
        results = search_votes(bill_id=565913, knesset_num=19)
        self.assertEqual(len(results), 0)


class TestCSVTotals(unittest.TestCase):
    """CSV-origin votes should always have totals."""

    def test_first_knesset_20_vote_totals(self):
        results = search_votes(knesset_num=20, date="2015-03-31")
        # Vote 21824 should be in results
        v = [r for r in results if r["vote_id"] == 21824]
        self.assertEqual(len(v), 1)
        self.assertEqual(v[0]["total_for"], 107)
        self.assertEqual(v[0]["total_against"], 0)
        self.assertEqual(v[0]["total_abstain"], 0)
        self.assertTrue(v[0]["is_accepted"])


class TestNullIsAccepted(unittest.TestCase):
    """OData-origin votes have NULL IsAccepted and NULL totals in the raw
    table.  The view must infer is_accepted from computed per-MK totals.

    Uses Knesset 25 date 2022-12-19 which has 26 OData-origin votes
    (2 accepted, 24 rejected) with no stored IsAccepted/TotalFor.
    """

    def test_odata_votes_have_inferred_is_accepted(self):
        """is_accepted must never be None when per-MK results exist."""
        results = search_votes(knesset_num=25, date="2022-12-19")
        self.assertGreater(len(results), 0)
        for v in results:
            self.assertIsNotNone(
                v["is_accepted"],
                f"vote {v['vote_id']} has is_accepted=None",
            )

    def test_odata_votes_have_computed_totals(self):
        """Totals must be populated from per-MK results for OData votes."""
        results = search_votes(knesset_num=25, date="2022-12-19")
        self.assertGreater(len(results), 0)
        for v in results:
            self.assertIsNotNone(
                v["total_for"],
                f"vote {v['vote_id']} has total_for=None",
            )
            self.assertIsNotNone(
                v["total_against"],
                f"vote {v['vote_id']} has total_against=None",
            )

    def test_accepted_filter_includes_odata_votes(self):
        """accepted=True must find OData-origin accepted votes."""
        results = search_votes(knesset_num=25, date="2022-12-19", accepted=True)
        self.assertGreater(len(results), 0, "No accepted votes found")
        for v in results:
            self.assertTrue(v["is_accepted"])

    def test_rejected_filter_includes_odata_votes(self):
        """accepted=False must find OData-origin rejected votes."""
        results = search_votes(knesset_num=25, date="2022-12-19", accepted=False)
        self.assertGreater(len(results), 0, "No rejected votes found")
        for v in results:
            self.assertFalse(v["is_accepted"])

    def test_accepted_plus_rejected_equals_total(self):
        """Accepted + rejected must equal total for the same date."""
        all_votes = search_votes(knesset_num=25, date="2022-12-19")
        accepted = search_votes(knesset_num=25, date="2022-12-19", accepted=True)
        rejected = search_votes(knesset_num=25, date="2022-12-19", accepted=False)
        self.assertEqual(
            len(accepted) + len(rejected), len(all_votes),
            f"accepted({len(accepted)}) + rejected({len(rejected)}) != total({len(all_votes)})",
        )

    def test_specific_odata_accepted_vote(self):
        """Vote 37683 (Basic Law amendment, Knesset 25) passed 61-51."""
        results = search_votes(knesset_num=25, date="2022-12-13", accepted=True)
        ids = {v["vote_id"] for v in results}
        self.assertIn(37683, ids)
        v = [r for r in results if r["vote_id"] == 37683][0]
        self.assertEqual(v["total_for"], 61)
        self.assertEqual(v["total_against"], 51)
        self.assertTrue(v["is_accepted"])

    def test_specific_odata_rejected_vote(self):
        """Vote 37692 (Knesset Law amendment) rejected 51-62."""
        results = search_votes(knesset_num=25, date="2022-12-19", accepted=False)
        ids = {v["vote_id"] for v in results}
        self.assertIn(37692, ids)
        v = [r for r in results if r["vote_id"] == 37692][0]
        self.assertEqual(v["total_for"], 51)
        self.assertEqual(v["total_against"], 62)
        self.assertFalse(v["is_accepted"])


if __name__ == "__main__":
    unittest.main()
