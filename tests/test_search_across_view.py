"""Tests for origins/search/search_across_view.py

Integration tests use the real PostgreSQL database with known historical
data from older Knessets (19, 20) which are stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.search.search_across_view import search_across


class TestSearchAcrossStructure(unittest.TestCase):
    """Verify the response structure of search_across."""

    @classmethod
    def setUpClass(cls):
        cls.result = search_across(query="חינוך", top_n=3)

    def test_top_level_keys(self):
        self.assertTrue(hasattr(self.result, "query"))
        self.assertTrue(hasattr(self.result, "results"))

    def test_query_echoed(self):
        self.assertEqual(self.result.query, "חינוך")

    def test_has_all_entity_types(self):
        expected = {"members", "bills", "committees", "votes", "plenums"}
        self.assertEqual(set(self.result.results.keys()), expected)

    def test_entity_structure(self):
        for entity, data in self.result.results.items():
            self.assertIsInstance(data.count, int)
            self.assertIsInstance(data.top, list)

    def test_top_n_respected(self):
        """Each entity should return at most top_n results."""
        for entity, data in self.result.results.items():
            self.assertLessEqual(
                len(data.top), 3,
                f"{entity} returned more than top_n=3 results"
            )


class TestSearchAcrossEmptyQuery(unittest.TestCase):
    """Edge case: empty or whitespace query with no other filters."""

    def test_empty_string(self):
        result = search_across(query="")
        self.assertEqual(result.results, {})

    def test_whitespace_only(self):
        result = search_across(query="   ")
        self.assertEqual(result.results, {})

    def test_none_query(self):
        result = search_across(query=None)
        self.assertEqual(result.results, {})

    def test_no_filters_at_all(self):
        """No query, no knesset_num, no date -> empty results."""
        result = search_across()
        self.assertEqual(result.results, {})


class TestSearchAcrossMembers(unittest.TestCase):
    """Test member search within search_across."""

    def test_netanyahu_found(self):
        """Searching for נתניהו should find at least one member."""
        result = search_across(query="נתניהו", top_n=5)
        members = result.results["members"]
        self.assertGreater(members.count, 0)
        self.assertGreater(len(members.top), 0)

        # Check the top result has an id and name
        top = members.top[0]
        self.assertIn("id", top)
        self.assertIn("name", top)

    def test_member_name_contains_query(self):
        """All returned member names should contain the search term."""
        result = search_across(query="לפיד", top_n=5)
        members = result.results["members"]
        for m in members.top:
            self.assertIn("לפיד", m["name"])


class TestSearchAcrossBills(unittest.TestCase):
    """Test bill search within search_across."""

    def test_education_bills(self):
        """Searching for חינוך should find education-related bills."""
        result = search_across(query="חינוך", top_n=3)
        bills = result.results["bills"]
        self.assertGreater(bills.count, 0)
        self.assertGreater(len(bills.top), 0)

    def test_bill_has_expected_fields(self):
        result = search_across(query="חינוך", top_n=1)
        bills = result.results["bills"]
        if bills.top:
            bill = bills.top[0]
            self.assertIn("id", bill)
            self.assertIn("name", bill)


class TestSearchAcrossCommittees(unittest.TestCase):
    """Test committee search within search_across."""

    def test_education_committee(self):
        """Searching for חינוך should find the Education committee."""
        result = search_across(query="חינוך", top_n=10)
        committees = result.results["committees"]
        self.assertGreater(committees.count, 0)
        names = [c["name"] for c in committees.top]
        has_education = any("חינוך" in n for n in names)
        self.assertTrue(has_education, f"Expected חינוך in committee names: {names}")


class TestSearchAcrossVotes(unittest.TestCase):
    """Test vote search within search_across."""

    def test_budget_votes(self):
        """Searching for תקציב should find budget-related votes."""
        result = search_across(query="תקציב", top_n=3)
        votes = result.results["votes"]
        self.assertGreater(votes.count, 0)

    def test_vote_has_expected_fields(self):
        result = search_across(query="תקציב", top_n=1)
        votes = result.results["votes"]
        if votes.top:
            vote = votes.top[0]
            self.assertIn("id", vote)
            self.assertIn("name", vote)


class TestSearchAcrossTopN(unittest.TestCase):
    """Test top_n parameter behavior."""

    def test_default_top_n(self):
        """Without top_n, should use config default (5)."""
        result = search_across(query="חוק")
        for entity, data in result.results.items():
            self.assertLessEqual(len(data.top), 5)

    def test_top_n_1(self):
        """top_n=1 should return at most 1 result per entity."""
        result = search_across(query="חוק", top_n=1)
        for entity, data in result.results.items():
            self.assertLessEqual(len(data.top), 1)

    def test_top_n_10(self):
        """top_n=10 returns up to 10 per entity."""
        result = search_across(query="חוק", top_n=10)
        for entity, data in result.results.items():
            self.assertLessEqual(len(data.top), 10)


class TestSearchAcrossNoResults(unittest.TestCase):
    """Test queries that should return zero results."""

    def test_gibberish_query(self):
        """A nonsense query should return 0 counts."""
        result = search_across(query="xyzzy12345nonexistent")
        for entity, data in result.results.items():
            self.assertEqual(data.count, 0,
                             f"{entity} unexpectedly had results")
            self.assertEqual(len(data.top), 0)


# ---------------------------------------------------------------------------
# New filter tests
# ---------------------------------------------------------------------------

class TestSearchAcrossKnessetNumFilter(unittest.TestCase):
    """Test knesset_num filter."""

    def test_knesset_num_only(self):
        """knesset_num alone (no query) should return results."""
        result = search_across(knesset_num=20, top_n=3)
        self.assertIn("bills", result.results)
        bills = result.results["bills"]
        self.assertGreater(bills.count, 0,
                           "Expected bills from Knesset 20")

    def test_knesset_num_echoed(self):
        result = search_across(knesset_num=20, top_n=1)
        self.assertEqual(result.knesset_num, 20)

    def test_knesset_num_with_query(self):
        """Combining query and knesset_num narrows results."""
        broad = search_across(query="חינוך", top_n=1)
        narrow = search_across(query="חינוך", knesset_num=20, top_n=1)
        # The narrow count should be <= broad count for bills
        self.assertLessEqual(
            narrow.results["bills"].count,
            broad.results["bills"].count,
            "knesset_num filter should narrow bill results"
        )

    def test_nonexistent_knesset_num(self):
        """knesset_num=999 should return 0 results."""
        result = search_across(knesset_num=999, top_n=1)
        for entity, data in result.results.items():
            self.assertEqual(data.count, 0,
                             f"{entity} unexpectedly had results for knesset 999")


class TestSearchAcrossDateFilter(unittest.TestCase):
    """Test date and date_to filters."""

    def test_date_only(self):
        """date alone (no query) should return results from that date."""
        result = search_across(date="2015-01-01", top_n=3)
        # Should get results for at least some entities
        has_any = any(d.count > 0 for d in result.results.values())
        self.assertTrue(has_any, "Expected at least one entity with results for date 2015-01-01")

    def test_date_echoed(self):
        result = search_across(date="2015-01-01", top_n=1)
        self.assertEqual(result.date, "2015-01-01")

    def test_date_range(self):
        """date + date_to should filter by range."""
        result = search_across(date="2015-01-01", date_to="2015-12-31", top_n=3)
        self.assertEqual(result.date_to, "2015-12-31")
        has_any = any(d.count > 0 for d in result.results.values())
        self.assertTrue(has_any, "Expected results in 2015 date range")

    def test_date_range_narrows(self):
        """A narrow date range should have fewer results than a broad one."""
        broad = search_across(query="חוק", date="2015-01-01", date_to="2015-12-31", top_n=1)
        narrow = search_across(query="חוק", date="2015-06-01", date_to="2015-06-30", top_n=1)
        # Check bills since they have date filtering
        self.assertLessEqual(
            narrow.results["bills"].count,
            broad.results["bills"].count,
            "Narrow date range should have <= results than broad range for bills"
        )

    def test_all_filters_combined(self):
        """query + knesset_num + date range should all work together."""
        result = search_across(
            query="חוק", knesset_num=20,
            date="2015-01-01", date_to="2015-12-31",
            top_n=3,
        )
        self.assertEqual(result.query, "חוק")
        self.assertEqual(result.knesset_num, 20)
        self.assertEqual(result.date, "2015-01-01")
        self.assertEqual(result.date_to, "2015-12-31")
        # Should have results structure
        self.assertIn("bills", result.results)

    def test_date_filters_members(self):
        """Date range should return only members with active positions, not all."""
        result = search_across(date="2016-01-01", date_to="2016-06-30", top_n=1)
        members = result.results["members"]
        # Knesset 20 had ~120 MKs, not 1000+
        self.assertGreater(members.count, 0)
        self.assertLess(members.count, 300,
                        "Date filter should limit members to those active in the period")

    def test_date_filters_committees(self):
        """Date range should return only committees with sessions, not all."""
        result = search_across(date="2016-01-01", date_to="2016-06-30", top_n=1)
        committees = result.results["committees"]
        # Should have some committees with sessions, but far fewer than all ~2900
        self.assertGreater(committees.count, 0)
        self.assertLess(committees.count, 200,
                        "Date filter should limit committees to those with sessions")

    def test_date_filters_bills(self):
        """Date range should return bills that appeared in plenum sessions in that period."""
        result = search_across(date="2016-01-01", date_to="2016-06-30", top_n=1)
        bills = result.results["bills"]
        # Should have a reasonable number of bills with plenum appearances
        self.assertGreater(bills.count, 0)


class TestSearchAcrossFilterEchoFields(unittest.TestCase):
    """Verify that filter parameters are echoed back in the response."""

    def test_query_only_echo(self):
        result = search_across(query="חוק", top_n=1)
        self.assertEqual(result.query, "חוק")
        self.assertIsNone(result.knesset_num)
        self.assertIsNone(result.date)
        self.assertIsNone(result.date_to)

    def test_all_filters_echo(self):
        result = search_across(
            query="חוק", knesset_num=20,
            date="2015-01-01", date_to="2015-12-31",
            top_n=1,
        )
        self.assertEqual(result.query, "חוק")
        self.assertEqual(result.knesset_num, 20)
        self.assertEqual(result.date, "2015-01-01")
        self.assertEqual(result.date_to, "2015-12-31")


if __name__ == "__main__":
    unittest.main()
