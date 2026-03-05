"""Tests for views/search_across_view.py

Integration tests use the real data.sqlite database with known historical
data from older Knessets (19, 20) which are stable and won't change.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from views.search_across_view import search_across


class TestSearchAcrossStructure(unittest.TestCase):
    """Verify the response structure of search_across."""

    @classmethod
    def setUpClass(cls):
        cls.result = search_across("חינוך", top_n=3)

    def test_top_level_keys(self):
        self.assertIn("query", self.result)
        self.assertIn("results", self.result)

    def test_query_echoed(self):
        self.assertEqual(self.result["query"], "חינוך")

    def test_has_all_entity_types(self):
        expected = {"members", "bills", "committees", "votes", "plenums"}
        self.assertEqual(set(self.result["results"].keys()), expected)

    def test_entity_structure(self):
        for entity, data in self.result["results"].items():
            self.assertIn("count", data, f"{entity} missing count")
            self.assertIn("top", data, f"{entity} missing top")
            self.assertIsInstance(data["count"], int)
            self.assertIsInstance(data["top"], list)

    def test_top_n_respected(self):
        """Each entity should return at most top_n results."""
        for entity, data in self.result["results"].items():
            self.assertLessEqual(
                len(data["top"]), 3,
                f"{entity} returned more than top_n=3 results"
            )


class TestSearchAcrossEmptyQuery(unittest.TestCase):
    """Edge case: empty or whitespace query."""

    def test_empty_string(self):
        result = search_across("")
        self.assertEqual(result["results"], {})

    def test_whitespace_only(self):
        result = search_across("   ")
        self.assertEqual(result["results"], {})

    def test_none_query(self):
        result = search_across(None)
        self.assertEqual(result["results"], {})


class TestSearchAcrossMembers(unittest.TestCase):
    """Test member search within search_across."""

    def test_netanyahu_found(self):
        """Searching for נתניהו should find at least one member."""
        result = search_across("נתניהו", top_n=5)
        members = result["results"]["members"]
        self.assertGreater(members["count"], 0)
        self.assertGreater(len(members["top"]), 0)

        # Check the top result has an id and name
        top = members["top"][0]
        self.assertIn("id", top)
        self.assertIn("name", top)

    def test_member_name_contains_query(self):
        """All returned member names should contain the search term."""
        result = search_across("לפיד", top_n=5)
        members = result["results"]["members"]
        for m in members["top"]:
            self.assertIn("לפיד", m["name"])


class TestSearchAcrossBills(unittest.TestCase):
    """Test bill search within search_across."""

    def test_education_bills(self):
        """Searching for חינוך should find education-related bills."""
        result = search_across("חינוך", top_n=3)
        bills = result["results"]["bills"]
        self.assertGreater(bills["count"], 0)
        self.assertGreater(len(bills["top"]), 0)

    def test_bill_has_expected_fields(self):
        result = search_across("חינוך", top_n=1)
        bills = result["results"]["bills"]
        if bills["top"]:
            bill = bills["top"][0]
            self.assertIn("id", bill)
            self.assertIn("name", bill)


class TestSearchAcrossCommittees(unittest.TestCase):
    """Test committee search within search_across."""

    def test_education_committee(self):
        """Searching for חינוך should find the Education committee."""
        result = search_across("חינוך", top_n=10)
        committees = result["results"]["committees"]
        self.assertGreater(committees["count"], 0)
        names = [c["name"] for c in committees["top"]]
        has_education = any("חינוך" in n for n in names)
        self.assertTrue(has_education, f"Expected חינוך in committee names: {names}")


class TestSearchAcrossVotes(unittest.TestCase):
    """Test vote search within search_across."""

    def test_budget_votes(self):
        """Searching for תקציב should find budget-related votes."""
        result = search_across("תקציב", top_n=3)
        votes = result["results"]["votes"]
        self.assertGreater(votes["count"], 0)

    def test_vote_has_expected_fields(self):
        result = search_across("תקציב", top_n=1)
        votes = result["results"]["votes"]
        if votes["top"]:
            vote = votes["top"][0]
            self.assertIn("id", vote)
            self.assertIn("name", vote)


class TestSearchAcrossTopN(unittest.TestCase):
    """Test top_n parameter behavior."""

    def test_default_top_n(self):
        """Without top_n, should use config default (5)."""
        result = search_across("חוק")
        for entity, data in result["results"].items():
            self.assertLessEqual(len(data["top"]), 5)

    def test_top_n_1(self):
        """top_n=1 should return at most 1 result per entity."""
        result = search_across("חוק", top_n=1)
        for entity, data in result["results"].items():
            self.assertLessEqual(len(data["top"]), 1)

    def test_top_n_10(self):
        """top_n=10 returns up to 10 per entity."""
        result = search_across("חוק", top_n=10)
        for entity, data in result["results"].items():
            self.assertLessEqual(len(data["top"]), 10)


class TestSearchAcrossNoResults(unittest.TestCase):
    """Test queries that should return zero results."""

    def test_gibberish_query(self):
        """A nonsense query should return 0 counts."""
        result = search_across("xyzzy12345nonexistent")
        for entity, data in result["results"].items():
            self.assertEqual(data["count"], 0,
                             f"{entity} unexpectedly had results")
            self.assertEqual(len(data["top"]), 0)


if __name__ == "__main__":
    unittest.main()
