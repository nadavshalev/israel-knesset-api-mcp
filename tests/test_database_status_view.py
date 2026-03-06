"""Tests for views/database_status_view.py

Integration tests use the real PostgreSQL database.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from views.database_status_view import get_database_status


class TestDatabaseStatusStructure(unittest.TestCase):
    """Verify the status response has the expected structure."""

    @classmethod
    def setUpClass(cls):
        cls.status = get_database_status()

    def test_top_level_keys(self):
        self.assertIn("entity_counts", self.status)
        self.assertIn("tools", self.status)
        self.assertIn("last_sync", self.status)

    def test_entity_counts_is_dict(self):
        self.assertIsInstance(self.status["entity_counts"], dict)

    def test_tools_is_list(self):
        self.assertIsInstance(self.status["tools"], list)

    def test_has_expected_entities(self):
        """All search-tool entities should be present in counts."""
        expected = {
            "Knesset Members", "Committees", "Plenum Sessions",
            "Bills", "Plenum Votes",
        }
        self.assertEqual(set(self.status["entity_counts"].keys()), expected)

    def test_entity_counts_are_positive(self):
        for entity, count in self.status["entity_counts"].items():
            self.assertIsNotNone(count, f"{entity} count is None")
            self.assertGreater(count, 0, f"{entity} has 0 records")


class TestDatabaseStatusTools(unittest.TestCase):
    """Verify tool descriptions in the status response."""

    @classmethod
    def setUpClass(cls):
        cls.status = get_database_status()
        cls.tools = cls.status["tools"]

    def test_has_12_tools(self):
        """All @mcp_tool-decorated views: 5 search + 7 detail (5 entity + database_status + search_across)."""
        self.assertEqual(len(self.tools), 12)

    def test_tool_has_required_keys(self):
        for tool in self.tools:
            for key in ("name", "entity", "description", "type", "filters"):
                self.assertIn(key, tool, f"Tool missing key: {key}")

    def test_tool_types(self):
        types = {t["type"] for t in self.tools}
        self.assertEqual(types, {"search", "detail"})

    def test_search_tools_count(self):
        search = [t for t in self.tools if t["type"] == "search"]
        self.assertEqual(len(search), 5)

    def test_detail_tools_count(self):
        detail = [t for t in self.tools if t["type"] == "detail"]
        self.assertEqual(len(detail), 7)

    def test_tool_names(self):
        names = {t["name"] for t in self.tools}
        expected = {
            "search_members", "get_member",
            "search_committees", "get_committee",
            "search_plenums", "get_plenum",
            "search_bills", "get_bill",
            "search_votes", "get_vote",
            "get_database_status", "search_across",
        }
        self.assertEqual(names, expected)

    def test_filters_structure(self):
        """Each filter should have name, type, required."""
        for tool in self.tools:
            for f in tool["filters"]:
                self.assertIn("name", f)
                self.assertIn("type", f)
                self.assertIn("required", f)


class TestDatabaseStatusSync(unittest.TestCase):
    """Verify last_sync field."""

    def test_last_sync_is_string_or_none(self):
        status = get_database_status()
        sync = status["last_sync"]
        self.assertTrue(sync is None or isinstance(sync, str))

    def test_last_sync_has_value(self):
        """If data has been synced, last_sync should be a non-empty string."""
        status = get_database_status()
        # PostgreSQL database has been populated, so metadata should exist
        self.assertIsNotNone(status["last_sync"])
        self.assertGreater(len(status["last_sync"]), 0)


if __name__ == "__main__":
    unittest.main()
