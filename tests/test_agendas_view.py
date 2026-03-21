"""Tests for the unified agendas tool.

Integration tests use the real PostgreSQL database with known historical data.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.agendas.agendas_view import agendas
from origins.agendas.agendas_models import AgendaResultPartial, AgendaResultFull, AgendasResults


class TestSearchMode(unittest.TestCase):
    """Basic agenda searches (partial results)."""

    def test_knesset_20_search(self):
        results = agendas(knesset_num=20, name_query="חינוך מיוחד")
        self.assertIsInstance(results, AgendasResults)
        self.assertGreater(len(results.items), 0)
        for a in results.items:
            self.assertIsInstance(a, AgendaResultPartial)
            self.assertEqual(a.knesset_num, 20)

    def test_no_full_detail_in_partial(self):
        """Partial results should be AgendaResultPartial, not AgendaResultFull."""
        results = agendas(knesset_num=20, name_query="חינוך מיוחד")
        for a in results.items:
            self.assertNotIsInstance(a, AgendaResultFull)

    def test_output_structure(self):
        results = agendas(knesset_num=20, name_query="חינוך מיוחד")
        self.assertGreater(len(results.items), 0)
        for a in results.items:
            self.assertIsNotNone(a.agenda_id)
            self.assertIsNotNone(a.name)
            self.assertIsNotNone(a.knesset_num)

    def test_name_no_match(self):
        results = agendas(name_query="xxxNOTEXISTxxx", knesset_num=20)
        self.assertEqual(len(results.items), 0)


class TestStatusFilter(unittest.TestCase):
    def test_status_filter(self):
        results = agendas(knesset_num=20, status="קביעת ועדה מטפלת")
        self.assertGreater(len(results.items), 0)


class TestTypeFilter(unittest.TestCase):
    def test_type_filter(self):
        results = agendas(knesset_num=20, type="בתקופת פגרה")
        self.assertGreater(len(results.items), 0)


class TestAgendaIdAutoFullDetails(unittest.TestCase):
    """agenda_id auto-enables full_details."""

    def test_single_agenda(self):
        results = agendas(agenda_id=570660)
        self.assertEqual(len(results.items), 1)
        a = results.items[0]
        self.assertEqual(a.agenda_id, 570660)
        self.assertEqual(a.knesset_num, 20)

    def test_nonexistent_agenda_returns_empty(self):
        results = agendas(agenda_id=999999999)
        self.assertEqual(len(results.items), 0)

    def test_stages_present(self):
        """Full detail should include session stages."""
        results = agendas(agenda_id=570660)
        a = results.items[0]
        # Agenda 570660 should have at least one session appearance
        if a.stages:
            for s in a.stages:
                has_session = s.plenum_session is not None or s.committee_session is not None
                self.assertTrue(has_session)


class TestLastUpdateDate(unittest.TestCase):
    def test_last_update_date_in_partial(self):
        results = agendas(knesset_num=20, name_query="חינוך מיוחד")
        self.assertGreater(len(results.items), 0)
        # At least some should have last_update_date
        has_date = any(a.last_update_date for a in results.items)
        self.assertTrue(has_date)


class TestDateFilter(unittest.TestCase):
    def test_session_date_filter(self):
        """Date range should find agendas discussed in sessions."""
        results = agendas(knesset_num=20, from_date="2015-11-01", to_date="2015-12-31", name_query="חינוך")
        self.assertGreater(len(results.items), 0)


if __name__ == "__main__":
    unittest.main()
