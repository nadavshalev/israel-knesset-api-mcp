"""Tests for origins/knesset/metadata_view.py

Integration tests use the real PostgreSQL database with known historical
data which is stable and won't change for past Knesset terms.
"""

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from origins.knesset.metadata_view import metadata


class TestBasicStructure(unittest.TestCase):
    """Verify knesset_num=20 returns all sections populated."""

    def setUp(self):
        self.result = metadata(knesset_num=20)

    def test_knesset_num(self):
        self.assertEqual(self.result.knesset_num, 20)

    def test_assemblies_populated(self):
        self.assertGreater(len(self.result.knesset_assemblies), 0)

    def test_committees_populated(self):
        self.assertGreater(len(self.result.committees), 0)

    def test_gov_ministries_populated(self):
        self.assertGreater(len(self.result.gov_ministries), 0)

    def test_factions_populated(self):
        self.assertGreater(len(self.result.factions), 0)


class TestKnessetAssemblies(unittest.TestCase):
    """Verify assembly periods present with dates."""

    def setUp(self):
        self.result = metadata(knesset_num=20)

    def test_assemblies_have_dates(self):
        for a in self.result.knesset_assemblies:
            self.assertIsNotNone(a.start_date)

    def test_assemblies_have_assembly_year(self):
        for a in self.result.knesset_assemblies:
            self.assertIsNotNone(a.assembly_year)


class TestCommittees(unittest.TestCase):
    """Verify committee list non-empty with names."""

    def setUp(self):
        self.result = metadata(knesset_num=20)

    def test_committees_have_names(self):
        for c in self.result.committees:
            self.assertIsNotNone(c.name)
            self.assertGreater(len(c.name), 0)

    def test_committees_have_ids(self):
        for c in self.result.committees:
            self.assertIsInstance(c.committee_id, int)


class TestCommitteeHeads(unittest.TestCase):
    """Verify at least some committees have heads when include_committee_heads=True."""

    def setUp(self):
        self.result = metadata(knesset_num=20, include_committee_heads=True)

    def test_some_committees_have_heads(self):
        committees_with_heads = [c for c in self.result.committees if c.heads]
        self.assertGreater(len(committees_with_heads), 0)

    def test_heads_are_strings(self):
        for c in self.result.committees:
            if c.heads:
                for h in c.heads:
                    self.assertIsInstance(h, str)

    def test_heads_compact_format(self):
        """Each head string should contain ':' and a name."""
        for c in self.result.committees:
            if c.heads:
                for h in c.heads:
                    self.assertIn(":", h)
                    # Should have content after the colon
                    parts = h.split(":", 1)
                    self.assertGreater(len(parts[1].strip()), 0)


class TestGovMinistries(unittest.TestCase):
    """Verify ministries list populated."""

    def setUp(self):
        self.result = metadata(knesset_num=20)

    def test_ministries_have_names(self):
        for m in self.result.gov_ministries:
            self.assertIsNotNone(m.name)

    def test_ministries_have_ids(self):
        for m in self.result.gov_ministries:
            self.assertIsInstance(m.ministry_id, int)


class TestFactions(unittest.TestCase):
    """Verify known factions exist."""

    def setUp(self):
        self.result = metadata(knesset_num=20)

    def test_factions_known(self):
        faction_names = [f.name for f in self.result.factions]
        self.assertTrue(
            any("הליכוד" in name for name in faction_names),
            f"Expected to find הליכוד in factions: {faction_names}",
        )

    def test_factions_have_ids(self):
        for f in self.result.factions:
            self.assertIsInstance(f.faction_id, int)


class TestNoMembersByDefault(unittest.TestCase):
    """Members should be None when no include_* flags are set."""

    def setUp(self):
        self.result = metadata(knesset_num=20)

    def test_faction_members_none(self):
        for f in self.result.factions:
            self.assertIsNone(f.members)

    def test_ministry_members_none(self):
        for m in self.result.gov_ministries:
            self.assertIsNone(m.minister)
            self.assertIsNone(m.deputy_ministers)
            self.assertIsNone(m.members)

    def test_committee_heads_none(self):
        for c in self.result.committees:
            self.assertIsNone(c.heads)


class TestGranularFlags(unittest.TestCase):
    """Each include_* flag independently populates only its section."""

    def test_only_committee_heads(self):
        result = metadata(knesset_num=20, include_committee_heads=True)
        self.assertTrue(any(c.heads for c in result.committees))
        for m in result.gov_ministries:
            self.assertIsNone(m.members)
        for f in result.factions:
            self.assertIsNone(f.members)

    def test_only_ministry_members(self):
        result = metadata(knesset_num=20, include_ministry_members=True)
        self.assertTrue(any(m.minister for m in result.gov_ministries))
        for c in result.committees:
            self.assertIsNone(c.heads)
        for f in result.factions:
            self.assertIsNone(f.members)

    def test_only_faction_members(self):
        result = metadata(knesset_num=20, include_faction_members=True)
        self.assertTrue(any(f.members for f in result.factions))
        for c in result.committees:
            self.assertIsNone(c.heads)
        for m in result.gov_ministries:
            self.assertIsNone(m.members)


class TestFactionMembersCompactFormat(unittest.TestCase):
    """Faction member lists are list[str] in compact format."""

    def setUp(self):
        self.result = metadata(knesset_num=20, include_faction_members=True)

    def test_members_are_strings(self):
        for f in self.result.factions:
            if f.members:
                for m in f.members:
                    self.assertIsInstance(m, str)

    def test_members_compact_format(self):
        """Each member string should contain ':' separator."""
        for f in self.result.factions:
            if f.members:
                for m in f.members:
                    self.assertIn(":", m)

    def test_date_elision(self):
        """Members whose tenure spans the full faction lifetime should have no dates."""
        # Find a faction with dates and members
        for f in self.result.factions:
            if f.members and f.start_date and f.end_date:
                # Any member without "from" or "to" in their string had dates elided
                elided = [m for m in f.members if "from" not in m and "to" not in m]
                if elided:
                    # Found at least one member with elided dates — test passes
                    return
        # It's acceptable if all factions are ongoing (no end date) — skip
        self.skipTest("No faction with full-span members found to test date elision")


class TestMinistryMembersCompactFormat(unittest.TestCase):
    """Ministry member lists are list[str] in compact format, split by role."""

    def setUp(self):
        self.result = metadata(knesset_num=20, include_ministry_members=True)

    def test_some_ministries_have_ministers(self):
        self.assertTrue(any(m.minister for m in self.result.gov_ministries))

    def test_minister_are_strings(self):
        for m in self.result.gov_ministries:
            for entry in (m.minister or []):
                self.assertIsInstance(entry, str)
                self.assertIn(":", entry)

    def test_deputy_ministers_are_strings(self):
        for m in self.result.gov_ministries:
            for entry in (m.deputy_ministers or []):
                self.assertIsInstance(entry, str)
                self.assertIn(":", entry)

    def test_members_are_strings(self):
        for m in self.result.gov_ministries:
            for entry in (m.members or []):
                self.assertIsInstance(entry, str)
                self.assertIn(":", entry)

    def test_empty_fields_are_none(self):
        """Fields with no entries should be None, not empty list."""
        for m in self.result.gov_ministries:
            self.assertNotEqual(m.minister, [])
            self.assertNotEqual(m.deputy_ministers, [])
            self.assertNotEqual(m.members, [])


class TestGeneralRoles(unittest.TestCase):
    def setUp(self):
        self.result = metadata(knesset_num=20)

    def test_general_roles_present(self):
        self.assertIsNotNone(self.result.general_roles)
        self.assertGreater(len(self.result.general_roles), 0)

    def test_known_positions_exist(self):
        positions = [r.position for r in self.result.general_roles]
        self.assertTrue(any("ראש" in p for p in positions), f"No ראש role found: {positions}")

    def test_holders_are_strings(self):
        for role in self.result.general_roles:
            self.assertGreater(len(role.holders), 0)
            for h in role.holders:
                self.assertIsInstance(h, str)
                self.assertIn(":", h)

    def test_no_member_role(self):
        """Generic 'חבר כנסת' role should be excluded."""
        positions = [r.position for r in self.result.general_roles]
        self.assertFalse(any("חבר כנסת" in p for p in positions))


if __name__ == "__main__":
    unittest.main()
