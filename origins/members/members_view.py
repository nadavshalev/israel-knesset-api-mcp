"""Unified members tool — search and detail via ``full_details`` flag.

Replaces the old ``search_members`` + ``get_member`` pair with a single
``members`` tool.

Search mode returns summaries (name, gender, knesset_num, factions,
role_types).  ``full_details=True`` or ``member_id`` returns full detail
including government roles, committee memberships, and parliamentary roles.
When ``member_id`` is given without ``knesset_num``, all Knesset terms are
returned.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from typing import Annotated
from pydantic import Field

from core.db import connect_readonly
from core.helpers import simple_date, format_person_name, normalize_inputs, check_search_count, resolve_pagination
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from origins.members.members_models import (
    MemberResultPartial, MemberResultFull, MembersResults,
    GovernmentRole, CommitteeRole, ParliamentaryRole, MemberRoles,
)


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_members_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity member search."""
    conditions = []
    params = []

    if query:
        conditions.append("(p.FirstName LIKE %s OR p.LastName LIKE %s)")
        params.extend([f"%{query}%", f"%{query}%"])

    if knesset_num is not None:
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw ptp
            WHERE ptp.PersonID = p.PersonID AND ptp.KnessetNum = %s
        )""")
        params.append(knesset_num)

    if date and date_to:
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw ptp
            WHERE ptp.PersonID = p.PersonID
            AND ptp.StartDate <= %s
            AND (ptp.FinishDate IS NULL OR ptp.FinishDate = '' OR ptp.FinishDate >= %s)
        )""")
        params.extend([date_to + "T99", date])
    elif date:
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw ptp
            WHERE ptp.PersonID = p.PersonID
            AND ptp.StartDate <= %s
            AND (ptp.FinishDate IS NULL OR ptp.FinishDate = '' OR ptp.FinishDate >= %s)
        )""")
        params.extend([date + "T99", date])

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(DISTINCT p.PersonID) FROM person_raw p
        WHERE {where}
    """
    search_sql = f"""
        SELECT DISTINCT p.PersonID AS id,
               p.FirstName || ' ' || p.LastName AS name,
               p.LastName, p.FirstName,
               COALESCE(
                   (SELECT MAX(ptp2.KnessetNum) FROM person_to_position_raw ptp2
                    WHERE ptp2.PersonID = p.PersonID),
                   0
               ) AS knesset_num
        FROM person_raw p
        WHERE {where}
        ORDER BY p.LastName, p.FirstName
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "members",
    "builder": _build_members_search,
    "mapper": lambda row: MemberResultPartial(
        member_id=row["id"],
        name=row["name"],
        knesset_num=row["knesset_num"] or 0,
    ),
})


# ---------------------------------------------------------------------------
# WHERE clause builder (shared between count and fetch)
# ---------------------------------------------------------------------------

def _build_members_where(*, knesset_num=None, first_name=None, last_name=None,
                          role=None, role_ids=None, party=None, member_id=None):
    """Build shared WHERE clause and params for member queries."""
    conditions = ["1=1"]
    params = []

    if knesset_num is not None:
        conditions.append("ptp.KnessetNum = %s")
        params.append(knesset_num)

    if member_id is not None:
        conditions.append("p.PersonID = %s")
        params.append(member_id)

    if first_name:
        conditions.append("p.FirstName LIKE %s")
        params.append(f"%{first_name}%")

    if last_name:
        conditions.append("p.LastName LIKE %s")
        params.append(f"%{last_name}%")

    if role:
        conditions.append("""(
            pos.Description LIKE %s OR
            ptp.DutyDesc LIKE %s OR
            ptp.GovMinistryName LIKE %s OR
            ptp.CommitteeName LIKE %s
        )""")
        params.extend([f"%{role}%"] * 4)

    if role_ids:
        placeholders = ", ".join(["%s"] * len(role_ids))
        conditions.append(f"ptp.PositionID IN ({placeholders})")
        params.extend(role_ids)

    if party:
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw f
            WHERE f.PersonID = p.PersonID
              AND f.KnessetNum = ptp.KnessetNum
              AND f.FactionName LIKE %s
        )""")
        params.append(f"%{party}%")

    return " AND ".join(conditions), params


# ---------------------------------------------------------------------------
# Bulk summary fetch
# ---------------------------------------------------------------------------

def _fetch_members_bulk(cursor, *, knesset_num=None, first_name=None,
                        last_name=None, role=None, role_ids=None,
                        party=None, member_id=None, top=50, offset=0):
    """Fetch matching (PersonID, KnessetNum) pairs via SQL GROUP BY.

    Returns a list of summary dicts sorted by (knesset_num DESC, member_id),
    paginated via LIMIT/OFFSET.
    """
    where, params = _build_members_where(
        knesset_num=knesset_num, first_name=first_name, last_name=last_name,
        role=role, role_ids=role_ids, party=party, member_id=member_id,
    )
    sql = f"""
    SELECT p.PersonID, p.FirstName, p.LastName, p.GenderDesc,
           ptp.KnessetNum,
           array_agg(DISTINCT ptp.FactionName)
               FILTER (WHERE ptp.FactionName IS NOT NULL AND ptp.FactionName != '') AS factions,
           array_agg(DISTINCT pos.Description)
               FILTER (WHERE pos.Description IS NOT NULL AND pos.Description != '') AS role_types
    FROM person_raw p
    JOIN person_to_position_raw ptp ON p.PersonID = ptp.PersonID
    LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
    WHERE {where}
    GROUP BY p.PersonID, p.FirstName, p.LastName, p.GenderDesc, ptp.KnessetNum
    ORDER BY ptp.KnessetNum DESC, p.PersonID
    LIMIT %s OFFSET %s
    """
    cursor.execute(sql, params + [top, offset])
    rows = cursor.fetchall()

    result = []
    for row in rows:
        pid = row["personid"]
        kns = row["knessetnum"]
        if not pid or not isinstance(kns, int):
            continue
        result.append({
            "member_id": pid,
            "name": format_person_name(row["firstname"], row["lastname"]),
            "gender": row["genderdesc"],
            "knesset_num": kns,
            "faction": row["factions"] or [],
            "role_types": row["role_types"] or [],
        })

    return result


# ---------------------------------------------------------------------------
# Full detail helpers
# ---------------------------------------------------------------------------

def _row_category(row) -> str:
    """Classify a person_to_position_raw row by which fields are populated."""
    def _get(key):
        lower = key.lower()
        for k in row:
            if k.lower() == lower:
                return row[k]
        return None

    if _get("FactionName"):
        return "faction"
    if _get("GovMinistryName"):
        return "government"
    if _get("CommitteeID"):
        return "committee"
    return "parliamentary"


def _is_transition_gov(knesset_num: int, gov_num: int) -> bool:
    if not gov_num or gov_num == 0 or knesset_num < 19:
        return False
    primary_gov_mapping = {
        25: 37, 24: 36, 23: 35, 22: 34, 21: 34, 20: 34, 19: 33,
    }
    return gov_num < primary_gov_mapping.get(knesset_num, 0)


def _fetch_member_roles(cursor, person_id, knesset_num) -> MemberRoles | None:
    """Fetch full roles for one member in one Knesset term."""
    cursor.execute(
        """
        SELECT ptp.*, pos.Description AS OfficialPositionTitle
        FROM person_to_position_raw ptp
        LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
        WHERE ptp.PersonID = %s AND ptp.KnessetNum = %s
        ORDER BY ptp.StartDate ASC
        """,
        (person_id, knesset_num),
    )
    role_rows = cursor.fetchall()
    if not role_rows:
        return None

    gov_roles: list[GovernmentRole] = []
    cmt_roles: list[CommitteeRole] = []
    parl_roles: list[ParliamentaryRole] = []

    for row in role_rows:
        cat = _row_category(row)
        display_title = row["dutydesc"] or row["officialpositiontitle"] or ""

        if cat == "faction":
            pass  # factions already captured in summary
        elif cat == "government":
            gov_roles.append(GovernmentRole(
                title=display_title or None,
                ministry=row["govministryname"] or None,
                is_transition=_is_transition_gov(knesset_num, row["governmentnum"]),
                start=simple_date(row["startdate"]) or None,
                end=simple_date(row["finishdate"]) or None,
            ))
        elif cat == "committee":
            cmt_roles.append(CommitteeRole(
                id=row["committeeid"],
                name=row["committeename"] or None,
                role=row["officialpositiontitle"] or None,
                start=simple_date(row["startdate"]) or None,
                end=simple_date(row["finishdate"]) or None,
            ))
        else:
            parl_roles.append(ParliamentaryRole(
                name=display_title or None,
                role=row["officialpositiontitle"] or None,
                start=simple_date(row["startdate"]) or None,
                end=simple_date(row["finishdate"]) or None,
            ))

    return MemberRoles(
        government=gov_roles,
        committees=cmt_roles,
        parliamentary=parl_roles,
    )


# ---------------------------------------------------------------------------
# Role type resolver
# ---------------------------------------------------------------------------

def _resolve_role_ids(cursor, role_type: str) -> list:
    cursor.execute(
        "SELECT Id FROM position_raw WHERE Description LIKE %s",
        (f"%{role_type}%",),
    )
    return [row["id"] for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="members",
    description=(
        "Search for Knesset members or get full detail for a single member. "
        "Returns summary info by default (name, gender, factions, role types). "
        "Set full_details=True or provide member_id for full detail including "
        "government roles, committee memberships, and parliamentary roles. "
        "When member_id is given without knesset_num, all terms are returned."
    ),
    entity="Knesset Members",
    count_sql="SELECT COUNT(DISTINCT PersonID) FROM person_to_position_raw",
    most_recent_date_sql="SELECT MAX(StartDate) FROM person_to_position_raw",
    enum_sql={
        "role_type": "SELECT DISTINCT Description FROM position_raw ORDER BY Description",
    },
    is_list=True,
)
def members(
    member_id: Annotated[int | None, Field(description="Get a specific member by ID (auto-enables full_details; returns all terms unless knesset_num is set)")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    first_name: Annotated[str | None, Field(description="First name contains text")] = None,
    last_name: Annotated[str | None, Field(description="Last name contains text")] = None,
    role: Annotated[str | None, Field(description="Free text search across roles, ministries, and committees")] = None,
    role_type: Annotated[str | None, Field(description="Position category")] = None,
    party: Annotated[str | None, Field(description="Party/faction name contains text")] = None,
    full_details: Annotated[bool, Field(description="Include government roles, committee memberships, parliamentary roles (auto-True when member_id is set)")] = False,
    top: Annotated[int | None, Field(description="Max results to return (default 50, max 200)")] = None,
    offset: Annotated[int | None, Field(description="Number of results to skip for pagination")] = None,
) -> MembersResults:
    """Search for Knesset members or get full detail for a single member.

    Filters (all ANDed): knesset_num, first_name, last_name, role
    (free text across roles/ministries/committees), role_type (position
    category), party (party/faction name), member_id.

    Returns a ``MembersResults`` with ``items`` sorted by
    (knesset_num DESC, member_id).  Each item contains general info
    and a ``role_types`` list.  Set ``full_details=True`` or provide
    ``member_id`` for full roles detail.
    """
    normalized = normalize_inputs(locals())
    member_id = normalized["member_id"]
    knesset_num = normalized["knesset_num"]
    first_name = normalized["first_name"]
    last_name = normalized["last_name"]
    role = normalized["role"]
    role_type = normalized["role_type"]
    party = normalized["party"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    if member_id is not None:
        full_details = True

    conn = connect_readonly()
    cursor = conn.cursor()

    # Resolve role_type -> position IDs
    role_ids = None
    if role_type:
        role_ids = _resolve_role_ids(cursor, role_type)
        if not role_ids:
            conn.close()
            return MembersResults(total_count=0, items=[])

    # Count (skip when fetching by member_id)
    if member_id is None:
        where, count_params = _build_members_where(
            knesset_num=knesset_num, first_name=first_name, last_name=last_name,
            role=role, role_ids=role_ids, party=party, member_id=member_id,
        )
        total_count = check_search_count(
            cursor,
            f"""SELECT COUNT(*) FROM (
                SELECT DISTINCT p.PersonID, ptp.KnessetNum
                FROM person_raw p
                JOIN person_to_position_raw ptp ON p.PersonID = ptp.PersonID
                LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
                WHERE {where}
            ) _cnt""",
            count_params,
            entity_name="members",
            paginated=True,
        )
    else:
        total_count = None

    raw = _fetch_members_bulk(
        cursor,
        knesset_num=knesset_num,
        first_name=first_name,
        last_name=last_name,
        role=role,
        role_ids=role_ids,
        party=party,
        member_id=member_id,
        top=top,
        offset=offset,
    )

    items = []
    for m in raw:
        if full_details:
            roles = _fetch_member_roles(cursor, m["member_id"], m["knesset_num"])
            items.append(MemberResultFull(
                member_id=m["member_id"],
                name=m["name"],
                gender=m["gender"] or None,
                knesset_num=m["knesset_num"],
                faction=m["faction"],
                role_types=m["role_types"],
                roles=roles,
            ))
        else:
            items.append(MemberResultPartial(
                member_id=m["member_id"],
                name=m["name"],
                gender=m["gender"] or None,
                knesset_num=m["knesset_num"],
                faction=m["faction"],
                role_types=m["role_types"],
            ))

    conn.close()
    if total_count is None:
        total_count = len(items)
    return MembersResults(total_count=total_count, items=items)


members.OUTPUT_MODEL = MembersResults
