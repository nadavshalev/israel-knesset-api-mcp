"""Unified agendas tool — search and detail via ``full_details`` flag.

Covers KNS_Agenda data (parliamentary motions for the agenda).
Search mode returns summaries; ``full_details=True`` or ``agenda_id``
returns full detail including documents, committee names, minister info,
and session stages.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from typing import Annotated, Literal
from pydantic import Field

from core.db import connect_readonly
from core.helpers import (
    simple_date, normalize_inputs, check_search_count, resolve_pagination,
    CountByConfig, build_count_by_query, fuzzy_condition, fuzzy_params,
)
from core.models import CountItem
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import (
    SessionDocument, build_session_date_exists, fetch_item_stages,
)
from origins.agendas.agendas_models import AgendaResultPartial, AgendaResultFull, AgendasResults

# ItemTypeID for agendas in session_item tables
_AGENDA_TYPE_IDS = [4]


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_agendas_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity agenda search."""
    conditions = []
    params = []

    if query:
        conditions.append(fuzzy_condition("a.Name"))
        params.extend(fuzzy_params(query))

    if knesset_num is not None:
        conditions.append("a.KnessetNum = %s")
        params.append(knesset_num)

    date_sql, date_params = build_session_date_exists(
        "a", "a.Id", _AGENDA_TYPE_IDS, date, date_to,
    )
    if date_sql:
        conditions.append(date_sql)
        params.extend(date_params)

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(*) FROM agenda_raw a
        WHERE {where}
    """
    search_sql = f"""
        SELECT a.Id AS id, a.Name AS name, a.KnessetNum AS knesset_num,
               a.SubTypeDesc AS type,
               st."Desc" AS status
        FROM agenda_raw a
        LEFT JOIN status_raw st ON a.StatusID = st.Id
        WHERE {where}
        ORDER BY a.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "agendas",
    "builder": _build_agendas_search,
    "mapper": lambda row: AgendaResultPartial(
        agenda_id=row["id"],
        name=row["name"],
        knesset_num=row["knesset_num"],
        type=row["type"],
        status=row["status"],
    ),
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _person_name_with_party(cursor, person_id, knesset_num):
    """Fetch a person's full name with party for a given Knesset term."""
    if not person_id:
        return None
    cursor.execute(
        """
        SELECT p.FirstName || ' ' || p.LastName AS full_name,
               ptp.FactionName
        FROM person_raw p
        LEFT JOIN LATERAL (
            SELECT ptp2.FactionName
            FROM person_to_position_raw ptp2
            WHERE ptp2.PersonID = p.PersonID
              AND ptp2.KnessetNum = %s
              AND ptp2.FactionName IS NOT NULL
              AND ptp2.FactionName != ''
            ORDER BY ptp2.IsCurrent DESC, ptp2.PersonToPositionID DESC
            LIMIT 1
        ) ptp ON true
        WHERE p.PersonID = %s
        """,
        (knesset_num, person_id),
    )
    row = cursor.fetchone()
    if not row:
        return None
    name = row["full_name"]
    if row["factionname"]:
        name = f"{name} ({row['factionname']})"
    return name


def _batch_person_names(cursor, person_ids, knesset_nums_by_id):
    """Batch-fetch person names with party for multiple person IDs."""
    if not person_ids:
        return {}
    unique_ids = list(set(person_ids))
    placeholders = ",".join(["%s"] * len(unique_ids))
    cursor.execute(
        f"""
        SELECT p.PersonID,
               p.FirstName || ' ' || p.LastName AS full_name
        FROM person_raw p
        WHERE p.PersonID IN ({placeholders})
        """,
        unique_ids,
    )
    names = {}
    for row in cursor.fetchall():
        pid = row["personid"]
        kn = knesset_nums_by_id.get(pid)
        name = row["full_name"]
        # Try to get party
        if kn:
            cursor.execute(
                """
                SELECT FactionName
                FROM person_to_position_raw
                WHERE PersonID = %s AND KnessetNum = %s
                  AND FactionName IS NOT NULL AND FactionName != ''
                ORDER BY IsCurrent DESC, PersonToPositionID DESC
                LIMIT 1
                """,
                (pid, kn),
            )
            faction_row = cursor.fetchone()
            if faction_row and faction_row["factionname"]:
                name = f"{name} ({faction_row['factionname']})"
        names[pid] = name
    return names


# ---------------------------------------------------------------------------
# count_by configuration
# ---------------------------------------------------------------------------

_CB_BASE_FROM = "agenda_raw a"
_CB_BASE_JOINS = "LEFT JOIN status_raw st ON a.StatusID = st.Id"

_COUNT_BY_OPTIONS: dict[str, CountByConfig] = {
    "initiator": CountByConfig(
        group_by="a.InitiatorPersonID, p.FirstName, p.LastName",
        id_select="a.InitiatorPersonID",
        value_select="p.FirstName || ' ' || p.LastName",
        extra_joins="JOIN person_raw p ON a.InitiatorPersonID = p.PersonID",
        extra_where="a.InitiatorPersonID IS NOT NULL",
    ),
    "status": CountByConfig(
        group_by='st."Desc"',
        id_select=None,
        value_select='st."Desc"',
        extra_where='st."Desc" IS NOT NULL',
    ),
    "type": CountByConfig(
        group_by="a.SubTypeDesc",
        id_select=None,
        value_select="a.SubTypeDesc",
        extra_where="a.SubTypeDesc IS NOT NULL",
    ),
    "knesset_num": CountByConfig(
        group_by="a.KnessetNum",
        id_select=None,
        value_select="a.KnessetNum::text",
        extra_where="a.KnessetNum IS NOT NULL",
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="agendas",
    description=(
        "Search for Knesset agendas (motions for the agenda / הצעות לסדר היום). "
        "Returns summary info by default; set full_details=True for documents, "
        "committee details, minister info, and session stages."
    ),
    entity="Agendas",
    count_sql="SELECT COUNT(*) FROM agenda_raw",
    most_recent_date_sql="SELECT MAX(LastUpdatedDate) FROM agenda_raw",
    enum_sql={
        "status": 'SELECT DISTINCT s."Desc" FROM agenda_raw a JOIN status_raw s ON a.StatusID = s.Id WHERE s."Desc" IS NOT NULL ORDER BY s."Desc"',
        "type": "SELECT DISTINCT SubTypeDesc FROM agenda_raw WHERE SubTypeDesc IS NOT NULL ORDER BY SubTypeDesc",
    },
    is_list=True,
)
def agendas(
    agenda_id: Annotated[int | None, Field(description="Filter by agenda ID")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name_query: Annotated[str | None, Field(description="Agenda name contains text")] = None,
    status: Annotated[str | None, Field(description="Agenda status")] = None,
    type: Annotated[str | None, Field(description="Agenda sub-type")] = None,
    initiator_id: Annotated[int | None, Field(description="Filter by initiator person ID")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD) — filters by session date (plenum or committee)")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD) — use with from_date")] = None,
    full_details: Annotated[bool, Field(description="Include documents, committee details, minister info, stages. Adds significant data per result — use conservatively. Preferred pattern: search first (full_details=False), then re-call with agenda_id for only the specific items you need detail on.")] = False,
    top: Annotated[int | None, Field(description="Max results (default 50, max 200). Results are sorted newest-first (date DESC) or by count DESC for count_by — so top=N gives the N most recent or highest.")] = None,
    offset: Annotated[int | None, Field(description="Results to skip for pagination. To get the oldest/smallest N: use offset=total_count-N (total_count is in every response).")] = None,
    count_by: Annotated[Literal["all", "initiator", "status", "type", "knesset_num"] | None, Field(description='Group and count results. "all" returns only total_count (no items). Other values group by field (sorted by count DESC).')] = None,
) -> AgendasResults:
    """Search for agendas or get full detail for a single agenda.

    Filters (all ANDed):
      - knesset_num: agenda's Knesset number
      - name_query: agenda name contains text
      - status: status description contains text
      - type: sub-type description contains text
      - initiator_id: initiator person ID
      - from_date / to_date: discussed in a session (plenum or committee) in date range
    """
    normalized = normalize_inputs(locals())
    agenda_id = normalized["agenda_id"]
    knesset_num = normalized["knesset_num"]
    name_query = normalized["name_query"]
    status = normalized["status"]
    type_ = normalized["type"]
    initiator_id = normalized["initiator_id"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if agenda_id is not None:
        conditions.append("a.Id = %s")
        params.append(agenda_id)

    if knesset_num is not None:
        conditions.append("a.KnessetNum = %s")
        params.append(knesset_num)

    if name_query:
        conditions.append(fuzzy_condition("a.Name"))
        params.extend(fuzzy_params(name_query))

    if type_:
        conditions.append("a.SubTypeDesc LIKE %s")
        params.append(f"%{type_}%")

    if status:
        conditions.append('st."Desc" LIKE %s')
        params.append(f"%{status}%")

    if initiator_id is not None:
        conditions.append("a.InitiatorPersonID = %s")
        params.append(initiator_id)

    # Session date filters (plenum + committee)
    date_sql, date_params = build_session_date_exists(
        "a", "a.Id", _AGENDA_TYPE_IDS, from_date, to_date,
    )
    if date_sql:
        conditions.append(date_sql)
        params.extend(date_params)

    where = " AND ".join(conditions) if conditions else "1=1"
    count_sql = f"SELECT COUNT(*) FROM {_CB_BASE_FROM} {_CB_BASE_JOINS} WHERE {where}"

    count_by_val = normalized.get("count_by")
    if count_by_val:
        if count_by_val == "all":
            total_count = check_search_count(cursor, count_sql, params, paginated=True)
            conn.close()
            return AgendasResults(total_count=total_count, items=[], counts=[])
        config = _COUNT_BY_OPTIONS.get(count_by_val)
        if config is None:
            raise ValueError(f"count_by must be one of: {', '.join(_COUNT_BY_OPTIONS)}")
        groups_count_sql, group_sql = build_count_by_query(
            base_from=_CB_BASE_FROM, base_joins=_CB_BASE_JOINS, where=where, config=config,
        )
        total_count = check_search_count(cursor, groups_count_sql, params, paginated=True)
        cursor.execute(group_sql, params + [top, offset])
        counts = [CountItem(id=row.get("id"), value=row.get("value"), count=row["count"])
                  for row in cursor.fetchall()]
        conn.close()
        return AgendasResults(total_count=total_count, items=[], counts=counts)

    total_count = check_search_count(cursor, count_sql, params, entity_name="agendas", paginated=True)

    cursor.execute(
        f"""SELECT a.Id, a.Name, a.KnessetNum, a.ClassificationDesc,
               a.SubTypeDesc, st."Desc" AS StatusDesc,
               a.InitiatorPersonID,
               a.LeadingAgendaID, a.GovRecommendationDesc,
               a.PostopenmentReasonDesc, a.PresidentDecisionDate,
               a.CommitteeID, a.RecommendCommitteeID, a.MinisterPersonID,
               a.LastUpdatedDate
        FROM agenda_raw a
        LEFT JOIN status_raw st ON a.StatusID = st.Id
        WHERE {where}
        ORDER BY a.Id DESC
        LIMIT %s OFFSET %s""",
        params + [top, offset],
    )
    rows = cursor.fetchall()

    if not full_details:
        # Batch fetch initiator names
        person_ids = [row["initiatorpersonid"] for row in rows if row["initiatorpersonid"]]
        kn_by_pid = {}
        for row in rows:
            if row["initiatorpersonid"]:
                kn_by_pid[row["initiatorpersonid"]] = row["knessetnum"]
        names = _batch_person_names(cursor, person_ids, kn_by_pid)

        results = []
        for row in rows:
            pid = row["initiatorpersonid"]
            results.append(AgendaResultPartial(
                agenda_id=row["id"],
                name=row["name"],
                knesset_num=row["knessetnum"],
                classification=row["classificationdesc"],
                type=row["subtypedesc"],
                status=row["statusdesc"],
                initiator_name=names.get(pid) if pid else None,
                last_update_date=simple_date(row["lastupdateddate"]) or None,
            ))
    else:
        results = []
        for row in rows:
            kn = row["knessetnum"]
            # Initiator name
            initiator_name = _person_name_with_party(cursor, row["initiatorpersonid"], kn)

            # Session stages
            stages = fetch_item_stages(cursor, row["id"], _AGENDA_TYPE_IDS)

            # Leading agenda name
            leading_name = None
            if row["leadingagendaid"]:
                cursor.execute("SELECT Name FROM agenda_raw WHERE Id = %s", (row["leadingagendaid"],))
                la_row = cursor.fetchone()
                if la_row:
                    leading_name = la_row["name"]

            # Committee names
            committee_name = None
            if row["committeeid"]:
                cursor.execute("SELECT Name FROM committee_raw WHERE Id = %s", (row["committeeid"],))
                c_row = cursor.fetchone()
                if c_row:
                    committee_name = c_row["name"]

            recommend_committee_name = None
            if row["recommendcommitteeid"]:
                cursor.execute("SELECT Name FROM committee_raw WHERE Id = %s", (row["recommendcommitteeid"],))
                rc_row = cursor.fetchone()
                if rc_row:
                    recommend_committee_name = rc_row["name"]

            # Minister name
            minister_name = _person_name_with_party(cursor, row["ministerpersonid"], kn)

            # Documents
            cursor.execute(
                """
                SELECT GroupTypeDesc, ApplicationDesc, FilePath
                FROM document_agenda_raw
                WHERE AgendaID = %s
                ORDER BY Id ASC
                """,
                (row["id"],),
            )
            documents = [
                SessionDocument(
                    name=dr["grouptypedesc"],
                    type=dr["applicationdesc"],
                    path=dr["filepath"],
                )
                for dr in cursor.fetchall()
            ]

            results.append(AgendaResultFull(
                agenda_id=row["id"],
                name=row["name"],
                knesset_num=kn,
                classification=row["classificationdesc"],
                type=row["subtypedesc"],
                status=row["statusdesc"],
                initiator_name=initiator_name,
                last_update_date=simple_date(row["lastupdateddate"]) or None,
                stages=stages,
                leading_agenda_id=row["leadingagendaid"],
                leading_agenda_name=leading_name,
                gov_recommendation=row["govrecommendationdesc"],
                postponement_reason=row["postopenmentreasondesc"],
                president_decision_date=simple_date(row["presidentdecisiondate"]) or None,
                committee_id=row["committeeid"],
                committee_name=committee_name,
                recommend_committee_id=row["recommendcommitteeid"],
                recommend_committee_name=recommend_committee_name,
                minister_name=minister_name,
                documents=documents or None,
            ))

    conn.close()
    return AgendasResults(total_count=total_count, items=results)


agendas.OUTPUT_MODEL = AgendasResults
