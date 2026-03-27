"""Unified secondary_laws tool — search and detail for secondary legislation.

Search mode returns summaries; ``full_details=True`` or ``secondary_law_id``
returns full detail including regulators, authorizing laws, bindings, and documents.
"""

import sys
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_readonly
from core.helpers import (
    simple_date, normalize_inputs, check_search_count, resolve_pagination,
    CountByConfig, build_count_by_query,
)
from core.models import CountItem
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import SessionDocument
from origins.laws.laws_models import LawResultPartial
from origins.secondary_laws.secondary_laws_models import (
    SecondaryLawResultPartial, SecondaryLawResultFull, SecondaryLawsResults,
    SecLawRegulator, SecLawBinding,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_partial(row) -> SecondaryLawResultPartial:
    return SecondaryLawResultPartial(
        secondary_law_id=row["id"],
        name=row.get("name"),
        knesset_num=row.get("knessetnum"),
        type=row.get("typedesc"),
        status=row.get("statusname"),
        is_current=bool(row["iscurrent"]) if row.get("iscurrent") is not None else None,
        publication_date=simple_date(row.get("publicationdate")),
        committee_name=row.get("committee_name"),
        major_authorizing_law_id=row.get("majorauthorizinglawid"),
        major_authorizing_law_name=row.get("major_authorizing_law_name"),
    )


def _build_sec_law_partial(row, prefix="r") -> SecondaryLawResultPartial:
    """Build a SecondaryLawResultPartial from a prefixed row."""
    return SecondaryLawResultPartial(
        secondary_law_id=row[f"{prefix}_id"],
        name=row.get(f"{prefix}_name"),
        knesset_num=row.get(f"{prefix}_knessetnum"),
        type=row.get(f"{prefix}_typedesc"),
        status=row.get(f"{prefix}_statusname"),
        is_current=bool(row[f"{prefix}_iscurrent"]) if row.get(f"{prefix}_iscurrent") is not None else None,
        publication_date=simple_date(row.get(f"{prefix}_publicationdate")),
    )


# ---------------------------------------------------------------------------
# Detail fetchers
# ---------------------------------------------------------------------------

def _fetch_regulators(cursor, law_id: int) -> list[SecLawRegulator] | None:
    cursor.execute(
        """SELECT RegulatorTypeDesc, RegulatorDesc
        FROM sec_law_regulator_raw
        WHERE SecondaryLawID = %s
        ORDER BY Id""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    return [
        SecLawRegulator(
            regulator_type=r["regulatortypedesc"],
            regulator_name=r["regulatordesc"],
        )
        for r in rows
    ]


def _fetch_authorizing_laws(cursor, law_id: int) -> list[LawResultPartial] | None:
    cursor.execute(
        """SELECT DISTINCT il.Id AS law_id, il.Name, il.KnessetNum,
                  il.IsBasicLaw, il.IsBudgetLaw, il.IsFavoriteLaw,
                  il.PublicationDate, il.LatestPublicationDate,
                  il.LawValidityDesc
        FROM sec_law_authorizing_law_raw sla
        JOIN israel_law_raw il ON sla.AuthorizingLawID = il.Id
        WHERE sla.SecondaryLawID = %s
        ORDER BY il.Id""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None

    from origins.laws.laws_view import _law_types
    return [
        LawResultPartial(
            law_id=r["law_id"],
            name=r["name"],
            knesset_num=r.get("knessetnum"),
            law_types=_law_types(r),
            publication_date=simple_date(r.get("publicationdate")),
            latest_publication_date=simple_date(r.get("latestpublicationdate")),
            law_validity=r.get("lawvaliditydesc"),
        )
        for r in rows
    ]


def _fetch_bindings(cursor, law_id: int) -> list[SecLawBinding] | None:
    """Fetch sec-to-sec bindings involving this law.

    A binding row has three FK columns: SecChildID, SecParentID, SecMainID.
    For each row, determine which role(s) the current law plays and present
    the "other" law(s) with their role.
    """
    cursor.execute(
        """SELECT b.SecChildID, b.SecParentID, b.SecMainID,
                  b.BindingTypeDesc, b.AmendmentTypeDesc,
                  b.IsTempLegislation, b.IsSecondaryAmendment,
                  b.CorrectionNumber, b.ParagraphNumber,
                  -- child info
                  sc.Id AS c_id, sc.Name AS c_name, sc.KnessetNum AS c_knessetnum,
                  sc.TypeDesc AS c_typedesc, sc.StatusName AS c_statusname,
                  sc.IsCurrent AS c_iscurrent, sc.PublicationDate AS c_publicationdate,
                  -- parent info
                  sp.Id AS p_id, sp.Name AS p_name, sp.KnessetNum AS p_knessetnum,
                  sp.TypeDesc AS p_typedesc, sp.StatusName AS p_statusname,
                  sp.IsCurrent AS p_iscurrent, sp.PublicationDate AS p_publicationdate,
                  -- main info
                  sm.Id AS m_id, sm.Name AS m_name, sm.KnessetNum AS m_knessetnum,
                  sm.TypeDesc AS m_typedesc, sm.StatusName AS m_statusname,
                  sm.IsCurrent AS m_iscurrent, sm.PublicationDate AS m_publicationdate
        FROM sec_to_sec_binding_raw b
        LEFT JOIN secondary_law_raw sc ON b.SecChildID = sc.Id
        LEFT JOIN secondary_law_raw sp ON b.SecParentID = sp.Id
        LEFT JOIN secondary_law_raw sm ON b.SecMainID = sm.Id
        WHERE b.SecChildID = %s OR b.SecParentID = %s OR b.SecMainID = %s""",
        [law_id, law_id, law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None

    bindings = []
    for r in rows:
        binding_meta = dict(
            binding_type=r["bindingtypedesc"],
            amendment_type=r["amendmenttypedesc"],
            is_temp_legislation=bool(r["istemplegislation"]) if r.get("istemplegislation") is not None else None,
            is_secondary_amendment=bool(r["issecondaryamendment"]) if r.get("issecondaryamendment") is not None else None,
            correction_number=str(r["correctionnumber"]) if r.get("correctionnumber") is not None else None,
            paragraph_number=r.get("paragraphnumber"),
        )

        # Map role → (fk_value, prefix)
        roles = [
            ("child", r.get("secchildid"), "c"),
            ("parent", r.get("secparentid"), "p"),
            ("main", r.get("secmainid"), "m"),
        ]

        # Find which role(s) the current law plays
        current_roles = [role for role, fk, _ in roles if fk == law_id]
        other_roles = [(role, prefix) for role, fk, prefix in roles if fk and fk != law_id and r.get(f"{prefix}_id")]

        for current_role in current_roles:
            for related_role, prefix in other_roles:
                related_law = _build_sec_law_partial(r, prefix)
                bindings.append(SecLawBinding(
                    related_law=related_law,
                    related_role=related_role,
                    current_role=current_role,
                    **binding_meta,
                ))

    return bindings or None


def _fetch_documents(cursor, law_id: int) -> list[SessionDocument] | None:
    cursor.execute(
        """SELECT GroupTypeDesc, ApplicationDesc, FilePath
        FROM document_secondary_law_raw
        WHERE SecondaryLawId = %s""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    return [
        SessionDocument(
            name=r["grouptypedesc"],
            type=r["applicationdesc"],
            path=r["filepath"],
        )
        for r in rows
    ]


def _fetch_full_detail(cursor, row, law_id: int):
    regulators = _fetch_regulators(cursor, law_id)
    authorizing_laws = _fetch_authorizing_laws(cursor, law_id)
    bindings = _fetch_bindings(cursor, law_id)
    documents = _fetch_documents(cursor, law_id)
    return regulators, authorizing_laws, bindings, documents


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_secondary_laws_search(*, query, knesset_num, date, date_to, top_n):
    conditions = []
    params = []

    if query:
        conditions.append("s.Name LIKE %s")
        params.append(f"%{query}%")

    if knesset_num is not None:
        conditions.append("s.KnessetNum = %s")
        params.append(knesset_num)

    if date:
        conditions.append("s.PublicationDate >= %s")
        params.append(date)

    if date_to:
        conditions.append("s.PublicationDate <= %s")
        params.append(date_to)

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"SELECT COUNT(*) FROM secondary_law_raw s WHERE {where}"
    search_sql = f"""
        SELECT s.Id AS id, s.Name AS name, s.KnessetNum AS knessetnum,
               s.TypeDesc AS typedesc, s.StatusName AS statusname,
               s.IsCurrent AS iscurrent, s.PublicationDate AS publicationdate,
               maj.AuthorizingLawID AS majorauthorizinglawid,
               c.Name AS committee_name,
               il.Name AS major_authorizing_law_name
        FROM secondary_law_raw s
        LEFT JOIN committee_raw c ON s.CommitteeID = c.Id
        LEFT JOIN LATERAL (
            SELECT MIN(AuthorizingLawID) AS AuthorizingLawID
            FROM sec_law_authorizing_law_raw
            WHERE SecondaryLawID = s.Id
        ) maj ON TRUE
        LEFT JOIN israel_law_raw il ON maj.AuthorizingLawID = il.Id
        WHERE {where}
        ORDER BY s.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "secondary_laws",
    "builder": _build_secondary_laws_search,
    "mapper": lambda row: SecondaryLawResultPartial(
        secondary_law_id=row["id"],
        name=row["name"],
        knesset_num=row.get("knessetnum"),
        type=row.get("typedesc"),
        status=row.get("statusname"),
        is_current=bool(row["iscurrent"]) if row.get("iscurrent") is not None else None,
        publication_date=simple_date(row.get("publicationdate")),
        committee_name=row.get("committee_name"),
        major_authorizing_law_id=row.get("majorauthorizinglawid"),
        major_authorizing_law_name=row.get("major_authorizing_law_name"),
    ),
})


# ---------------------------------------------------------------------------
# count_by configuration
# ---------------------------------------------------------------------------

_CB_BASE_FROM = "secondary_law_raw s"
_CB_BASE_JOINS = "LEFT JOIN committee_raw c ON s.CommitteeID = c.Id"

_COUNT_BY_OPTIONS: dict[str, CountByConfig] = {
    "type": CountByConfig(
        group_by="s.TypeDesc",
        id_select=None,
        value_select="s.TypeDesc",
        extra_where="s.TypeDesc IS NOT NULL AND s.TypeDesc != ''",
    ),
    "status": CountByConfig(
        group_by="s.StatusName",
        id_select=None,
        value_select="s.StatusName",
        extra_where="s.StatusName IS NOT NULL AND s.StatusName != ''",
    ),
    "classification": CountByConfig(
        group_by="s.ClassificationDesc",
        id_select=None,
        value_select="s.ClassificationDesc",
        extra_where="s.ClassificationDesc IS NOT NULL AND s.ClassificationDesc != ''",
    ),
    "knesset_num": CountByConfig(
        group_by="s.KnessetNum",
        id_select=None,
        value_select="s.KnessetNum::text",
        extra_where="s.KnessetNum IS NOT NULL",
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="secondary_laws",
    description=(
        "Search secondary legislation (חקיקת משנה — regulations, orders, rules) "
        "or get full detail for one secondary law. Returns summary info by default; "
        "set full_details=True for regulators, authorizing laws, bindings, and documents. "
        "Use full_details=True for complete detail."
    ),
    entity="Secondary Laws",
    count_sql="SELECT COUNT(*) FROM secondary_law_raw",
    most_recent_date_sql="SELECT MAX(PublicationDate) FROM secondary_law_raw",
    enum_sql={
        "type": "SELECT DISTINCT TypeDesc FROM secondary_law_raw WHERE TypeDesc IS NOT NULL AND TypeDesc != '' ORDER BY 1",
        "status": "SELECT DISTINCT StatusName FROM secondary_law_raw WHERE StatusName IS NOT NULL AND StatusName != '' ORDER BY 1",
        "classification": "SELECT DISTINCT ClassificationDesc FROM secondary_law_raw WHERE ClassificationDesc IS NOT NULL AND ClassificationDesc != '' ORDER BY 1",
    },
    is_list=True,
)
def secondary_laws(
    secondary_law_id: Annotated[int | None, Field(description="Filter by secondary law ID")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name_query: Annotated[str | None, Field(description="Secondary law name contains text")] = None,
    type: Annotated[str | None, Field(description="Filter by type (e.g. תקנות, צו)")] = None,
    status: Annotated[str | None, Field(description="Filter by status")] = None,
    classification: Annotated[str | None, Field(description="Filter by classification")] = None,
    is_current: Annotated[bool | None, Field(description="Filter by current version (true/false)")] = None,
    authorizing_law_id: Annotated[int | None, Field(description="Filter by authorizing primary law ID")] = None,
    from_date: Annotated[str | None, Field(description="Start of publication date range (YYYY-MM-DD)")] = None,
    to_date: Annotated[str | None, Field(description="End of publication date range (YYYY-MM-DD)")] = None,
    full_details: Annotated[bool, Field(description="Include regulators, authorizing laws, bindings, documents")] = False,
    top: Annotated[int | None, Field(description="Max results (default 50, max 200). Results are sorted newest-first (date DESC) or by count DESC for count_by — so top=N gives the N most recent or highest.")] = None,
    offset: Annotated[int | None, Field(description="Results to skip for pagination. To get the oldest/smallest N: use offset=total_count-N (total_count is in every response).")] = None,
    count_by: Annotated[Literal["all", "type", "status", "classification", "knesset_num"] | None, Field(description='Group and count results. "all" returns only total_count (no items). Other values group by field (sorted by count DESC).')] = None,
) -> SecondaryLawsResults:
    """Search for secondary legislation or get full detail for a single secondary law."""
    normalized = normalize_inputs(locals())
    secondary_law_id = normalized["secondary_law_id"]
    knesset_num = normalized["knesset_num"]
    name_query = normalized["name_query"]
    type_filter = normalized["type"]
    status_filter = normalized["status"]
    classification_filter = normalized["classification"]
    is_current_filter = normalized["is_current"]
    authorizing_law_id = normalized["authorizing_law_id"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])


    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if secondary_law_id is not None:
        conditions.append("s.Id = %s")
        params.append(secondary_law_id)

    if knesset_num is not None:
        conditions.append("s.KnessetNum = %s")
        params.append(knesset_num)

    if name_query:
        conditions.append("s.Name LIKE %s")
        params.append(f"%{name_query}%")

    if type_filter:
        conditions.append("s.TypeDesc LIKE %s")
        params.append(f"%{type_filter}%")

    if status_filter:
        conditions.append("s.StatusName LIKE %s")
        params.append(f"%{status_filter}%")

    if classification_filter:
        conditions.append("s.ClassificationDesc LIKE %s")
        params.append(f"%{classification_filter}%")

    if is_current_filter is not None:
        conditions.append("s.IsCurrent = %s")
        params.append(1 if is_current_filter else 0)

    if authorizing_law_id is not None:
        conditions.append(
            "EXISTS (SELECT 1 FROM sec_law_authorizing_law_raw sla "
            "WHERE sla.SecondaryLawID = s.Id AND sla.AuthorizingLawID = %s)"
        )
        params.append(authorizing_law_id)

    if from_date:
        conditions.append("s.PublicationDate >= %s")
        params.append(from_date)

    if to_date:
        conditions.append("s.PublicationDate <= %s")
        params.append(to_date)

    where = " AND ".join(conditions) if conditions else "1=1"
    count_sql = f"SELECT COUNT(*) FROM {_CB_BASE_FROM} {_CB_BASE_JOINS} WHERE {where}"

    count_by_val = normalized.get("count_by")
    if count_by_val:
        if count_by_val == "all":
            total_count = check_search_count(cursor, count_sql, params, paginated=True)
            conn.close()
            return SecondaryLawsResults(total_count=total_count, items=[], counts=[])
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
        return SecondaryLawsResults(total_count=total_count, items=[], counts=counts)

    total_count = check_search_count(cursor, count_sql, params, entity_name="secondary_laws", paginated=True)

    cursor.execute(
        f"""SELECT s.Id, s.Name, s.KnessetNum, s.TypeDesc, s.StatusName,
               s.IsCurrent, s.PublicationDate, s.CommitteeID,
               s.CompletionCauseDesc, s.PostponementReasonDesc,
               s.KnessetInvolvementDesc,
               s.PublicationSeriesDesc, s.MagazineNumber, s.PageNumber,
               s.CommitteeReceivedDate, s.CommitteeApprovalDate,
               s.ApprovalDateWithoutDiscussion,
               s.SecretaryReceivedDate, s.PlenumApprovalDate,
               s.IsAmmendingLawOriginal, s.ClassificationDesc,
               s.IsEmergency,
               c.Name AS committee_name,
               maj.AuthorizingLawID AS majorauthorizinglawid,
               il.Name AS major_authorizing_law_name
        FROM secondary_law_raw s
        LEFT JOIN committee_raw c ON s.CommitteeID = c.Id
        LEFT JOIN LATERAL (
            SELECT MIN(AuthorizingLawID) AS AuthorizingLawID
            FROM sec_law_authorizing_law_raw
            WHERE SecondaryLawID = s.Id
        ) maj ON TRUE
        LEFT JOIN israel_law_raw il ON maj.AuthorizingLawID = il.Id
        WHERE {where}
        ORDER BY s.PublicationDate DESC, s.Id DESC
        LIMIT %s OFFSET %s""",
        params + [top, offset],
    )
    rows = cursor.fetchall()

    if not full_details:
        results = [_build_partial(row) for row in rows]
    else:
        results = []
        for row in rows:
            lid = row["id"]
            regulators, authorizing_laws, bindings, documents = _fetch_full_detail(cursor, row, lid)
            results.append(SecondaryLawResultFull(
                secondary_law_id=lid,
                name=row.get("name"),
                knesset_num=row.get("knessetnum"),
                type=row.get("typedesc"),
                status=row.get("statusname"),
                is_current=bool(row["iscurrent"]) if row.get("iscurrent") is not None else None,
                publication_date=simple_date(row.get("publicationdate")),
                committee_name=row.get("committee_name"),
                major_authorizing_law_id=row.get("majorauthorizinglawid"),
                major_authorizing_law_name=row.get("major_authorizing_law_name"),
                classification=row.get("classificationdesc"),
                completion_cause=row.get("completioncausedesc"),
                postponement_reason=row.get("postponementreasondesc"),
                knesset_involvement=row.get("knessetinvolvementdesc"),
                publication_series=row.get("publicationseriesdesc"),
                magazine_number=row.get("magazinenumber"),
                page_number=row.get("pagenumber"),
                committee_received_date=simple_date(row.get("committeereceiveddate")),
                committee_approval_date=simple_date(row.get("committeeapprovaldate")),
                approval_without_discussion_date=simple_date(row.get("approvaldatewithoutdiscussion")),
                secretary_received_date=simple_date(row.get("secretaryreceiveddate")),
                plenum_approval_date=simple_date(row.get("plenumapprovaldate")),
                is_amending_law_original=bool(row["isammendinglaworiginal"]) if row.get("isammendinglaworiginal") is not None else None,
                is_emergency=bool(row["isemergency"]) if row.get("isemergency") is not None else None,
                committee_id=row.get("committeeid"),
                regulators=regulators,
                authorizing_laws=authorizing_laws,
                bindings=bindings,
                documents=documents,
            ))

    conn.close()
    return SecondaryLawsResults(total_count=total_count, items=results)


secondary_laws.OUTPUT_MODEL = SecondaryLawsResults
