"""Unified laws tool — search and detail via ``full_details`` flag.

Search mode returns summaries; ``full_details=True`` or ``law_id``
returns full detail including classifications, ministries, bindings,
corrections, documents, and connected bills.
"""

import sys
from pathlib import Path
from typing import Annotated

from pydantic import Field

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_readonly
from core.helpers import simple_date, normalize_inputs, check_search_count, resolve_pagination
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import SessionDocument
from origins.bills.bills_models import BillResultPartial
from origins.laws.laws_models import (
    LawResultPartial, LawResultFull, LawsResults,
    ReplacedLaw, LawAmendment, LawCorrection, LawChange,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LAW_TYPE_FLAGS = [
    ("isbasiclaw", "חוק יסוד"),
    ("isbudgetlaw", "חוק תקציב"),
    ("isfavoritelaw", "חוק מועדף"),
]


def _law_types(row) -> list[str] | None:
    types = [label for col, label in _LAW_TYPE_FLAGS if row.get(col)]
    return types or None


def _build_partial(row) -> LawResultPartial:
    return LawResultPartial(
        law_id=row["id"],
        name=row["name"],
        knesset_num=row.get("knessetnum"),
        law_types=_law_types(row),
        publication_date=simple_date(row.get("publicationdate")),
        latest_publication_date=simple_date(row.get("latestpublicationdate")),
        law_validity=row.get("lawvaliditydesc"),
    )


def _build_law_partial_from_row(row) -> LawResultPartial:
    """Build a LawResultPartial from a israel_law_raw row (prefixed columns)."""
    return LawResultPartial(
        law_id=row["rl_id"],
        name=row.get("rl_name"),
        knesset_num=row.get("rl_knessetnum"),
        law_types=_law_types({
            "isbasiclaw": row.get("rl_isbasiclaw"),
            "isbudgetlaw": row.get("rl_isbudgetlaw"),
            "isfavoritelaw": row.get("rl_isfavoritelaw"),
        }),
        publication_date=simple_date(row.get("rl_publicationdate")),
        latest_publication_date=simple_date(row.get("rl_latestpublicationdate")),
        law_validity=row.get("rl_lawvaliditydesc"),
    )


def _build_bill_partial_from_row(row, prefix="b") -> BillResultPartial | None:
    """Build a BillResultPartial from a bill_raw row (prefixed columns)."""
    bill_id = row.get(f"{prefix}_id")
    if not bill_id:
        return None
    return BillResultPartial(
        bill_id=bill_id,
        name=row.get(f"{prefix}_name"),
        knesset_num=row.get(f"{prefix}_knessetnum"),
        type=row.get(f"{prefix}_subtypedesc"),
        status=row.get(f"{prefix}_statusdesc"),
        publication_date=simple_date(row.get(f"{prefix}_publicationdate")),
    )


# ---------------------------------------------------------------------------
# Detail fetchers
# ---------------------------------------------------------------------------

def _fetch_classifications(cursor, law_id: int) -> list[str] | None:
    cursor.execute(
        """SELECT ClassificiationDesc FROM israel_law_classification_raw
        WHERE IsraelLawID = %s AND ClassificiationDesc IS NOT NULL
        ORDER BY ClassificiationID""",
        [law_id],
    )
    result = [r["classificiationdesc"] for r in cursor.fetchall()]
    return result or None


def _fetch_ministries(cursor, law_id: int) -> list[str] | None:
    cursor.execute(
        """SELECT gm.Name FROM israel_law_ministry_raw ilm
        JOIN gov_ministry_raw gm ON ilm.GovMinistryID = gm.Id
        WHERE ilm.IsraelLawID = %s AND gm.Name IS NOT NULL
        ORDER BY gm.Name""",
        [law_id],
    )
    result = [r["name"] for r in cursor.fetchall()]
    return result or None


def _fetch_alternative_names(cursor, law_id: int, current_name: str | None) -> list[str] | None:
    cursor.execute(
        """SELECT Name FROM israel_law_name_raw
        WHERE IsraelLawID = %s AND Name IS NOT NULL AND Name != %s
        ORDER BY Id""",
        [law_id, current_name or ""],
    )
    result = [r["name"] for r in cursor.fetchall()]
    return result or None


def _fetch_replaced_laws(cursor, law_id: int) -> list[ReplacedLaw] | None:
    cursor.execute(
        """SELECT
            rl.Id AS rl_id, rl.Name AS rl_name, rl.KnessetNum AS rl_knessetnum,
            rl.IsBasicLaw AS rl_isbasiclaw, rl.IsBudgetLaw AS rl_isbudgetlaw,
            rl.IsFavoriteLaw AS rl_isfavoritelaw,
            rl.PublicationDate AS rl_publicationdate,
            rl.LatestPublicationDate AS rl_latestpublicationdate,
            rl.LawValidityDesc AS rl_lawvaliditydesc,
            b.Id AS b_id, b.Name AS b_name, b.KnessetNum AS b_knessetnum,
            b.SubTypeDesc AS b_subtypedesc,
            st."Desc" AS b_statusdesc,
            b.PublicationDate AS b_publicationdate
        FROM israel_law_binding_raw ilb
        LEFT JOIN israel_law_raw rl ON ilb.IsraelLawReplacedID = rl.Id
        LEFT JOIN bill_raw b ON ilb.LawID = b.Id
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE ilb.IsraelLawID = %s""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    result = []
    for r in rows:
        replaced_law = _build_law_partial_from_row(r) if r.get("rl_id") else None
        bill = _build_bill_partial_from_row(r) if r.get("b_id") else None
        result.append(ReplacedLaw(replaced_law=replaced_law, bill=bill))
    return result or None


def _fetch_bill_partial(cursor, bill_id: int) -> BillResultPartial | None:
    cursor.execute(
        """SELECT b.Id AS b_id, b.Name AS b_name, b.KnessetNum AS b_knessetnum,
                  b.SubTypeDesc AS b_subtypedesc, st."Desc" AS b_statusdesc,
                  b.PublicationDate AS b_publicationdate
        FROM bill_raw b
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE b.Id = %s""",
        [bill_id],
    )
    r = cursor.fetchone()
    return _build_bill_partial_from_row(r) if r else None


def _fetch_changes_and_original(cursor, law_id: int):
    """Fetch all changes (amendments + corrections) grouped by bill, and resolve original_bill.

    Returns (changes, original_bill).
    """
    # Fetch all binding rows
    cursor.execute(
        """SELECT lb.LawID, lb.ParentLawID, lb.BindingTypeDesc,
                  lb.AmendmentTypeDesc, lb.PageNumber,
                  lb.ParagraphNumber, lb.CorrectionNumber,
                  lb.LastUpdatedDate,
                  b.Id AS b_id, b.Name AS b_name, b.KnessetNum AS b_knessetnum,
                  b.SubTypeDesc AS b_subtypedesc,
                  st."Desc" AS b_statusdesc,
                  b.PublicationDate AS b_publicationdate
        FROM law_binding_raw lb
        LEFT JOIN bill_raw b ON lb.LawID = b.Id
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE lb.IsraelLawID = %s""",
        [law_id],
    )
    binding_rows = cursor.fetchall()

    # Fetch all correction rows
    cursor.execute(
        """SELECT lc.CorrectionTypeDesc, lc.CorrectionStatusDesc,
                  lc.PublicationDate, lc.MagazineNumber, lc.PageNumber,
                  lc.IsKnessetInvolvement, lc.BillID,
                  b.Id AS b_id, b.Name AS b_name, b.KnessetNum AS b_knessetnum,
                  b.SubTypeDesc AS b_subtypedesc,
                  st."Desc" AS b_statusdesc,
                  b.PublicationDate AS b_publicationdate
        FROM israel_law_law_corrections_raw illc
        JOIN law_corrections_raw lc ON illc.LawCorrectionID = lc.Id
        LEFT JOIN bill_raw b ON lc.BillID = b.Id
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE illc.IsraelLawID = %s
        ORDER BY lc.PublicationDate DESC""",
        [law_id],
    )
    correction_rows = cursor.fetchall()

    if not binding_rows and not correction_rows:
        return None, None

    # Group amendments by bill_id
    amendments_by_bill: dict[int, tuple] = {}
    for r in binding_rows:
        bill_id = r.get("lawid")
        if bill_id is None:
            continue
        if bill_id not in amendments_by_bill:
            amendments_by_bill[bill_id] = (_build_bill_partial_from_row(r), [])
        amendments_by_bill[bill_id][1].append(LawAmendment(
            binding_type=r["bindingtypedesc"],
            amendment_type=r["amendmenttypedesc"],
            date=simple_date(r.get("lastupdateddate")),
            page_number=r["pagenumber"],
            paragraph_number=str(r["paragraphnumber"]) if r.get("paragraphnumber") is not None else None,
            amendment_number=str(r["correctionnumber"]) if r.get("correctionnumber") is not None else None,
        ))

    # Group corrections by bill_id
    corrections_by_bill: dict[int, tuple] = {}
    for r in correction_rows:
        bill_id = r.get("billid")
        if bill_id is None:
            continue
        if bill_id not in corrections_by_bill:
            corrections_by_bill[bill_id] = (_build_bill_partial_from_row(r), [])
        corrections_by_bill[bill_id][1].append(LawCorrection(
            correction_type=r["correctiontypedesc"],
            status=r["correctionstatusdesc"],
            publication_date=simple_date(r.get("publicationdate")),
            magazine_number=r.get("magazinenumber"),
            page_number=r.get("pagenumber"),
            is_knesset_involvement=bool(r["isknessetinvolvement"]) if r.get("isknessetinvolvement") is not None else None,
        ))

    # Merge by bill_id, preserving insertion order
    seen: dict[int, None] = {}
    for bill_id in amendments_by_bill:
        seen[bill_id] = None
    for bill_id in corrections_by_bill:
        seen[bill_id] = None

    changes = []
    for bill_id in seen:
        bill = None
        if bill_id in amendments_by_bill:
            bill = amendments_by_bill[bill_id][0]
        if bill is None and bill_id in corrections_by_bill:
            bill = corrections_by_bill[bill_id][0]
        if bill is None:
            continue
        changes.append(LawChange(
            bill=bill,
            amendments=amendments_by_bill[bill_id][1] if bill_id in amendments_by_bill else None,
            corrections=corrections_by_bill[bill_id][1] if bill_id in corrections_by_bill else None,
        ))

    # Resolve original_bill from binding rows
    original_bill_id = next(
        (r["lawid"] for r in binding_rows if r.get("bindingtypedesc") == "\u05d4\u05d7\u05d5\u05e7 \u05d4\u05de\u05e7\u05d5\u05e8\u05d9"),
        None,
    )
    if original_bill_id is None:
        parent_ids = {
            r["parentlawid"] for r in binding_rows
            if r.get("parentlawid") is not None and r["parentlawid"] != law_id
        }
        if len(parent_ids) == 1:
            original_bill_id = parent_ids.pop()

    original_bill = _fetch_bill_partial(cursor, original_bill_id) if original_bill_id else None
    return changes or None, original_bill


def _fetch_secondary_laws(cursor, law_id: int):
    from origins.secondary_laws.secondary_laws_models import SecondaryLawResultPartial
    from origins.secondary_laws.secondary_laws_view import _build_partial as _sec_build_partial
    cursor.execute(
        """SELECT DISTINCT s.Id, s.Name, s.KnessetNum, s.TypeDesc, s.StatusName,
                  s.IsCurrent, s.PublicationDate, s.CommitteeID,
                  maj.AuthorizingLawID AS majorauthorizinglawid,
                  c.Name AS committee_name,
                  il.Name AS major_authorizing_law_name
        FROM sec_law_authorizing_law_raw sla
        JOIN secondary_law_raw s ON sla.SecondaryLawID = s.Id
        LEFT JOIN committee_raw c ON s.CommitteeID = c.Id
        LEFT JOIN LATERAL (
            SELECT MIN(AuthorizingLawID) AS AuthorizingLawID
            FROM sec_law_authorizing_law_raw
            WHERE SecondaryLawID = s.Id
        ) maj ON TRUE
        LEFT JOIN israel_law_raw il ON maj.AuthorizingLawID = il.Id
        WHERE sla.AuthorizingLawID = %s
        ORDER BY s.PublicationDate DESC, s.Id DESC""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    return [_sec_build_partial(row) for row in rows]


def _fetch_documents(cursor, law_id: int) -> list[SessionDocument] | None:
    cursor.execute(
        """SELECT GroupTypeDesc, ApplicationDesc, FilePath
        FROM document_israel_law_raw
        WHERE IsraelLawID = %s""",
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
    classifications = _fetch_classifications(cursor, law_id)
    ministries = _fetch_ministries(cursor, law_id)
    alt_names = _fetch_alternative_names(cursor, law_id, row.get("name"))
    replaced_laws = _fetch_replaced_laws(cursor, law_id)
    changes, original_bill = _fetch_changes_and_original(cursor, law_id)
    secondary_laws = _fetch_secondary_laws(cursor, law_id)
    documents = _fetch_documents(cursor, law_id)
    return classifications, ministries, alt_names, replaced_laws, original_bill, changes, secondary_laws, documents


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_laws_search(*, query, knesset_num, date, date_to, top_n):
    conditions = []
    params = []

    if query:
        conditions.append("l.Name LIKE %s")
        params.append(f"%{query}%")

    if knesset_num is not None:
        conditions.append("l.KnessetNum = %s")
        params.append(knesset_num)

    if date:
        conditions.append("(l.PublicationDate >= %s OR l.LatestPublicationDate >= %s)")
        params.extend([date, date])

    if date_to:
        conditions.append("(l.PublicationDate <= %s OR l.LatestPublicationDate <= %s)")
        params.extend([date_to, date_to])

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"SELECT COUNT(*) FROM israel_law_raw l WHERE {where}"
    search_sql = f"""
        SELECT l.Id AS id, l.Name AS name, l.KnessetNum AS knesset_num,
               l.IsBasicLaw AS isbasiclaw, l.IsBudgetLaw AS isbudgetlaw,
               l.IsFavoriteLaw AS isfavoritelaw,
               l.PublicationDate AS publicationdate,
               l.LatestPublicationDate AS latestpublicationdate,
               l.LawValidityDesc AS lawvaliditydesc
        FROM israel_law_raw l
        WHERE {where}
        ORDER BY l.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "laws",
    "builder": _build_laws_search,
    "mapper": lambda row: LawResultPartial(
        law_id=row["id"],
        name=row["name"],
        knesset_num=row["knesset_num"],
        law_types=_law_types(row),
        publication_date=simple_date(row.get("publicationdate")),
        latest_publication_date=simple_date(row.get("latestpublicationdate")),
        law_validity=row.get("lawvaliditydesc"),
    ),
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="laws",
    description=(
        "Search enacted Israeli laws (ספר החוקים) or get full detail for one law. "
        "Returns summary info by default; set full_details=True for classifications, "
        "ministries, bindings, corrections, documents, and connected bills. "
        "Provide law_id for a single law (auto-enables full_details)."
    ),
    entity="Laws",
    count_sql="SELECT COUNT(*) FROM israel_law_raw",
    most_recent_date_sql="SELECT MAX(LatestPublicationDate) FROM israel_law_raw",
    enum_sql={
        "law_validity": "SELECT DISTINCT LawValidityDesc FROM israel_law_raw WHERE LawValidityDesc IS NOT NULL AND LawValidityDesc != '' ORDER BY 1",
    },
    is_list=True,
)
def laws(
    law_id: Annotated[int | None, Field(description="Get a specific law by ID (auto-enables full_details)")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name_query: Annotated[str | None, Field(description="Law name contains text")] = None,
    is_basic_law: Annotated[bool, Field(description="Filter: חוק יסוד (basic law)")] = False,
    is_budget_law: Annotated[bool, Field(description="Filter: חוק תקציב (budget law)")] = False,
    is_favorite_law: Annotated[bool, Field(description="Filter: חוק מועדף (preferred law)")] = False,
    law_validity: Annotated[str | None, Field(description="Filter by law validity status")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD) — matches PublicationDate or LatestPublicationDate")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD) — matches PublicationDate or LatestPublicationDate")] = None,
    full_details: Annotated[bool, Field(description="Include classifications, ministries, bindings, corrections, documents, connected bills (auto-True when law_id is set)")] = False,
    top: Annotated[int | None, Field(description="Max results to return (default 50, max 200)")] = None,
    offset: Annotated[int | None, Field(description="Number of results to skip for pagination")] = None,
) -> LawsResults:
    """Search for enacted Israeli laws or get full detail for a single law."""
    normalized = normalize_inputs(locals())
    law_id = normalized["law_id"]
    knesset_num = normalized["knesset_num"]
    name_query = normalized["name_query"]
    is_basic_law = normalized["is_basic_law"]
    is_budget_law = normalized["is_budget_law"]
    is_favorite_law = normalized["is_favorite_law"]
    law_validity = normalized["law_validity"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    if law_id is not None:
        full_details = True

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if law_id is not None:
        conditions.append("l.Id = %s")
        params.append(law_id)

    if knesset_num is not None:
        conditions.append("l.KnessetNum = %s")
        params.append(knesset_num)

    if name_query:
        conditions.append("l.Name LIKE %s")
        params.append(f"%{name_query}%")

    type_cols = []
    if is_basic_law:
        type_cols.append("l.IsBasicLaw = 1")
    if is_budget_law:
        type_cols.append("l.IsBudgetLaw = 1")
    if is_favorite_law:
        type_cols.append("l.IsFavoriteLaw = 1")
    if type_cols:
        conditions.append("(" + " OR ".join(type_cols) + ")")

    if law_validity:
        conditions.append("l.LawValidityDesc LIKE %s")
        params.append(f"%{law_validity}%")

    if from_date:
        conditions.append("(l.PublicationDate >= %s OR l.LatestPublicationDate >= %s)")
        params.extend([from_date, from_date])

    if to_date:
        conditions.append("(l.PublicationDate <= %s OR l.LatestPublicationDate <= %s)")
        params.extend([to_date, to_date])

    where = " AND ".join(conditions) if conditions else "1=1"

    if law_id is None:
        total_count = check_search_count(
            cursor,
            f"SELECT COUNT(*) FROM israel_law_raw l WHERE {where}",
            params,
            entity_name="laws",
            paginated=True,
        )
    else:
        total_count = None

    cursor.execute(
        f"""SELECT l.Id, l.Name, l.KnessetNum,
               l.IsBasicLaw, l.IsBudgetLaw, l.IsFavoriteLaw,
               l.PublicationDate, l.LatestPublicationDate,
               l.LawValidityDesc,
               l.ValidityStartDate, l.ValidityStartDateNotes,
               l.ValidityFinishDate, l.ValidityFinishDateNotes
        FROM israel_law_raw l
        WHERE {where}
        ORDER BY l.PublicationDate DESC, l.Id DESC
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
            (classifications, ministries, alt_names, replaced_laws,
             original_bill, changes, secondary_laws, documents) = \
                _fetch_full_detail(cursor, row, lid)
            results.append(LawResultFull(
                law_id=lid,
                name=row["name"],
                knesset_num=row.get("knessetnum"),
                law_types=_law_types(row),
                publication_date=simple_date(row.get("publicationdate")),
                latest_publication_date=simple_date(row.get("latestpublicationdate")),
                law_validity=row.get("lawvaliditydesc"),
                validity_start_date=simple_date(row.get("validitystartdate")),
                validity_start_date_notes=row.get("validitystartdatenotes"),
                validity_finish_date=simple_date(row.get("validityfinishdate")),
                validity_finish_date_notes=row.get("validityfinishdatenotes"),
                classifications=classifications,
                ministries=ministries,
                alternative_names=alt_names,
                replaced_laws=replaced_laws,
                original_bill=original_bill,
                changes=changes,
                secondary_laws=secondary_laws,
                documents=documents,
            ))

    conn.close()
    if total_count is None:
        total_count = len(results)
    return LawsResults(total_count=total_count, items=results)


laws.OUTPUT_MODEL = LawsResults
