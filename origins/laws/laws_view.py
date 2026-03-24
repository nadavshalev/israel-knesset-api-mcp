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
from core.helpers import simple_date, normalize_inputs, check_search_count
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import SessionDocument
from origins.bills.bills_models import BillResultPartial
from origins.laws.laws_models import (
    LawResultPartial, LawResultFull, LawsResults,
    ReplacedLaw, LawBinding, LawCorrection,
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


def _fetch_law_bindings_and_original(cursor, law_id: int):
    """Fetch law bindings and resolve a single original_bill.

    Priority:
    1. Binding with BindingTypeDesc = 'החוק המקורי' — use its LawID.
    2. All bindings share the same ParentLawID (excluding self) — use that.
    If both apply, prefer (1).

    Returns (bindings, original_bill).
    """
    cursor.execute(
        """SELECT lb.LawID, lb.ParentLawID, lb.BindingTypeDesc,
                  lb.AmendmentTypeDesc, lb.PageNumber,
                  lb.ParagraphNumber, lb.CorrectionNumber,
                  lb.LastUpdatedDate,
                  b.Name AS bill_name
        FROM law_binding_raw lb
        LEFT JOIN bill_raw b ON lb.LawID = b.Id
        WHERE lb.IsraelLawID = %s""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None, None

    bindings = [
        LawBinding(
            bill_id=r["lawid"],
            bill_name=r["bill_name"],
            binding_type=r["bindingtypedesc"],
            amendment_type=r["amendmenttypedesc"],
            date=simple_date(r.get("lastupdateddate")),
            page_number=r["pagenumber"],
            paragraph_number=str(r["paragraphnumber"]) if r.get("paragraphnumber") is not None else None,
            correction_number=str(r["correctionnumber"]) if r.get("correctionnumber") is not None else None,
        )
        for r in rows
    ]

    # Priority 1: explicit "החוק המקורי" binding
    original_bill_id = next(
        (r["lawid"] for r in rows if r.get("bindingtypedesc") == "החוק המקורי"),
        None,
    )

    # Priority 2 (fallback): all bindings share the same ParentLawID
    if original_bill_id is None:
        parent_ids = {
            r["parentlawid"] for r in rows
            if r.get("parentlawid") is not None and r["parentlawid"] != law_id
        }
        if len(parent_ids) == 1:
            original_bill_id = parent_ids.pop()

    original_bill = _fetch_bill_partial(cursor, original_bill_id) if original_bill_id else None
    return bindings or None, original_bill


def _fetch_corrections(cursor, law_id: int) -> list[LawCorrection] | None:
    cursor.execute(
        """SELECT lc.CorrectionTypeDesc, lc.CorrectionStatusDesc,
                  lc.PublicationDate, lc.MagazineNumber, lc.PageNumber,
                  lc.IsKnessetInvolvement, lc.BillID, b.Name AS bill_name
        FROM israel_law_law_corrections_raw illc
        JOIN law_corrections_raw lc ON illc.LawCorrectionID = lc.Id
        LEFT JOIN bill_raw b ON lc.BillID = b.Id
        WHERE illc.IsraelLawID = %s
        ORDER BY lc.PublicationDate DESC""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    return [
        LawCorrection(
            correction_type=r["correctiontypedesc"],
            status=r["correctionstatusdesc"],
            publication_date=simple_date(r.get("publicationdate")),
            magazine_number=r.get("magazinenumber"),
            page_number=r.get("pagenumber"),
            is_knesset_involvement=bool(r["isknessetinvolvement"]) if r.get("isknessetinvolvement") is not None else None,
            bill_id=r["billid"],
            bill_name=r["bill_name"],
        )
        for r in rows
    ]


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


def _fetch_connected_bills(cursor, law_id: int) -> list[BillResultPartial] | None:
    cursor.execute(
        """SELECT DISTINCT b.Id, b.Name, b.KnessetNum, b.SubTypeDesc,
               st."Desc" AS StatusDesc, b.PublicationDate,
               b.PublicationSeriesDesc, b.SummaryLaw
        FROM israel_law_law_corrections_raw illc
        JOIN law_corrections_raw lc ON illc.LawCorrectionID = lc.Id
        JOIN bill_raw b ON lc.BillID = b.Id
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE illc.IsraelLawID = %s
        ORDER BY b.PublicationDate DESC""",
        [law_id],
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    return [
        BillResultPartial(
            bill_id=r["id"],
            name=r["name"],
            knesset_num=r["knessetnum"],
            type=r["subtypedesc"],
            status=r["statusdesc"],
            publication_date=simple_date(r.get("publicationdate")),
            publication_series=r.get("publicationseriesdesc"),
            summary=r.get("summarylaw"),
        )
        for r in rows
    ]


def _fetch_full_detail(cursor, row, law_id: int):
    classifications = _fetch_classifications(cursor, law_id)
    ministries = _fetch_ministries(cursor, law_id)
    alt_names = _fetch_alternative_names(cursor, law_id, row.get("name"))
    replaced_laws = _fetch_replaced_laws(cursor, law_id)
    bindings, original_bill = _fetch_law_bindings_and_original(cursor, law_id)
    corrections = _fetch_corrections(cursor, law_id)
    documents = _fetch_documents(cursor, law_id)
    bills = _fetch_connected_bills(cursor, law_id)
    return classifications, ministries, alt_names, replaced_laws, original_bill, bindings, corrections, documents, bills


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
        check_search_count(
            cursor,
            f"SELECT COUNT(*) FROM israel_law_raw l WHERE {where}",
            params,
            entity_name="laws",
        )

    cursor.execute(
        f"""SELECT l.Id, l.Name, l.KnessetNum,
               l.IsBasicLaw, l.IsBudgetLaw, l.IsFavoriteLaw,
               l.PublicationDate, l.LatestPublicationDate,
               l.LawValidityDesc,
               l.ValidityStartDate, l.ValidityStartDateNotes,
               l.ValidityFinishDate, l.ValidityFinishDateNotes
        FROM israel_law_raw l
        WHERE {where}
        ORDER BY l.PublicationDate DESC, l.Id DESC""",
        params,
    )
    rows = cursor.fetchall()

    if not full_details:
        results = [_build_partial(row) for row in rows]
    else:
        results = []
        for row in rows:
            lid = row["id"]
            (classifications, ministries, alt_names, replaced_laws,
             original_bill, bindings, corrections, documents, bills) = \
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
                bindings=bindings,
                corrections=corrections,
                documents=documents,
                bills=bills,
            ))

    conn.close()
    return LawsResults(items=results)


laws.OUTPUT_MODEL = LawsResults
