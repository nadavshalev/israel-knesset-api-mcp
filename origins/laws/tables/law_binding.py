from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_LawBinding"
TABLE_NAME = "law_binding_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_lawbinding_israellawid ON law_binding_raw (IsraelLawID)",
    "CREATE INDEX IF NOT EXISTS idx_lawbinding_lawid ON law_binding_raw (LawID)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS law_binding_raw (
            Id INTEGER PRIMARY KEY,
            LawID INTEGER,
            IsraelLawID INTEGER,
            ParentLawID INTEGER,
            LawTypeID INTEGER,
            LawParentTypeID INTEGER,
            BindingType INTEGER,
            BindingTypeDesc TEXT,
            PageNumber TEXT,
            AmendmentType INTEGER,
            AmendmentTypeDesc TEXT,
            IsTempLegislation INTEGER,
            IsSecondaryAmendment INTEGER,
            CorrectionNumber INTEGER,
            ParagraphNumber TEXT,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT INTO {TABLE_NAME} "
        "(Id, LawID, IsraelLawID, ParentLawID, LawTypeID, LawParentTypeID, BindingType, BindingTypeDesc, "
        "PageNumber, AmendmentType, AmendmentTypeDesc, IsTempLegislation, IsSecondaryAmendment, "
        "CorrectionNumber, ParagraphNumber, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(LawID)s, %(IsraelLawID)s, %(ParentLawID)s, %(LawTypeID)s, %(LawParentTypeID)s, "
        "%(BindingType)s, %(BindingTypeDesc)s, %(PageNumber)s, %(AmendmentType)s, %(AmendmentTypeDesc)s, "
        "%(IsTempLegislation)s, %(IsSecondaryAmendment)s, %(CorrectionNumber)s, %(ParagraphNumber)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "LawID=EXCLUDED.LawID, IsraelLawID=EXCLUDED.IsraelLawID, ParentLawID=EXCLUDED.ParentLawID, "
        "LawTypeID=EXCLUDED.LawTypeID, LawParentTypeID=EXCLUDED.LawParentTypeID, "
        "BindingType=EXCLUDED.BindingType, BindingTypeDesc=EXCLUDED.BindingTypeDesc, "
        "PageNumber=EXCLUDED.PageNumber, AmendmentType=EXCLUDED.AmendmentType, "
        "AmendmentTypeDesc=EXCLUDED.AmendmentTypeDesc, IsTempLegislation=EXCLUDED.IsTempLegislation, "
        "IsSecondaryAmendment=EXCLUDED.IsSecondaryAmendment, CorrectionNumber=EXCLUDED.CorrectionNumber, "
        "ParagraphNumber=EXCLUDED.ParagraphNumber, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
        "fetched_at=EXCLUDED.fetched_at"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("Id"),
                "LawID": row.get("LawID") or None,
                "IsraelLawID": row.get("IsraelLawID") or None,
                "ParentLawID": row.get("ParentLawID") or None,
                "LawTypeID": row.get("LawTypeID") or None,
                "LawParentTypeID": row.get("LawParentTypeID") or None,
                "BindingType": row.get("BindingType") or None,
                "BindingTypeDesc": row.get("BindingTypeDesc") or None,
                "PageNumber": row.get("PageNumber") or None,
                "AmendmentType": row.get("AmendmentType") or None,
                "AmendmentTypeDesc": row.get("AmendmentTypeDesc") or None,
                "IsTempLegislation": 1 if row.get("IsTempLegislation") else 0,
                "IsSecondaryAmendment": 1 if row.get("IsSecondaryAmendment") else 0,
                "CorrectionNumber": row.get("CorrectionNumber") or None,
                "ParagraphNumber": row.get("ParagraphNumber") or None,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = fetch_odata_table(
        table=ODATA_NAME,
        since=since,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
