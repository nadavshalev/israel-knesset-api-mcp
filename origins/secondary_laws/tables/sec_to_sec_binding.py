from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_SecToSecBinding"
TABLE_NAME = "sec_to_sec_binding_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_sectosec_childid ON sec_to_sec_binding_raw (SecChildID)",
    "CREATE INDEX IF NOT EXISTS idx_sectosec_parentid ON sec_to_sec_binding_raw (SecParentID)",
    "CREATE INDEX IF NOT EXISTS idx_sectosec_mainid ON sec_to_sec_binding_raw (SecMainID)",
]

_COLS = [
    "Id", "SecChildID", "SecChildTypeID",
    "SecParentID", "SecParentTypeID",
    "SecMainID", "SecMainTypeID",
    "BindingTypeID", "BindingTypeDesc",
    "IsTempLegislation", "IsSecondaryAmendment",
    "CorrectionNumber", "AmendmentTypeID", "AmendmentTypeDesc",
    "ParagraphNumber", "LastUpdatedDate",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sec_to_sec_binding_raw (
            Id INTEGER PRIMARY KEY,
            SecChildID INTEGER,
            SecChildTypeID INTEGER,
            SecParentID INTEGER,
            SecParentTypeID INTEGER,
            SecMainID INTEGER,
            SecMainTypeID INTEGER,
            BindingTypeID INTEGER,
            BindingTypeDesc TEXT,
            IsTempLegislation INTEGER,
            IsSecondaryAmendment INTEGER,
            CorrectionNumber INTEGER,
            AmendmentTypeID INTEGER,
            AmendmentTypeDesc TEXT,
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
    _BOOL_COLS = {"IsTempLegislation", "IsSecondaryAmendment"}
    cols_with_fetch = _COLS + ["fetched_at"]
    placeholders = ", ".join(f"%({c})s" for c in cols_with_fetch)
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols_with_fetch if c != "Id")
    sql = (
        f"INSERT INTO {TABLE_NAME} ({', '.join(cols_with_fetch)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (Id) DO UPDATE SET {updates}"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        record = {"fetched_at": now}
        for c in _COLS:
            v = row.get(c)
            if c in _BOOL_COLS:
                record[c] = 1 if v else 0
            else:
                record[c] = v if v else None
        record["LastUpdatedDate"] = last_updated
        record["Id"] = row.get("Id")
        payload.append(record)
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = fetch_odata_table(table=ODATA_NAME, since=since)
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
