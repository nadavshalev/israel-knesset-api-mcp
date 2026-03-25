from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_DocumentSecondaryLaw"
TABLE_NAME = "document_secondary_law_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_docseclaw_seclawid ON document_secondary_law_raw (SecondaryLawId)",
]

_COLS = [
    "Id", "SecondaryLawId", "GroupTypeID", "GroupTypeDesc",
    "ApplicationID", "ApplicationDesc", "FilePath", "LastUpdatedDate",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS document_secondary_law_raw (
            Id INTEGER PRIMARY KEY,
            SecondaryLawId INTEGER,
            GroupTypeID INTEGER,
            GroupTypeDesc TEXT,
            ApplicationID INTEGER,
            ApplicationDesc TEXT,
            FilePath TEXT,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
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
            record[c] = row.get(c) or None
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
