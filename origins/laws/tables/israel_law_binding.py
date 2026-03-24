from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_IsraelLawBinding"
TABLE_NAME = "israel_law_binding_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_israellawbinding_israellawid ON israel_law_binding_raw (IsraelLawID)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS israel_law_binding_raw (
            Id INTEGER PRIMARY KEY,
            IsraelLawID INTEGER,
            IsraelLawReplacedID INTEGER,
            LawID INTEGER,
            LawTypeID INTEGER,
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
        "(Id, IsraelLawID, IsraelLawReplacedID, LawID, LawTypeID, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(IsraelLawID)s, %(IsraelLawReplacedID)s, %(LawID)s, %(LawTypeID)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "IsraelLawID=EXCLUDED.IsraelLawID, IsraelLawReplacedID=EXCLUDED.IsraelLawReplacedID, "
        "LawID=EXCLUDED.LawID, LawTypeID=EXCLUDED.LawTypeID, "
        "LastUpdatedDate=EXCLUDED.LastUpdatedDate, fetched_at=EXCLUDED.fetched_at"
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
                "IsraelLawID": row.get("IsraelLawID") or None,
                "IsraelLawReplacedID": row.get("IsraelLawReplacedID") or None,
                "LawID": row.get("LawID") or None,
                "LawTypeID": row.get("LawTypeID") or None,
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
