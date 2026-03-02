from typing import Any, Dict, Iterable, Optional, Tuple

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_Status"
TABLE_NAME = "status_raw"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS status_raw (
            Id INTEGER PRIMARY KEY,
            Desc TEXT,
            TypeID INTEGER,
            TypeDesc TEXT,
            OrderTransition INTEGER,
            IsActive INTEGER,
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
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        "(Id, [Desc], TypeID, TypeDesc, OrderTransition, IsActive, LastUpdatedDate, fetched_at) "
        "VALUES (:Id, :Desc, :TypeID, :TypeDesc, :OrderTransition, :IsActive, :LastUpdatedDate, :fetched_at)"
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
                "Desc": row.get("Desc"),
                "TypeID": row.get("TypeID"),
                "TypeDesc": row.get("TypeDesc"),
                "OrderTransition": row.get("OrderTransition"),
                "IsActive": 1 if row.get("IsActive") else 0,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        cur.executemany(sql, payload)
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
