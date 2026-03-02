from typing import Any, Dict, Iterable, Optional, Tuple

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_DocumentPlenumSession"
TABLE_NAME = "document_plenum_session_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/plenum/kns_documentplenumsession/kns_documentplenumsession.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS document_plenum_session_raw (
            Id INTEGER PRIMARY KEY,
            PlenumSessionID INTEGER,
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
    sql = (
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        "(Id, PlenumSessionID, GroupTypeID, GroupTypeDesc, ApplicationID, ApplicationDesc, FilePath, LastUpdatedDate, fetched_at) "
        "VALUES (:Id, :PlenumSessionID, :GroupTypeID, :GroupTypeDesc, :ApplicationID, :ApplicationDesc, :FilePath, :LastUpdatedDate, :fetched_at)"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("DocumentPlenumSessionID") or row.get("Id"),
                "PlenumSessionID": row.get("PlenumSessionID"),
                "GroupTypeID": row.get("GroupTypeID"),
                "GroupTypeDesc": row.get("GroupTypeDesc"),
                "ApplicationID": row.get("ApplicationID"),
                "ApplicationDesc": row.get("ApplicationDesc"),
                "FilePath": row.get("FilePath"),
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        cur.executemany(sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = fetch_table_with_csv_first(
        csv_url=CSV_URL,
        odata_table=ODATA_NAME,
        since=since,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
