from typing import Any, Dict, Iterable, Optional, Tuple

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_PlenumSession"
TABLE_NAME = "plenum_session_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/plenum/kns_plenumsession/kns_plenumsession.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plenum_session_raw (
            Id INTEGER PRIMARY KEY,
            Number INTEGER,
            KnessetNum INTEGER,
            Name TEXT,
            StartDate TEXT,
            FinishDate TEXT,
            IsSpecialMeeting INTEGER,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_startdate ON plenum_session_raw (StartDate)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_knessetnum ON plenum_session_raw (KnessetNum)")
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        "(Id, Number, KnessetNum, Name, StartDate, FinishDate, IsSpecialMeeting, LastUpdatedDate, fetched_at) "
        "VALUES (:Id, :Number, :KnessetNum, :Name, :StartDate, :FinishDate, :IsSpecialMeeting, :LastUpdatedDate, :fetched_at)"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("PlenumSessionID") or row.get("Id"),
                "Number": row.get("Number"),
                "KnessetNum": row.get("KnessetNum"),
                "Name": row.get("Name"),
                "StartDate": row.get("StartDate"),
                "FinishDate": row.get("FinishDate"),
                "IsSpecialMeeting": 1 if row.get("IsSpecialMeeting") else 0,
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
