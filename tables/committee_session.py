from typing import Any, Dict, Iterable, Optional, Tuple

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_CommitteeSession"
TABLE_NAME = "committee_session_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/committees/kns_committeesession/kns_committeesession.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS committee_session_raw (
            Id INTEGER PRIMARY KEY,
            Number INTEGER,
            KnessetNum INTEGER,
            TypeID INTEGER,
            TypeDesc TEXT,
            CommitteeID INTEGER,
            StatusID INTEGER,
            StatusDesc TEXT,
            Location TEXT,
            SessionUrl TEXT,
            BroadcastUrl TEXT,
            StartDate TEXT,
            FinishDate TEXT,
            Note TEXT,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cs_committee ON committee_session_raw (CommitteeID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cs_knessetnum ON committee_session_raw (KnessetNum)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cs_startdate ON committee_session_raw (StartDate)")
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        "(Id, Number, KnessetNum, TypeID, TypeDesc, CommitteeID, "
        "StatusID, StatusDesc, Location, SessionUrl, BroadcastUrl, "
        "StartDate, FinishDate, Note, LastUpdatedDate, fetched_at) "
        "VALUES (:Id, :Number, :KnessetNum, :TypeID, :TypeDesc, :CommitteeID, "
        ":StatusID, :StatusDesc, :Location, :SessionUrl, :BroadcastUrl, "
        ":StartDate, :FinishDate, :Note, :LastUpdatedDate, :fetched_at)"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("CommitteeSessionID") or row.get("Id"),
                "Number": row.get("Number") or None,
                "KnessetNum": row.get("KnessetNum") or None,
                "TypeID": row.get("TypeID") or None,
                "TypeDesc": row.get("TypeDesc") or None,
                "CommitteeID": row.get("CommitteeID") or None,
                "StatusID": row.get("StatusID") or None,
                "StatusDesc": row.get("StatusDesc") or None,
                "Location": row.get("Location") or None,
                "SessionUrl": row.get("SessionUrl") or None,
                "BroadcastUrl": row.get("BroadcastUrl") or None,
                "StartDate": row.get("StartDate") or None,
                "FinishDate": row.get("FinishDate") or None,
                "Note": row.get("Note") or None,
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
