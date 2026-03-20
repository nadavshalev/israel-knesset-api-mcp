from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_DocumentAgenda"
TABLE_NAME = "document_agenda_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_documentagenda_agendaid ON document_agenda_raw (AgendaID)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/knesset/kns_documentagenda/kns_documentagenda.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS document_agenda_raw (
            Id INTEGER PRIMARY KEY,
            AgendaID INTEGER,
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
        f"INSERT INTO {TABLE_NAME} "
        "(Id, AgendaID, GroupTypeID, GroupTypeDesc, ApplicationID, ApplicationDesc, FilePath, "
        "LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(AgendaID)s, %(GroupTypeID)s, %(GroupTypeDesc)s, %(ApplicationID)s, "
        "%(ApplicationDesc)s, %(FilePath)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "AgendaID=EXCLUDED.AgendaID, GroupTypeID=EXCLUDED.GroupTypeID, GroupTypeDesc=EXCLUDED.GroupTypeDesc, "
        "ApplicationID=EXCLUDED.ApplicationID, ApplicationDesc=EXCLUDED.ApplicationDesc, "
        "FilePath=EXCLUDED.FilePath, LastUpdatedDate=EXCLUDED.LastUpdatedDate, fetched_at=EXCLUDED.fetched_at"
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
                "AgendaID": row.get("AgendaID"),
                "GroupTypeID": row.get("GroupTypeID") or None,
                "GroupTypeDesc": row.get("GroupTypeDesc") or None,
                "ApplicationID": row.get("ApplicationID") or None,
                "ApplicationDesc": row.get("ApplicationDesc") or None,
                "FilePath": row.get("FilePath") or None,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
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
