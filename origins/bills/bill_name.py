from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_BillName"
TABLE_NAME = "bill_name_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_billname_billid ON bill_name_raw (BillID)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/bills/kns_billname/kns_billname.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bill_name_raw (
            Id INTEGER PRIMARY KEY,
            BillID INTEGER,
            Name TEXT,
            NameHistoryTypeID INTEGER,
            NameHistoryTypeDesc TEXT,
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
        "(Id, BillID, Name, NameHistoryTypeID, NameHistoryTypeDesc, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(BillID)s, %(Name)s, %(NameHistoryTypeID)s, %(NameHistoryTypeDesc)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "BillID=EXCLUDED.BillID, Name=EXCLUDED.Name, NameHistoryTypeID=EXCLUDED.NameHistoryTypeID, "
        "NameHistoryTypeDesc=EXCLUDED.NameHistoryTypeDesc, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
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
                "Id": row.get("BillNameID") or row.get("Id"),
                "BillID": row.get("BillID"),
                "Name": row.get("Name"),
                "NameHistoryTypeID": row.get("NameHistoryTypeID") or None,
                "NameHistoryTypeDesc": row.get("NameHistoryTypeDesc") or None,
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
