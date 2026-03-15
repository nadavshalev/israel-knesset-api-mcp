from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_BillUnion"
TABLE_NAME = "bill_union_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_billunion_mainbillid ON bill_union_raw (MainBillID)",
    "CREATE INDEX IF NOT EXISTS idx_billunion_unionbillid ON bill_union_raw (UnionBillID)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/bills/kns_billunion/kns_billunion.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bill_union_raw (
            Id INTEGER PRIMARY KEY,
            MainBillID INTEGER,
            UnionBillID INTEGER,
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
        "(Id, MainBillID, UnionBillID, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(MainBillID)s, %(UnionBillID)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "MainBillID=EXCLUDED.MainBillID, UnionBillID=EXCLUDED.UnionBillID, "
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
                "Id": row.get("BillUnionID") or row.get("Id"),
                "MainBillID": row.get("MainBillID"),
                "UnionBillID": row.get("UnionBillID"),
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
