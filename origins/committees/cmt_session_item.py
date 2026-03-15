from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_CmtSessionItem"
TABLE_NAME = "cmt_session_item_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_csi_itemid ON cmt_session_item_raw (ItemID)",
    "CREATE INDEX IF NOT EXISTS idx_csi_session ON cmt_session_item_raw (CommitteeSessionID)",
    "CREATE INDEX IF NOT EXISTS idx_csi_itemtype ON cmt_session_item_raw (ItemTypeID)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/committees/kns_cmtsessionitem/kns_cmtsessionitem.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cmt_session_item_raw (
            Id INTEGER PRIMARY KEY,
            ItemID INTEGER,
            CommitteeSessionID INTEGER,
            Ordinal INTEGER,
            StatusID INTEGER,
            Name TEXT,
            ItemTypeID INTEGER,
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
        "(Id, ItemID, CommitteeSessionID, Ordinal, StatusID, Name, ItemTypeID, "
        "LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(ItemID)s, %(CommitteeSessionID)s, %(Ordinal)s, %(StatusID)s, %(Name)s, "
        "%(ItemTypeID)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "ItemID=EXCLUDED.ItemID, CommitteeSessionID=EXCLUDED.CommitteeSessionID, "
        "Ordinal=EXCLUDED.Ordinal, StatusID=EXCLUDED.StatusID, Name=EXCLUDED.Name, "
        "ItemTypeID=EXCLUDED.ItemTypeID, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
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
                "Id": row.get("CmtSessionItemID") or row.get("Id"),
                "ItemID": row.get("ItemID") or None,
                "CommitteeSessionID": row.get("CommitteeSessionID") or None,
                "Ordinal": row.get("Ordinal") or None,
                "StatusID": row.get("StatusID") or None,
                "Name": row.get("Name") or None,
                "ItemTypeID": row.get("ItemTypeID") or None,
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
