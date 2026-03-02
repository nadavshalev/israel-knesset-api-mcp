from typing import Any, Dict, Iterable, Optional, Tuple

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_PlmSessionItem"
TABLE_NAME = "plm_session_item_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/plenum/kns_plmsessionitem/kns_plmsessionitem.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plm_session_item_raw (
            Id INTEGER PRIMARY KEY,
            ItemID INTEGER,
            PlenumSessionID INTEGER,
            ItemTypeID INTEGER,
            ItemTypeDesc TEXT,
            Ordinal INTEGER,
            Name TEXT,
            StatusID INTEGER,
            IsDiscussion INTEGER,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_psi_itemid ON plm_session_item_raw (ItemID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_psi_session ON plm_session_item_raw (PlenumSessionID)")
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        "(Id, ItemID, PlenumSessionID, ItemTypeID, ItemTypeDesc, Ordinal, Name, StatusID, IsDiscussion, LastUpdatedDate, fetched_at) "
        "VALUES (:Id, :ItemID, :PlenumSessionID, :ItemTypeID, :ItemTypeDesc, :Ordinal, :Name, :StatusID, :IsDiscussion, :LastUpdatedDate, :fetched_at)"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("plmPlenumSessionID") or row.get("Id"),
                "ItemID": row.get("ItemID"),
                "PlenumSessionID": row.get("PlenumSessionID"),
                "ItemTypeID": row.get("ItemTypeID"),
                "ItemTypeDesc": row.get("ItemTypeDesc"),
                "Ordinal": row.get("Ordinal"),
                "Name": row.get("Name"),
                "StatusID": row.get("StatusID"),
                "IsDiscussion": 1 if row.get("IsDiscussion") else 0,
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
