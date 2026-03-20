from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_GovMinistry"
TABLE_NAME = "gov_ministry_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/knesset/kns_govministry/kns_govministry.csv"
ENSURE_INDEXES: list[str] = []


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_ministry_raw (
            Id INTEGER PRIMARY KEY,
            Name TEXT,
            IsActive INTEGER,
            CategoryID INTEGER,
            CategoryName TEXT,
            GovID INTEGER,
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
        "(Id, Name, IsActive, CategoryID, CategoryName, GovID, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(Name)s, %(IsActive)s, %(CategoryID)s, %(CategoryName)s, %(GovID)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "Name=EXCLUDED.Name, IsActive=EXCLUDED.IsActive, "
        "CategoryID=EXCLUDED.CategoryID, CategoryName=EXCLUDED.CategoryName, "
        "GovID=EXCLUDED.GovID, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
        "fetched_at=EXCLUDED.fetched_at"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        is_active = row.get("IsActive")
        payload.append(
            {
                # CSV uses GovMinistryID; OData uses Id — both are the entity's Id
                "Id": row.get("GovMinistryID") or row.get("Id"),
                "Name": row.get("Name"),
                "IsActive": 1 if is_active and str(is_active).lower() not in ("", "false", "0", "none") else 0,
                "CategoryID": row.get("CategoryID"),
                "CategoryName": row.get("CategoryName"),
                "GovID": row.get("GovID"),
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
