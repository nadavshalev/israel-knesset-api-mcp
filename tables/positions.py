from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_Position"
TABLE_NAME = "position_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/members/kns_position/kns_position.csv"

'''
                <Key>
                    <PropertyRef Name="Id" />
                </Key>
                <Property Name="Id" Type="Edm.Int32" Nullable="false" />
                <Property Name="Description" Type="Edm.String" />
                <Property Name="GenderID" Type="Edm.Int32" />
                <Property Name="GenderDesc" Type="Edm.String" />
                <Property Name="LastUpdatedDate" Type="Edm.DateTimeOffset" />
                '''


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS position_raw (
            Id INTEGER PRIMARY KEY,
            Description TEXT,
            GenderID INTEGER,
            GenderDesc TEXT,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )"""
    )
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT INTO {TABLE_NAME} (Id, Description, GenderID, GenderDesc, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(Description)s, %(GenderID)s, %(GenderDesc)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "Description=EXCLUDED.Description, GenderID=EXCLUDED.GenderID, GenderDesc=EXCLUDED.GenderDesc, "
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
                "Id": row.get("Id") or row.get("PositionID"),
                "Description": row.get("Description"),
                "GenderID": row.get("GenderID"),
                "GenderDesc": row.get("GenderDesc"),
                "Email": row.get("Email"),
                "IsCurrent": 1 if row.get("IsCurrent") else 0,
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
