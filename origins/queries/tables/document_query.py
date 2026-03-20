from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras
import requests

from core.odata_client import _utc_now_iso
from core.db import update_metadata

TABLE_NAME = "document_query_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_documentquery_queryid ON document_query_raw (QueryID)",
]

# The KNS_DocumentQuery OData endpoint returns a raw JSON array
# (not the standard {value: [...]} wrapper), so we fetch directly.
ODATA_URL = "https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_DocumentQuery"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS document_query_raw (
            Id INTEGER PRIMARY KEY,
            QueryID INTEGER,
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
        "(Id, QueryID, GroupTypeID, GroupTypeDesc, ApplicationID, ApplicationDesc, FilePath, "
        "LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(QueryID)s, %(GroupTypeID)s, %(GroupTypeDesc)s, %(ApplicationID)s, "
        "%(ApplicationDesc)s, %(FilePath)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "QueryID=EXCLUDED.QueryID, GroupTypeID=EXCLUDED.GroupTypeID, GroupTypeDesc=EXCLUDED.GroupTypeDesc, "
        "ApplicationID=EXCLUDED.ApplicationID, ApplicationDesc=EXCLUDED.ApplicationDesc, "
        "FilePath=EXCLUDED.FilePath, LastUpdatedDate=EXCLUDED.LastUpdatedDate, fetched_at=EXCLUDED.fetched_at"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("lastUpdatedDate") or row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("id") or row.get("Id"),
                "QueryID": row.get("queryID") or row.get("QueryID"),
                "GroupTypeID": row.get("groupTypeID") or row.get("GroupTypeID") or None,
                "GroupTypeDesc": row.get("groupTypeDesc") or row.get("GroupTypeDesc") or None,
                "ApplicationID": row.get("applicationID") or row.get("ApplicationID") or None,
                "ApplicationDesc": row.get("applicationDesc") or row.get("ApplicationDesc") or None,
                "FilePath": row.get("filePath") or row.get("FilePath") or None,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload, page_size=500)
    conn.commit()
    return len(payload), max_updated


def _fetch_all_pages():
    """Fetch all document_query rows via paginated OData requests."""
    page_size = 1000
    skip = 0
    all_rows = []
    while True:
        params = {"$top": page_size, "$skip": skip, "$orderby": "lastUpdatedDate asc"}
        print(f"Requesting {ODATA_URL} skip={skip}")
        resp = requests.get(ODATA_URL, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        # Handle both raw array and {value: [...]} formats
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("value", [])
        else:
            break
        if not batch:
            break
        all_rows.extend(batch)
        print(f"  Fetched {len(batch)} rows (total: {len(all_rows)})")
        if len(batch) < page_size:
            break
        skip += page_size
    return all_rows


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = _fetch_all_pages()
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
