from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_IsraelLawClassificiation"
TABLE_NAME = "israel_law_classification_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_israellawclassification_israellawid ON israel_law_classification_raw (IsraelLawID)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS israel_law_classification_raw (
            Id INTEGER PRIMARY KEY,
            IsraelLawID INTEGER,
            ClassificiationID INTEGER,
            ClassificiationDesc TEXT,
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
        "(Id, IsraelLawID, ClassificiationID, ClassificiationDesc, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(IsraelLawID)s, %(ClassificiationID)s, %(ClassificiationDesc)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "IsraelLawID=EXCLUDED.IsraelLawID, ClassificiationID=EXCLUDED.ClassificiationID, "
        "ClassificiationDesc=EXCLUDED.ClassificiationDesc, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
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
                "Id": row.get("Id"),
                "IsraelLawID": row.get("IsraelLawID") or None,
                "ClassificiationID": row.get("ClassificiationID") or None,
                "ClassificiationDesc": row.get("ClassificiationDesc") or None,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = fetch_odata_table(
        table=ODATA_NAME,
        since=since,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
