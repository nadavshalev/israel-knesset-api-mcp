from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_IsraelLawLawCorrections"
TABLE_NAME = "israel_law_law_corrections_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_israellawlawcorrections_israellawid ON israel_law_law_corrections_raw (IsraelLawID)",
    "CREATE INDEX IF NOT EXISTS idx_israellawlawcorrections_lawcorrectionid ON israel_law_law_corrections_raw (LawCorrectionID)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS israel_law_law_corrections_raw (
            Id INTEGER PRIMARY KEY,
            LawCorrectionID INTEGER,
            IsraelLawID INTEGER,
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
        "(Id, LawCorrectionID, IsraelLawID, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(LawCorrectionID)s, %(IsraelLawID)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "LawCorrectionID=EXCLUDED.LawCorrectionID, IsraelLawID=EXCLUDED.IsraelLawID, "
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
                "Id": row.get("Id"),
                "LawCorrectionID": row.get("LawCorrectionID") or None,
                "IsraelLawID": row.get("IsraelLawID") or None,
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
        since_field="Id",
        numeric=True,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
