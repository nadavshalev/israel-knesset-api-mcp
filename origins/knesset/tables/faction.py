from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_Faction"
TABLE_NAME = "faction_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/knesset/kns_faction/kns_faction.csv"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_faction_knessetnum ON faction_raw (KnessetNum)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS faction_raw (
            Id INTEGER PRIMARY KEY,
            Name TEXT,
            KnessetNum INTEGER,
            StartDate TEXT,
            FinishDate TEXT,
            IsCurrent INTEGER,
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
        "(Id, Name, KnessetNum, StartDate, FinishDate, IsCurrent, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(Name)s, %(KnessetNum)s, %(StartDate)s, %(FinishDate)s, %(IsCurrent)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "Name=EXCLUDED.Name, KnessetNum=EXCLUDED.KnessetNum, "
        "StartDate=EXCLUDED.StartDate, FinishDate=EXCLUDED.FinishDate, "
        "IsCurrent=EXCLUDED.IsCurrent, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
        "fetched_at=EXCLUDED.fetched_at"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        is_current = row.get("IsCurrent")
        payload.append(
            {
                "Id": row.get("FactionID") or row.get("Id"),
                "Name": row.get("Name"),
                "KnessetNum": row.get("KnessetNum"),
                "StartDate": row.get("StartDate"),
                "FinishDate": row.get("FinishDate"),
                "IsCurrent": 1 if is_current and str(is_current).lower() not in ("", "false", "0", "none") else 0,
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
