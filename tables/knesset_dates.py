from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_KnessetDates"
TABLE_NAME = "knesset_dates_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_kd_knessetnum ON knesset_dates_raw (KnessetNum)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS knesset_dates_raw (
            Id INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name TEXT,
            Assembly INTEGER,
            Plenum INTEGER,
            PlenumStart TEXT,
            PlenumFinish TEXT,
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
        "(Id, KnessetNum, Name, Assembly, Plenum, PlenumStart, PlenumFinish, IsCurrent, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(KnessetNum)s, %(Name)s, %(Assembly)s, %(Plenum)s, %(PlenumStart)s, %(PlenumFinish)s, %(IsCurrent)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "KnessetNum=EXCLUDED.KnessetNum, Name=EXCLUDED.Name, Assembly=EXCLUDED.Assembly, "
        "Plenum=EXCLUDED.Plenum, PlenumStart=EXCLUDED.PlenumStart, PlenumFinish=EXCLUDED.PlenumFinish, "
        "IsCurrent=EXCLUDED.IsCurrent, LastUpdatedDate=EXCLUDED.LastUpdatedDate, fetched_at=EXCLUDED.fetched_at"
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
                "KnessetNum": row.get("KnessetNum"),
                "Name": row.get("Name"),
                "Assembly": row.get("Assembly"),
                "Plenum": row.get("Plenum"),
                "PlenumStart": row.get("PlenumStart"),
                "PlenumFinish": row.get("PlenumFinish"),
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
    rows = fetch_odata_table(
        table=ODATA_NAME,
        since=since,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
