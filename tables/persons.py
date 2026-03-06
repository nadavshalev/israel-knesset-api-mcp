from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_Person"
TABLE_NAME = "person_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/members/kns_person/kns_person.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS person_raw (
            PersonID INTEGER PRIMARY KEY,
            FirstName TEXT,
            LastName TEXT,
            GenderID INTEGER,
            GenderDesc TEXT,
            Email TEXT,
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
        f"INSERT INTO {TABLE_NAME} (PersonID, FirstName, LastName, GenderID, GenderDesc, Email, IsCurrent, LastUpdatedDate, fetched_at) "
        "VALUES (%(PersonID)s, %(FirstName)s, %(LastName)s, %(GenderID)s, %(GenderDesc)s, %(Email)s, %(IsCurrent)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (PersonID) DO UPDATE SET "
        "FirstName=EXCLUDED.FirstName, LastName=EXCLUDED.LastName, GenderID=EXCLUDED.GenderID, "
        "GenderDesc=EXCLUDED.GenderDesc, Email=EXCLUDED.Email, IsCurrent=EXCLUDED.IsCurrent, "
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
                "PersonID": row.get("PersonID") or row.get("Id"),
                "FirstName": row.get("FirstName"),
                "LastName": row.get("LastName"),
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
