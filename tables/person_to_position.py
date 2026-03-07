from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_PersonToPosition"
TABLE_NAME = "person_to_position_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ptp_committeeid ON person_to_position_raw (CommitteeID)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/members/kns_persontoposition/kns_persontoposition.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS person_to_position_raw (
            PersonToPositionID INTEGER PRIMARY KEY,
            PersonID INTEGER,
            PositionID INTEGER,
            KnessetNum INTEGER,
            GovMinistryID INTEGER,
            GovMinistryName TEXT,
            DutyDesc TEXT,
            FactionID INTEGER,
            FactionName TEXT,
            GovernmentNum INTEGER,
            CommitteeID INTEGER,
            CommitteeName TEXT,
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
        f"INSERT INTO {TABLE_NAME} (PersonToPositionID, PersonID, PositionID, KnessetNum, GovMinistryID, GovMinistryName, DutyDesc, "
        "FactionID, FactionName, GovernmentNum, CommitteeID, CommitteeName, StartDate, FinishDate, IsCurrent, LastUpdatedDate, fetched_at) "
        "VALUES (%(PersonToPositionID)s, %(PersonID)s, %(PositionID)s, %(KnessetNum)s, %(GovMinistryID)s, %(GovMinistryName)s, %(DutyDesc)s, "
        "%(FactionID)s, %(FactionName)s, %(GovernmentNum)s, %(CommitteeID)s, %(CommitteeName)s, %(StartDate)s, %(FinishDate)s, %(IsCurrent)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (PersonToPositionID) DO UPDATE SET "
        "PersonID=EXCLUDED.PersonID, PositionID=EXCLUDED.PositionID, KnessetNum=EXCLUDED.KnessetNum, "
        "GovMinistryID=EXCLUDED.GovMinistryID, GovMinistryName=EXCLUDED.GovMinistryName, DutyDesc=EXCLUDED.DutyDesc, "
        "FactionID=EXCLUDED.FactionID, FactionName=EXCLUDED.FactionName, GovernmentNum=EXCLUDED.GovernmentNum, "
        "CommitteeID=EXCLUDED.CommitteeID, CommitteeName=EXCLUDED.CommitteeName, StartDate=EXCLUDED.StartDate, "
        "FinishDate=EXCLUDED.FinishDate, IsCurrent=EXCLUDED.IsCurrent, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
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
                "PersonToPositionID": row.get("PersonToPositionID") or row.get("Id"),
                "PersonID": row.get("PersonID"),
                "PositionID": row.get("PositionID"),
                "KnessetNum": row.get("KnessetNum"),
                "GovMinistryID": row.get("GovMinistryID"),
                "GovMinistryName": row.get("GovMinistryName"),
                "DutyDesc": row.get("DutyDesc"),
                "FactionID": row.get("FactionID"),
                "FactionName": row.get("FactionName"),
                "GovernmentNum": row.get("GovernmentNum"),
                "CommitteeID": row.get("CommitteeID"),
                "CommitteeName": row.get("CommitteeName"),
                "StartDate": row.get("StartDate"),
                "FinishDate": row.get("FinishDate"),
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
