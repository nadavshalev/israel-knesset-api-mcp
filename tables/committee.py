from typing import Any, Dict, Iterable, Optional, Tuple

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_Committee"
TABLE_NAME = "committee_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/committees/kns_committee/kns_committee.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS committee_raw (
            Id INTEGER PRIMARY KEY,
            Name TEXT,
            CategoryID INTEGER,
            CategoryDesc TEXT,
            KnessetNum INTEGER,
            CommitteeTypeID INTEGER,
            CommitteeTypeDesc TEXT,
            Email TEXT,
            StartDate TEXT,
            FinishDate TEXT,
            AdditionalTypeID INTEGER,
            AdditionalTypeDesc TEXT,
            ParentCommitteeID INTEGER,
            CommitteeParentName TEXT,
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
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        "(Id, Name, CategoryID, CategoryDesc, KnessetNum, "
        "CommitteeTypeID, CommitteeTypeDesc, Email, StartDate, FinishDate, "
        "AdditionalTypeID, AdditionalTypeDesc, ParentCommitteeID, CommitteeParentName, "
        "IsCurrent, LastUpdatedDate, fetched_at) "
        "VALUES (:Id, :Name, :CategoryID, :CategoryDesc, :KnessetNum, "
        ":CommitteeTypeID, :CommitteeTypeDesc, :Email, :StartDate, :FinishDate, "
        ":AdditionalTypeID, :AdditionalTypeDesc, :ParentCommitteeID, :CommitteeParentName, "
        ":IsCurrent, :LastUpdatedDate, :fetched_at)"
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
                "Id": row.get("CommitteeID") or row.get("Id"),
                "Name": row.get("Name"),
                "CategoryID": row.get("CategoryID") or None,
                "CategoryDesc": row.get("CategoryDesc") or None,
                "KnessetNum": row.get("KnessetNum") or None,
                "CommitteeTypeID": row.get("CommitteeTypeID") or None,
                "CommitteeTypeDesc": row.get("CommitteeTypeDesc") or None,
                "Email": row.get("Email") or None,
                "StartDate": row.get("StartDate") or None,
                "FinishDate": row.get("FinishDate") or None,
                "AdditionalTypeID": row.get("AdditionalTypeID") or None,
                "AdditionalTypeDesc": row.get("AdditionalTypeDesc") or None,
                "ParentCommitteeID": row.get("ParentCommitteeID") or None,
                "CommitteeParentName": row.get("CommitteeParentName") or None,
                "IsCurrent": 1 if is_current and str(is_current).lower() not in ("", "false", "0", "none") else 0,
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
