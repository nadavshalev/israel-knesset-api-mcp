from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_SecondaryLaw"
TABLE_NAME = "secondary_law_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_seclaw_knessetnum ON secondary_law_raw (KnessetNum)",
    "CREATE INDEX IF NOT EXISTS idx_seclaw_name ON secondary_law_raw (Name text_pattern_ops)",
    "CREATE INDEX IF NOT EXISTS idx_seclaw_majorauth ON secondary_law_raw (MajorAuthorizingLawID)",
    "CREATE INDEX IF NOT EXISTS idx_seclaw_committee ON secondary_law_raw (CommitteeID)",
    "CREATE INDEX IF NOT EXISTS idx_seclaw_status ON secondary_law_raw (StatusID)",
    "CREATE INDEX IF NOT EXISTS idx_seclaw_type ON secondary_law_raw (TypeID)",
    "CREATE INDEX IF NOT EXISTS idx_seclaw_name_fts ON secondary_law_raw USING GIN (to_tsvector('simple', normalize_hebrew_fts(name)))",
    "CREATE INDEX IF NOT EXISTS idx_seclaw_name_trgm ON secondary_law_raw USING GIN (normalize_hebrew(name) gin_trgm_ops)",
]

_COLS = [
    "Id", "KnessetNum", "Name",
    "CompletionCauseID", "CompletionCauseDesc",
    "PostponementReasonID", "PostponementReasonDesc",
    "KnessetInvolvementID", "KnessetInvolvementDesc",
    "CommitteeID",
    "PublicationSeriesID", "PublicationSeriesDesc",
    "MagazineNumber", "PageNumber", "PublicationDate",
    "MajorAuthorizingLawID",
    "CommitteeReceivedDate", "CommitteeApprovalDate",
    "ApprovalDateWithoutDiscussion",
    "IsAmmendingLawOriginal",
    "ClassificationID", "ClassificationDesc",
    "IsEmergency",
    "SecretaryReceivedDate", "PlenumApprovalDate",
    "TypeID", "TypeDesc",
    "StatusID", "StatusName",
    "IsCurrent",
    "LastUpdatedDate",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS secondary_law_raw (
            Id INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name TEXT,
            CompletionCauseID INTEGER,
            CompletionCauseDesc TEXT,
            PostponementReasonID INTEGER,
            PostponementReasonDesc TEXT,
            KnessetInvolvementID INTEGER,
            KnessetInvolvementDesc TEXT,
            CommitteeID INTEGER,
            PublicationSeriesID INTEGER,
            PublicationSeriesDesc TEXT,
            MagazineNumber TEXT,
            PageNumber TEXT,
            PublicationDate TEXT,
            MajorAuthorizingLawID INTEGER,
            CommitteeReceivedDate TEXT,
            CommitteeApprovalDate TEXT,
            ApprovalDateWithoutDiscussion TEXT,
            IsAmmendingLawOriginal INTEGER,
            ClassificationID INTEGER,
            ClassificationDesc TEXT,
            IsEmergency INTEGER,
            SecretaryReceivedDate TEXT,
            PlenumApprovalDate TEXT,
            TypeID INTEGER,
            TypeDesc TEXT,
            StatusID INTEGER,
            StatusName TEXT,
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
    cols_with_fetch = _COLS + ["fetched_at"]
    placeholders = ", ".join(f"%({c})s" for c in cols_with_fetch)
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols_with_fetch if c != "Id")
    sql = (
        f"INSERT INTO {TABLE_NAME} ({', '.join(cols_with_fetch)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (Id) DO UPDATE SET {updates}"
    )
    payload = []
    max_updated: Optional[str] = None
    _BOOL_COLS = {"IsAmmendingLawOriginal", "IsEmergency", "IsCurrent"}
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        record = {"fetched_at": now}
        for c in _COLS:
            v = row.get(c)
            if c in _BOOL_COLS:
                record[c] = 1 if v else 0
            else:
                record[c] = v if v else None
        record["LastUpdatedDate"] = last_updated
        record["Id"] = row.get("Id")
        payload.append(record)
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = fetch_odata_table(table=ODATA_NAME, since=since)
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
