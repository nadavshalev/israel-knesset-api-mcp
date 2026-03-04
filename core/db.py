import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from config import DEFAULT_DB


def connect_db(path: Path = DEFAULT_DB) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes needed by views.  Idempotent (IF NOT EXISTS)."""
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_psi_itemid ON plm_session_item_raw (ItemID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_psi_session ON plm_session_item_raw (PlenumSessionID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_startdate ON plenum_session_raw (StartDate)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_knessetnum ON plenum_session_raw (KnessetNum)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bill_knessetnum ON bill_raw (KnessetNum)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pvr_vote_id ON plenum_vote_result_raw (VoteID)")
    # Committee indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_committee_knessetnum ON committee_raw (KnessetNum)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cs_committee ON committee_session_raw (CommitteeID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cs_startdate ON committee_session_raw (StartDate)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_csi_session ON cmt_session_item_raw (CommitteeSessionID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_csi_itemtype ON cmt_session_item_raw (ItemTypeID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dcs_session ON document_committee_session_raw (CommitteeSessionID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ptp_committeeid ON person_to_position_raw (CommitteeID)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bill_committeeid ON bill_raw (CommitteeID)")
    conn.commit()


def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def update_metadata(
    conn: sqlite3.Connection,
    table_name: str,
    last_sync_completed_at: str,
    last_updated_cutoff: Optional[str],
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            table_name TEXT PRIMARY KEY,
            last_sync_completed_at TEXT,
            last_updated_cutoff TEXT
        )
        """
    )

    cur.execute(
        """
        INSERT INTO metadata (table_name, last_sync_completed_at, last_updated_cutoff)
        VALUES (?, ?, ?)
        ON CONFLICT(table_name) DO UPDATE SET
            last_sync_completed_at=excluded.last_sync_completed_at,
            last_updated_cutoff=excluded.last_updated_cutoff
        """,
        (table_name, last_sync_completed_at, last_updated_cutoff),
    )
    conn.commit()
