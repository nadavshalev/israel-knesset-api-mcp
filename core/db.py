import atexit
from typing import Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool

from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# ---------------------------------------------------------------------------
# Connection pool (thread-safe, lazily initialised)
# ---------------------------------------------------------------------------

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Return the global connection pool, creating it on first call."""
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        atexit.register(_shutdown_pool)
    return _pool


def _shutdown_pool() -> None:
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        _pool = None


# ---------------------------------------------------------------------------
# Connection wrapper — delegates everything to the real psycopg2 connection
# but overrides .close() to return the connection to the pool.
# ---------------------------------------------------------------------------


class _PooledConnection:
    """Thin wrapper around a psycopg2 connection.

    Delegates all attribute access to the underlying connection object,
    but overrides ``close()`` to return the connection to the pool
    instead of destroying it.
    """

    __slots__ = ("_conn", "_pool")

    def __init__(self, conn, pool):
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_pool", pool)

    def close(self) -> None:
        """Return the connection to the pool instead of closing it."""
        conn = object.__getattribute__(self, "_conn")
        pool = object.__getattribute__(self, "_pool")
        try:
            conn.rollback()
            conn.cursor_factory = None
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("RESET ALL")
            cur.close()
            conn.autocommit = False
        except Exception:
            pass
        pool.putconn(conn)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_conn"), name)

    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, "_conn"), name, value)


# ---------------------------------------------------------------------------
# Public connection helpers
# ---------------------------------------------------------------------------


def connect_db():
    """Get a read-write connection from the pool.

    The caller MUST call ``conn.close()`` when finished — this returns the
    connection to the pool rather than destroying it.
    """
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = False
    return _PooledConnection(conn, pool)


def connect_readonly():
    """Get a read-only connection from the pool.

    Identical to ``connect_db()`` but sets the transaction to read-only mode
    and uses ``RealDictCursor`` so rows are returned as dicts.
    The settings are automatically reset when the connection is returned
    to the pool via ``conn.close()``.
    """
    conn = connect_db()
    conn.set_session(readonly=True)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


def return_conn(conn) -> None:
    """Return a connection to the pool (alternative to conn.close())."""
    conn.close()


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def ensure_indexes(conn) -> None:
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
    # Name indexes for search_across
    cur.execute("CREATE INDEX IF NOT EXISTS idx_person_lastname ON person_raw (LastName)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vote_title ON plenum_vote_raw (VoteTitle)")
    cur.close()
    conn.commit()


def update_metadata(
    conn,
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
        VALUES (%s, %s, %s)
        ON CONFLICT(table_name) DO UPDATE SET
            last_sync_completed_at=EXCLUDED.last_sync_completed_at,
            last_updated_cutoff=EXCLUDED.last_updated_cutoff
        """,
        (table_name, last_sync_completed_at, last_updated_cutoff),
    )
    cur.close()
    conn.commit()
