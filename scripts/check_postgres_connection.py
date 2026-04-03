import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

print(f"Connecting to {POSTGRES_HOST}:{POSTGRES_PORT}, db={POSTGRES_DB}, user={POSTGRES_USER} ...")
try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    print("Connection successful!")
    print(f"PostgreSQL version: {version}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
