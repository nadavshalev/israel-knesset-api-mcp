import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()
host = os.getenv('POSTGRES_PATH')
port = os.getenv('POSTGRES_PORT', '5432')
db   = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
pw   = os.getenv('POSTGRES_PASSWORD')
print(f'Connecting to {host}:{port}, db={db}, user={user} ...')
try:
    conn = psycopg2.connect(
        host=host,
        port=int(port),
        dbname=db,
        user=user,
        password=pw,
        connect_timeout=10
    )
    cur = conn.cursor()
    cur.execute('SELECT version();')
    version = cur.fetchone()[0]
    print(f'Connection successful!')
    print(f'PostgreSQL version: {version}')
    cur.close()
    conn.close()
except Exception as e:
    print(f'Connection failed: {e}')