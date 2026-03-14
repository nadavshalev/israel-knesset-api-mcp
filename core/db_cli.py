import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path when run as a script
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import connect_db
from tables import get_table_spec, get_table_specs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local Knesset data tooling")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create database schema if missing")
    fetch = sub.add_parser("fetch", help="Fetch one or more tables")
    fetch.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help="Fetch only specific table label(s). "
             "Valid names: " + ", ".join(spec.label for spec in get_table_specs()),
    )
    fetch.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    return parser.parse_args()


def ensure_tables(conn) -> None:
    for spec in get_table_specs():
        create_fn = getattr(spec.module, "create_table", None)
        if callable(create_fn):
            create_fn(conn)


def main() -> None:
    args = parse_args()
    conn = connect_db()

    if args.command == "init-db":
        ensure_tables(conn)
        print("Initialized schema in PostgreSQL database")
        return

    if args.command == "fetch":
        ensure_tables(conn)
        try:
            selected = [get_table_spec(name) for name in args.tables] if args.tables else list(get_table_specs())
        except KeyError as exc:
            print(f"ERROR: {exc.args[0]}")
            sys.exit(1)
        for spec in selected:
            print()
            print(f"=== {spec.label} ===")
            print(f"Fetching {spec.label}...")
            spec.module.fetch_rows(conn, since=args.since)
        return


if __name__ == "__main__":
    main()
