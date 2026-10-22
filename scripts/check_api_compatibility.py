#!/usr/bin/env python3
"""Run read-only Knesset contract checks, or explicitly refresh the baseline.

    python scripts/check_api_compatibility.py --proxy
    python scripts/check_api_compatibility.py --proxy --update-contract
"""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import ISRAEL_PROXY_URL
from core import api_compatibility, odata_client
from core.db import connect_db


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proxy", action="store_true", help="Use ISRAEL_PROXY_URL")
    parser.add_argument("--update-contract", action="store_true",
                        help="Record current implementation and upstream schema; review the diff")
    args = parser.parse_args()
    if args.proxy:
        if not ISRAEL_PROXY_URL:
            parser.error("--proxy requires ISRAEL_PROXY_URL")
        odata_client.enable_proxy(ISRAEL_PROXY_URL)
    if args.update_contract:
        contract = api_compatibility.refresh_contract()
        print(f"Recorded {len(contract['entities'])} importers in {api_compatibility.CONTRACT_PATH}")
        print("Review the contract diff; recording a baseline does not prove compatibility.")
        return 0
    conn = connect_db()
    try:
        report = api_compatibility.latest_report(conn, force=True)
        api_compatibility.print_report(report)
        return int(any(row["status"] in ("FAIL", "ERROR") for row in report["rows"]))
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
