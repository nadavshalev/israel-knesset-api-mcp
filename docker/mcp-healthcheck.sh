#!/usr/bin/env sh
set -eu

PORT="${MCP_PORT:-8000}"
ENDPOINT="${MCP_ENDPOINT:-/mcp}"
URL="http://127.0.0.1:${PORT}${ENDPOINT}"

python -c "
import sys
import urllib.error
import urllib.request

req = urllib.request.Request(
    '${URL}',
    data=b'{}',
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/event-stream',
    },
    method='POST',
)

try:
    urllib.request.urlopen(req, timeout=5)
    sys.exit(0)
except urllib.error.HTTPError as e:
    sys.exit(0 if e.code in (200, 400) else 1)
"
