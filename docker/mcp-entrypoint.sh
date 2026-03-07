#!/usr/bin/env sh
set -eu

HOST="${MCP_HOST:-0.0.0.0}"
PORT="${MCP_PORT:-8000}"
ENDPOINT="${MCP_ENDPOINT:-/mcp}"

echo "Starting MCP server on ${HOST}:${PORT}${ENDPOINT}"
exec uvicorn mcp_server:app --host "${HOST}" --port "${PORT}"

