#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MCP_SERVICE="mcp"
UPDATER_SERVICE="updater"
MCP_PORT="${MCP_PORT:-8000}"
MCP_ENDPOINT="${MCP_ENDPOINT:-/mcp}"
MCP_URL="http://127.0.0.1:${MCP_PORT}${MCP_ENDPOINT}"

fail() {
  echo "[FAIL] $1"
  exit 1
}

pass() {
  echo "[OK]   $1"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

container_id() {
  docker compose ps -q "$1"
}

container_state() {
  docker inspect --format '{{.State.Status}}' "$1"
}

container_health() {
  docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$1"
}

check_services_running() {
  local mcp_id updater_id
  mcp_id="$(container_id "$MCP_SERVICE")"
  updater_id="$(container_id "$UPDATER_SERVICE")"

  [[ -n "$mcp_id" ]] || fail "Service '$MCP_SERVICE' is not created. Run: docker compose up -d --build"
  [[ -n "$updater_id" ]] || fail "Service '$UPDATER_SERVICE' is not created. Run: docker compose up -d --build"

  [[ "$(container_state "$mcp_id")" == "running" ]] || fail "Service '$MCP_SERVICE' is not running"
  [[ "$(container_state "$updater_id")" == "running" ]] || fail "Service '$UPDATER_SERVICE' is not running"

  pass "Containers are running"

  local health
  health="$(container_health "$mcp_id")"
  [[ "$health" == "healthy" ]] || fail "MCP container health is '$health' (expected: healthy)"
  pass "MCP container health is healthy"
}

check_mcp_http() {
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    --data '{}' \
    "$MCP_URL" || true)"
  [[ "$code" == "200" || "$code" == "400" ]] || fail "MCP endpoint probe failed at $MCP_URL (HTTP $code)"
  pass "MCP endpoint is reachable at $MCP_URL (HTTP $code)"
}

check_updater_logs() {
  local logs
  logs="$(docker compose logs --no-color --tail=200 "$UPDATER_SERVICE" 2>&1 || true)"

  echo "$logs" | rg -q "Updater schedule:" || fail "Updater did not print schedule configuration"
  echo "$logs" | rg -q "Running update_all.py" || fail "Updater has not attempted update_all.py yet"

  if echo "$logs" | rg -q "ERROR: UPDATE_CYCLE_DAYS|ERROR: UPDATE_HOUR_IN_DAY"; then
    fail "Updater has invalid scheduler configuration (UPDATE_CYCLE_DAYS/UPDATE_HOUR_IN_DAY)"
  fi

  pass "Updater loop is active and update_all.py was triggered"
}

check_app_query_inside_mcp() {
  docker compose exec -T "$MCP_SERVICE" python local_query.py status >/dev/null \
    || fail "MCP container cannot run local_query.py status"
  pass "Application query works inside MCP container"
}

main() {
  require_cmd docker
  require_cmd curl
  require_cmd rg

  check_services_running
  check_mcp_http
  check_updater_logs
  check_app_query_inside_mcp

  echo
  echo "Smoke check passed: stack is up and basic functionality works."
}

main "$@"
