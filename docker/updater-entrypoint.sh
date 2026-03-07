#!/usr/bin/env sh
set -eu

CYCLE_MINUTES="${UPDATE_CYCLE_MINUTES:-30}"
RUN_ON_START="${UPDATE_RUN_ON_START:-true}"

if ! echo "${CYCLE_MINUTES}" | grep -Eq '^[0-9]+$'; then
  echo "ERROR: UPDATE_CYCLE_MINUTES must be a positive integer, got '${CYCLE_MINUTES}'"
  exit 1
fi

if [ "${CYCLE_MINUTES}" -lt 1 ]; then
  echo "ERROR: UPDATE_CYCLE_MINUTES must be >= 1, got '${CYCLE_MINUTES}'"
  exit 1
fi

SLEEP_SECONDS=$((CYCLE_MINUTES * 60))

echo "Updater cycle is every ${CYCLE_MINUTES} minute(s)"

run_update() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Running update_all.py"
  if ! python update_all.py; then
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] update_all.py failed (will retry next cycle)"
  fi
}

case "${RUN_ON_START}" in
  true|TRUE|1|yes|YES)
    run_update
    ;;
  *)
    echo "Skipping initial run (UPDATE_RUN_ON_START=${RUN_ON_START})"
    ;;
esac

while true; do
  sleep "${SLEEP_SECONDS}"
  run_update
done

