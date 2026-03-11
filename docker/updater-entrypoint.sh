#!/usr/bin/env sh
set -eu

CYCLE_DAYS="${UPDATE_CYCLE_DAYS:-1}"
UPDATE_HOUR="${UPDATE_HOUR_IN_DAY:-3}"
RUN_ON_START="${UPDATE_RUN_ON_START:-true}"

if ! echo "${CYCLE_DAYS}" | grep -Eq '^[0-9]+$'; then
  echo "ERROR: UPDATE_CYCLE_DAYS must be a positive integer, got '${CYCLE_DAYS}'"
  exit 1
fi

if [ "${CYCLE_DAYS}" -lt 1 ]; then
  echo "ERROR: UPDATE_CYCLE_DAYS must be >= 1, got '${CYCLE_DAYS}'"
  exit 1
fi

if ! echo "${UPDATE_HOUR}" | grep -Eq '^[0-9]+$'; then
  echo "ERROR: UPDATE_HOUR_IN_DAY must be an integer in [0,23], got '${UPDATE_HOUR}'"
  exit 1
fi

if [ "${UPDATE_HOUR}" -lt 0 ] || [ "${UPDATE_HOUR}" -gt 23 ]; then
  echo "ERROR: UPDATE_HOUR_IN_DAY must be in [0,23], got '${UPDATE_HOUR}'"
  exit 1
fi

SCHEDULE_SECONDS=$((CYCLE_DAYS * 24 * 60 * 60))

compute_next_run_epoch() {
  now_epoch="$(date +%s)"
  target_today_epoch="$(date -d "$(date +%F) ${UPDATE_HOUR}:00:00" +%s)"

  if [ "${now_epoch}" -lt "${target_today_epoch}" ]; then
    echo "${target_today_epoch}"
    return
  fi

  elapsed_since_anchor=$((now_epoch - target_today_epoch))
  intervals_passed=$((elapsed_since_anchor / SCHEDULE_SECONDS + 1))
  echo $((target_today_epoch + intervals_passed * SCHEDULE_SECONDS))
}

echo "Updater schedule: every ${CYCLE_DAYS} day(s) at ${UPDATE_HOUR}:00 (container local time)"

run_update() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Running update_all.py"
  if ! python ./scripts/update_all.py; then
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

next_run_epoch="$(compute_next_run_epoch)"

while true; do
  now_epoch="$(date +%s)"
  sleep_seconds=$((next_run_epoch - now_epoch))
  if [ "${sleep_seconds}" -gt 0 ]; then
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Next scheduled run at $(date -d "@${next_run_epoch}" +'%Y-%m-%d %H:%M:%S %Z')"
    sleep "${sleep_seconds}"
  fi

  run_update

  next_run_epoch=$((next_run_epoch + SCHEDULE_SECONDS))
  now_epoch="$(date +%s)"
  while [ "${next_run_epoch}" -le "${now_epoch}" ]; do
    next_run_epoch=$((next_run_epoch + SCHEDULE_SECONDS))
  done
done
