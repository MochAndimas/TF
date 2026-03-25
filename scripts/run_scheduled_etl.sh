#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
LOCK_DIR="${PROJECT_ROOT}/run"
LOCK_FILE="${LOCK_DIR}/scheduled_etl.lock"
LOG_FILE="${LOG_DIR}/scheduled_etl.log"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S %Z')"

mkdir -p "${LOG_DIR}"
mkdir -p "${LOCK_DIR}"

cd "${PROJECT_ROOT}"

if [[ -f "/.dockerenv" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="${PYTHON_BIN:-python3}"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="${PYTHON_BIN:-python}"
  else
    printf '[%s] Scheduled ETL failed: Python interpreter not found in container.\n' "${TIMESTAMP}" >> "${LOG_FILE}"
    exit 127
  fi
elif [[ -x "${PROJECT_ROOT}/venv/bin/python" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/venv/bin/python"
elif [[ -x "${PROJECT_ROOT}/bin/python" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/bin/python"
elif [[ -x "${PROJECT_ROOT}/venv/Scripts/python" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/venv/Scripts/python"
else
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="${PYTHON_BIN:-python3}"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="${PYTHON_BIN:-python}"
  else
    printf '[%s] Scheduled ETL failed: Python interpreter not found.\n' "${TIMESTAMP}" >> "${LOG_FILE}"
    exit 127
  fi
fi

cleanup_lock() {
  if [[ -n "${LOCK_FD:-}" ]]; then
    eval "exec ${LOCK_FD}>&-"
  fi
}

trap cleanup_lock EXIT

if command -v flock >/dev/null 2>&1; then
  LOCK_FD=9
  eval "exec ${LOCK_FD}>\"${LOCK_FILE}\""
  if ! flock -n "${LOCK_FD}"; then
    printf '[%s] Scheduled ETL skipped: another run is still active.\n' "${TIMESTAMP}" >> "${LOG_FILE}"
    exit 0
  fi
else
  if [[ -f "${LOCK_FILE}" ]]; then
    printf '[%s] Scheduled ETL skipped: lock file already exists at %s.\n' "${TIMESTAMP}" "${LOCK_FILE}" >> "${LOG_FILE}"
    exit 0
  fi
  printf '%s\n' "$$" > "${LOCK_FILE}"
  cleanup_lock() {
    rm -f "${LOCK_FILE}"
  }
  trap cleanup_lock EXIT
fi

{
  printf '[%s] Scheduled ETL started.\n' "${TIMESTAMP}"
  set +e
  "${PYTHON_BIN}" "${PROJECT_ROOT}/run_scheduled_etl.py" --triggered-by cron
  EXIT_CODE=$?
  set -e
  FINISHED_AT="$(date '+%Y-%m-%d %H:%M:%S %Z')"
  printf '[%s] Scheduled ETL finished with exit code %s.\n' "${FINISHED_AT}" "${EXIT_CODE}"
  exit "${EXIT_CODE}"
} >> "${LOG_FILE}" 2>&1
