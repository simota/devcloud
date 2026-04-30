#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${LOOP_DIR}/state.env"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
JSON_LOG="${LOOP_DIR}/runner.jsonl"
RUNNER_LOG="${LOOP_DIR}/runner.log"

atomic_write_state() {
  local tmp
  tmp="$(mktemp "${STATE_FILE}.XXXXXX")"
  cat > "${tmp}" <<EOF
CONTRACT_VERSION=1.0.0
NEXT_ITERATION=1
LAST_STATUS=READY
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VERIFY_STAGE=foundation
DONE_VERIFY_STAGE=full
CIRCUIT_STATE=CLOSED
CIRCUIT_FAIL_COUNT=0
EOF
  mv "${tmp}" "${STATE_FILE}"
}

if [[ ! -f "${STATE_FILE}" ]]; then
  atomic_write_state
fi

if [[ ! -f "${PROGRESS_FILE}" ]]; then
  cat > "${PROGRESS_FILE}" <<EOF
# Dashboard Design Renewal Autoloop Progress

- Bootstrapped at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
- Goal: scripts/dashboard-design-renewal-autoloop/goal.md

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Dashboard design renewal loop is ready.
EOF
fi

: > "${JSON_LOG}"
: > "${RUNNER_LOG}"

bash -n "${LOOP_DIR}/run-loop.sh"
bash -n "${LOOP_DIR}/verify.sh"
bash -n "${LOOP_DIR}/recover.sh"

echo "[OK] Dashboard design renewal autoloop bootstrapped"
