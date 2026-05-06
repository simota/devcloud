#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${LOOP_DIR}/state.env"

atomic_write_state() {
  local tmp
  tmp="$(mktemp "${STATE_FILE}.XXXXXX")"
  cat > "${tmp}" <<EOF
CONTRACT_VERSION=1.0.0
NEXT_ITERATION=1
LAST_STATUS=READY
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VERIFY_STAGE=foundation
DONE_VERIFY_STAGE=full-sdk-compat
CIRCUIT_STATE=CLOSED
CIRCUIT_FAIL_COUNT=0
EOF
  mv "${tmp}" "${STATE_FILE}"
  shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"
}

atomic_write_state
touch "${LOOP_DIR}/runner.log" "${LOOP_DIR}/runner.jsonl"

echo "NEXUS_LOOP_STATUS: READY"
echo "NEXUS_LOOP_SUMMARY: GCS SDK compatibility autoloop bootstrapped."
