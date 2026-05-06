#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${LOOP_DIR}/state.env"
PROGRESS_FILE="${LOOP_DIR}/progress.md"

if [[ ! -f "${LOOP_DIR}/goal.md" ]]; then
  echo "[FAIL] Missing ${LOOP_DIR}/goal.md" >&2
  exit 1
fi

if [[ ! -f "${PROGRESS_FILE}" ]]; then
  tmp_progress="$(mktemp "${PROGRESS_FILE}.XXXXXX")"
  {
    echo "# S3 Test Coverage Autoloop Progress"
    echo ""
    echo "Created at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo ""
    echo "NEXUS_LOOP_STATUS: READY"
    echo "NEXUS_LOOP_SUMMARY: S3 test coverage loop initialized."
  } > "${tmp_progress}"
  mv "${tmp_progress}" "${PROGRESS_FILE}"
fi

tmp_state="$(mktemp "${STATE_FILE}.XXXXXX")"
cat > "${tmp_state}" <<EOF
CONTRACT_VERSION=1.1.0
NEXT_ITERATION=1
LAST_STATUS=READY
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TOTAL_TOKENS=0
TOTAL_API_CALLS=0
ESTIMATED_COST_USD=0
ITER_TOKENS=0
ITER_API_CALLS=0
VERIFY_STAGE=foundation
DONE_VERIFY_STAGE=full-test-coverage
CIRCUIT_STATE=CLOSED
CIRCUIT_FAIL_COUNT=0
EOF
mv "${tmp_state}" "${STATE_FILE}"
shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"

bash -n "${LOOP_DIR}/run-loop.sh"
bash -n "${LOOP_DIR}/recover.sh"
bash -n "${LOOP_DIR}/verify.sh"

echo "[OK] S3 test coverage autoloop bootstrap complete"
echo "Run: bash scripts/s3-test-coverage-autoloop/run-loop.sh"
