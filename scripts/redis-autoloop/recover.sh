#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
STATE_FILE="${LOOP_DIR}/state.env"

echo "=== Redis Autoloop Recovery ==="

if [[ -f "${LOOP_DIR}/.run-loop.lock" ]]; then
  LOCK_PID="$(cat "${LOOP_DIR}/.run-loop.lock" 2>/dev/null || true)"
  if [[ -n "${LOCK_PID}" ]] && kill -0 "${LOCK_PID}" 2>/dev/null; then
    echo "[BLOCKED] Active runner lock: PID ${LOCK_PID}"
    exit 1
  fi
  rm -f "${LOOP_DIR}/.run-loop.lock"
  echo "[OK] Removed stale runner lock"
fi

LATEST_ITER="$(grep -oE 'Iteration [0-9]+' "${PROGRESS_FILE}" 2>/dev/null | awk '{print $2}' | tail -1 || true)"
if [[ -z "${LATEST_ITER}" ]]; then
  LATEST_ITER=0
fi
NEXT_ITER=$((LATEST_ITER + 1))

TAIL_CONTENT="$(tail -40 "${PROGRESS_FILE}" 2>/dev/null || true)"
if echo "${TAIL_CONTENT}" | grep -q 'NEXUS_LOOP_STATUS: DONE'; then
  STATUS="DONE"
else
  STATUS="CONTINUE"
fi

TMP_STATE="$(mktemp "${STATE_FILE}.XXXXXX")"
cat > "${TMP_STATE}" <<EOF
CONTRACT_VERSION=1.1.0
NEXT_ITERATION=${NEXT_ITER}
LAST_STATUS=${STATUS}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
RECOVERED_FROM=progress_evidence
TOTAL_TOKENS=0
TOTAL_API_CALLS=0
ESTIMATED_COST_USD=0
ITER_TOKENS=0
ITER_API_CALLS=0
VERIFY_STAGE=foundation
DONE_VERIFY_STAGE=full
CIRCUIT_STATE=CLOSED
CIRCUIT_FAIL_COUNT=0
EOF
mv "${TMP_STATE}" "${STATE_FILE}"
shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"

{
  echo ""
  echo "## Recovery - $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Latest iteration found: ${LATEST_ITER}"
  echo "- Rebuilt state.env with NEXT_ITERATION=${NEXT_ITER}"
  echo "- Cleared circuit state"
  echo "- Decision: ${STATUS}"
  echo ""
  echo "NEXUS_LOOP_STATUS: ${STATUS}"
  echo "NEXUS_LOOP_SUMMARY: Recovery completed; loop can resume from iteration ${NEXT_ITER}."
} >> "${PROGRESS_FILE}"

rm -f "${LOOP_DIR}/.circuit-state"
echo "[OK] Recovery complete; next iteration ${NEXT_ITER}"
