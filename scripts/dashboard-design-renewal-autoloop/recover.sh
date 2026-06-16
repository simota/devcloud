#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
STATE_FILE="${LOOP_DIR}/state.env"
LOCK_FILE="${LOOP_DIR}/.run-loop.lock"
CIRCUIT_FILE="${LOOP_DIR}/.circuit-state"

echo "=== Dashboard Design Renewal Autoloop Recovery ==="

if [[ -f "${LOCK_FILE}" ]]; then
  lock_pid="$(cat "${LOCK_FILE}" 2>/dev/null || true)"
  if [[ -n "${lock_pid}" ]] && kill -0 "${lock_pid}" 2>/dev/null; then
    echo "[BLOCKED] Active runner lock: PID ${lock_pid}"
    exit 1
  fi
  rm -f "${LOCK_FILE}"
  echo "[OK] Removed stale runner lock"
fi

latest_iter="$(grep -oE 'Iteration [0-9]+' "${PROGRESS_FILE}" 2>/dev/null | awk '{print $2}' | tail -1 || true)"
if [[ -z "${latest_iter}" ]]; then
  latest_iter=0
fi
next_iter=$((latest_iter + 1))

tail_content="$(tail -40 "${PROGRESS_FILE}" 2>/dev/null || true)"
if echo "${tail_content}" | grep -q 'NEXUS_LOOP_STATUS: DONE'; then
  status="DONE"
else
  status="CONTINUE"
fi

tmp_state="$(mktemp "${STATE_FILE}.XXXXXX")"
cat > "${tmp_state}" <<EOF
CONTRACT_VERSION=1.0.0
NEXT_ITERATION=${next_iter}
LAST_STATUS=${status}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
RECOVERED_FROM=progress_evidence
VERIFY_STAGE=foundation
DONE_VERIFY_STAGE=full
CIRCUIT_STATE=CLOSED
CIRCUIT_FAIL_COUNT=0
EOF
mv "${tmp_state}" "${STATE_FILE}"

{
  echo ""
  echo "## Recovery — $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Latest iteration found: ${latest_iter}"
  echo "- Rebuilt state.env with NEXT_ITERATION=${next_iter}"
  echo "- Cleared circuit state"
  echo "- Decision: CONTINUE"
  echo ""
  echo "NEXUS_LOOP_STATUS: CONTINUE"
  echo "NEXUS_LOOP_SUMMARY: Recovery completed; loop can resume from iteration ${next_iter}."
} >> "${PROGRESS_FILE}"

rm -f "${CIRCUIT_FILE}"
echo "[OK] Recovery complete; next iteration ${next_iter}"
