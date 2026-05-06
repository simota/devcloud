#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOOP_DIR="${ROOT_DIR}/scripts/sqs-server-refactor-autoloop"
STATE_FILE="${LOOP_DIR}/state.env"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
LOCK_FILE="${LOOP_DIR}/.run-loop.lock"

cd "${ROOT_DIR}"
rm -f "${LOCK_FILE}"

last_iteration=0
if [[ -f "${PROGRESS_FILE}" ]]; then
  last_iteration="$(env -u RIPGREP_CONFIG_PATH rg -o '## Iteration [0-9]+' "${PROGRESS_FILE}" | awk '{print $3}' | sort -n | tail -1 || true)"
fi
last_iteration="${last_iteration:-0}"
next_iteration=$((last_iteration + 1))

last_status="READY"
if [[ "${last_iteration}" -gt 0 && -f "${PROGRESS_FILE}" ]]; then
  last_status="$(awk '/NEXUS_LOOP_STATUS:/ {status=$2} END {print status}' "${PROGRESS_FILE}")"
  last_status="${last_status:-CONTINUE}"
fi

tmp="$(mktemp "${STATE_FILE}.XXXXXX")"
cat > "${tmp}" <<EOF
CONTRACT_VERSION=1.0.0
NEXT_ITERATION=${next_iteration}
LAST_STATUS=${last_status}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VERIFY_STAGE=foundation
DONE_VERIFY_STAGE=full
CIRCUIT_STATE=CLOSED
CIRCUIT_FAIL_COUNT=0
EOF
mv "${tmp}" "${STATE_FILE}"
shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"

{
  echo ""
  echo "## Recovery - $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Rebuilt state from progress evidence."
  echo "- NEXT_ITERATION=${next_iteration}"
  echo "- LAST_STATUS=${last_status}"
} >> "${PROGRESS_FILE}"

echo "[OK] recovered ${STATE_FILE}"
