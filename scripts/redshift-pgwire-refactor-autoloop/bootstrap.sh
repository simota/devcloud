#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOOP_DIR="${ROOT_DIR}/scripts/redshift-pgwire-refactor-autoloop"
STATE_FILE="${LOOP_DIR}/state.env"
PROGRESS_FILE="${LOOP_DIR}/progress.md"

mkdir -p "${LOOP_DIR}"
cd "${ROOT_DIR}"

if [[ ! -f "${PROGRESS_FILE}" ]]; then
  cat > "${PROGRESS_FILE}" <<EOF
# Redshift PG Wire Refactor Autoloop Progress

Created at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Redshift pgwire refactor loop initialized.
EOF
fi

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
shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"

bash -n "${LOOP_DIR}/bootstrap.sh"
bash -n "${LOOP_DIR}/recover.sh"
bash -n "${LOOP_DIR}/run-loop.sh"
bash -n "${LOOP_DIR}/verify.sh"

echo "[OK] Redshift pgwire refactor autoloop bootstrap complete"
echo "Run: bash scripts/redshift-pgwire-refactor-autoloop/run-loop.sh"
