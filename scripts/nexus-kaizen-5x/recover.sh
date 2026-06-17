#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${LOOP_DIR}/state.env"
LOCK_FILE="${LOOP_DIR}/.run-loop.lock"

usage() {
  cat <<'EOF'
Usage:
  recover.sh --clear-lock
  recover.sh --rebuild-state
  recover.sh --set-next <iteration>
EOF
}

atomic_write_state() {
  local next_iteration="$1"
  local status="$2"
  local tmp
  tmp="$(mktemp "${STATE_FILE}.XXXXXX")"
  cat > "${tmp}" <<EOF
CONTRACT_VERSION=1.2.0
NEXT_ITERATION=${next_iteration}
LAST_STATUS=${status}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
MAX_ITERATIONS=${MAX_ITERATIONS:-5}
ITER_TIMEOUT=${ITER_TIMEOUT:-1200}
LOOP_TIMEOUT=${LOOP_TIMEOUT:-7200}
TOTAL_TOKENS=0
TOTAL_API_CALLS=0
ESTIMATED_COST_USD=0
EOF
  mv "${tmp}" "${STATE_FILE}"
  shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"
}

case "${1:-}" in
  --clear-lock)
    rm -f "${LOCK_FILE}"
    ;;
  --rebuild-state)
    atomic_write_state "1" "READY"
    ;;
  --set-next)
    if [[ -z "${2:-}" ]]; then
      usage
      exit 1
    fi
    atomic_write_state "$2" "READY"
    ;;
  *)
    usage
    exit 1
    ;;
esac

printf 'NEXUS_LOOP_STATUS: READY\n'
printf 'NEXUS_LOOP_SUMMARY: recovery action completed\n'
