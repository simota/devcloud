#!/usr/bin/env bash
# Recover from state drift, stale lock, or open circuit.
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
STATE_FILE="${LOOP_DIR}/state.env"
LOCK_FILE="${LOOP_DIR}/.run-loop.lock"
CIRCUIT_FILE="${LOOP_DIR}/.circuit-state"
RUNNER_LOG="${LOOP_DIR}/runner.log"

action="${1:-status}"

log() { printf '[recover] %s\n' "$*"; }

case "${action}" in
  status)
    log "lock: $([[ -f "${LOCK_FILE}" ]] && cat "${LOCK_FILE}" || echo "none")"
    log "circuit: $([[ -f "${CIRCUIT_FILE}" ]] && cat "${CIRCUIT_FILE}" || echo "CLOSED|0")"
    log "state:"
    [[ -f "${STATE_FILE}" ]] && sed 's/^/    /' "${STATE_FILE}" || echo "    (missing)"
    log "last 5 progress rows:"
    [[ -f "${PROGRESS_FILE}" ]] && tail -5 "${PROGRESS_FILE}" | sed 's/^/    /' || echo "    (missing)"
    ;;
  --reset-circuit|reset-circuit)
    rm -f "${CIRCUIT_FILE}"
    log "circuit reset"
    ;;
  --clear-lock|clear-lock)
    if [[ -f "${LOCK_FILE}" ]]; then
      pid="$(cat "${LOCK_FILE}" 2>/dev/null || echo 0)"
      if kill -0 "${pid}" 2>/dev/null; then
        log "refusing to clear lock — pid ${pid} is alive"
        exit 1
      fi
      rm -f "${LOCK_FILE}"
      log "stale lock removed (pid=${pid})"
    else
      log "no lock present"
    fi
    ;;
  --rebuild-state|rebuild-state)
    if [[ ! -f "${PROGRESS_FILE}" ]]; then
      log "no progress.md — cannot rebuild"
      exit 1
    fi
    last_iter="$(grep -cE '^\| [0-9]+ \|' "${PROGRESS_FILE}" || echo 0)"
    last_row="$(tail -1 "${PROGRESS_FILE}")"
    last_status="$(printf '%s' "${last_row}" | awk -F'|' '{gsub(/ /,"",$3); print $3}')"
    next=$((last_iter + 1))
    tmp="${STATE_FILE}.tmp.$$"
    cat > "${tmp}" <<EOF
NEXT_ITERATION=${next}
LAST_STATUS=${last_status:-UNKNOWN}
LAST_ITEM=
EOF
    mv "${tmp}" "${STATE_FILE}"
    log "state rebuilt: NEXT_ITERATION=${next} LAST_STATUS=${last_status:-UNKNOWN}"
    ;;
  --full-reset|full-reset)
    log "wiping loop artifacts (keeping goal.md and scripts)"
    rm -f "${PROGRESS_FILE}" "${STATE_FILE}" "${LOCK_FILE}" "${CIRCUIT_FILE}" "${RUNNER_LOG}" "${LOOP_DIR}/runner.jsonl"
    bash "${LOOP_DIR}/bootstrap.sh"
    ;;
  -h|--help|help)
    cat <<EOF
Usage: bash $(basename "${BASH_SOURCE[0]}") [action]

Actions:
  status              show lock, circuit, state, and recent progress (default)
  reset-circuit       remove .circuit-state to allow retries
  clear-lock          remove stale .run-loop.lock if process is dead
  rebuild-state       reconstruct state.env from progress.md tail
  full-reset          wipe runtime artifacts and re-bootstrap (preserves goal.md)
EOF
    ;;
  *)
    echo "unknown action: ${action}" >&2
    exit 2
    ;;
esac
