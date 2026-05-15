#!/usr/bin/env bash
# Initialize loop artifacts for redshift-translator-autoloop.
# Idempotent: safe to re-run; preserves existing progress.md and state.env unless --reset is given.
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${LOOP_DIR}/../.." && pwd)"
GOAL_FILE="${LOOP_DIR}/goal.md"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
STATE_FILE="${LOOP_DIR}/state.env"
RUNNER_LOG="${LOOP_DIR}/runner.log"
JSON_LOG="${LOOP_DIR}/runner.jsonl"
LOCK_FILE="${LOOP_DIR}/.run-loop.lock"
CIRCUIT_FILE="${LOOP_DIR}/.circuit-state"
CHECKLIST_FILE="${ROOT_DIR}/internal/services/redshift/translator/COMPATIBILITY.md"

RESET=false
for arg in "$@"; do
  case "${arg}" in
    --reset) RESET=true ;;
    -h|--help)
      cat <<EOF
Usage: bash $(basename "${BASH_SOURCE[0]}") [--reset]

  --reset    Wipe progress.md, state.env, runner.log, runner.jsonl, .circuit-state, .run-loop.lock
EOF
      exit 0
      ;;
    *) echo "unknown arg: ${arg}" >&2; exit 2 ;;
  esac
done

log() {
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$1" | tee -a "${RUNNER_LOG}"
}

if ! [[ -f "${GOAL_FILE}" ]]; then
  echo "missing goal.md: ${GOAL_FILE}" >&2
  exit 1
fi
if ! [[ -f "${CHECKLIST_FILE}" ]]; then
  echo "missing checklist: ${CHECKLIST_FILE}" >&2
  exit 1
fi
if ! command -v go >/dev/null 2>&1; then
  echo "go toolchain not found in PATH" >&2
  exit 1
fi
if ! command -v "${CODEX_BIN:-codex}" >/dev/null 2>&1; then
  echo "codex CLI not found (set CODEX_BIN env or install codex)" >&2
  exit 1
fi

if "${RESET}"; then
  rm -f "${PROGRESS_FILE}" "${STATE_FILE}" "${RUNNER_LOG}" "${JSON_LOG}" \
        "${CIRCUIT_FILE}" "${LOCK_FILE}"
fi

if ! [[ -f "${PROGRESS_FILE}" ]]; then
  cat > "${PROGRESS_FILE}" <<EOF
# Progress — redshift-translator-autoloop

| iter | status | item |
|------|--------|------|
EOF
fi

if ! [[ -f "${STATE_FILE}" ]]; then
  tmp="${STATE_FILE}.tmp.$$"
  cat > "${tmp}" <<EOF
NEXT_ITERATION=1
LAST_STATUS=INIT
LAST_ITEM=
EOF
  mv "${tmp}" "${STATE_FILE}"
fi

touch "${RUNNER_LOG}" "${JSON_LOG}"

log "bootstrap complete (reset=${RESET})"
log "checklist=${CHECKLIST_FILE}"
log "remaining R-only/R≠P unchecked: $(grep -cE '^- \[ \] \*\*(R-only|R≠P)\*\*' "${CHECKLIST_FILE}" || true)"
