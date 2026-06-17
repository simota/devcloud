#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${LOOP_DIR}/../.." && pwd)"
cd "${ROOT_DIR}"

PASS=0
FAIL=0

check() {
  local name="$1"
  shift
  if "$@"; then
    printf '[PASS] %s\n' "${name}"
    PASS=$((PASS + 1))
  else
    printf '[FAIL] %s\n' "${name}"
    FAIL=$((FAIL + 1))
  fi
}

contains() {
  local file="$1"
  local pattern="$2"
  grep -Fq -- "${pattern}" "${file}"
}

not_contains() {
  local file="$1"
  local pattern="$2"
  ! grep -Fq -- "${pattern}" "${file}"
}

check "run-loop syntax" bash -n "${LOOP_DIR}/run-loop.sh"
check "verify syntax" bash -n "${LOOP_DIR}/verify.sh"
check "recover syntax" bash -n "${LOOP_DIR}/recover.sh"
check "default max iterations is five" contains "${LOOP_DIR}/run-loop.sh" 'MAX_ITERATIONS="${MAX_ITERATIONS:-5}"'
check "loop timeout is externally enforced" contains "${LOOP_DIR}/run-loop.sh" 'LOOP_TIMEOUT="${LOOP_TIMEOUT:-7200}"'
check "codex exec is used" contains "${LOOP_DIR}/run-loop.sh" '"${CODEX_BIN}" exec'
check "latest codex model is configured" contains "${LOOP_DIR}/run-loop.sh" 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"'
check "unsupported codex approval flag is absent" not_contains "${LOOP_DIR}/run-loop.sh" '-a "${APPROVAL_POLICY}"'
check "nexus kaizen prompt is present" contains "${LOOP_DIR}/run-loop.sh" '$nexus kaizen'
check "last message artifact is captured" contains "${LOOP_DIR}/run-loop.sh" '-o "${output_file}"'
check "state writes are atomic" contains "${LOOP_DIR}/run-loop.sh" 'mktemp "${STATE_FILE}.XXXXXX"'
check "footer status is emitted" contains "${LOOP_DIR}/run-loop.sh" 'NEXUS_LOOP_STATUS: DONE'
check "goal has acceptance criteria" contains "${LOOP_DIR}/goal.md" '## Acceptance Criteria'

printf '\nSummary: %s passed, %s failed\n' "${PASS}" "${FAIL}"
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi

if [[ "${VERIFY_ONLY:-false}" != "true" ]]; then
  printf 'NEXUS_LOOP_STATUS: READY\n'
  printf 'NEXUS_LOOP_SUMMARY: nexus-kaizen-5x script set passed static verification\n'
fi
