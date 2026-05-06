#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOOP_DIR="${ROOT_DIR}/scripts/redshift-pgwire-refactor-autoloop"
VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PGWIRE_LINE_LIMIT="${PGWIRE_LINE_LIMIT:-700}"

cd "${ROOT_DIR}"
export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redshift-pgwire-refactor-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redshift-pgwire-refactor-verify.err"

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -80
    FAIL=$((FAIL + 1))
  fi
}

has_text() {
  local path="$1"
  local pattern="$2"
  env -u RIPGREP_CONFIG_PATH rg -q "${pattern}" "${path}"
}

script_contract() {
  bash -n "${LOOP_DIR}/bootstrap.sh" &&
    bash -n "${LOOP_DIR}/recover.sh" &&
    bash -n "${LOOP_DIR}/run-loop.sh" &&
    bash -n "${LOOP_DIR}/verify.sh" &&
    has_text "${LOOP_DIR}/run-loop.sh" 'NEXUS_LOOP_STATUS' &&
    has_text "${LOOP_DIR}/run-loop.sh" 'NEXUS_LOOP_SUMMARY' &&
    has_text "${LOOP_DIR}/run-loop.sh" 'mktemp .*STATE_FILE' &&
    has_text "${LOOP_DIR}/run-loop.sh" 'CIRCUIT_THRESHOLD'
}

goal_contract() {
  has_text "${LOOP_DIR}/goal.md" 'behavior-preserving' &&
    has_text "${LOOP_DIR}/goal.md" 'pgwire.go.*700 lines or fewer' &&
    has_text "${LOOP_DIR}/goal.md" 'AC-009'
}

package_tests() {
  go test ./internal/services/redshift
}

repository_tests() {
  go test ./...
}

gofmt_clean() {
  local files
  files="$(gofmt -l internal/services/redshift/*.go internal/services/redshift/**/*.go 2>/dev/null || true)"
  [[ -z "${files}" ]]
}

pgwire_line_budget() {
  local lines
  lines="$(wc -l < internal/services/redshift/pgwire.go | tr -d ' ')"
  echo "pgwire.go lines=${lines} limit=${PGWIRE_LINE_LIMIT}" >&2
  [[ "${lines}" -le "${PGWIRE_LINE_LIMIT}" ]]
}

focused_file_layout() {
  local count
  count="$(find internal/services/redshift -maxdepth 1 -type f \( -name 'pgwire*.go' -o -name 'sql_*.go' -o -name 'catalog.go' \) \
    ! -name 'pgwire.go' ! -name '*_test.go' | wc -l | tr -d ' ')"
  echo "focused pgwire/sql files=${count}" >&2
  [[ "${count}" -ge 6 ]]
}

package_name_consistent() {
  for file in internal/services/redshift/*.go; do
    awk 'NR == 1 && $0 != "package redshift" { exit 1 }' "${file}" || return 1
  done
}

existing_redshift_full_gate() {
  VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh
}

echo "=== Redshift pgwire refactor verification: ${VERIFY_STAGE} ==="

case "${VERIFY_STAGE}" in
  foundation)
    run_check "Redshift pgwire refactor goal contract" goal_contract
    run_check "Redshift pgwire refactor script contract" script_contract
    run_check "Redshift package tests pass" package_tests
    run_check "gofmt is clean for Redshift package" gofmt_clean
    ;;
  shape)
    run_check "pgwire.go line budget met" pgwire_line_budget
    run_check "focused Redshift pgwire/sql source files exist" focused_file_layout
    run_check "Redshift package name remains consistent" package_name_consistent
    ;;
  redshift-full)
    run_check "Redshift full compatibility gate passes" existing_redshift_full_gate
    ;;
  full)
    run_check "Redshift pgwire refactor goal contract" goal_contract
    run_check "Redshift pgwire refactor script contract" script_contract
    run_check "pgwire.go line budget met" pgwire_line_budget
    run_check "focused Redshift pgwire/sql source files exist" focused_file_layout
    run_check "Redshift package name remains consistent" package_name_consistent
    run_check "gofmt is clean for Redshift package" gofmt_clean
    run_check "Redshift package tests pass" package_tests
    run_check "repository tests pass" repository_tests
    run_check "Redshift full compatibility gate passes" existing_redshift_full_gate
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE: ${VERIFY_STAGE}"
    FAIL=$((FAIL + 1))
    ;;
esac

echo "=== Redshift pgwire refactor verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
