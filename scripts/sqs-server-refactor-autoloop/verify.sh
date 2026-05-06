#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOOP_DIR="${ROOT_DIR}/scripts/sqs-server-refactor-autoloop"
VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
SERVER_LINE_LIMIT="${SERVER_LINE_LIMIT:-700}"
SQS_VERIFY_PORT="${SQS_VERIFY_PORT:-40024}"
GCS_VERIFY_PORT="${GCS_VERIFY_PORT:-40025}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-40026}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-40027}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-40028}"
DYNAMODB_VERIFY_PORT="${DYNAMODB_VERIFY_PORT:-40029}"
BIGQUERY_VERIFY_PORT="${BIGQUERY_VERIFY_PORT:-40030}"

cd "${ROOT_DIR}"
export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-sqs-server-refactor-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-sqs-server-refactor-verify.err"

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -60
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
    has_text "${LOOP_DIR}/goal.md" 'server.go.*700 lines or fewer' &&
    has_text "${LOOP_DIR}/goal.md" 'AC-009'
}

package_tests() {
  go test ./internal/services/sqs
}

repository_tests() {
  go test ./...
}

gofmt_clean() {
  local files
  files="$(gofmt -l internal/services/sqs/*.go)"
  [[ -z "${files}" ]]
}

server_line_budget() {
  local lines
  lines="$(wc -l < internal/services/sqs/server.go | tr -d ' ')"
  echo "server.go lines=${lines} limit=${SERVER_LINE_LIMIT}" >&2
  [[ "${lines}" -le "${SERVER_LINE_LIMIT}" ]]
}

focused_file_layout() {
  local count
  count="$(find internal/services/sqs -maxdepth 1 -type f -name '*.go' \
    ! -name 'server.go' ! -name '*_test.go' | wc -l | tr -d ' ')"
  echo "focused non-test files=${count}" >&2
  [[ "${count}" -ge 8 ]]
}

package_name_consistent() {
  for file in internal/services/sqs/*.go; do
    awk 'NR == 1 && $0 != "package sqs" { exit 1 }' "${file}" || return 1
  done
}

existing_sqs_full_gate() {
  SQS_VERIFY_PORT="${SQS_VERIFY_PORT}" \
    GCS_VERIFY_PORT="${GCS_VERIFY_PORT}" \
    S3_VERIFY_PORT="${S3_VERIFY_PORT}" \
    DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT}" \
    SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT}" \
    DYNAMODB_VERIFY_PORT="${DYNAMODB_VERIFY_PORT}" \
    BIGQUERY_VERIFY_PORT="${BIGQUERY_VERIFY_PORT}" \
    VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh
}

echo "=== SQS server refactor verification: ${VERIFY_STAGE} ==="

case "${VERIFY_STAGE}" in
  foundation)
    run_check "SQS server refactor goal contract" goal_contract
    run_check "SQS server refactor script contract" script_contract
    run_check "SQS package tests pass" package_tests
    run_check "gofmt is clean for SQS package" gofmt_clean
    ;;
  shape)
    run_check "server.go line budget met" server_line_budget
    run_check "focused SQS source files exist" focused_file_layout
    run_check "SQS package name remains consistent" package_name_consistent
    ;;
  sqs-full)
    run_check "SQS full compatibility gate passes" existing_sqs_full_gate
    ;;
  full)
    run_check "SQS server refactor goal contract" goal_contract
    run_check "SQS server refactor script contract" script_contract
    run_check "server.go line budget met" server_line_budget
    run_check "focused SQS source files exist" focused_file_layout
    run_check "SQS package name remains consistent" package_name_consistent
    run_check "gofmt is clean for SQS package" gofmt_clean
    run_check "SQS package tests pass" package_tests
    run_check "repository tests pass" repository_tests
    run_check "SQS full compatibility gate passes" existing_sqs_full_gate
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE: ${VERIFY_STAGE}"
    FAIL=$((FAIL + 1))
    ;;
esac

echo "=== SQS server refactor verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
