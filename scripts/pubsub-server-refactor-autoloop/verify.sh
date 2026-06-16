#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOOP_DIR="${ROOT_DIR}/scripts/pubsub-server-refactor-autoloop"
VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
SERVER_LINE_LIMIT="${SERVER_LINE_LIMIT:-700}"

cd "${ROOT_DIR}"

PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-pubsub-server-refactor-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-pubsub-server-refactor-verify.err"

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
    has_text "${LOOP_DIR}/goal.md" 'server.rs.*700 lines or fewer' &&
    has_text "${LOOP_DIR}/goal.md" 'AC-009'
}

package_tests() {
  cargo test --workspace
}

repository_tests() {
  cargo test --workspace
}

rustfmt_clean() {
  cargo fmt --all -- --check
}

server_line_budget() {
  local lines
  lines="$(wc -l < services/pubsub/server.rs | tr -d ' ')"
  echo "server.rs lines=${lines} limit=${SERVER_LINE_LIMIT}" >&2
  [[ "${lines}" -le "${SERVER_LINE_LIMIT}" ]]
}

focused_file_layout() {
  local count
  count="$(find services/pubsub -maxdepth 1 -type f -name '*.rs' \
    ! -name 'server.rs' ! -name 'grpc.rs' ! -name '*_test.rs' | wc -l | tr -d ' ')"
  echo "focused non-test files=${count}" >&2
  [[ "${count}" -ge 8 ]]
}

package_name_consistent() {
  for file in services/pubsub/*.rs; do
    awk 'NR == 1 && $0 != "package pubsub" { exit 1 }' "${file}" || return 1
  done
}

existing_pubsub_full_gate() {
  VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh
}

echo "=== Pub/Sub server refactor verification: ${VERIFY_STAGE} ==="

case "${VERIFY_STAGE}" in
  foundation)
    run_check "Pub/Sub server refactor goal contract" goal_contract
    run_check "Pub/Sub server refactor script contract" script_contract
    run_check "Pub/Sub package tests pass" package_tests
    run_check "rustfmt is clean for Pub/Sub package" rustfmt_clean
    ;;
  shape)
    run_check "server.rs line budget met" server_line_budget
    run_check "focused Pub/Sub source files exist" focused_file_layout
    run_check "Pub/Sub package name remains consistent" package_name_consistent
    ;;
  pubsub-full)
    run_check "Pub/Sub full compatibility gate passes" existing_pubsub_full_gate
    ;;
  full)
    run_check "Pub/Sub server refactor goal contract" goal_contract
    run_check "Pub/Sub server refactor script contract" script_contract
    run_check "server.rs line budget met" server_line_budget
    run_check "focused Pub/Sub source files exist" focused_file_layout
    run_check "Pub/Sub package name remains consistent" package_name_consistent
    run_check "rustfmt is clean for Pub/Sub package" rustfmt_clean
    run_check "Pub/Sub package tests pass" package_tests
    run_check "repository tests pass" repository_tests
    run_check "Pub/Sub full compatibility gate passes" existing_pubsub_full_gate
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE: ${VERIFY_STAGE}"
    FAIL=$((FAIL + 1))
    ;;
esac

echo "=== Pub/Sub server refactor verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
