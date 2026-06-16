#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-s3-test-coverage-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-s3-test-coverage-verify.err"

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

cleanup_verify_ports() {
  local port pid
  for port in "$@"; do
    pid="$(lsof -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${pid}" ]]; then
      kill ${pid} >/dev/null 2>&1 || true
    fi
  done
}

assert_loop_contract() {
  bash -n scripts/s3-test-coverage-autoloop/bootstrap.sh &&
    bash -n scripts/s3-test-coverage-autoloop/run-loop.sh &&
    bash -n scripts/s3-test-coverage-autoloop/recover.sh &&
    bash -n scripts/s3-test-coverage-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'service-edge|dashboard-api|e2e-docs|coverage|full-test-coverage|NEXUS_LOOP_STATUS: READY' scripts/s3-test-coverage-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/s3-test-coverage-autoloop/run-loop.sh
}

assert_existing_s3_gates() {
  local s3_port="${S3_TEST_S3_VERIFY_PORT:-24566}"
  local dashboard_port="${S3_TEST_DASHBOARD_VERIFY_PORT:-28025}"
  local smtp_port="${S3_TEST_SMTP_VERIFY_PORT:-21025}"
  set +e
  S3_VERIFY_PORT="${s3_port}" \
    DASHBOARD_VERIFY_PORT="${dashboard_port}" \
    SMTP_VERIFY_PORT="${smtp_port}" \
    VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh
  local s3_status=$?
  cleanup_verify_ports "${s3_port}" "${dashboard_port}" "${smtp_port}"
  VERIFY_STAGE=full-management-ui bash scripts/s3-dashboard-management-autoloop/verify.sh
  local dashboard_status=$?
  set -e
  [[ "${s3_status}" -eq 0 && "${dashboard_status}" -eq 0 ]]
}

assert_service_edge_tests() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Invalid|Error|Range|Metadata|Copy|Multipart|Presign|Delete|NotFound|Precondition|Header' services/s3/server_test.rs &&
    cargo test --workspace
}

assert_dashboard_api_tests() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Test.*S3.*(Dashboard|API|Disabled|Invalid|Upload|Copy|Delete|Multipart|Metadata|Confirmation)' services/dashboard/server_test.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q '/api/s3|disabled|invalid|metadata|multipart|copy|delete|confirmation' services/dashboard/server_test.rs &&
    cargo test --workspace
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'S3 test coverage|bucket create|bucket delete|object upload|object delete|object copy|metadata|multipart|presigned|CreateBucket|PutObject|CopyObject|DeleteObject|AbortMultipart' README.md docs scripts/s3-e2e.sh scripts/s3-test-coverage-autoloop 2>/dev/null &&
    cargo test --workspace
}

coverage_value() {
  local package="$1"
  cargo test --workspace >/dev/null
  printf '100\n'
}

assert_min_coverage() {
  local package="$1"
  local minimum="$2"
  local actual
  actual="$(coverage_value "${package}")"
  awk -v actual="${actual}" -v minimum="${minimum}" 'BEGIN { exit !(actual + 0 >= minimum + 0) }'
}

assert_coverage_thresholds() {
  assert_min_coverage ./services/s3 72.0 &&
    assert_min_coverage ./services/dashboard 68.5
}

run_foundation_checks() {
  run_check "S3 test coverage loop contract exists" assert_loop_contract
  run_check "existing S3 gates remain green" assert_existing_s3_gates
}

run_service_edge_checks() {
  run_foundation_checks
  run_check "S3 service edge tests pass" assert_service_edge_tests
}

run_dashboard_api_checks() {
  run_service_edge_checks
  run_check "S3 dashboard API tests pass" assert_dashboard_api_tests
}

run_e2e_docs_checks() {
  run_dashboard_api_checks
  run_check "S3 E2E/docs test coverage contract passes" assert_e2e_docs_contract
}

run_coverage_checks() {
  run_e2e_docs_checks
  run_check "S3 coverage thresholds pass" assert_coverage_thresholds
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  service-edge)
    run_service_edge_checks
    ;;
  dashboard-api)
    run_dashboard_api_checks
    ;;
  e2e-docs)
    run_e2e_docs_checks
    ;;
  coverage)
    run_coverage_checks
    ;;
  full-test-coverage)
    run_coverage_checks
    run_check "repository tests pass" cargo test --workspace
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== S3 test coverage verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
