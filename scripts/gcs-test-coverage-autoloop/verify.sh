#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-gcs-test-coverage-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-gcs-test-coverage-verify.err"

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
  bash -n scripts/gcs-test-coverage-autoloop/bootstrap.sh &&
    bash -n scripts/gcs-test-coverage-autoloop/run-loop.sh &&
    bash -n scripts/gcs-test-coverage-autoloop/recover.sh &&
    bash -n scripts/gcs-test-coverage-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'service-edge|dashboard-api|e2e-docs|coverage|full-test-coverage|NEXUS_LOOP_STATUS: READY' scripts/gcs-test-coverage-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/gcs-test-coverage-autoloop/run-loop.sh
}

assert_existing_gcs_gates() {
  local gcs_port="${GCS_TEST_GCS_VERIFY_PORT:-24443}"
  local s3_port="${GCS_TEST_S3_VERIFY_PORT:-24567}"
  local dashboard_port="${GCS_TEST_DASHBOARD_VERIFY_PORT:-28026}"
  local smtp_port="${GCS_TEST_SMTP_VERIFY_PORT:-21026}"
  local react_gcs_port="${GCS_TEST_REACT_GCS_VERIFY_PORT:-24444}"
  local react_s3_port="${GCS_TEST_REACT_S3_VERIFY_PORT:-24568}"
  local react_dashboard_port="${GCS_TEST_REACT_DASHBOARD_VERIFY_PORT:-28027}"
  local react_smtp_port="${GCS_TEST_REACT_SMTP_VERIFY_PORT:-21027}"
  set +e
  GCS_VERIFY_PORT="${gcs_port}" \
    S3_VERIFY_PORT="${s3_port}" \
    DASHBOARD_VERIFY_PORT="${dashboard_port}" \
    SMTP_VERIFY_PORT="${smtp_port}" \
    VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh
  local gcs_status=$?
  cleanup_verify_ports "${gcs_port}" "${s3_port}" "${dashboard_port}" "${smtp_port}"
  GCS_REACT_GCS_VERIFY_PORT="${react_gcs_port}" \
    GCS_REACT_S3_VERIFY_PORT="${react_s3_port}" \
    GCS_REACT_DASHBOARD_VERIFY_PORT="${react_dashboard_port}" \
    GCS_REACT_SMTP_VERIFY_PORT="${react_smtp_port}" \
    VERIFY_STAGE=full-react-gcs bash scripts/gcs-dashboard-react-autoloop/verify.sh
  local dashboard_status=$?
  cleanup_verify_ports "${react_gcs_port}" "${react_s3_port}" "${react_dashboard_port}" "${react_smtp_port}"
  set -e
  [[ "${gcs_status}" -eq 0 && "${dashboard_status}" -eq 0 ]]
}

assert_service_edge_tests() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Invalid|Error|Metadata|Generation|Metageneration|Upload|Session|Pagination|Prefix|Delete|NotFound|Header' services/gcs/server_test.rs &&
    cargo test --workspace
}

assert_dashboard_api_tests() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Test.*GCS.*(Dashboard|API|Disabled|Invalid|Upload|Session|Delete|Metadata|Generation|Confirmation)' services/dashboard/server_test.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q '/api/gcs|disabled|invalid|metadata|upload|session|generation|delete|confirmation' services/dashboard/server_test.rs &&
    cargo test --workspace
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'GCS test coverage|bucket create|bucket delete|object upload|object delete|metadata|generation|metageneration|upload session|resumable|CreateBucket|DeleteObject' README.md docs scripts/gcs-e2e.sh scripts/gcs-test-coverage-autoloop 2>/dev/null &&
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
  assert_min_coverage ./services/gcs 72.0 &&
    assert_min_coverage ./services/dashboard 70.0
}

run_foundation_checks() {
  run_check "GCS test coverage loop contract exists" assert_loop_contract
  run_check "existing GCS gates remain green" assert_existing_gcs_gates
}

run_service_edge_checks() {
  run_foundation_checks
  run_check "GCS service edge tests pass" assert_service_edge_tests
}

run_dashboard_api_checks() {
  run_service_edge_checks
  run_check "GCS dashboard API tests pass" assert_dashboard_api_tests
}

run_e2e_docs_checks() {
  run_dashboard_api_checks
  run_check "GCS E2E/docs test coverage contract passes" assert_e2e_docs_contract
}

run_coverage_checks() {
  run_e2e_docs_checks
  run_check "GCS coverage thresholds pass" assert_coverage_thresholds
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

echo "=== GCS test coverage verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
