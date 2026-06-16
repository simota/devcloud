#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-bigquery-sdk-compat-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-bigquery-sdk-compat-verify.err"

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

assert_loop_contract() {
  bash -n scripts/bigquery-sdk-compat-autoloop/bootstrap.sh &&
    bash -n scripts/bigquery-sdk-compat-autoloop/recover.sh &&
    bash -n scripts/bigquery-sdk-compat-autoloop/run-loop.sh &&
    bash -n scripts/bigquery-sdk-compat-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'sdk-client|sdk-e2e|compat-docs|full-sdk-compat|NEXUS_LOOP_STATUS: READY' scripts/bigquery-sdk-compat-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/bigquery-sdk-compat-autoloop/run-loop.sh
}

assert_sdk_probe_retired() {
  [[ ! -f scripts/bigquery-sdk-e2e.sh ]]
}

assert_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'BigQuery|full-sdk-compat' README.md docs scripts/bigquery-sdk-compat-autoloop
}

assert_existing_bigquery_gates() {
  local bigquery_port="${BIGQUERY_SDK_COMPAT_BIGQUERY_VERIFY_PORT:-39060}"
  local gcs_port="${BIGQUERY_SDK_COMPAT_GCS_VERIFY_PORT:-34463}"
  local s3_port="${BIGQUERY_SDK_COMPAT_S3_VERIFY_PORT:-34587}"
  local dynamodb_port="${BIGQUERY_SDK_COMPAT_DYNAMODB_VERIFY_PORT:-38010}"
  local dashboard_port="${BIGQUERY_SDK_COMPAT_DASHBOARD_VERIFY_PORT:-38046}"
  local smtp_port="${BIGQUERY_SDK_COMPAT_SMTP_VERIFY_PORT:-31046}"
  scripts/bigquery-e2e.sh &&
    BIGQUERY_VERIFY_PORT="${bigquery_port}" \
    GCS_VERIFY_PORT="${gcs_port}" \
    S3_VERIFY_PORT="${s3_port}" \
    DYNAMODB_VERIFY_PORT="${dynamodb_port}" \
    DASHBOARD_VERIFY_PORT="${dashboard_port}" \
    SMTP_VERIFY_PORT="${smtp_port}" \
    VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh
}

run_foundation_checks() {
  run_check "BigQuery SDK compatibility loop contract exists" assert_loop_contract
}

run_sdk_retired_checks() {
  run_foundation_checks
  run_check "BigQuery SDK probe is retired" assert_sdk_probe_retired
}

run_sdk_e2e_checks() {
  run_sdk_retired_checks
  run_check "existing BigQuery E2E and compatibility gates pass" assert_existing_bigquery_gates
}

run_docs_checks() {
  run_sdk_retired_checks
  run_check "BigQuery SDK docs contract exists" assert_docs_contract
}

run_full_checks() {
  run_sdk_e2e_checks
  run_check "BigQuery SDK docs contract exists" assert_docs_contract
  run_check "repository tests pass" cargo test --workspace
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  sdk-retired|sdk-client)
    run_sdk_retired_checks
    ;;
  sdk-e2e)
    run_sdk_e2e_checks
    ;;
  compat-docs)
    run_docs_checks
    ;;
  full-sdk-compat)
    run_full_checks
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== BigQuery SDK compatibility verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
