#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-gcs-sdk-compat-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-gcs-sdk-compat-verify.err"

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
  bash -n scripts/gcs-sdk-compat-autoloop/bootstrap.sh &&
    bash -n scripts/gcs-sdk-compat-autoloop/recover.sh &&
    bash -n scripts/gcs-sdk-compat-autoloop/run-loop.sh &&
    bash -n scripts/gcs-sdk-compat-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'sdk-go|sdk-e2e|compat-docs|full-sdk-compat|cloud.google.com/go/storage|NEXUS_LOOP_STATUS: READY' scripts/gcs-sdk-compat-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/gcs-sdk-compat-autoloop/run-loop.sh
}

assert_go_sdk_contract() {
  [[ -f scripts/gcs-sdk-e2e.sh ]] &&
    bash -n scripts/gcs-sdk-e2e.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'cloud.google.com/go/storage|storage.NewClient|option.WithEndpoint|option.WithoutAuthentication' scripts/gcs-sdk-e2e.sh internal docs README.md
}

assert_sdk_workflow_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Create|Bucket|Object|NewWriter|NewReader|Attrs|Delete|Buckets|Objects' scripts/gcs-sdk-e2e.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'find_free_port|mktemp|devcloud.*up|trap cleanup|E2E_GCS_PORT|E2E_DASHBOARD_PORT' scripts/gcs-sdk-e2e.sh
}

assert_sdk_e2e_runs() {
  scripts/gcs-sdk-e2e.sh
}

assert_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'gcs-sdk-e2e|GCS SDK|Google Cloud Storage SDK|cloud.google.com/go/storage' README.md docs scripts/gcs-sdk-compat-autoloop
}

assert_existing_gcs_gates() {
  local gcs_port="${GCS_SDK_COMPAT_GCS_VERIFY_PORT:-24453}"
  local s3_port="${GCS_SDK_COMPAT_S3_VERIFY_PORT:-24577}"
  local dashboard_port="${GCS_SDK_COMPAT_DASHBOARD_VERIFY_PORT:-28036}"
  local smtp_port="${GCS_SDK_COMPAT_SMTP_VERIFY_PORT:-21036}"
  scripts/gcs-e2e.sh &&
    GCS_VERIFY_PORT="${gcs_port}" \
    S3_VERIFY_PORT="${s3_port}" \
    DASHBOARD_VERIFY_PORT="${dashboard_port}" \
    SMTP_VERIFY_PORT="${smtp_port}" \
    VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh
}

run_foundation_checks() {
  run_check "GCS SDK compatibility loop contract exists" assert_loop_contract
}

run_sdk_go_checks() {
  run_foundation_checks
  run_check "Go Storage SDK contract exists" assert_go_sdk_contract
  run_check "SDK workflow contract exists" assert_sdk_workflow_contract
}

run_sdk_e2e_checks() {
  run_sdk_go_checks
  run_check "GCS SDK E2E script passes" assert_sdk_e2e_runs
}

run_docs_checks() {
  run_sdk_go_checks
  run_check "GCS SDK docs contract exists" assert_docs_contract
}

run_full_checks() {
  run_sdk_e2e_checks
  run_check "GCS SDK docs contract exists" assert_docs_contract
  run_check "existing GCS E2E and compatibility gates pass" assert_existing_gcs_gates
  run_check "repository tests pass" go test ./...
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  sdk-go)
    run_sdk_go_checks
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

echo "=== GCS SDK compatibility verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
