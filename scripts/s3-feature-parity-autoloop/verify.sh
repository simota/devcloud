#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOOP_DIR="${ROOT_DIR}/scripts/s3-feature-parity-autoloop"
VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-34566}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-38025}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-31025}"

cd "${ROOT_DIR}"

PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-s3-feature-parity-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-s3-feature-parity-verify.err"

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -40
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
  for feature in \
    'Versioning' \
    'Bucket policy' \
    'Lifecycle' \
    'Notifications' \
    'SSE/KMS' \
    'Virtual-Host' \
    'Object Lock' \
    'S3 Select' \
    'Inventory' \
    'Replication'
  do
    has_text "${LOOP_DIR}/goal.md" "${feature}" || return 1
  done
  has_text "${LOOP_DIR}/goal.md" 'AC-015'
}

existing_s3_full_gate() {
  S3_VERIFY_PORT="${S3_VERIFY_PORT}" \
    DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT}" \
    SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT}" \
    VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh
}

feature_contract() {
  local source_paths
  source_paths="README.md services/s3 services/dashboard docs"
  for pattern in \
    'versioning|Versioning|VersionId|versionId' \
    'bucket policy|BucketPolicy|policy' \
    'ACL|AccessControl|acl' \
    'lifecycle|Lifecycle' \
    'notification|Notification' \
    'SSE|KMS|server-side encryption|ServerSideEncryption' \
    'virtual-host|Host|host-style|bucket.*host' \
    'Object Lock|LegalHold|Retention|object-lock' \
    'SelectObjectContent|S3 Select|select-object' \
    'Inventory|Analytics' \
    'Replication|replication'
  do
    env -u RIPGREP_CONFIG_PATH rg -qi "${pattern}" ${source_paths} || return 1
  done
}

echo "=== S3 feature parity verification: ${VERIFY_STAGE} ==="

case "${VERIFY_STAGE}" in
  foundation)
    run_check "S3 feature parity goal contract" goal_contract
    run_check "S3 feature parity script contract" script_contract
    run_check "repository tests pass" cargo test --workspace
    ;;
  s3-core)
    run_check "existing S3 full gate passes" existing_s3_full_gate
    ;;
  feature-contract)
    run_check "feature keywords are represented in implementation/tests/docs" feature_contract
    ;;
  full)
    run_check "S3 feature parity goal contract" goal_contract
    run_check "S3 feature parity script contract" script_contract
    run_check "repository tests pass" cargo test --workspace
    run_check "existing S3 full gate passes" existing_s3_full_gate
    run_check "feature keywords are represented in implementation/tests/docs" feature_contract
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE: ${VERIFY_STAGE}"
    FAIL=$((FAIL + 1))
    ;;
esac

echo "=== S3 feature parity verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
