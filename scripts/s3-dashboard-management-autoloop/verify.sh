#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-s3-dashboard-management-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-s3-dashboard-management-verify.err"

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

assert_loop_contract() {
  bash -n scripts/s3-dashboard-management-autoloop/bootstrap.sh &&
    bash -n scripts/s3-dashboard-management-autoloop/run-loop.sh &&
    bash -n scripts/s3-dashboard-management-autoloop/recover.sh &&
    bash -n scripts/s3-dashboard-management-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'CreateBucket|PutObject|CopyObject|DeleteObject|DeleteBucket|presigned|multipart|full-management-ui|NEXUS_LOOP_STATUS: READY' scripts/s3-dashboard-management-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/s3-dashboard-management-autoloop/run-loop.sh
}

assert_existing_s3_gate() {
  local s3_port="${S3_DASHBOARD_S3_VERIFY_PORT:-24566}"
  local dashboard_port="${S3_DASHBOARD_DASHBOARD_VERIFY_PORT:-28025}"
  local smtp_port="${S3_DASHBOARD_SMTP_VERIFY_PORT:-21025}"
  set +e
  S3_VERIFY_PORT="${s3_port}" \
    DASHBOARD_VERIFY_PORT="${dashboard_port}" \
    SMTP_VERIFY_PORT="${smtp_port}" \
    VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh
  local status=$?
  set -e
  cleanup_verify_ports "${s3_port}" "${dashboard_port}" "${smtp_port}"
  return "${status}"
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

assert_bucket_object_management_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'createS3Bucket|deleteS3Bucket|deleteS3Object|CreateBucket|DeleteBucket|DeleteObject|confirmation|disabled' web/dashboard/src/app/services/s3 services/dashboard README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q "method: 'POST'|method: 'PUT'|method: 'DELETE'|/api/s3/buckets" web/dashboard/src/app/services/s3/api.ts services/dashboard/server.rs &&
    cargo test --workspace
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_upload_copy_download_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'uploadS3Object|copyS3Object|getS3Object|PutObject|CopyObject|downloadUrl|contentType|metadata|ETag|Last-Modified' web/dashboard/src/app/services/s3 services/dashboard README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Content-Type|x-amz-meta|copy source|copy destination|download' web/dashboard/src/app/services/s3 &&
    cargo test --workspace
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_multipart_presign_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'multipart|uploadId|abort|presigned|signed URL|presign|X-Amz-Signature' web/dashboard/src/app/services/s3 services/dashboard README.md docs scripts/s3-e2e.sh scripts/s3-dashboard-management-autoloop 2>/dev/null &&
    env -u RIPGREP_CONFIG_PATH rg -q 'abort|confirmation|disabled|UploadId|PartNumber|presigned' web/dashboard/src/app/services/s3 services/dashboard/server.rs &&
    cargo test --workspace
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'S3 dashboard management|CreateBucket|PutObject|CopyObject|DeleteObject|DeleteBucket|multipart|presigned|metadata' README.md docs scripts/s3-e2e.sh scripts/s3-dashboard-management-autoloop 2>/dev/null &&
    cargo test --workspace
}

run_foundation_checks() {
  run_check "S3 dashboard management loop contract exists" assert_loop_contract
  run_check "existing S3 compatibility gate remains green" assert_existing_s3_gate
}

run_bucket_object_management_checks() {
  run_foundation_checks
  run_check "S3 bucket/object management contract passes" assert_bucket_object_management_contract
}

run_upload_copy_download_checks() {
  run_bucket_object_management_checks
  run_check "S3 upload/copy/download contract passes" assert_upload_copy_download_contract
}

run_multipart_presign_checks() {
  run_upload_copy_download_checks
  run_check "S3 multipart/presign contract passes" assert_multipart_presign_contract
}

run_e2e_docs_checks() {
  run_multipart_presign_checks
  run_check "S3 dashboard E2E/docs contract passes" assert_e2e_docs_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  bucket-object-management)
    run_bucket_object_management_checks
    ;;
  upload-copy-download)
    run_upload_copy_download_checks
    ;;
  multipart-presign)
    run_multipart_presign_checks
    ;;
  e2e-docs)
    run_e2e_docs_checks
    ;;
  full-management-ui)
    run_e2e_docs_checks
    run_check "repository tests pass" cargo test --workspace
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== S3 dashboard management verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
