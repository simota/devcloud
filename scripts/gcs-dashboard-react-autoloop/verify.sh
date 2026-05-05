#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-gcs-dashboard-react-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-gcs-dashboard-react-verify.err"

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
  bash -n scripts/gcs-dashboard-react-autoloop/bootstrap.sh &&
    bash -n scripts/gcs-dashboard-react-autoloop/run-loop.sh &&
    bash -n scripts/gcs-dashboard-react-autoloop/recover.sh &&
    bash -n scripts/gcs-dashboard-react-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'React|GCSDashboard|/gcs|upload sessions|full-react-gcs|NEXUS_LOOP_STATUS: READY' scripts/gcs-dashboard-react-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/gcs-dashboard-react-autoloop/run-loop.sh
}

assert_existing_gcs_gate() {
  GCS_VERIFY_PORT="${GCS_REACT_GCS_VERIFY_PORT:-14443}" \
    S3_VERIFY_PORT="${GCS_REACT_S3_VERIFY_PORT:-14566}" \
    DASHBOARD_VERIFY_PORT="${GCS_REACT_DASHBOARD_VERIFY_PORT:-18025}" \
    SMTP_VERIFY_PORT="${GCS_REACT_SMTP_VERIFY_PORT:-11025}" \
    VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh
}

assert_react_route_contract() {
  test -f web/dashboard/src/app/services/gcs/GCSDashboard.tsx &&
    test -f web/dashboard/src/app/services/gcs/api.ts &&
    test -f web/dashboard/src/app/services/gcs/types.ts &&
    env -u RIPGREP_CONFIG_PATH rg -q "GCSDashboard|service\\.id === 'gcs'|path === '/gcs'|from './services/gcs/GCSDashboard'" web/dashboard/src/app/routes.tsx &&
    env -u RIPGREP_CONFIG_PATH rg -q 'devcloud GCS|GCS' web/dashboard/src/app/services/gcs/GCSDashboard.tsx &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_inspection_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'getGCSStatus|listGCSBuckets|listGCSObjects|getGCSObject|listGCSUploadSessions|downloadUrl' web/dashboard/src/app/services/gcs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'generation|metageneration|storageClass|crc32c|contentType|metadata|gcsUri|gs://' web/dashboard/src/app/services/gcs &&
    go test ./internal/dashboard -run 'TestGCSDashboard|TestDashboard.*GCS' -count=1 &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_management_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'createGCSBucket|deleteGCSBucket|deleteGCSObject|deleteGCSUploadSession' web/dashboard/src/app/services/gcs/api.ts &&
    env -u RIPGREP_CONFIG_PATH rg -q 'confirmation|Confirm|Delete bucket|Delete object|Delete session|disabled' web/dashboard/src/app/services/gcs/GCSDashboard.tsx &&
    env -u RIPGREP_CONFIG_PATH rg -q "method: 'POST'|method: 'DELETE'|/api/gcs/buckets|/api/gcs/uploads" web/dashboard/src/app/services/gcs/api.ts &&
    go test ./internal/dashboard -run 'TestGCSDashboard.*Delete|TestGCSDashboard.*Create|TestGCS.*Dashboard' -count=1 &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'React GCS|GCS dashboard|/gcs|upload session|generation|metageneration|delete object|create bucket' README.md docs scripts/gcs-e2e.sh scripts/gcs-dashboard-react-autoloop 2>/dev/null &&
    go test ./internal/dashboard ./internal/services/gcs ./internal/services/s3 -count=1
}

run_foundation_checks() {
  run_check "GCS React dashboard loop contract exists" assert_loop_contract
  run_check "existing GCS compatibility gate remains green" assert_existing_gcs_gate
}

run_react_route_checks() {
  run_foundation_checks
  run_check "GCS React route contract passes" assert_react_route_contract
}

run_inspection_checks() {
  run_react_route_checks
  run_check "GCS React inspection contract passes" assert_inspection_contract
}

run_management_checks() {
  run_inspection_checks
  run_check "GCS React management contract passes" assert_management_contract
}

run_e2e_docs_checks() {
  run_management_checks
  run_check "GCS React E2E/docs contract passes" assert_e2e_docs_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  react-route)
    run_react_route_checks
    ;;
  inspect)
    run_inspection_checks
    ;;
  management)
    run_management_checks
    ;;
  e2e-docs)
    run_e2e_docs_checks
    ;;
  full-react-gcs)
    run_e2e_docs_checks
    run_check "repository tests pass" go test ./...
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== GCS React dashboard verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
