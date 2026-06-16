#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-dynamodb-dashboard-advanced-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-dynamodb-dashboard-advanced-verify.err"

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
  bash -n scripts/dynamodb-dashboard-advanced-autoloop/bootstrap.sh &&
    bash -n scripts/dynamodb-dashboard-advanced-autoloop/run-loop.sh &&
    bash -n scripts/dynamodb-dashboard-advanced-autoloop/recover.sh &&
    bash -n scripts/dynamodb-dashboard-advanced-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'pagination|saved query|recent operation|wizard|validation|DeleteItem|DeleteTable|full-advanced-ui|NEXUS_LOOP_STATUS: READY' scripts/dynamodb-dashboard-advanced-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/dynamodb-dashboard-advanced-autoloop/run-loop.sh
}

assert_management_gate() {
  VERIFY_STAGE=full-management-ui bash scripts/dynamodb-dashboard-management-autoloop/verify.sh
}

assert_pagination_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'LastEvaluatedKey|ExclusiveStartKey|nextPage|previousPage|pageHistory|pagination' web/dashboard/src/app/services/dynamodb services/dashboard scripts &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Count|ScannedCount|Query|Scan' web/dashboard/src/app/services/dynamodb/DynamoDBDashboard.tsx &&
    cargo test --workspace
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_saved_recent_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'saved query|savedQuery|recent operation|recentOperation|operationHistory|localStorage' web/dashboard/src/app/services/dynamodb services/dashboard README.md docs &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_wizard_validation_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'CreateTable wizard|wizard|partition key|sort key|BillingMode|GlobalSecondaryIndex|validateJSON|validation' web/dashboard/src/app/services/dynamodb README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'JSON validation|client-side validation|validation error' web/dashboard/src/app/services/dynamodb README.md docs &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_delete_confirmation_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'two-step|second step|Confirm delete|DeleteItem confirmation|DeleteTable confirmation|confirmation text' web/dashboard/src/app/services/dynamodb README.md docs &&
    cargo test --workspace
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'pagination|saved query|recent operation|wizard|validation|confirmation' README.md docs scripts/dynamodb-e2e.sh scripts/*dynamodb* 2>/dev/null &&
    cargo test --workspace
}

run_foundation_checks() {
  run_check "DynamoDB dashboard advanced loop contract exists" assert_loop_contract
  run_check "existing DynamoDB dashboard management gate remains green" assert_management_gate
}

run_pagination_checks() {
  run_foundation_checks
  run_check "DynamoDB Query/Scan pagination contract passes" assert_pagination_contract
}

run_saved_recent_checks() {
  run_pagination_checks
  run_check "DynamoDB saved/recent operation contract passes" assert_saved_recent_contract
}

run_wizard_validation_checks() {
  run_saved_recent_checks
  run_check "DynamoDB wizard and JSON validation contract passes" assert_wizard_validation_contract
}

run_delete_confirmation_checks() {
  run_wizard_validation_checks
  run_check "DynamoDB destructive confirmation contract passes" assert_delete_confirmation_contract
}

run_e2e_docs_checks() {
  run_delete_confirmation_checks
  run_check "DynamoDB advanced dashboard E2E/docs contract passes" assert_e2e_docs_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  pagination)
    run_pagination_checks
    ;;
  saved-recent)
    run_saved_recent_checks
    ;;
  wizard-validation)
    run_wizard_validation_checks
    ;;
  delete-confirmation)
    run_delete_confirmation_checks
    ;;
  e2e-docs)
    run_e2e_docs_checks
    ;;
  full-advanced-ui)
    run_e2e_docs_checks
    run_check "repository tests pass" cargo test --workspace
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== DynamoDB dashboard advanced UI verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
