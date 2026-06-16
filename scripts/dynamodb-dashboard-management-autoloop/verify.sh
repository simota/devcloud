#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-dynamodb-dashboard-management-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-dynamodb-dashboard-management-verify.err"

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

find_free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

assert_loop_contract() {
  bash -n scripts/dynamodb-dashboard-management-autoloop/bootstrap.sh &&
    bash -n scripts/dynamodb-dashboard-management-autoloop/run-loop.sh &&
    bash -n scripts/dynamodb-dashboard-management-autoloop/recover.sh &&
    bash -n scripts/dynamodb-dashboard-management-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'CreateTable|PutItem|UpdateItem|DeleteItem|DeleteTable|Query and Scan|full-management-ui|NEXUS_LOOP_STATUS: READY' scripts/dynamodb-dashboard-management-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/dynamodb-dashboard-management-autoloop/run-loop.sh
}

assert_existing_dynamodb_gate() {
  DYNAMODB_VERIFY_PORT="$(find_free_port)" \
    GCS_VERIFY_PORT="$(find_free_port)" \
    S3_VERIFY_PORT="$(find_free_port)" \
    DASHBOARD_VERIFY_PORT="$(find_free_port)" \
    SMTP_VERIFY_PORT="$(find_free_port)" \
    BIGQUERY_VERIFY_PORT="$(find_free_port)" \
    REDSHIFT_VERIFY_PORT="$(find_free_port)" \
    REDSHIFT_API_VERIFY_PORT="$(find_free_port)" \
    SQS_VERIFY_PORT="$(find_free_port)" \
    PUBSUB_GRPC_VERIFY_PORT="$(find_free_port)" \
    PUBSUB_REST_VERIFY_PORT="$(find_free_port)" \
    VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh
}

assert_current_dashboard_foundation() {
  env -u RIPGREP_CONFIG_PATH rg -q 'getDynamoDBTable|getDynamoDBIndexes|getDynamoDBTTL|getDynamoDBStreams' web/dashboard/src/app/services/dynamodb/api.ts &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Key lookup|Find loaded item|Refresh detail|IndexSummary|normalizedItemLimit' web/dashboard/src/app/services/dynamodb/DynamoDBDashboard.tsx &&
    (cd web/dashboard && npm run typecheck && npm run build) &&
    cargo test --workspace
}

assert_operations_ui_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'CreateTable|PutItem|UpdateItem|DeleteItem|DeleteTable|UpdateTimeToLive|confirm|confirmation|delete' web/dashboard/src/app/services/dynamodb services/dashboard &&
    env -u RIPGREP_CONFIG_PATH rg -q 'createDynamoDBTable|putDynamoDBItem|updateDynamoDBItem|deleteDynamoDBItem|deleteDynamoDBTable|updateDynamoDBTTL' web/dashboard/src/app/services/dynamodb/api.ts &&
    cargo test --workspace
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_query_scan_ui_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Query|Scan|KeyConditionExpression|FilterExpression|ExpressionAttributeValues|IndexName|ScannedCount|Count' web/dashboard/src/app/services/dynamodb services/dashboard &&
    env -u RIPGREP_CONFIG_PATH rg -q 'queryDynamoDBItems|scanDynamoDBItems|DynamoDBQuery|DynamoDBScan' web/dashboard/src/app/services/dynamodb &&
    cargo test --workspace
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'DynamoDB dashboard API|CreateTable|PutItem|Query|Scan|TTL|streams|indexes' README.md docs scripts &&
    env -u RIPGREP_CONFIG_PATH rg -q '/api/dynamodb|dashboard/dynamodb|CreateTable|PutItem|Query|Scan|UpdateTimeToLive' scripts/dynamodb-e2e.sh scripts/*dynamodb* 2>/dev/null &&
    cargo test --workspace
}

run_foundation_checks() {
  run_check "DynamoDB dashboard management loop contract exists" assert_loop_contract
  run_check "current DynamoDB dashboard foundation passes" assert_current_dashboard_foundation
  run_check "existing DynamoDB full gate remains green" assert_existing_dynamodb_gate
}

run_operations_checks() {
  run_foundation_checks
  run_check "DynamoDB guarded operations UI contract passes" assert_operations_ui_contract
}

run_query_scan_checks() {
  run_operations_checks
  run_check "DynamoDB Query/Scan UI contract passes" assert_query_scan_ui_contract
}

run_e2e_docs_checks() {
  run_query_scan_checks
  run_check "DynamoDB dashboard E2E/docs contract passes" assert_e2e_docs_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  operations-ui)
    run_operations_checks
    ;;
  query-scan-ui)
    run_query_scan_checks
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

echo "=== DynamoDB dashboard management UI verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
