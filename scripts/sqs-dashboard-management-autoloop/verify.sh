#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-sqs-dashboard-management-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-sqs-dashboard-management-verify.err"

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
  bash -n scripts/sqs-dashboard-management-autoloop/bootstrap.sh &&
    bash -n scripts/sqs-dashboard-management-autoloop/run-loop.sh &&
    bash -n scripts/sqs-dashboard-management-autoloop/recover.sh &&
    bash -n scripts/sqs-dashboard-management-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'CreateQueue|SendMessage|ReceiveMessage|DeleteMessage|ChangeMessageVisibility|PurgeQueue|full-management-ui|NEXUS_LOOP_STATUS: READY' scripts/sqs-dashboard-management-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/sqs-dashboard-management-autoloop/run-loop.sh
}

assert_existing_sqs_gate() {
  SQS_VERIFY_PORT="${SQS_DASHBOARD_SQS_VERIFY_PORT:-19324}" \
    GCS_VERIFY_PORT="${SQS_DASHBOARD_GCS_VERIFY_PORT:-14443}" \
    S3_VERIFY_PORT="${SQS_DASHBOARD_S3_VERIFY_PORT:-14566}" \
    DASHBOARD_VERIFY_PORT="${SQS_DASHBOARD_DASHBOARD_VERIFY_PORT:-18025}" \
    SMTP_VERIFY_PORT="${SQS_DASHBOARD_SMTP_VERIFY_PORT:-11025}" \
    DYNAMODB_VERIFY_PORT="${SQS_DASHBOARD_DYNAMODB_VERIFY_PORT:-18000}" \
    BIGQUERY_VERIFY_PORT="${SQS_DASHBOARD_BIGQUERY_VERIFY_PORT:-19050}" \
    VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh
}

assert_queue_send_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'createSQSQueue|sendSQSMessage|CreateQueue|SendMessage|message attributes|MessageGroupId|MessageDeduplicationId|FIFO' web/dashboard/src/app/services/sqs internal/dashboard README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q "method: 'POST'|/api/sqs/queues|/messages|/send" web/dashboard/src/app/services/sqs/api.ts internal/dashboard/server.go &&
    go test ./internal/dashboard -run 'TestSQS.*Create|TestSQS.*Send|TestSQSDashboard' -count=1 &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_receive_delete_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'receiveSQSMessage|deleteSQSMessage|ReceiveMessage|DeleteMessage|receiptHandle|receipt handle|selected receipt' web/dashboard/src/app/services/sqs internal/dashboard README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'MaxNumberOfMessages|VisibilityTimeout|WaitTimeSeconds|ReceiptHandle' web/dashboard/src/app/services/sqs &&
    go test ./internal/dashboard -run 'TestSQS.*Receive|TestSQS.*Delete|TestSQSDashboard' -count=1 &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_visibility_purge_dlq_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'changeSQSMessageVisibility|ChangeMessageVisibility|visibility timeout|PurgeQueue|purge confirmation|DLQ|dead-letter|redrive' web/dashboard/src/app/services/sqs internal/dashboard README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'confirmation|Confirm|danger|disabled' web/dashboard/src/app/services/sqs/SQSDashboard.tsx &&
    go test ./internal/dashboard -run 'TestSQS.*Visibility|TestSQS.*Purge|TestSQS.*DLQ|TestSQSDashboard' -count=1 &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'CreateQueue|SendMessage|ReceiveMessage|DeleteMessage|ChangeMessageVisibility|PurgeQueue|SQS dashboard management|receipt handle|DLQ' README.md docs scripts/sqs-e2e.sh scripts/sqs-dashboard-management-autoloop 2>/dev/null &&
    go test ./internal/dashboard ./internal/services/sqs -count=1
}

run_foundation_checks() {
  run_check "SQS dashboard management loop contract exists" assert_loop_contract
  run_check "existing SQS compatibility gate remains green" assert_existing_sqs_gate
}

run_queue_send_checks() {
  run_foundation_checks
  run_check "SQS queue/send UI contract passes" assert_queue_send_contract
}

run_receive_delete_checks() {
  run_queue_send_checks
  run_check "SQS receive/delete UI contract passes" assert_receive_delete_contract
}

run_visibility_purge_dlq_checks() {
  run_receive_delete_checks
  run_check "SQS visibility/purge/DLQ contract passes" assert_visibility_purge_dlq_contract
}

run_e2e_docs_checks() {
  run_visibility_purge_dlq_checks
  run_check "SQS dashboard E2E/docs contract passes" assert_e2e_docs_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  queue-send)
    run_queue_send_checks
    ;;
  receive-delete)
    run_receive_delete_checks
    ;;
  visibility-purge-dlq)
    run_visibility_purge_dlq_checks
    ;;
  e2e-docs)
    run_e2e_docs_checks
    ;;
  full-management-ui)
    run_e2e_docs_checks
    run_check "repository tests pass" go test ./...
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== SQS dashboard management verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
