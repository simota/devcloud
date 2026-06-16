#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
SQS_VERIFY_PORT="${SQS_VERIFY_PORT:-19324}"
GCS_VERIFY_PORT="${GCS_VERIFY_PORT:-14443}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-14566}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-18025}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-11025}"
DYNAMODB_VERIFY_PORT="${DYNAMODB_VERIFY_PORT:-18000}"
BIGQUERY_VERIFY_PORT="${BIGQUERY_VERIFY_PORT:-19050}"
VERIFY_HOST="127.0.0.1"
SQS_ENDPOINT="http://${VERIFY_HOST}:${SQS_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"
QUEUE_NAME="${SQS_VERIFY_QUEUE:-DevcloudSqsLoop}"
FIFO_QUEUE_NAME="${SQS_VERIFY_FIFO_QUEUE:-DevcloudSqsLoop.fifo}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-sqs-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-sqs-verify.err"
QUEUE_URL=""
RECEIPT_HANDLE=""

cleanup() {
  if [[ -n "${DEV_PID}" ]]; then
    kill "${DEV_PID}" >/dev/null 2>&1 || true
    wait "${DEV_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -30
    FAIL=$((FAIL + 1))
  fi
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 12))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

sqs_json() {
  local target="$1"
  local payload="$2"
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/x-amz-json-1.0' \
    -H "X-Amz-Target: AmazonSQS.${target}" \
    --data "${payload}" \
    "${SQS_ENDPOINT}/"
}

sqs_query() {
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    "$@" \
    "${SQS_ENDPOINT}/"
}

wait_for_sqs() {
  local deadline=$((SECONDS + 12))
  until sqs_json ListQueues '{}' >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: sqs-e2e

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}
  gcsPort: ${GCS_VERIFY_PORT}
  dynamodbPort: ${DYNAMODB_VERIFY_PORT}
  bigQueryPort: ${BIGQUERY_VERIFY_PORT}
  sqsPort: ${SQS_VERIFY_PORT}

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  gcs:
    mode: relaxed
    project: devcloud
  dynamodb:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  bigquery:
    mode: relaxed
    project: devcloud
    bearerToken: dev
  sqs:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"

storage:
  path: .devcloud/data

services:
  mail:
    enabled: true
    maxMessageBytes: 10485760
  s3:
    enabled: true
    region: us-east-1
  gcs:
    enabled: true
    project: devcloud
    location: US
  dynamodb:
    enabled: true
    region: us-east-1
  bigquery:
    enabled: true
    project: devcloud
    location: US
  redshift:
    enabled: false
  pubsub:
    enabled: false
  sqs:
    enabled: true
    region: us-east-1
    queueUrlHost: 127.0.0.1
    maxQueues: 256
    maxMessageBytes: 1048576
    maxReceiveBatchSize: 10
    defaultVisibilityTimeoutSeconds: 2
    defaultDelaySeconds: 0
    defaultMessageRetentionSeconds: 345600
    defaultReceiveWaitTimeSeconds: 0
    schedulerIntervalSeconds: 1
EOF

  run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
  if [[ "${FAIL}" -gt 0 ]]; then
    return 1
  fi

  (
    cd "${TMP_DIR}"
    "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

ensure_started() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
    wait_for_sqs
  fi
}

assert_sqs_design_contract() {
  test -f docs/design-sqs-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'SQS Compatibility Design|AmazonSQS\.SendMessage|Action=SendMessage|VisibilityTimeout|MessageDeduplicationId|RedrivePolicy|AC-001' docs/design-sqs-compat.md
}

assert_script_contract() {
  bash -n scripts/sqs-autoloop/bootstrap.sh &&
    bash -n scripts/sqs-autoloop/run-loop.sh &&
    bash -n scripts/sqs-autoloop/recover.sh &&
    bash -n scripts/sqs-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS: READY' scripts/sqs-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY' scripts/sqs-autoloop/run-loop.sh
}

assert_sqs_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'sqsPort|services\\.sqs|auth\\.sqs|SQS|sqs' rust docs &&
    cargo test --workspace
}

sqs_endpoint_starts() {
  ensure_started
}

json_list_queues() {
  sqs_json ListQueues '{}' | grep -q 'QueueUrls\|{}'
}

query_list_queues() {
  sqs_query \
    --data-urlencode 'Action=ListQueues' \
    --data-urlencode 'Version=2012-11-05' |
    grep -q 'ListQueuesResponse'
}

create_queue() {
  QUEUE_URL="$(sqs_json CreateQueue "{
    \"QueueName\":\"${QUEUE_NAME}\",
    \"Attributes\":{\"VisibilityTimeout\":\"2\",\"ReceiveMessageWaitTimeSeconds\":\"0\"}
  }" | python3 -c 'import json,sys; print(json.load(sys.stdin)["QueueUrl"])')"
  [[ "${QUEUE_URL}" == *"/${QUEUE_NAME}" ]]
}

get_queue_url() {
  sqs_json GetQueueUrl "{\"QueueName\":\"${QUEUE_NAME}\"}" |
    grep -q "\"QueueUrl\"[[:space:]]*:[[:space:]]*\"${QUEUE_URL}\""
}

list_queues_with_queue() {
  sqs_json ListQueues '{}' | grep -q "${QUEUE_NAME}"
}

get_queue_attributes() {
  sqs_json GetQueueAttributes "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"AttributeNames\":[\"All\"]
  }" | grep -q 'VisibilityTimeout'
}

set_queue_attributes() {
  sqs_json SetQueueAttributes "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"Attributes\":{\"DelaySeconds\":\"0\",\"VisibilityTimeout\":\"2\"}
  }" >/dev/null
}

query_create_queue() {
  sqs_query \
    --data-urlencode 'Action=CreateQueue' \
    --data-urlencode 'Version=2012-11-05' \
    --data-urlencode 'QueueName=DevcloudSqsQueryLoop' |
    grep -q 'CreateQueueResponse'
}

send_message() {
  sqs_json SendMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"MessageBody\":\"devcloud sqs loop message\",
    \"MessageAttributes\":{\"kind\":{\"DataType\":\"String\",\"StringValue\":\"loop\"}}
  }" | grep -q 'MD5OfMessageBody'
}

receive_message() {
  sqs_json ReceiveMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"MaxNumberOfMessages\":1,
    \"VisibilityTimeout\":2,
    \"AttributeNames\":[\"All\"],
    \"MessageAttributeNames\":[\"All\"]
  }" > "${VERIFY_OUT}"
  RECEIPT_HANDLE="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); print(data["Messages"][0]["ReceiptHandle"])' "${VERIFY_OUT}")"
  grep -q 'devcloud sqs loop message' "${VERIFY_OUT}"
}

message_is_invisible_before_timeout() {
  sqs_json ReceiveMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"MaxNumberOfMessages\":1,
    \"WaitTimeSeconds\":0
  }" > "${VERIFY_OUT}"
  ! grep -q 'devcloud sqs loop message' "${VERIFY_OUT}"
}

message_reappears_after_timeout() {
  sleep 3
  sqs_json ReceiveMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"MaxNumberOfMessages\":1,
    \"VisibilityTimeout\":2,
    \"AttributeNames\":[\"All\"]
  }" > "${VERIFY_OUT}"
  RECEIPT_HANDLE="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); print(data["Messages"][0]["ReceiptHandle"])' "${VERIFY_OUT}")"
  grep -q 'ApproximateReceiveCount' "${VERIFY_OUT}"
}

delete_message() {
  sqs_json DeleteMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"ReceiptHandle\":\"${RECEIPT_HANDLE}\"
  }" >/dev/null
}

deleted_message_not_received() {
  sleep 3
  sqs_json ReceiveMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"MaxNumberOfMessages\":1,
    \"WaitTimeSeconds\":0
  }" > "${VERIFY_OUT}"
  ! grep -q 'devcloud sqs loop message' "${VERIFY_OUT}"
}

fifo_validation_available() {
  sqs_json CreateQueue "{
    \"QueueName\":\"${FIFO_QUEUE_NAME}\",
    \"Attributes\":{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"true\"}
  }" > "${VERIFY_OUT}"
  grep -q "${FIFO_QUEUE_NAME}" "${VERIFY_OUT}"
}

dashboard_starts() {
  ensure_started &&
    wait_for_http "${DASHBOARD_ENDPOINT}/"
}

dashboard_service_registry_has_sqs() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" |
    grep -q '"id"[[:space:]]*:[[:space:]]*"sqs"'
}

dashboard_sqs_page_loads() {
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/sqs" |
    grep -q 'devcloud Dashboard'
}

dashboard_sqs_api_lists_queues() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sqs/queues" |
    grep -q "${QUEUE_NAME}"
}

run_foundation_checks() {
  run_check "SQS design contract exists" assert_sqs_design_contract
  run_check "SQS autoloop script contract" assert_script_contract
  run_check "repository tests pass" cargo test --workspace
  run_check "devcloud help works" cargo run -p devcloud-orchestrator -- help
}

run_config_checks() {
  run_check "SQS config shape" assert_sqs_config_shape
}

run_protocol_checks() {
  run_check "SQS endpoint starts" sqs_endpoint_starts
  run_check "AWS JSON ListQueues works" json_list_queues
  run_check "Query ListQueues works" query_list_queues
}

run_queue_checks() {
  run_check "CreateQueue works" create_queue
  run_check "GetQueueUrl works" get_queue_url
  run_check "ListQueues includes queue" list_queues_with_queue
  run_check "GetQueueAttributes works" get_queue_attributes
  run_check "SetQueueAttributes works" set_queue_attributes
  run_check "Query CreateQueue works" query_create_queue
}

run_message_checks() {
  run_check "SendMessage works" send_message
  run_check "ReceiveMessage works" receive_message
  run_check "visibility timeout hides message" message_is_invisible_before_timeout
  run_check "visibility timeout requeues message" message_reappears_after_timeout
  run_check "DeleteMessage works" delete_message
  run_check "deleted message is not received again" deleted_message_not_received
}

run_fifo_checks() {
  run_check "FIFO queue validation and creation works" fifo_validation_available
}

run_dashboard_checks() {
  run_check "dashboard starts" dashboard_starts
  run_check "dashboard service registry has SQS" dashboard_service_registry_has_sqs
  run_check "dashboard SQS page loads" dashboard_sqs_page_loads
  run_check "dashboard SQS API lists queues" dashboard_sqs_api_lists_queues
}

run_e2e_checks() {
  run_check "SQS standalone E2E script passes" bash scripts/sqs-e2e.sh
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  config)
    run_foundation_checks
    run_config_checks
    ;;
  protocol|server)
    run_foundation_checks
    run_config_checks
    run_protocol_checks
    ;;
  queue)
    run_foundation_checks
    run_config_checks
    run_protocol_checks
    run_queue_checks
    ;;
  message)
    run_foundation_checks
    run_config_checks
    run_protocol_checks
    run_queue_checks
    run_message_checks
    ;;
  fifo)
    run_foundation_checks
    run_config_checks
    run_protocol_checks
    run_queue_checks
    run_message_checks
    run_fifo_checks
    ;;
  dashboard)
    run_foundation_checks
    run_config_checks
    run_protocol_checks
    run_queue_checks
    run_message_checks
    run_dashboard_checks
    ;;
  full)
    run_foundation_checks
    run_config_checks
    run_protocol_checks
    run_queue_checks
    run_message_checks
    run_fifo_checks
    run_dashboard_checks
    run_e2e_checks
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== SQS autoloop verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
