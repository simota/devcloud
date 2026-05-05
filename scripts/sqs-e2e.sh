#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

SQS_PORT="${E2E_SQS_PORT:-}"
GCS_PORT="${E2E_GCS_PORT:-}"
S3_PORT="${E2E_S3_PORT:-}"
SMTP_PORT="${E2E_SMTP_PORT:-}"
DYNAMODB_PORT="${E2E_DYNAMODB_PORT:-}"
BIGQUERY_PORT="${E2E_BIGQUERY_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
SQS_ENDPOINT=""
DASHBOARD_ENDPOINT=""
REGION="${E2E_AWS_REGION:-us-east-1}"
ACCOUNT_ID="${E2E_SQS_ACCOUNT_ID:-123456789012}"
QUEUE="${E2E_SQS_QUEUE:-devcloud-sqs-e2e-$(date +%s)}"
QUERY_QUEUE="${E2E_SQS_QUERY_QUEUE:-devcloud-sqs-query-e2e-$(date +%s)}"
FIFO_QUEUE="${E2E_SQS_FIFO_QUEUE:-devcloud-sqs-e2e-$(date +%s).fifo}"
DLQ="${E2E_SQS_DLQ:-devcloud-sqs-dlq-e2e-$(date +%s)}"
SOURCE_QUEUE="${E2E_SQS_SOURCE_QUEUE:-devcloud-sqs-source-e2e-$(date +%s)}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

TMP_DIR=""
DEV_PID=""
WORKSPACE=""
QUEUE_URL=""
QUERY_QUEUE_URL=""
FIFO_QUEUE_URL=""
DLQ_URL=""
SOURCE_QUEUE_URL=""
RECEIPT_HANDLE=""
BATCH_RECEIPTS_JSON=""

usage() {
  cat <<'EOF'
Usage:
  scripts/sqs-e2e.sh

Environment:
  E2E_SQS_PORT=19324              Override the SQS endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025        Override the dashboard port. Defaults to an available port.
  E2E_S3_PORT=14566               Override the S3 endpoint port used by devcloud. Defaults to an available port.
  E2E_GCS_PORT=14443              Override the GCS endpoint port used by devcloud. Defaults to an available port.
  E2E_DYNAMODB_PORT=18000         Override the DynamoDB endpoint port used by devcloud. Defaults to an available port.
  E2E_BIGQUERY_PORT=19050         Override the BigQuery endpoint port used by devcloud. Defaults to an available port.
  E2E_SMTP_PORT=11025             Override the Mail SMTP port used by devcloud. Defaults to an available port.
  E2E_SQS_QUEUE=devcloud-sqs-e2e  Override the standard queue name.
  E2E_DELETE_DATA=false           Keep queues/messages after assertions.
  E2E_KEEP_WORKDIR=true           Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true            Keep devcloud running and keep SQS data after assertions.

Examples:
  scripts/sqs-e2e.sh
  E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/sqs-e2e.sh
  E2E_SQS_PORT=19324 E2E_DASHBOARD_PORT=18025 scripts/sqs-e2e.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${INTERACTIVE}" == "true" && -z "${E2E_DELETE_DATA+x}" ]]; then
  DELETE_DATA="false"
fi
if [[ "${INTERACTIVE}" == "true" && -z "${E2E_KEEP_WORKDIR+x}" ]]; then
  KEEP_WORKDIR="true"
fi
if [[ "${DELETE_DATA}" == "false" && -z "${E2E_KEEP_WORKDIR+x}" ]]; then
  KEEP_WORKDIR="true"
fi

log() {
  printf '[sqs-e2e] %s\n' "$1"
}

cleanup() {
  if [[ -n "${DEV_PID}" ]]; then
    kill "${DEV_PID}" >/dev/null 2>&1 || true
    wait "${DEV_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_WORKDIR}" != "true" && -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  elif [[ -n "${TMP_DIR}" ]]; then
    log "kept workdir: ${TMP_DIR}"
  fi
}
trap cleanup EXIT

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "[sqs-e2e] missing command: ${name}" >&2
    exit 1
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

assign_ports() {
  if [[ -z "${SQS_PORT}" ]]; then
    SQS_PORT="$(find_free_port)"
  fi
  if [[ -z "${GCS_PORT}" ]]; then
    GCS_PORT="$(find_free_port)"
  fi
  if [[ -z "${S3_PORT}" ]]; then
    S3_PORT="$(find_free_port)"
  fi
  if [[ -z "${SMTP_PORT}" ]]; then
    SMTP_PORT="$(find_free_port)"
  fi
  if [[ -z "${DYNAMODB_PORT}" ]]; then
    DYNAMODB_PORT="$(find_free_port)"
  fi
  if [[ -z "${BIGQUERY_PORT}" ]]; then
    BIGQUERY_PORT="$(find_free_port)"
  fi
  if [[ -z "${DASHBOARD_PORT}" ]]; then
    DASHBOARD_PORT="$(find_free_port)"
  fi
  SQS_ENDPOINT="http://127.0.0.1:${SQS_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

json_value() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "${expression}"
}

json_assert() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data}), data' "${expression}"
}

json_quote() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"
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

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 20))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[sqs-e2e] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[sqs-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

wait_for_sqs() {
  local deadline=$((SECONDS + 20))
  until sqs_json ListQueues '{}' >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[sqs-e2e] devcloud exited while waiting for SQS" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[sqs-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

write_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: sqs-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}
  dynamodbPort: ${DYNAMODB_PORT}
  bigqueryPort: ${BIGQUERY_PORT}
  sqsPort: ${SQS_PORT}

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
    accountId: "${ACCOUNT_ID}"

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
    region: ${REGION}
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
}

create_queue() {
  QUEUE_URL="$(sqs_json CreateQueue "{
    \"QueueName\":\"${QUEUE}\",
    \"Attributes\":{\"VisibilityTimeout\":\"2\",\"ReceiveMessageWaitTimeSeconds\":\"0\"},
    \"Tags\":{\"suite\":\"sqs-e2e\"}
  }" | json_value 'data["QueueUrl"]')"
  [[ "${QUEUE_URL}" == "${SQS_ENDPOINT}/${ACCOUNT_ID}/${QUEUE}" ]]
  log "created queue: ${QUEUE_URL}"
}

exercise_queue_core() {
  sqs_json GetQueueUrl "{\"QueueName\":\"${QUEUE}\"}" |
    json_assert 'data["QueueUrl"].endswith("/'"${QUEUE}"'")'
  sqs_json ListQueues '{}' |
    json_assert 'any(url.endswith("/'"${QUEUE}"'") for url in data["QueueUrls"])'
  sqs_json GetQueueAttributes "{\"QueueUrl\":\"${QUEUE_URL}\",\"AttributeNames\":[\"All\"]}" |
    json_assert 'data["Attributes"]["VisibilityTimeout"] == "2" and data["Attributes"]["QueueArn"].endswith(":'"${QUEUE}"'")'
  sqs_json SetQueueAttributes "{\"QueueUrl\":\"${QUEUE_URL}\",\"Attributes\":{\"DelaySeconds\":\"0\",\"VisibilityTimeout\":\"2\"}}" >/dev/null
}

exercise_query_protocol() {
  sqs_query \
    --data-urlencode 'Action=CreateQueue' \
    --data-urlencode 'Version=2012-11-05' \
    --data-urlencode "QueueName=${QUERY_QUEUE}" > "${TMP_DIR}/query-create.xml"
  grep -q '<CreateQueueResponse' "${TMP_DIR}/query-create.xml"
  QUERY_QUEUE_URL="${SQS_ENDPOINT}/${ACCOUNT_ID}/${QUERY_QUEUE}"

  sqs_query \
    --data-urlencode 'Action=SendMessage' \
    --data-urlencode 'Version=2012-11-05' \
    --data-urlencode "QueueUrl=${QUERY_QUEUE_URL}" \
    --data-urlencode 'MessageBody=query protocol message' > "${TMP_DIR}/query-send.xml"
  grep -q '<SendMessageResponse' "${TMP_DIR}/query-send.xml"

  sqs_query \
    --data-urlencode 'Action=ReceiveMessage' \
    --data-urlencode 'Version=2012-11-05' \
    --data-urlencode "QueueUrl=${QUERY_QUEUE_URL}" \
    --data-urlencode 'MaxNumberOfMessages=1' \
    --data-urlencode 'WaitTimeSeconds=0' > "${TMP_DIR}/query-receive.xml"
  grep -q 'query protocol message' "${TMP_DIR}/query-receive.xml"
}

exercise_message_lifecycle() {
  sqs_json SendMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"MessageBody\":\"standard message\",
    \"MessageAttributes\":{\"kind\":{\"DataType\":\"String\",\"StringValue\":\"standard\"}},
    \"MessageSystemAttributes\":{\"AWSTraceHeader\":{\"DataType\":\"String\",\"StringValue\":\"Root=1-devcloud-e2e\"}}
  }" | json_assert 'data["MessageId"] and data["MD5OfMessageBody"]'

  sqs_json ReceiveMessage "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"MaxNumberOfMessages\":1,
    \"VisibilityTimeout\":2,
    \"AttributeNames\":[\"All\"],
    \"MessageAttributeNames\":[\"All\"],
    \"MessageSystemAttributeNames\":[\"AWSTraceHeader\"]
  }" > "${TMP_DIR}/receive.json"
  python3 - <<'PY' "${TMP_DIR}/receive.json"
import json, sys
data = json.load(open(sys.argv[1]))
msg = data["Messages"][0]
assert msg["Body"] == "standard message", data
assert msg["MessageAttributes"]["kind"]["StringValue"] == "standard", data
assert msg["Attributes"]["ApproximateReceiveCount"] == "1", data
assert msg["ReceiptHandle"], data
PY
  RECEIPT_HANDLE="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["Messages"][0]["ReceiptHandle"])' "${TMP_DIR}/receive.json")"

  sqs_json ReceiveMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"MaxNumberOfMessages\":1,\"WaitTimeSeconds\":0}" |
    json_assert 'not data.get("Messages")'

  sleep 3
  sqs_json ReceiveMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"MaxNumberOfMessages\":1,\"VisibilityTimeout\":2,\"AttributeNames\":[\"All\"]}" > "${TMP_DIR}/receive-again.json"
  RECEIPT_HANDLE="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); assert int(data["Messages"][0]["Attributes"]["ApproximateReceiveCount"]) >= 2; print(data["Messages"][0]["ReceiptHandle"])' "${TMP_DIR}/receive-again.json")"

  sqs_json ChangeMessageVisibility "{\"QueueUrl\":\"${QUEUE_URL}\",\"ReceiptHandle\":\"${RECEIPT_HANDLE}\",\"VisibilityTimeout\":0}" >/dev/null
  sqs_json ReceiveMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"MaxNumberOfMessages\":1,\"VisibilityTimeout\":2}" > "${TMP_DIR}/receive-after-change.json"
  RECEIPT_HANDLE="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["Messages"][0]["ReceiptHandle"])' "${TMP_DIR}/receive-after-change.json")"
  sqs_json DeleteMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"ReceiptHandle\":\"${RECEIPT_HANDLE}\"}" >/dev/null
  sleep 3
  sqs_json ReceiveMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"MaxNumberOfMessages\":1,\"WaitTimeSeconds\":0}" |
    json_assert 'not data.get("Messages")'
}

exercise_batch_and_purge() {
  sqs_json SendMessageBatch "{
    \"QueueUrl\":\"${QUEUE_URL}\",
    \"Entries\":[
      {\"Id\":\"a\",\"MessageBody\":\"batch-a\"},
      {\"Id\":\"b\",\"MessageBody\":\"batch-b\"}
    ]
  }" | json_assert 'len(data["Successful"]) == 2'

  sqs_json ReceiveMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"MaxNumberOfMessages\":2,\"VisibilityTimeout\":30}" > "${TMP_DIR}/batch-receive.json"
  BATCH_RECEIPTS_JSON="$(python3 - <<'PY' "${TMP_DIR}/batch-receive.json"
import json, sys
data = json.load(open(sys.argv[1]))
entries = [
    {"Id": str(i), "ReceiptHandle": msg["ReceiptHandle"]}
    for i, msg in enumerate(data["Messages"], 1)
]
assert len(entries) == 2, data
print(json.dumps(entries))
PY
)"
  sqs_json DeleteMessageBatch "{\"QueueUrl\":\"${QUEUE_URL}\",\"Entries\":${BATCH_RECEIPTS_JSON}}" |
    json_assert 'len(data["Successful"]) == 2'

  sqs_json SendMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"MessageBody\":\"purge me\"}" >/dev/null
  sqs_json PurgeQueue "{\"QueueUrl\":\"${QUEUE_URL}\"}" >/dev/null
  sqs_json ReceiveMessage "{\"QueueUrl\":\"${QUEUE_URL}\",\"MaxNumberOfMessages\":1,\"WaitTimeSeconds\":0}" |
    json_assert 'not data.get("Messages")'
}

exercise_fifo() {
  FIFO_QUEUE_URL="$(sqs_json CreateQueue "{
    \"QueueName\":\"${FIFO_QUEUE}\",
    \"Attributes\":{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"true\",\"VisibilityTimeout\":\"30\"}
  }" | json_value 'data["QueueUrl"]')"

  sqs_json SendMessage "{
    \"QueueUrl\":\"${FIFO_QUEUE_URL}\",
    \"MessageBody\":\"fifo-one\",
    \"MessageGroupId\":\"group-a\"
  }" | json_assert 'data["MessageId"] and data["SequenceNumber"]'
  sqs_json SendMessage "{
    \"QueueUrl\":\"${FIFO_QUEUE_URL}\",
    \"MessageBody\":\"fifo-one\",
    \"MessageGroupId\":\"group-a\"
  }" | json_assert 'data["MessageId"]'

  sqs_json ReceiveMessage "{\"QueueUrl\":\"${FIFO_QUEUE_URL}\",\"MaxNumberOfMessages\":10}" |
    json_assert 'len(data.get("Messages", [])) == 1 and data["Messages"][0]["Body"] == "fifo-one"'
}

exercise_dlq() {
  DLQ_URL="$(sqs_json CreateQueue "{\"QueueName\":\"${DLQ}\"}" | json_value 'data["QueueUrl"]')"
  local dlq_arn
  dlq_arn="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${DLQ}"
  local redrive_policy
  redrive_policy="$(python3 - <<'PY' "${dlq_arn}"
import json, sys
print(json.dumps({"deadLetterTargetArn": sys.argv[1], "maxReceiveCount": "1"}))
PY
)"
  SOURCE_QUEUE_URL="$(sqs_json CreateQueue "{
    \"QueueName\":\"${SOURCE_QUEUE}\",
    \"Attributes\":{\"VisibilityTimeout\":\"0\",\"RedrivePolicy\":$(json_quote "${redrive_policy}")}
  }" | json_value 'data["QueueUrl"]')"

  sqs_json SendMessage "{\"QueueUrl\":\"${SOURCE_QUEUE_URL}\",\"MessageBody\":\"move to dlq\"}" >/dev/null
  sqs_json ReceiveMessage "{\"QueueUrl\":\"${SOURCE_QUEUE_URL}\",\"MaxNumberOfMessages\":1,\"WaitTimeSeconds\":0}" |
    json_assert 'data["Messages"][0]["Body"] == "move to dlq"'
  sqs_json ReceiveMessage "{\"QueueUrl\":\"${SOURCE_QUEUE_URL}\",\"MaxNumberOfMessages\":1,\"WaitTimeSeconds\":0}" |
    json_assert 'not data.get("Messages")'
  sqs_json ReceiveMessage "{\"QueueUrl\":\"${DLQ_URL}\",\"MaxNumberOfMessages\":1,\"WaitTimeSeconds\":0}" |
    json_assert 'data["Messages"][0]["Body"] == "move to dlq"'
  sqs_json ListDeadLetterSourceQueues "{\"QueueUrl\":\"${DLQ_URL}\"}" |
    json_assert 'any(url.endswith("/'"${SOURCE_QUEUE}"'") for url in data["QueueUrls"])'
}

exercise_dashboard() {
  wait_for_http "${DASHBOARD_ENDPOINT}/"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" |
    grep -q '"id"[[:space:]]*:[[:space:]]*"sqs"'
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/sqs" |
    grep -q 'devcloud Dashboard'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sqs/status" |
    grep -q 'sqs'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sqs/queues" |
    grep -q "${QUEUE}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sqs/queues/${QUEUE}" |
    grep -q "${QUEUE}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sqs/queues/${QUEUE}/messages" >/dev/null
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sqs/queues/${QUEUE}/leases" >/dev/null
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sqs/queues/${DLQ}/dlq" |
    grep -q "${SOURCE_QUEUE}"
}

delete_queue_if_needed() {
  local queue_url="$1"
  if [[ "${DELETE_DATA}" == "true" && -n "${queue_url}" ]]; then
    sqs_json DeleteQueue "{\"QueueUrl\":\"${queue_url}\"}" >/dev/null || true
  fi
}

delete_created_queues() {
  delete_queue_if_needed "${QUEUE_URL}"
  delete_queue_if_needed "${QUERY_QUEUE_URL}"
  delete_queue_if_needed "${FIFO_QUEUE_URL}"
  delete_queue_if_needed "${SOURCE_QUEUE_URL}"
  delete_queue_if_needed "${DLQ_URL}"
}

print_interactive_info() {
  cat <<EOF

[sqs-e2e] interactive mode
  SQS endpoint:       ${SQS_ENDPOINT}
  Dashboard:          ${DASHBOARD_ENDPOINT}/dashboard/sqs
  Workspace:          ${WORKSPACE}
  Standard queue URL: ${QUEUE_URL}

Example:
  curl -sS -X POST \\
    -H 'Content-Type: application/x-amz-json-1.0' \\
    -H 'X-Amz-Target: AmazonSQS.ListQueues' \\
    --data '{}' \\
    ${SQS_ENDPOINT}/

Press Ctrl-C to stop devcloud.
EOF
  while kill -0 "${DEV_PID}" 2>/dev/null; do
    sleep 3600
  done
}

main() {
  require_command curl
  require_command python3
  assign_ports

  TMP_DIR="$(mktemp -d)"
  WORKSPACE="${TMP_DIR}/workspace"
  mkdir -p "${WORKSPACE}"
  write_config "${WORKSPACE}"

  log "building devcloud"
  go build -o "${TMP_DIR}/devcloud" ./cmd/devcloud

  log "starting devcloud"
  (
    cd "${WORKSPACE}"
    "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"

  wait_for_sqs
  wait_for_http "${DASHBOARD_ENDPOINT}/"

  log "exercising queue core"
  create_queue
  exercise_queue_core
  exercise_query_protocol

  log "exercising message lifecycle"
  exercise_message_lifecycle
  exercise_batch_and_purge

  log "exercising FIFO and DLQ flows"
  exercise_fifo
  exercise_dlq

  log "checking dashboard"
  exercise_dashboard

  delete_created_queues

  if [[ "${INTERACTIVE}" == "true" ]]; then
    print_interactive_info
  fi

  log "SQS E2E passed"
}

main "$@"
