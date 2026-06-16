#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
VERIFY_HOST="127.0.0.1"

free_port() {
  echo $((20000 + RANDOM % 40000))
}

PUBSUB_GRPC_VERIFY_PORT="${PUBSUB_GRPC_VERIFY_PORT:-$(free_port)}"
PUBSUB_REST_VERIFY_PORT="${PUBSUB_REST_VERIFY_PORT:-$(free_port)}"
GCS_VERIFY_PORT="${GCS_VERIFY_PORT:-$(free_port)}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-$(free_port)}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-$(free_port)}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-$(free_port)}"
EVENT_RELAY_VERIFY_PORT="${EVENT_RELAY_VERIFY_PORT:-$(free_port)}"
APP_AUTOSCALING_VERIFY_PORT="${APP_AUTOSCALING_VERIFY_PORT:-$(free_port)}"
REDIS_HTTP_VERIFY_PORT="${REDIS_HTTP_VERIFY_PORT:-$(free_port)}"
DYNAMODB_VERIFY_PORT="${DYNAMODB_VERIFY_PORT:-$(free_port)}"
BIGQUERY_VERIFY_PORT="${BIGQUERY_VERIFY_PORT:-$(free_port)}"
SQS_VERIFY_PORT="${SQS_VERIFY_PORT:-$(free_port)}"
PUBSUB_REST_ENDPOINT="http://${VERIFY_HOST}:${PUBSUB_REST_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"
PROJECT="${PUBSUB_VERIFY_PROJECT:-devcloud}"
TOPIC="${PUBSUB_VERIFY_TOPIC:-devcloud-pubsub-loop-topic}"
SECOND_TOPIC="${PUBSUB_VERIFY_SECOND_TOPIC:-devcloud-pubsub-loop-dlq}"
SUBSCRIPTION="${PUBSUB_VERIFY_SUBSCRIPTION:-devcloud-pubsub-loop-sub}"

export PUBSUB_EMULATOR_HOST="${PUBSUB_EMULATOR_HOST:-${VERIFY_HOST}:${PUBSUB_GRPC_VERIFY_PORT}}"
export PUBSUB_PROJECT_ID="${PUBSUB_PROJECT_ID:-${PROJECT}}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-pubsub-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-pubsub-verify.err"
ACK_ID=""

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

loopback_bind_available() {
  python3 - <<'PY' >/dev/null 2>&1
import socket

sock = socket.socket()
try:
    sock.bind(("127.0.0.1", 0))
finally:
    sock.close()
PY
}

skip_runtime_checks_without_loopback() {
  if loopback_bind_available; then
    return 1
  fi
  echo "[SKIP] loopback TCP bind unavailable; skipping Pub/Sub runtime smoke checks"
  return 0
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 12))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[pubsub-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[pubsub-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    sleep 0.2
  done
}

pubsub_rest() {
  local method="$1"
  local path="$2"
  local payload="${3:-}"
  if [[ -n "${payload}" ]]; then
    curl -fsS -X "${method}" \
      -H "Content-Type: application/json" \
      --data "${payload}" \
      "${PUBSUB_REST_ENDPOINT}${path}"
  else
    curl -fsS -X "${method}" "${PUBSUB_REST_ENDPOINT}${path}"
  fi
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: pubsub-e2e

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  eventRelayPort: ${EVENT_RELAY_VERIFY_PORT}
  appAutoScalingPort: ${APP_AUTOSCALING_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}
  gcsPort: ${GCS_VERIFY_PORT}
  redisHttpPort: ${REDIS_HTTP_VERIFY_PORT}
  dynamodbPort: ${DYNAMODB_VERIFY_PORT}
  bigQueryPort: ${BIGQUERY_VERIFY_PORT}
  sqsPort: ${SQS_VERIFY_PORT}
  pubsubGrpcPort: ${PUBSUB_GRPC_VERIFY_PORT}
  pubsubRestPort: ${PUBSUB_REST_VERIFY_PORT}

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
  pubsub:
    mode: relaxed
    projectID: ${PROJECT}

storage:
  path: .devcloud/data

services:
  mail:
    enabled: false
    maxMessageBytes: 10485760
  s3:
    enabled: false
    region: us-east-1
  gcs:
    enabled: false
    project: devcloud
    location: US
  dynamodb:
    enabled: false
    region: us-east-1
  bigquery:
    enabled: false
    project: devcloud
    location: US
  redshift:
    enabled: false
  sqs:
    enabled: false
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
  pubsub:
    enabled: true
    project: ${PROJECT}
    defaultAckDeadlineSeconds: 2
    messageRetentionSeconds: 604800
    maxAckDeadlineSeconds: 600
    maxPullMessages: 1000
    enableREST: true
    enableStreamingPull: true
    enablePush: false
EOF

  local failures_before_build="${FAIL}"
  run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
  if [[ "${FAIL}" -gt "${failures_before_build}" ]]; then
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
    wait_for_http "${PUBSUB_REST_ENDPOINT}/v1/projects/${PROJECT}/topics"
  fi
}

assert_pubsub_design_contract() {
  test -f docs/design-pubsub-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Google Cloud Pub/Sub Compatibility Design|PUBSUB_EMULATOR_HOST|StreamingPull|ModifyAckDeadline|DeadLetterPolicy|AC-001' docs/design-pubsub-compat.md
}

assert_script_contract() {
  bash -n scripts/pubsub-autoloop/bootstrap.sh &&
    bash -n scripts/pubsub-autoloop/run-loop.sh &&
    bash -n scripts/pubsub-autoloop/recover.sh &&
    bash -n scripts/pubsub-autoloop/verify.sh &&
    bash -n scripts/pubsub-e2e.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS: READY' scripts/pubsub-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY' scripts/pubsub-autoloop/run-loop.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'mktemp .*state.env|mv .*state.env|shasum -a 256' scripts/pubsub-autoloop/bootstrap.sh scripts/pubsub-autoloop/recover.sh scripts/pubsub-autoloop/run-loop.sh
}

assert_pubsub_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'pubsubGrpcPort|pubsubRestPort|services\\.pubsub|auth\\.pubsub|Pub/Sub|pubsub' rust docs &&
    cargo test --workspace
}

pubsub_endpoint_starts() {
  ensure_started
}

create_topic() {
  pubsub_rest PUT "/v1/projects/${PROJECT}/topics/${TOPIC}" '{}' |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"projects/${PROJECT}/topics/${TOPIC}\""
}

create_second_topic() {
  pubsub_rest PUT "/v1/projects/${PROJECT}/topics/${SECOND_TOPIC}" '{}' |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"projects/${PROJECT}/topics/${SECOND_TOPIC}\""
}

get_topic() {
  pubsub_rest GET "/v1/projects/${PROJECT}/topics/${TOPIC}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"projects/${PROJECT}/topics/${TOPIC}\""
}

list_topics() {
  pubsub_rest GET "/v1/projects/${PROJECT}/topics" |
    grep -q "${TOPIC}"
}

create_subscription() {
  pubsub_rest PUT "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}" "{
    \"topic\":\"projects/${PROJECT}/topics/${TOPIC}\",
    \"ackDeadlineSeconds\":2
  }" | grep -q "\"name\"[[:space:]]*:[[:space:]]*\"projects/${PROJECT}/subscriptions/${SUBSCRIPTION}\""
}

get_subscription() {
  pubsub_rest GET "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}" |
    grep -q "\"topic\"[[:space:]]*:[[:space:]]*\"projects/${PROJECT}/topics/${TOPIC}\""
}

list_subscriptions() {
  pubsub_rest GET "/v1/projects/${PROJECT}/subscriptions" |
    grep -q "${SUBSCRIPTION}"
}

list_topic_subscriptions() {
  pubsub_rest GET "/v1/projects/${PROJECT}/topics/${TOPIC}/subscriptions" |
    grep -q "projects/${PROJECT}/subscriptions/${SUBSCRIPTION}"
}

grpc_client_smoke() {
  cargo test -p devcloud-pubsub --test grpc_conformance
}

publish_message() {
  pubsub_rest POST "/v1/projects/${PROJECT}/topics/${TOPIC}:publish" '{
    "messages":[
      {
        "data":"ZGV2Y2xvdWQgcHVic3ViIGxvb3AgbWVzc2FnZQ==",
        "attributes":{"kind":"loop"},
        "orderingKey":"group-1"
      }
    ]
  }' | grep -q '"messageIds"'
}

pull_message() {
  pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:pull" '{"maxMessages":1}' > "${VERIFY_OUT}"
  ACK_ID="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); print(data["receivedMessages"][0]["ackId"])' "${VERIFY_OUT}")"
  grep -q 'ZGV2Y2xvdWQgcHVic3ViIGxvb3AgbWVzc2FnZQ==' "${VERIFY_OUT}"
}

message_is_invisible_before_ack_deadline() {
  pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:pull" '{"maxMessages":1}' > "${VERIFY_OUT}"
  ! grep -q 'receivedMessages' "${VERIFY_OUT}"
}

message_reappears_after_ack_deadline() {
  sleep 3
  pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:pull" '{"maxMessages":1}' > "${VERIFY_OUT}"
  ACK_ID="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); print(data["receivedMessages"][0]["ackId"])' "${VERIFY_OUT}")"
  grep -q 'deliveryAttempt' "${VERIFY_OUT}"
}

modify_ack_deadline_releases_message() {
  pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:modifyAckDeadline" "{
    \"ackIds\":[\"${ACK_ID}\"],
    \"ackDeadlineSeconds\":0
  }" >/dev/null
  pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:pull" '{"maxMessages":1}' > "${VERIFY_OUT}"
  ACK_ID="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); print(data["receivedMessages"][0]["ackId"])' "${VERIFY_OUT}")"
  grep -q 'receivedMessages' "${VERIFY_OUT}"
}

acknowledge_message() {
  pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:acknowledge" "{
    \"ackIds\":[\"${ACK_ID}\"]
  }" >/dev/null
}

acked_message_not_received() {
  sleep 3
  pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:pull" '{"maxMessages":1}' > "${VERIFY_OUT}"
  ! grep -q 'receivedMessages' "${VERIFY_OUT}"
}

subscription_metadata_accepts_advanced_fields() {
  pubsub_rest PUT "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}-advanced" "{
    \"topic\":\"projects/${PROJECT}/topics/${TOPIC}\",
    \"ackDeadlineSeconds\":10,
    \"enableMessageOrdering\":true,
    \"deadLetterPolicy\":{\"deadLetterTopic\":\"projects/${PROJECT}/topics/${SECOND_TOPIC}\",\"maxDeliveryAttempts\":5},
    \"retryPolicy\":{\"minimumBackoff\":\"1s\",\"maximumBackoff\":\"10s\"},
    \"pushConfig\":{\"pushEndpoint\":\"http://127.0.0.1:65535/push\"}
  }" | grep -q 'enableMessageOrdering'
}

dashboard_starts() {
  ensure_started &&
    wait_for_http "${DASHBOARD_ENDPOINT}/"
}

dashboard_service_registry_has_pubsub() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" |
    grep -q '"id"[[:space:]]*:[[:space:]]*"pubsub"'
}

dashboard_pubsub_page_loads() {
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/pubsub" |
    grep -q 'devcloud Dashboard'
}

dashboard_pubsub_api_lists_topics() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/pubsub/topics" |
    grep -q "${TOPIC}"
}

run_foundation_checks() {
  run_check "Pub/Sub design contract exists" assert_pubsub_design_contract
  run_check "Pub/Sub autoloop script contract" assert_script_contract
  run_check "repository tests pass" cargo test --workspace
  run_check "devcloud help works" cargo run -p devcloud-orchestrator -- help
}

run_config_checks() {
  run_check "Pub/Sub config shape" assert_pubsub_config_shape
}

run_resource_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Pub/Sub REST endpoint starts" pubsub_endpoint_starts
  run_check "CreateTopic works" create_topic
  run_check "Create dead-letter topic works" create_second_topic
  run_check "GetTopic works" get_topic
  run_check "ListTopics includes topic" list_topics
  run_check "CreateSubscription works" create_subscription
  run_check "GetSubscription works" get_subscription
  run_check "ListSubscriptions includes subscription" list_subscriptions
  run_check "ListTopicSubscriptions works" list_topic_subscriptions
  run_check "Pub/Sub gRPC client smoke works" grpc_client_smoke
}

run_message_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Publish works" publish_message
  run_check "Pull works" pull_message
  run_check "leased message is invisible before ack deadline" message_is_invisible_before_ack_deadline
  run_check "ModifyAckDeadline releases message" modify_ack_deadline_releases_message
  run_check "Acknowledge works" acknowledge_message
  run_check "acked message is not received again" acked_message_not_received
}

run_scheduler_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Publish works for scheduler check" publish_message
  run_check "Pull works for scheduler check" pull_message
  run_check "ack deadline requeues message" message_reappears_after_ack_deadline
}

run_advanced_metadata_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "advanced subscription metadata accepted" subscription_metadata_accepts_advanced_fields
}

run_dashboard_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "dashboard starts" dashboard_starts
  run_check "dashboard service registry has Pub/Sub" dashboard_service_registry_has_pubsub
  run_check "dashboard Pub/Sub page loads" dashboard_pubsub_page_loads
  run_check "dashboard Pub/Sub API lists topics" dashboard_pubsub_api_lists_topics
}

run_e2e_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Pub/Sub standalone E2E script passes" bash scripts/pubsub-e2e.sh
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  config)
    run_foundation_checks
    run_config_checks
    ;;
  resource)
    run_foundation_checks
    run_config_checks
    run_resource_checks
    ;;
  message)
    run_foundation_checks
    run_config_checks
    run_resource_checks
    run_message_checks
    ;;
  scheduler)
    run_foundation_checks
    run_config_checks
    run_resource_checks
    run_scheduler_checks
    ;;
  dashboard)
    run_foundation_checks
    run_config_checks
    run_resource_checks
    run_message_checks
    run_advanced_metadata_checks
    run_dashboard_checks
    ;;
  e2e)
    run_foundation_checks
    run_e2e_checks
    ;;
  full)
    run_foundation_checks
    run_config_checks
    run_resource_checks
    run_message_checks
    run_scheduler_checks
    run_advanced_metadata_checks
    run_dashboard_checks
    run_e2e_checks
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Pub/Sub autoloop verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
