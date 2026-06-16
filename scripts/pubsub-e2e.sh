#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

HOST="127.0.0.1"

free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

PUBSUB_GRPC_PORT="${PUBSUB_GRPC_PORT:-$(free_port)}"
PUBSUB_REST_PORT="${PUBSUB_REST_PORT:-$(free_port)}"
DASHBOARD_PORT="${DASHBOARD_PORT:-$(free_port)}"
EVENT_RELAY_PORT="${EVENT_RELAY_PORT:-$(free_port)}"
APP_AUTOSCALING_PORT="${APP_AUTOSCALING_PORT:-$(free_port)}"
REDIS_HTTP_PORT="${REDIS_HTTP_PORT:-$(free_port)}"
SMTP_PORT="${SMTP_PORT:-$(free_port)}"
S3_PORT="${S3_PORT:-$(free_port)}"
GCS_PORT="${GCS_PORT:-$(free_port)}"
DYNAMODB_PORT="${DYNAMODB_PORT:-$(free_port)}"
BIGQUERY_PORT="${BIGQUERY_PORT:-$(free_port)}"
SQS_PORT="${SQS_PORT:-$(free_port)}"
PROJECT="${PUBSUB_PROJECT_ID:-devcloud}"
TOPIC="${PUBSUB_E2E_TOPIC:-devcloud-pubsub-e2e-topic}"
SECOND_TOPIC="${PUBSUB_E2E_SECOND_TOPIC:-devcloud-pubsub-e2e-second-topic}"
SUBSCRIPTION="${PUBSUB_E2E_SUBSCRIPTION:-devcloud-pubsub-e2e-sub}"
RELEASE_SUBSCRIPTION="${PUBSUB_E2E_RELEASE_SUBSCRIPTION:-devcloud-pubsub-e2e-release-sub}"
REST_ENDPOINT="http://${HOST}:${PUBSUB_REST_PORT}"
DASHBOARD_ENDPOINT="http://${HOST}:${DASHBOARD_PORT}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"

TMP_DIR=""
DEV_PID=""
ACK_ID=""
MESSAGE_ID=""

usage() {
  cat <<'EOF'
Usage:
  scripts/pubsub-e2e.sh

Environment:
  PUBSUB_GRPC_PORT=18085                 Override the Pub/Sub gRPC port.
  PUBSUB_REST_PORT=18086                 Override the Pub/Sub REST port.
  DASHBOARD_PORT=18025                   Override the dashboard port.
  EVENT_RELAY_PORT=18027                 Override the dashboard event relay port.
  APP_AUTOSCALING_PORT=18030             Override the Application Auto Scaling port.
  REDIS_HTTP_PORT=16380                  Override the Redis control HTTP port.
  PUBSUB_PROJECT_ID=devcloud             Override the Pub/Sub project id.
  PUBSUB_E2E_TOPIC=devcloud-topic        Override the primary topic name.
  PUBSUB_E2E_SUBSCRIPTION=devcloud-sub   Override the primary subscription name.
  E2E_KEEP_WORKDIR=true                  Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true                   Keep devcloud running after assertions.

Examples:
  scripts/pubsub-e2e.sh
  E2E_KEEP_WORKDIR=true scripts/pubsub-e2e.sh
  PUBSUB_REST_PORT=18086 DASHBOARD_PORT=18025 scripts/pubsub-e2e.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${INTERACTIVE}" == "true" && -z "${E2E_KEEP_WORKDIR+x}" ]]; then
  KEEP_WORKDIR="true"
fi

log() {
  printf '[pubsub-e2e] %s\n' "$1"
}

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "[pubsub-e2e] missing command: ${name}" >&2
    exit 1
  fi
}

json_value() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data, "any": any, "len": len}))' "${expression}"
}

json_assert() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data, "any": any, "len": len}), data' "${expression}"
}

cleanup() {
  if [[ -n "${DEV_PID}" ]]; then
    if [[ "${INTERACTIVE}" == "true" ]]; then
      log "devcloud still running: pid=${DEV_PID}"
    else
      kill "${DEV_PID}" >/dev/null 2>&1 || true
      wait "${DEV_PID}" >/dev/null 2>&1 || true
    fi
  fi
  if [[ "${KEEP_WORKDIR}" != "true" && "${INTERACTIVE}" != "true" && -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  elif [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    log "kept workdir: ${TMP_DIR}"
  fi
}
trap cleanup EXIT

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[FAIL] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[pubsub-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      echo "[FAIL] timed out waiting for ${url}" >&2
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
      "${REST_ENDPOINT}${path}"
  else
    curl -fsS -X "${method}" "${REST_ENDPOINT}${path}"
  fi
}

for command in curl go grep python3; do
  require_command "${command}"
done

TMP_DIR="$(mktemp -d)"
mkdir -p "${TMP_DIR}/.devcloud"
cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: pubsub-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  eventRelayPort: ${EVENT_RELAY_PORT}
  appAutoScalingPort: ${APP_AUTOSCALING_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}
  redisHttpPort: ${REDIS_HTTP_PORT}
  dynamodbPort: ${DYNAMODB_PORT}
  bigQueryPort: ${BIGQUERY_PORT}
  sqsPort: ${SQS_PORT}
  pubsubGrpcPort: ${PUBSUB_GRPC_PORT}
  pubsubRestPort: ${PUBSUB_REST_PORT}

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

log "building devcloud"
devcloud_build "${TMP_DIR}/devcloud"
log "starting devcloud on REST=${REST_ENDPOINT} dashboard=${DASHBOARD_ENDPOINT}"
(
  cd "${TMP_DIR}"
  "${TMP_DIR}/devcloud" up
) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

wait_for_http "${REST_ENDPOINT}/readyz"
wait_for_http "${DASHBOARD_ENDPOINT}/api/dashboard/services"

SERVICES_OUT="${TMP_DIR}/dashboard-services.json"
curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" > "${SERVICES_OUT}"
json_assert "any(service['id'] == 'pubsub' and service['status'] == 'running' and service['path'] == '/dashboard/pubsub' for service in data['services'])" < "${SERVICES_OUT}"

DASHBOARD_HTML="${TMP_DIR}/dashboard-pubsub.html"
curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/pubsub" > "${DASHBOARD_HTML}"
grep -q '<div id="root"></div>' "${DASHBOARD_HTML}"
grep -q '/dashboard/assets/' "${DASHBOARD_HTML}"

log "creating topics and subscriptions"
CREATE_TOPIC_OUT="${TMP_DIR}/create-topic.json"
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data "{\"topicId\":\"${TOPIC}\"}" \
  "${DASHBOARD_ENDPOINT}/api/pubsub/topics" > "${CREATE_TOPIC_OUT}"
json_assert "data['name'] == 'projects/${PROJECT}/topics/${TOPIC}'" < "${CREATE_TOPIC_OUT}"

CREATE_SECOND_TOPIC_OUT="${TMP_DIR}/create-second-topic.json"
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data "{\"topicId\":\"${SECOND_TOPIC}\"}" \
  "${DASHBOARD_ENDPOINT}/api/pubsub/topics" > "${CREATE_SECOND_TOPIC_OUT}"
json_assert "data['name'] == 'projects/${PROJECT}/topics/${SECOND_TOPIC}'" < "${CREATE_SECOND_TOPIC_OUT}"

CREATE_SUB_OUT="${TMP_DIR}/create-subscription.json"
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data "{
  \"subscriptionId\":\"${SUBSCRIPTION}\",
  \"topicId\":\"${TOPIC}\",
  \"ackDeadlineSeconds\":2
}" \
  "${DASHBOARD_ENDPOINT}/api/pubsub/subscriptions" > "${CREATE_SUB_OUT}"
json_assert "data['name'] == 'projects/${PROJECT}/subscriptions/${SUBSCRIPTION}' and data['topic'] == 'projects/${PROJECT}/topics/${TOPIC}'" < "${CREATE_SUB_OUT}"

CREATE_RELEASE_SUB_OUT="${TMP_DIR}/create-release-subscription.json"
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data "{
  \"subscriptionId\":\"${RELEASE_SUBSCRIPTION}\",
  \"topicId\":\"${TOPIC}\",
  \"ackDeadlineSeconds\":2
}" \
  "${DASHBOARD_ENDPOINT}/api/pubsub/subscriptions" > "${CREATE_RELEASE_SUB_OUT}"
json_assert "data['name'] == 'projects/${PROJECT}/subscriptions/${RELEASE_SUBSCRIPTION}'" < "${CREATE_RELEASE_SUB_OUT}"

TOPICS_PAGE_ONE="${TMP_DIR}/topics-page-one.json"
pubsub_rest GET "/v1/projects/${PROJECT}/topics?pageSize=1" > "${TOPICS_PAGE_ONE}"
TOPIC_PAGE_TOKEN="$(json_value "data['nextPageToken']" < "${TOPICS_PAGE_ONE}")"
json_assert "len(data['topics']) == 1 and data['nextPageToken'] != ''" < "${TOPICS_PAGE_ONE}"
TOPICS_PAGE_TWO="${TMP_DIR}/topics-page-two.json"
pubsub_rest GET "/v1/projects/${PROJECT}/topics?pageSize=1&pageToken=${TOPIC_PAGE_TOKEN}" > "${TOPICS_PAGE_TWO}"
json_assert "len(data['topics']) >= 1" < "${TOPICS_PAGE_TWO}"

TOPIC_SUBSCRIPTIONS_OUT="${TMP_DIR}/topic-subscriptions.json"
pubsub_rest GET "/v1/projects/${PROJECT}/topics/${TOPIC}/subscriptions?pageSize=1" > "${TOPIC_SUBSCRIPTIONS_OUT}"
json_assert "len(data['subscriptions']) == 1 and data['nextPageToken'] != ''" < "${TOPIC_SUBSCRIPTIONS_OUT}"

SUBSCRIPTIONS_OUT="${TMP_DIR}/subscriptions.json"
pubsub_rest GET "/v1/projects/${PROJECT}/subscriptions" > "${SUBSCRIPTIONS_OUT}"
json_assert "any(subscription['name'] == 'projects/${PROJECT}/subscriptions/${SUBSCRIPTION}' for subscription in data['subscriptions'])" < "${SUBSCRIPTIONS_OUT}"

log "publishing and inspecting via dashboard API"
PUBLISH_OUT="${TMP_DIR}/publish.json"
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data '{
  "messages":[{"data":"ZGV2Y2xvdWQgcHVic3ViIGUyZQ==","attributes":{"source":"e2e"}}]
}' \
  "${DASHBOARD_ENDPOINT}/api/pubsub/topics/${TOPIC}/publish" > "${PUBLISH_OUT}"
MESSAGE_ID="$(json_value "data['messageIds'][0]" < "${PUBLISH_OUT}")"
if [[ -z "${MESSAGE_ID}" ]]; then
  echo "[FAIL] publish response did not include a message id" >&2
  exit 1
fi

DASHBOARD_STATUS="${TMP_DIR}/dashboard-pubsub-status.json"
curl -fsS "${DASHBOARD_ENDPOINT}/api/pubsub/status" > "${DASHBOARD_STATUS}"
json_assert "data['service'] == 'pubsub' and data['running'] == True and data['topicCount'] == 2 and data['subscriptionCount'] == 2" < "${DASHBOARD_STATUS}"

DASHBOARD_TOPICS="${TMP_DIR}/dashboard-pubsub-topics.json"
curl -fsS "${DASHBOARD_ENDPOINT}/api/pubsub/topics" > "${DASHBOARD_TOPICS}"
json_assert "any(topic['name'] == 'projects/${PROJECT}/topics/${TOPIC}' and topic['subscriptionCount'] == 2 for topic in data['topics'])" < "${DASHBOARD_TOPICS}"

DASHBOARD_TOPIC="${TMP_DIR}/dashboard-pubsub-topic.json"
curl -fsS "${DASHBOARD_ENDPOINT}/api/pubsub/topics/${TOPIC}" > "${DASHBOARD_TOPIC}"
json_assert "data['topic']['name'] == 'projects/${PROJECT}/topics/${TOPIC}' and data['topic']['subscriptionCount'] == 2" < "${DASHBOARD_TOPIC}"

DASHBOARD_SUBSCRIPTIONS="${TMP_DIR}/dashboard-pubsub-subscriptions.json"
curl -fsS "${DASHBOARD_ENDPOINT}/api/pubsub/subscriptions" > "${DASHBOARD_SUBSCRIPTIONS}"
json_assert "any(subscription['name'] == 'projects/${PROJECT}/subscriptions/${SUBSCRIPTION}' and subscription['backlogMessages'] == 1 for subscription in data['subscriptions'])" < "${DASHBOARD_SUBSCRIPTIONS}"

DASHBOARD_MESSAGE="${TMP_DIR}/dashboard-pubsub-message.json"
curl -fsS "${DASHBOARD_ENDPOINT}/api/pubsub/messages/${MESSAGE_ID}" > "${DASHBOARD_MESSAGE}"
json_assert "data['message']['messageId'] == '${MESSAGE_ID}' and len(data['message']['subscriptions']) == 2" < "${DASHBOARD_MESSAGE}"

PULL_OUT="${TMP_DIR}/pull.json"
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data '{"maxMessages":1}' \
  "${DASHBOARD_ENDPOINT}/api/pubsub/subscriptions/${SUBSCRIPTION}/pull" > "${PULL_OUT}"
ACK_ID="$(json_value "data['receivedMessages'][0]['ackId']" < "${PULL_OUT}")"
json_assert "data['receivedMessages'][0]['message']['data'] == 'ZGV2Y2xvdWQgcHVic3ViIGUyZQ==' and data['receivedMessages'][0]['deliveryAttempt'] == 1" < "${PULL_OUT}"

IN_FLIGHT_MESSAGE="${TMP_DIR}/dashboard-pubsub-message-in-flight.json"
curl -fsS "${DASHBOARD_ENDPOINT}/api/pubsub/messages/${MESSAGE_ID}" > "${IN_FLIGHT_MESSAGE}"
json_assert "any(item['subscription'] == 'projects/${PROJECT}/subscriptions/${SUBSCRIPTION}' and item['state'] == 'in-flight' for item in data['message']['subscriptions'])" < "${IN_FLIGHT_MESSAGE}"

curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data "{\"ackIds\":[\"${ACK_ID}\"],\"ackDeadlineSeconds\":0}" \
  "${DASHBOARD_ENDPOINT}/api/pubsub/subscriptions/${SUBSCRIPTION}/modifyAckDeadline" >/dev/null

REDELIVERY_OUT="${TMP_DIR}/redelivery.json"
pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:pull" '{"maxMessages":1}' > "${REDELIVERY_OUT}"
ACK_ID="$(json_value "data['receivedMessages'][0]['ackId']" < "${REDELIVERY_OUT}")"
json_assert "data['receivedMessages'][0]['deliveryAttempt'] == 2" < "${REDELIVERY_OUT}"

pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:acknowledge" "{
  \"ackIds\":[\"${ACK_ID}\"]
}" >/dev/null

EMPTY_PULL="${TMP_DIR}/empty-pull.json"
pubsub_rest POST "/v1/projects/${PROJECT}/subscriptions/${SUBSCRIPTION}:pull" '{"maxMessages":1}' > "${EMPTY_PULL}"
if grep -q 'receivedMessages' "${EMPTY_PULL}"; then
  echo "[FAIL] acknowledged message was received again" >&2
  exit 1
fi

if [[ "${INTERACTIVE}" == "true" ]]; then
  log "interactive mode: dashboard=${DASHBOARD_ENDPOINT}/dashboard/pubsub"
  while kill -0 "${DEV_PID}" 2>/dev/null; do
    sleep 1
  done
fi

echo "[OK] Pub/Sub E2E passed"
