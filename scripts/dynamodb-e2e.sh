#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION}}"
export AWS_DEFAULT_OUTPUT="${AWS_DEFAULT_OUTPUT:-json}"
export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"
unset AWS_SESSION_TOKEN

AWS_BIN="${AWS_BIN:-aws}"
DYNAMODB_PORT="${E2E_DYNAMODB_PORT:-}"
GCS_PORT="${E2E_GCS_PORT:-}"
S3_PORT="${E2E_S3_PORT:-}"
SMTP_PORT="${E2E_SMTP_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
DYNAMODB_ENDPOINT=""
DASHBOARD_ENDPOINT=""
TABLE="${E2E_TABLE:-DevcloudDynamoE2E$(date +%s)}"
GSI_TABLE="${E2E_GSI_TABLE:-DevcloudDynamoGsiE2E$(date +%s)}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"
DYNAMODB_AUTH_MODE="${E2E_DYNAMODB_AUTH_MODE:-relaxed}"

usage() {
  cat <<'EOF'
Usage:
  scripts/dynamodb-e2e.sh

Environment:
  AWS_BIN=aws                         AWS CLI executable. Defaults to aws.
  E2E_DYNAMODB_PORT=18000             Override the DynamoDB endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025            Override the dashboard port. Defaults to an available port.
  E2E_S3_PORT=14566                   Override the S3 endpoint port used by devcloud. Defaults to an available port.
  E2E_GCS_PORT=14443                  Override the GCS endpoint port used by devcloud. Defaults to an available port.
  E2E_SMTP_PORT=11025                 Override the Mail SMTP port used by devcloud. Defaults to an available port.
  E2E_DYNAMODB_AUTH_MODE=relaxed      DynamoDB auth mode: relaxed, signed-relaxed, or strict.
  E2E_TABLE=DevcloudDynamoE2E         Override the main test table name.
  E2E_GSI_TABLE=DevcloudDynamoGsiE2E  Override the GSI test table name.
  E2E_DELETE_DATA=false               Keep tables after assertions.
  E2E_KEEP_WORKDIR=true               Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true                Keep devcloud running and keep DynamoDB data after assertions.

Examples:
  scripts/dynamodb-e2e.sh
  E2E_DYNAMODB_AUTH_MODE=strict scripts/dynamodb-e2e.sh
  E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/dynamodb-e2e.sh
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

TMP_DIR=""
DEV_PID=""
WORKSPACE=""

log() {
  printf '[dynamodb-e2e] %s\n' "$1"
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
    echo "[dynamodb-e2e] missing command: ${name}" >&2
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
  if [[ -z "${DYNAMODB_PORT}" ]]; then
    DYNAMODB_PORT="$(find_free_port)"
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
  if [[ -z "${DASHBOARD_PORT}" ]]; then
    DASHBOARD_PORT="$(find_free_port)"
  fi
  DYNAMODB_ENDPOINT="http://127.0.0.1:${DYNAMODB_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

aws_cmd() {
  "${AWS_BIN}" --endpoint-url "${DYNAMODB_ENDPOINT}" "$@"
}

dynamodb_call() {
  local target="$1"
  local payload="$2"
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/x-amz-json-1.0' \
    -H "X-Amz-Target: DynamoDB_20120810.${target}" \
    --data "${payload}" \
    "${DYNAMODB_ENDPOINT}/"
}

json_assert() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data}), data' "${expression}"
}

wait_for_dynamodb() {
  local deadline=$((SECONDS + 20))
  until aws_cmd dynamodb list-tables >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[dynamodb-e2e] devcloud exited while waiting for DynamoDB" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[dynamodb-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[dynamodb-e2e] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[dynamodb-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
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
project: dynamodb-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}
  dynamodbPort: ${DYNAMODB_PORT}

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
    mode: ${DYNAMODB_AUTH_MODE}
    accessKeyId: ${AWS_ACCESS_KEY_ID}
    secretAccessKey: ${AWS_SECRET_ACCESS_KEY}

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
    region: ${AWS_REGION}
    billingMode: PAY_PER_REQUEST
    maxItemBytes: 400000
    maxTables: 256
    streams:
      enabled: true
    ttl:
      schedulerIntervalSeconds: 60
  bigquery:
    enabled: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
  redshift:
    enabled: false
EOF
}

create_main_table() {
  aws_cmd dynamodb create-table \
    --table-name "${TABLE}" \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES >/dev/null

  aws_cmd dynamodb describe-table --table-name "${TABLE}" |
    json_assert 'data["Table"]["TableStatus"] == "ACTIVE" and data["Table"]["TableName"]'
  aws_cmd dynamodb list-tables | grep -q "${TABLE}"
}

exercise_items() {
  aws_cmd dynamodb put-item \
    --table-name "${TABLE}" \
    --item '{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"age":{"N":"37"},"active":{"BOOL":true},"tags":{"SS":["engineer","admin"]},"meta":{"M":{"team":{"S":"core"}}},"scores":{"L":[{"N":"1"},{"N":"2"}]}}' >/dev/null

  aws_cmd dynamodb get-item \
    --table-name "${TABLE}" \
    --key '{"pk":{"S":"user#1"},"sk":{"S":"profile"}}' |
    json_assert 'data["Item"]["name"]["S"] == "Ada" and data["Item"]["active"]["BOOL"] is True'

  if aws_cmd dynamodb put-item \
    --table-name "${TABLE}" \
    --item '{"pk":{"S":"user#1"},"sk":{"S":"profile"}}' \
    --condition-expression 'attribute_not_exists(pk)' >/dev/null 2>"${TMP_DIR}/conditional.err"; then
    echo "[dynamodb-e2e] conditional put unexpectedly succeeded" >&2
    return 1
  fi
  grep -q 'ConditionalCheckFailedException' "${TMP_DIR}/conditional.err"

  aws_cmd dynamodb update-item \
    --table-name "${TABLE}" \
    --key '{"pk":{"S":"user#1"},"sk":{"S":"profile"}}' \
    --update-expression 'SET #n = :name, visits = :one' \
    --expression-attribute-names '{"#n":"name"}' \
    --expression-attribute-values '{":name":{"S":"Ada Lovelace"},":one":{"N":"1"}}' \
    --return-values ALL_NEW |
    json_assert 'data["Attributes"]["name"]["S"] == "Ada Lovelace" and data["Attributes"]["visits"]["N"] == "1"'
}

exercise_query_scan() {
  aws_cmd dynamodb put-item \
    --table-name "${TABLE}" \
    --item '{"pk":{"S":"user#1"},"sk":{"S":"event#001"},"kind":{"S":"event"},"active":{"BOOL":true}}' >/dev/null
  aws_cmd dynamodb put-item \
    --table-name "${TABLE}" \
    --item '{"pk":{"S":"user#1"},"sk":{"S":"event#002"},"kind":{"S":"event"},"active":{"BOOL":false}}' >/dev/null

  aws_cmd dynamodb query \
    --table-name "${TABLE}" \
    --key-condition-expression 'pk = :pk AND begins_with(sk, :prefix)' \
    --expression-attribute-values '{":pk":{"S":"user#1"},":prefix":{"S":"event#"}}' \
    --limit 1 |
    json_assert 'data["Count"] == 1 and "LastEvaluatedKey" in data'

  aws_cmd dynamodb scan \
    --table-name "${TABLE}" \
    --filter-expression 'active = :active' \
    --expression-attribute-values '{":active":{"BOOL":true}}' \
    --projection-expression 'pk, sk, active' |
    json_assert 'data["Count"] >= 1'
}

exercise_index_table() {
  aws_cmd dynamodb create-table \
    --table-name "${GSI_TABLE}" \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S AttributeName=gpk,AttributeType=S AttributeName=gsk,AttributeType=N \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --global-secondary-indexes '[{"IndexName":"gsi1","KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"},{"AttributeName":"gsk","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]' \
    --billing-mode PAY_PER_REQUEST >/dev/null

  aws_cmd dynamodb put-item \
    --table-name "${GSI_TABLE}" \
    --item '{"pk":{"S":"item#1"},"sk":{"S":"v1"},"gpk":{"S":"group#1"},"gsk":{"N":"10"}}' >/dev/null

  aws_cmd dynamodb query \
    --table-name "${GSI_TABLE}" \
    --index-name gsi1 \
    --key-condition-expression 'gpk = :gpk' \
    --expression-attribute-values '{":gpk":{"S":"group#1"}}' |
    json_assert 'data["Items"][0]["pk"]["S"] == "item#1"'
}

exercise_streams_ttl_tags() {
  aws_cmd dynamodb update-time-to-live \
    --table-name "${TABLE}" \
    --time-to-live-specification Enabled=true,AttributeName=expiresAt >/dev/null
  aws_cmd dynamodb describe-time-to-live --table-name "${TABLE}" |
    json_assert 'data["TimeToLiveDescription"]["TimeToLiveStatus"] == "ENABLED"'

  local table_arn stream_arn
  table_arn="$(aws_cmd dynamodb describe-table --table-name "${TABLE}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Table"]["TableArn"])')"
  stream_arn="$(dynamodb_call ListStreams "{\"TableName\":\"${TABLE}\"}" | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data["Streams"][0]["StreamArn"])')"

  dynamodb_call DescribeStream "{\"StreamArn\":\"${stream_arn}\"}" |
    json_assert 'data["StreamDescription"]["StreamStatus"] == "ENABLED"'

  aws_cmd dynamodb tag-resource \
    --resource-arn "${table_arn}" \
    --tags Key=env,Value=e2e Key=owner,Value=devcloud >/dev/null
  aws_cmd dynamodb list-tags-of-resource --resource-arn "${table_arn}" |
    json_assert 'sorted(tag["Key"] for tag in data["Tags"]) == ["env", "owner"]'
  aws_cmd dynamodb untag-resource --resource-arn "${table_arn}" --tag-keys owner >/dev/null
}

assert_dashboard_api() {
  local dashboard_table="${TABLE}Dashboard"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id":"dynamodb"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dynamodb/status" | grep -q '"running"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dynamodb/tables" | grep -q "${TABLE}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${TABLE}/items?limit=10" | grep -q 'Ada Lovelace'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${TABLE}/ttl" | grep -q 'ENABLED'

  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    --data "{\"input\":{\"TableName\":\"${dashboard_table}\",\"AttributeDefinitions\":[{\"AttributeName\":\"pk\",\"AttributeType\":\"S\"}],\"KeySchema\":[{\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"}],\"BillingMode\":\"PAY_PER_REQUEST\"}}" \
    "${DASHBOARD_ENDPOINT}/api/dynamodb/tables" |
    json_assert 'data["TableDescription"]["TableName"].endswith("Dashboard")'
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    --data '{"input":{"Item":{"pk":{"S":"dashboard#1"},"name":{"S":"dashboard-managed"}}}}' \
    "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${dashboard_table}/items" >/dev/null
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    --data '{"input":{"KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{":pk":{"S":"dashboard#1"}},"Limit":1}}' \
    "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${dashboard_table}/query" |
    json_assert 'data["Count"] == 1 and data["Items"][0]["name"]["S"] == "dashboard-managed"'
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    --data '{"input":{"FilterExpression":"#name = :name","ExpressionAttributeNames":{"#name":"name"},"ExpressionAttributeValues":{":name":{"S":"dashboard-managed"}},"Limit":5}}' \
    "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${dashboard_table}/scan" |
    json_assert 'data["ScannedCount"] == 1 and data["Items"][0]["pk"]["S"] == "dashboard#1"'
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    --data '{"input":{"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}}' \
    "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${dashboard_table}/ttl" |
    json_assert 'data["TimeToLiveSpecification"]["AttributeName"] == "expiresAt"'
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    --data "{\"input\":{\"Key\":{\"pk\":{\"S\":\"dashboard#1\"}}},\"confirmation\":\"${dashboard_table}\"}" \
    "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${dashboard_table}/items/delete" >/dev/null
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    --data "{\"input\":{},\"confirmation\":\"${dashboard_table}\"}" \
    "${DASHBOARD_ENDPOINT}/api/dynamodb/tables/${dashboard_table}/delete" >/dev/null

  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/dynamodb" | grep -qi 'devcloud Dashboard'
}

delete_data() {
  aws_cmd dynamodb delete-table --table-name "${GSI_TABLE}" >/dev/null 2>&1 || true
  aws_cmd dynamodb delete-table --table-name "${TABLE}" >/dev/null 2>&1 || true
}

run_dynamodb_journey() {
  create_main_table
  exercise_items
  exercise_query_scan
  exercise_index_table
  exercise_streams_ttl_tags
  assert_dashboard_api

  if [[ "${DELETE_DATA}" == "true" ]]; then
    delete_data
  fi
}

show_interactive_hint() {
  cat <<EOF
[dynamodb-e2e] browser check:
[dynamodb-e2e]   Dashboard: ${DASHBOARD_ENDPOINT}/dashboard/dynamodb
[dynamodb-e2e]   DynamoDB endpoint: ${DYNAMODB_ENDPOINT}
[dynamodb-e2e]   Table used: ${TABLE}
[dynamodb-e2e] AWS CLI examples:
[dynamodb-e2e]   AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} ${AWS_BIN} --endpoint-url ${DYNAMODB_ENDPOINT} dynamodb list-tables
[dynamodb-e2e]   ${AWS_BIN} --endpoint-url ${DYNAMODB_ENDPOINT} dynamodb scan --table-name ${TABLE}
EOF
}

show_retained_data() {
  if [[ "${DELETE_DATA}" != "false" ]]; then
    return 0
  fi
  log "retained DynamoDB tables: ${TABLE}, ${GSI_TABLE}"
  log "retained workspace: ${WORKSPACE}"
  aws_cmd dynamodb list-tables || true
}

require_command curl
require_command python3
require_command "${AWS_BIN}"

assign_ports

TMP_DIR="$(mktemp -d)"
BIN="${TMP_DIR}/devcloud"
WORKSPACE="${TMP_DIR}/workspace"
mkdir -p "${WORKSPACE}"

log "building devcloud"
go build -o "${BIN}" ./cmd/devcloud

write_config "${WORKSPACE}"

log "starting devcloud up on dynamodb=${DYNAMODB_PORT}, dashboard=${DASHBOARD_PORT}, smtp=${SMTP_PORT}, s3=${S3_PORT}, gcs=${GCS_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for DynamoDB endpoint"
wait_for_dynamodb
log "waiting for dashboard"
wait_for_http "${DASHBOARD_ENDPOINT}/"

log "running DynamoDB AWS CLI journey"
run_dynamodb_journey
show_retained_data

log "passed"

if [[ "${INTERACTIVE}" == "true" ]]; then
  show_interactive_hint
  log "press Ctrl-C to stop devcloud"
  wait "${DEV_PID}"
fi
