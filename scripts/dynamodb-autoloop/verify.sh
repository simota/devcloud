#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
DYNAMODB_VERIFY_PORT="${DYNAMODB_VERIFY_PORT:-8000}"
GCS_VERIFY_PORT="${GCS_VERIFY_PORT:-4443}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-4566}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-8025}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-1025}"
VERIFY_HOST="127.0.0.1"
DYNAMODB_ENDPOINT="http://${VERIFY_HOST}:${DYNAMODB_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-dynamodb-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-dynamodb-verify.err"
TABLE="DevcloudDynamoLoop"
GSI_TABLE="DevcloudDynamoLoopIndex"

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

wait_for_dynamodb() {
  local deadline=$((SECONDS + 12))
  until dynamodb_call ListTables '{}' >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
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

dynamodb_status_call() {
  local target="$1"
  local payload="$2"
  curl -sS \
    -o "${VERIFY_OUT}" \
    -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: application/x-amz-json-1.0' \
    -H "X-Amz-Target: DynamoDB_20120810.${target}" \
    --data "${payload}" \
    "${DYNAMODB_ENDPOINT}/"
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: dynamodb-e2e

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}
  gcsPort: ${GCS_VERIFY_PORT}
  dynamodbPort: ${DYNAMODB_VERIFY_PORT}

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
    billingMode: PAY_PER_REQUEST
    maxItemBytes: 400000
EOF

  run_check "devcloud binary builds" go build -o "${TMP_DIR}/devcloud" ./cmd/devcloud
  if [[ "${FAIL}" -gt 0 ]]; then
    return 1
  fi

  (
    cd "${TMP_DIR}"
    "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

assert_dynamodb_design_contract() {
  test -f docs/design-dynamodb-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'DynamoDB Compatibility Design|X-Amz-Target|AttributeValue|ConditionExpression|GlobalSecondaryIndex|PartiQL' docs/design-dynamodb-compat.md
}

assert_dynamodb_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'dynamodbPort|services\\.dynamodb|auth\\.dynamodb|DynamoDB|dynamodb' internal cmd docs &&
    go test ./internal/app -run 'Test.*DynamoDB|TestDefaultConfig|TestInitWorkspace|TestLoadConfig' -count=1
}

dynamodb_endpoint_starts() {
  start_devcloud || return 1
  wait_for_dynamodb
}

dashboard_starts() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
  fi
  wait_for_http "${DASHBOARD_ENDPOINT}/"
}

create_table() {
  dynamodb_call CreateTable "{
    \"TableName\":\"${TABLE}\",
    \"AttributeDefinitions\":[
      {\"AttributeName\":\"pk\",\"AttributeType\":\"S\"},
      {\"AttributeName\":\"sk\",\"AttributeType\":\"S\"}
    ],
    \"KeySchema\":[
      {\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"},
      {\"AttributeName\":\"sk\",\"KeyType\":\"RANGE\"}
    ],
    \"BillingMode\":\"PAY_PER_REQUEST\"
  }" | grep -q "\"TableName\"[[:space:]]*:[[:space:]]*\"${TABLE}\""
}

describe_table() {
  dynamodb_call DescribeTable "{\"TableName\":\"${TABLE}\"}" |
    grep -q "\"TableStatus\"[[:space:]]*:[[:space:]]*\"ACTIVE\""
}

list_tables() {
  dynamodb_call ListTables '{}' |
    grep -q "\"${TABLE}\""
}

put_item() {
  dynamodb_call PutItem "{
    \"TableName\":\"${TABLE}\",
    \"Item\":{
      \"pk\":{\"S\":\"user#1\"},
      \"sk\":{\"S\":\"profile\"},
      \"name\":{\"S\":\"Ada\"},
      \"age\":{\"N\":\"37\"},
      \"active\":{\"BOOL\":true},
      \"tags\":{\"SS\":[\"engineer\",\"admin\"]},
      \"meta\":{\"M\":{\"team\":{\"S\":\"core\"}}},
      \"scores\":{\"L\":[{\"N\":\"1\"},{\"N\":\"2\"}]}
    }
  }" >/dev/null
}

get_item() {
  dynamodb_call GetItem "{
    \"TableName\":\"${TABLE}\",
    \"Key\":{\"pk\":{\"S\":\"user#1\"},\"sk\":{\"S\":\"profile\"}}
  }" | grep -q "\"name\"[[:space:]]*:[[:space:]]*{[[:space:]]*\"S\"[[:space:]]*:[[:space:]]*\"Ada\""
}

conditional_put_rejects_duplicate() {
  local code
  code="$(dynamodb_status_call PutItem "{
    \"TableName\":\"${TABLE}\",
    \"Item\":{\"pk\":{\"S\":\"user#1\"},\"sk\":{\"S\":\"profile\"}},
    \"ConditionExpression\":\"attribute_not_exists(pk)\"
  }")"
  [[ "${code}" == "400" ]] && grep -q 'ConditionalCheckFailedException' "${VERIFY_OUT}"
}

update_item() {
  dynamodb_call UpdateItem "{
    \"TableName\":\"${TABLE}\",
    \"Key\":{\"pk\":{\"S\":\"user#1\"},\"sk\":{\"S\":\"profile\"}},
    \"UpdateExpression\":\"SET #n = :name, visits = :one\",
    \"ExpressionAttributeNames\":{\"#n\":\"name\"},
    \"ExpressionAttributeValues\":{
      \":name\":{\"S\":\"Ada Lovelace\"},
      \":one\":{\"N\":\"1\"}
    },
    \"ReturnValues\":\"ALL_NEW\"
  }" | grep -q 'Ada Lovelace'
}

query_items() {
  dynamodb_call PutItem "{
    \"TableName\":\"${TABLE}\",
    \"Item\":{\"pk\":{\"S\":\"user#1\"},\"sk\":{\"S\":\"event#001\"},\"kind\":{\"S\":\"event\"}}
  }" >/dev/null
  dynamodb_call PutItem "{
    \"TableName\":\"${TABLE}\",
    \"Item\":{\"pk\":{\"S\":\"user#1\"},\"sk\":{\"S\":\"event#002\"},\"kind\":{\"S\":\"event\"}}
  }" >/dev/null
  dynamodb_call Query "{
    \"TableName\":\"${TABLE}\",
    \"KeyConditionExpression\":\"pk = :pk AND begins_with(sk, :prefix)\",
    \"ExpressionAttributeValues\":{
      \":pk\":{\"S\":\"user#1\"},
      \":prefix\":{\"S\":\"event#\"}
    },
    \"Limit\":1
  }" | grep -q '"LastEvaluatedKey"'
}

scan_items() {
  dynamodb_call Scan "{
    \"TableName\":\"${TABLE}\",
    \"FilterExpression\":\"active = :active\",
    \"ExpressionAttributeValues\":{\":active\":{\"BOOL\":true}},
    \"ProjectionExpression\":\"pk, sk, active\"
  }" | grep -q '"Count"'
}

delete_item() {
  dynamodb_call DeleteItem "{
    \"TableName\":\"${TABLE}\",
    \"Key\":{\"pk\":{\"S\":\"user#1\"},\"sk\":{\"S\":\"event#001\"}}
  }" >/dev/null
}

delete_table() {
  dynamodb_call DeleteTable "{\"TableName\":\"${TABLE}\"}" |
    grep -q "\"TableName\"[[:space:]]*:[[:space:]]*\"${TABLE}\""
}

create_index_table() {
  dynamodb_call CreateTable "{
    \"TableName\":\"${GSI_TABLE}\",
    \"AttributeDefinitions\":[
      {\"AttributeName\":\"pk\",\"AttributeType\":\"S\"},
      {\"AttributeName\":\"sk\",\"AttributeType\":\"S\"},
      {\"AttributeName\":\"gpk\",\"AttributeType\":\"S\"},
      {\"AttributeName\":\"gsk\",\"AttributeType\":\"N\"}
    ],
    \"KeySchema\":[
      {\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"},
      {\"AttributeName\":\"sk\",\"KeyType\":\"RANGE\"}
    ],
    \"GlobalSecondaryIndexes\":[{
      \"IndexName\":\"gsi1\",
      \"KeySchema\":[
        {\"AttributeName\":\"gpk\",\"KeyType\":\"HASH\"},
        {\"AttributeName\":\"gsk\",\"KeyType\":\"RANGE\"}
      ],
      \"Projection\":{\"ProjectionType\":\"ALL\"}
    }],
    \"BillingMode\":\"PAY_PER_REQUEST\"
  }" >/dev/null
  dynamodb_call PutItem "{
    \"TableName\":\"${GSI_TABLE}\",
    \"Item\":{\"pk\":{\"S\":\"item#1\"},\"sk\":{\"S\":\"v1\"},\"gpk\":{\"S\":\"group#1\"},\"gsk\":{\"N\":\"10\"}}
  }" >/dev/null
  dynamodb_call Query "{
    \"TableName\":\"${GSI_TABLE}\",
    \"IndexName\":\"gsi1\",
    \"KeyConditionExpression\":\"gpk = :gpk\",
    \"ExpressionAttributeValues\":{\":gpk\":{\"S\":\"group#1\"}}
  }" | grep -q 'item#1'
}

dashboard_dynamodb_api() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id":"dynamodb"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dynamodb/status" | grep -q '"running"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dynamodb/tables" | grep -q "${TABLE}"
}

dashboard_dynamodb_page() {
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/dynamodb" | grep -qi 'devcloud Dashboard'
}

aws_cli_smoke() {
  command -v aws >/dev/null 2>&1 || return 1
  aws --endpoint-url "${DYNAMODB_ENDPOINT}" dynamodb list-tables |
    grep -q "${TABLE}"
}

run_protocol_checks() {
  run_check "DynamoDB endpoint starts" dynamodb_endpoint_starts
  run_check "Dashboard HTTP starts" dashboard_starts
  run_check "CreateTable works" create_table
  run_check "DescribeTable returns ACTIVE" describe_table
  run_check "ListTables returns table" list_tables
}

run_item_core_checks() {
  run_protocol_checks
  run_check "PutItem accepts AttributeValue shapes" put_item
  run_check "GetItem returns stored item" get_item
  run_check "Conditional PutItem failure is compatible" conditional_put_rejects_duplicate
  run_check "UpdateItem applies SET/arithmetic" update_item
  run_check "DeleteItem removes item" delete_item
}

run_query_index_checks() {
  run_item_core_checks
  run_check "Query supports key condition and pagination" query_items
  run_check "Scan supports filter/projection" scan_items
  run_check "GSI Query reflects projected index state" create_index_table
}

echo "=== Verification stage: ${VERIFY_STAGE} ==="

run_check "Go tests pass" go test ./...

case "${VERIFY_STAGE}" in
  foundation)
    run_check "DynamoDB design contract exists" assert_dynamodb_design_contract
    run_check "devcloud help works" go run ./cmd/devcloud help
    run_check "devcloudd help works" go run ./cmd/devcloudd -h
    TMP_DIR="$(mktemp -d)"
    run_check "devcloud binary builds" go build -o "${TMP_DIR}/devcloud" ./cmd/devcloud
    ;;
  config)
    run_check "DynamoDB config tests pass" assert_dynamodb_config_shape
    ;;
  protocol|table)
    run_protocol_checks
    ;;
  item|item-core)
    run_item_core_checks
    ;;
  query|index|query-index)
    run_query_index_checks
    ;;
  dashboard)
    run_query_index_checks
    run_check "DynamoDB dashboard API exposes tables" dashboard_dynamodb_api
    run_check "DynamoDB dashboard page renders" dashboard_dynamodb_page
    ;;
  hardening|full)
    run_check "DynamoDB config tests pass" assert_dynamodb_config_shape
    run_check "devcloud help works" go run ./cmd/devcloud help
    run_check "devcloudd help works" go run ./cmd/devcloudd -h
    run_query_index_checks
    run_check "DynamoDB dashboard API exposes tables" dashboard_dynamodb_api
    run_check "DynamoDB dashboard page renders" dashboard_dynamodb_page
    run_check "AWS CLI endpoint override smoke passes" aws_cli_smoke
    run_check "DeleteTable removes table" delete_table
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE: ${VERIFY_STAGE}"
    FAIL=$((FAIL + 1))
    ;;
esac

echo ""
TOTAL=$((PASS + FAIL))
echo "=== Verification: ${PASS}/${TOTAL} passed, ${FAIL} failed ==="
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
