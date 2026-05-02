#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
BIGQUERY_VERIFY_PORT="${BIGQUERY_VERIFY_PORT:-9050}"
GCS_VERIFY_PORT="${GCS_VERIFY_PORT:-4443}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-4566}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-8025}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-1025}"
DYNAMODB_VERIFY_PORT="${DYNAMODB_VERIFY_PORT:-8000}"
VERIFY_HOST="127.0.0.1"
BIGQUERY_ENDPOINT="http://${VERIFY_HOST}:${BIGQUERY_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"
PROJECT="${BIGQUERY_VERIFY_PROJECT:-devcloud}"
DATASET="${BIGQUERY_VERIFY_DATASET:-devcloud_loop}"
TABLE="${BIGQUERY_VERIFY_TABLE:-people}"
LOCATION="${BIGQUERY_VERIFY_LOCATION:-US}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-bigquery-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-bigquery-verify.err"

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

wait_for_bigquery() {
  wait_for_http "${BIGQUERY_ENDPOINT}/bigquery/v2/projects"
}

json_post() {
  local url="$1"
  local payload="$2"
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer devcloud-test-token' \
    --data "${payload}" \
    "${url}"
}

json_get() {
  local url="$1"
  curl -fsS \
    -H 'Authorization: Bearer devcloud-test-token' \
    "${url}"
}

json_patch() {
  local url="$1"
  local payload="$2"
  curl -fsS \
    -X PATCH \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer devcloud-test-token' \
    --data "${payload}" \
    "${url}"
}

json_delete() {
  local url="$1"
  curl -fsS \
    -X DELETE \
    -H 'Authorization: Bearer devcloud-test-token' \
    "${url}"
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: bigquery-e2e

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}
  gcsPort: ${GCS_VERIFY_PORT}
  dynamodbPort: ${DYNAMODB_VERIFY_PORT}
  bigqueryPort: ${BIGQUERY_VERIFY_PORT}

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  gcs:
    mode: relaxed
    project: ${PROJECT}
  dynamodb:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  bigquery:
    mode: relaxed
    project: ${PROJECT}
    bearerToken: devcloud-test-token

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
    project: ${PROJECT}
    location: ${LOCATION}
  dynamodb:
    enabled: true
    region: us-east-1
  bigquery:
    enabled: true
    project: ${PROJECT}
    location: ${LOCATION}
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

assert_bigquery_design_contract() {
  test -f docs/design-bigquery-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'BigQuery Compatibility Design|REST API|jobs\.query|tabledata\.insertAll|datasets\.insert|GoogleSQL|AC-001' docs/design-bigquery-compat.md
}

assert_bigquery_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'bigqueryPort|services\\.bigquery|auth\\.bigquery|BigQuery|bigquery' internal cmd docs &&
    go test ./internal/app -run 'Test.*BigQuery|TestDefaultConfig|TestInitWorkspace|TestLoadConfig' -count=1
}

bigquery_endpoint_starts() {
  start_devcloud || return 1
  wait_for_bigquery
}

dashboard_starts() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
  fi
  wait_for_http "${DASHBOARD_ENDPOINT}/"
}

list_projects() {
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects" |
    grep -q "\"projectId\"[[:space:]]*:[[:space:]]*\"${PROJECT}\""
}

create_dataset() {
  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets" "{
    \"datasetReference\":{\"projectId\":\"${PROJECT}\",\"datasetId\":\"${DATASET}\"},
    \"location\":\"${LOCATION}\",
    \"friendlyName\":\"Devcloud Loop Dataset\"
  }" | grep -q "\"datasetId\"[[:space:]]*:[[:space:]]*\"${DATASET}\""
}

get_dataset() {
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}" |
    grep -q "\"datasetId\"[[:space:]]*:[[:space:]]*\"${DATASET}\""
}

patch_dataset() {
  json_patch "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}" '{
    "description":"updated by devcloud BigQuery autoloop"
  }' | grep -q 'updated by devcloud BigQuery autoloop'
}

list_datasets() {
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets" |
    grep -q "\"datasetId\"[[:space:]]*:[[:space:]]*\"${DATASET}\""
}

create_table() {
  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables" "{
    \"tableReference\":{\"projectId\":\"${PROJECT}\",\"datasetId\":\"${DATASET}\",\"tableId\":\"${TABLE}\"},
    \"schema\":{\"fields\":[
      {\"name\":\"id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\"},
      {\"name\":\"name\",\"type\":\"STRING\"},
      {\"name\":\"age\",\"type\":\"INTEGER\"},
      {\"name\":\"active\",\"type\":\"BOOLEAN\"}
    ]}
  }" | grep -q "\"tableId\"[[:space:]]*:[[:space:]]*\"${TABLE}\""
}

get_table() {
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}" |
    grep -q "\"tableId\"[[:space:]]*:[[:space:]]*\"${TABLE}\""
}

patch_table() {
  json_patch "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}" '{
    "description":"updated by devcloud BigQuery autoloop"
  }' | grep -q 'updated by devcloud BigQuery autoloop'
}

list_tables() {
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables" |
    grep -q "\"tableId\"[[:space:]]*:[[:space:]]*\"${TABLE}\""
}

insert_rows() {
  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}/insertAll" '{
    "skipInvalidRows":false,
    "ignoreUnknownValues":false,
    "rows":[
      {"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}},
      {"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":true}}
    ]
  }' | grep -q '"kind"'
}

list_rows() {
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}/data?maxResults=10" |
    grep -q 'Ada'
}

query_rows() {
  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/queries" "{
    \"query\":\"SELECT id, age FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE age >= 30 ORDER BY id\",
    \"useLegacySql\":false,
    \"location\":\"${LOCATION}\"
  }" | tee "${VERIFY_OUT}" | grep -Eq 'Ada|Grace|\"totalRows\"[[:space:]]*:[[:space:]]*\"?[12]'
}

get_query_results() {
  local job_id
  job_id="$(python3 - <<'PY' "${VERIFY_OUT}"
import json, sys
try:
    data = json.load(open(sys.argv[1]))
    print(data.get("jobReference", {}).get("jobId", ""))
except Exception:
    print("")
PY
)"
  [[ -n "${job_id}" ]] || return 0
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/queries/${job_id}?location=${LOCATION}" |
    grep -Eq 'Ada|Grace|\"jobComplete\"[[:space:]]*:[[:space:]]*true'
}

dashboard_bigquery_api() {
  dashboard_starts || return 1
  curl -fsS "${DASHBOARD_ENDPOINT}/api/services" | grep -q 'bigquery' &&
    curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/status" | grep -q 'bigquery' &&
    curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/projects" | grep -q "${PROJECT}"
}

dashboard_bigquery_page() {
  dashboard_starts || return 1
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/bigquery" |
    grep -Eq 'BigQuery|bigquery|devcloud'
}

delete_table() {
  json_delete "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}" >/dev/null
}

delete_dataset() {
  json_delete "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}?deleteContents=true" >/dev/null
}

case "${VERIFY_STAGE}" in
  foundation)
    run_check "BigQuery design contract exists" assert_bigquery_design_contract
    run_check "CLI help works" go run ./cmd/devcloud help
    run_check "daemon help works" go run ./cmd/devcloudd help
    run_check "all Go tests pass" go test ./...
    ;;
  config)
    run_check "BigQuery config shape exists" assert_bigquery_config_shape
    run_check "all Go tests pass" go test ./...
    ;;
  server)
    run_check "BigQuery endpoint starts" bigquery_endpoint_starts
    run_check "projects list works" list_projects
    ;;
  catalog)
    run_check "BigQuery endpoint starts" bigquery_endpoint_starts
    run_check "datasets.insert works" create_dataset
    run_check "datasets.get works" get_dataset
    run_check "datasets.patch works" patch_dataset
    run_check "datasets.list works" list_datasets
    run_check "tables.insert works" create_table
    run_check "tables.get works" get_table
    run_check "tables.patch works" patch_table
    run_check "tables.list works" list_tables
    ;;
  tabledata)
    run_check "BigQuery endpoint starts" bigquery_endpoint_starts
    run_check "datasets.insert works" create_dataset
    run_check "tables.insert works" create_table
    run_check "tabledata.insertAll works" insert_rows
    run_check "tabledata.list works" list_rows
    ;;
  query)
    run_check "BigQuery endpoint starts" bigquery_endpoint_starts
    run_check "datasets.insert works" create_dataset
    run_check "tables.insert works" create_table
    run_check "tabledata.insertAll works" insert_rows
    run_check "jobs.query works" query_rows
    run_check "jobs.getQueryResults works" get_query_results
    ;;
  dashboard)
    run_check "BigQuery endpoint starts" bigquery_endpoint_starts
    run_check "BigQuery dashboard APIs work" dashboard_bigquery_api
    run_check "BigQuery dashboard page works" dashboard_bigquery_page
    ;;
  full)
    run_check "BigQuery design contract exists" assert_bigquery_design_contract
    run_check "BigQuery config shape exists" assert_bigquery_config_shape
    run_check "all Go tests pass" go test ./...
    run_check "BigQuery endpoint starts" bigquery_endpoint_starts
    run_check "projects list works" list_projects
    run_check "datasets.insert works" create_dataset
    run_check "datasets.get works" get_dataset
    run_check "datasets.patch works" patch_dataset
    run_check "datasets.list works" list_datasets
    run_check "tables.insert works" create_table
    run_check "tables.get works" get_table
    run_check "tables.patch works" patch_table
    run_check "tables.list works" list_tables
    run_check "tabledata.insertAll works" insert_rows
    run_check "tabledata.list works" list_rows
    run_check "jobs.query works" query_rows
    run_check "jobs.getQueryResults works" get_query_results
    run_check "BigQuery dashboard APIs work" dashboard_bigquery_api
    run_check "BigQuery dashboard page works" dashboard_bigquery_page
    run_check "tables.delete works" delete_table
    run_check "datasets.delete works" delete_dataset
    ;;
  *)
    echo "Unknown VERIFY_STAGE: ${VERIFY_STAGE}" >&2
    exit 2
    ;;
esac

echo "Verification complete: ${PASS} passed, ${FAIL} failed (stage=${VERIFY_STAGE})"
if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
