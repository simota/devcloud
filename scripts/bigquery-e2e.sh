#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

BIGQUERY_PORT="${E2E_BIGQUERY_PORT:-}"
GCS_PORT="${E2E_GCS_PORT:-}"
S3_PORT="${E2E_S3_PORT:-}"
SMTP_PORT="${E2E_SMTP_PORT:-}"
DYNAMODB_PORT="${E2E_DYNAMODB_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
BIGQUERY_ENDPOINT=""
GCS_ENDPOINT=""
DASHBOARD_ENDPOINT=""
PROJECT="${E2E_BIGQUERY_PROJECT:-devcloud}"
LOCATION="${E2E_BIGQUERY_LOCATION:-US}"
DATASET="${E2E_BIGQUERY_DATASET:-devcloud_e2e_$(date +%s)}"
TABLE="${E2E_BIGQUERY_TABLE:-people}"
COPY_TABLE="${E2E_BIGQUERY_COPY_TABLE:-people_copy}"
BUCKET="${E2E_BIGQUERY_BUCKET:-devcloud-bigquery-e2e-$(date +%s)}"
BEARER_TOKEN="${E2E_BIGQUERY_BEARER_TOKEN:-devcloud-e2e-token}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

usage() {
  cat <<'EOF'
Usage:
  scripts/bigquery-e2e.sh

Environment:
  E2E_BIGQUERY_PORT=19050             Override the BigQuery endpoint port. Defaults to an available port.
  E2E_GCS_PORT=14443                  Override the GCS endpoint port used by load/extract checks.
  E2E_DASHBOARD_PORT=18025            Override the dashboard port. Defaults to an available port.
  E2E_S3_PORT=14566                   Override the S3 endpoint port used by devcloud. Defaults to an available port.
  E2E_DYNAMODB_PORT=18000             Override the DynamoDB endpoint port used by devcloud. Defaults to an available port.
  E2E_SMTP_PORT=11025                 Override the Mail SMTP port used by devcloud. Defaults to an available port.
  E2E_BIGQUERY_PROJECT=devcloud       Override the BigQuery project id.
  E2E_BIGQUERY_DATASET=devcloud_e2e   Override the test dataset id.
  E2E_BIGQUERY_TABLE=people           Override the primary test table id.
  E2E_BIGQUERY_BUCKET=devcloud-bq     Override the local GCS bucket for extract checks.
  E2E_DELETE_DATA=false               Keep datasets, tables, rows, jobs, and GCS extract output after assertions.
  E2E_KEEP_WORKDIR=true               Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true                Keep devcloud running and keep BigQuery data after assertions.

Examples:
  scripts/bigquery-e2e.sh
  E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/bigquery-e2e.sh
  E2E_BIGQUERY_PORT=19050 E2E_DASHBOARD_PORT=18025 scripts/bigquery-e2e.sh
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
  printf '[bigquery-e2e] %s\n' "$1"
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
    echo "[bigquery-e2e] missing command: ${name}" >&2
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
  if [[ -z "${BIGQUERY_PORT}" ]]; then
    BIGQUERY_PORT="$(find_free_port)"
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
  if [[ -z "${DASHBOARD_PORT}" ]]; then
    DASHBOARD_PORT="$(find_free_port)"
  fi
  BIGQUERY_ENDPOINT="http://127.0.0.1:${BIGQUERY_PORT}"
  GCS_ENDPOINT="http://127.0.0.1:${GCS_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 20))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[bigquery-e2e] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[bigquery-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

json_get() {
  local url="$1"
  curl -fsS -H "Authorization: Bearer ${BEARER_TOKEN}" "${url}"
}

json_post() {
  local url="$1"
  local payload="$2"
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${BEARER_TOKEN}" \
    --data "${payload}" \
    "${url}"
}

json_patch() {
  local url="$1"
  local payload="$2"
  curl -fsS \
    -X PATCH \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${BEARER_TOKEN}" \
    --data "${payload}" \
    "${url}"
}

json_delete() {
  local url="$1"
  curl -fsS -X DELETE -H "Authorization: Bearer ${BEARER_TOKEN}" "${url}"
}

json_assert() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data}), data' "${expression}"
}

json_value() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "${expression}"
}

write_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: bigquery-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}
  dynamodbPort: ${DYNAMODB_PORT}
  bigqueryPort: ${BIGQUERY_PORT}

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
    bearerToken: ${BEARER_TOKEN}

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
    maxRowsPerTable: 1000000
    maxRequestBytes: 10485760
    query:
      maxResultRows: 10000
      maxExecutionSeconds: 30
      defaultUseLegacySql: false
  redshift:
    enabled: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
EOF
}

create_gcs_bucket() {
  curl -fsS -X POST \
    "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${BUCKET}\",\"location\":\"${LOCATION}\",\"storageClass\":\"STANDARD\"}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BUCKET}\""
}

create_dataset_and_table() {
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects" |
    json_assert 'data["projects"][0]["projectReference"]["projectId"] == "'"${PROJECT}"'"'

  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets" "{
    \"datasetReference\":{\"projectId\":\"${PROJECT}\",\"datasetId\":\"${DATASET}\"},
    \"location\":\"${LOCATION}\",
    \"friendlyName\":\"BigQuery E2E Dataset\"
  }" | json_assert 'data["datasetReference"]["datasetId"] == "'"${DATASET}"'"'

  json_patch "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}" \
    '{"description":"created by scripts/bigquery-e2e.sh"}' |
    json_assert 'data["description"] == "created by scripts/bigquery-e2e.sh"'

  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables" "{
    \"tableReference\":{\"projectId\":\"${PROJECT}\",\"datasetId\":\"${DATASET}\",\"tableId\":\"${TABLE}\"},
    \"schema\":{\"fields\":[
      {\"name\":\"id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\"},
      {\"name\":\"name\",\"type\":\"STRING\"},
      {\"name\":\"age\",\"type\":\"INTEGER\"},
      {\"name\":\"active\",\"type\":\"BOOLEAN\"}
    ]}
  }" | json_assert 'data["tableReference"]["tableId"] == "'"${TABLE}"'"'

  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables" |
    grep -q "\"tableId\"[[:space:]]*:[[:space:]]*\"${TABLE}\""
}

insert_and_query_rows() {
  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}/insertAll" '{
    "skipInvalidRows":false,
    "ignoreUnknownValues":false,
    "rows":[
      {"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}},
      {"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":true}}
    ]
  }' | json_assert 'data["kind"] == "bigquery#tableDataInsertAllResponse" and len(data.get("insertErrors", [])) == 0'

  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}/data?maxResults=10" |
    json_assert 'data["totalRows"] == "2" and any(cell["v"] == "Ada" for row in data["rows"] for cell in row["f"])'

  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/queries" "{
    \"query\":\"SELECT id, age FROM ${PROJECT}.${DATASET}.${TABLE} WHERE age >= 30 ORDER BY id\",
    \"useLegacySql\":false,
    \"location\":\"${LOCATION}\"
  }" > "${TMP_DIR}/query.json"
  json_assert 'data["kind"] == "bigquery#queryResponse" and data["jobComplete"] is True and data["totalRows"] == "2"' < "${TMP_DIR}/query.json"

  local job_id
  job_id="$(json_value 'data["jobReference"]["jobId"]' < "${TMP_DIR}/query.json")"
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/jobs/${job_id}/getQueryResults?location=${LOCATION}" |
    json_assert 'data["jobComplete"] is True and data["totalRows"] == "2"'
}

exercise_copy_and_extract_jobs() {
  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/jobs" "{
    \"jobReference\":{\"projectId\":\"${PROJECT}\",\"jobId\":\"copy_people_e2e\",\"location\":\"${LOCATION}\"},
    \"configuration\":{\"copy\":{
      \"sourceTable\":{\"projectId\":\"${PROJECT}\",\"datasetId\":\"${DATASET}\",\"tableId\":\"${TABLE}\"},
      \"destinationTable\":{\"projectId\":\"${PROJECT}\",\"datasetId\":\"${DATASET}\",\"tableId\":\"${COPY_TABLE}\"},
      \"writeDisposition\":\"WRITE_TRUNCATE\"
    }}
  }" | json_assert 'data["status"]["state"] == "DONE" and data["configuration"]["copy"]["destinationTable"]["tableId"] == "'"${COPY_TABLE}"'"'

  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${COPY_TABLE}/data?maxResults=10" |
    json_assert 'data["totalRows"] == "2"'

  json_post "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/jobs" "{
    \"jobReference\":{\"projectId\":\"${PROJECT}\",\"jobId\":\"extract_people_e2e\",\"location\":\"${LOCATION}\"},
    \"configuration\":{\"extract\":{
      \"sourceTable\":{\"projectId\":\"${PROJECT}\",\"datasetId\":\"${DATASET}\",\"tableId\":\"${TABLE}\"},
      \"destinationUris\":[\"gs://${BUCKET}/exports/people.ndjson\"],
      \"destinationFormat\":\"NEWLINE_DELIMITED_JSON\"
    }}
  }" | json_assert 'data["status"]["state"] == "DONE"'

  curl -fsS "${GCS_ENDPOINT}/download/storage/v1/b/${BUCKET}/o/exports%2Fpeople.ndjson?alt=media" > "${TMP_DIR}/people.ndjson"
  grep -q '"name":"Ada"' "${TMP_DIR}/people.ndjson"
  grep -q '"name":"Grace"' "${TMP_DIR}/people.ndjson"
}

assert_dashboard_api() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id":"bigquery"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/status" | grep -q '"running":true'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/projects" | grep -q "${PROJECT}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/projects/${PROJECT}/datasets" | grep -q "${DATASET}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/projects/${PROJECT}/datasets/${DATASET}/tables" | grep -q "${TABLE}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}/rows?limit=10" | grep -q 'Ada'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/bigquery/projects/${PROJECT}/jobs" | grep -q 'extract_people_e2e'
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/bigquery" | grep -qi 'devcloud Dashboard'
}

delete_data() {
  json_delete "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${COPY_TABLE}" >/dev/null 2>&1 || true
  json_delete "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}" >/dev/null 2>&1 || true
  json_delete "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}?deleteContents=true" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/exports%2Fpeople.ndjson" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}" >/dev/null 2>&1 || true
}

run_bigquery_journey() {
  create_gcs_bucket
  create_dataset_and_table
  insert_and_query_rows
  exercise_copy_and_extract_jobs
  assert_dashboard_api

  if [[ "${DELETE_DATA}" == "true" ]]; then
    delete_data
  fi
}

show_interactive_hint() {
  cat <<EOF
[bigquery-e2e] browser check:
[bigquery-e2e]   Dashboard: ${DASHBOARD_ENDPOINT}/dashboard/bigquery
[bigquery-e2e]   BigQuery endpoint: ${BIGQUERY_ENDPOINT}
[bigquery-e2e]   GCS endpoint: ${GCS_ENDPOINT}
[bigquery-e2e]   Project: ${PROJECT}
[bigquery-e2e]   Dataset: ${DATASET}
[bigquery-e2e]   Table: ${TABLE}
[bigquery-e2e] curl examples:
[bigquery-e2e]   curl -H "Authorization: Bearer ${BEARER_TOKEN}" ${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets
[bigquery-e2e]   curl -H "Authorization: Bearer ${BEARER_TOKEN}" ${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables/${TABLE}/data
EOF
}

show_retained_data() {
  if [[ "${DELETE_DATA}" != "false" ]]; then
    return 0
  fi
  log "retained BigQuery dataset: ${PROJECT}.${DATASET}"
  log "retained BigQuery tables: ${TABLE}, ${COPY_TABLE}"
  log "retained GCS bucket: ${BUCKET}"
  log "retained workspace: ${WORKSPACE}"
  json_get "${BIGQUERY_ENDPOINT}/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}/tables" || true
  echo ""
}

require_command curl
require_command python3

assign_ports

TMP_DIR="$(mktemp -d)"
BIN="${TMP_DIR}/devcloud"
WORKSPACE="${TMP_DIR}/workspace"
mkdir -p "${WORKSPACE}"

log "building devcloud"
go build -o "${BIN}" ./cmd/devcloud

write_config "${WORKSPACE}"

log "starting devcloud up on bigquery=${BIGQUERY_PORT}, gcs=${GCS_PORT}, dashboard=${DASHBOARD_PORT}, smtp=${SMTP_PORT}, s3=${S3_PORT}, dynamodb=${DYNAMODB_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for BigQuery endpoint"
wait_for_http "${BIGQUERY_ENDPOINT}/bigquery/v2/projects"
log "waiting for GCS endpoint"
wait_for_http "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}"
log "waiting for dashboard"
wait_for_http "${DASHBOARD_ENDPOINT}/"

log "running BigQuery REST API and dashboard journey"
run_bigquery_journey
show_retained_data

log "passed"

if [[ "${INTERACTIVE}" == "true" ]]; then
  show_interactive_hint
  log "press Ctrl-C to stop devcloud"
  wait "${DEV_PID}"
fi
