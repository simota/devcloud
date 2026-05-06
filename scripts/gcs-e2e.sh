#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

GCS_PORT="${E2E_GCS_PORT:-}"
S3_PORT="${E2E_S3_PORT:-}"
SMTP_PORT="${E2E_SMTP_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
GCS_ENDPOINT=""
DASHBOARD_ENDPOINT=""
PROJECT="${E2E_GCS_PROJECT:-devcloud}"
BUCKET="${E2E_BUCKET:-devcloud-gcs-e2e-$(date +%s)}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

usage() {
  cat <<'EOF'
Usage:
  scripts/gcs-e2e.sh

Environment:
  E2E_GCS_PORT=14443          Override the GCS endpoint port. Defaults to an available port.
  E2E_S3_PORT=14566           Override the S3 endpoint port used by devcloud. Defaults to an available port.
  E2E_SMTP_PORT=11025         Override the Mail SMTP port used by devcloud. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025    Override the dashboard port. Defaults to an available port.
  E2E_GCS_PROJECT=devcloud    Override the GCS project id.
  E2E_BUCKET=devcloud-gcs-e2e Override the test bucket name.
  E2E_DELETE_DATA=false       Keep the bucket and objects after assertions.
  E2E_KEEP_WORKDIR=true       Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true        Keep devcloud running and keep GCS data after assertions.

Examples:
  scripts/gcs-e2e.sh
  E2E_GCS_PORT=14443 E2E_DASHBOARD_PORT=18025 E2E_SMTP_PORT=11025 scripts/gcs-e2e.sh
  E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/gcs-e2e.sh
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
  printf '[gcs-e2e] %s\n' "$1"
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
    echo "[gcs-e2e] missing command: ${name}" >&2
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
  GCS_ENDPOINT="http://127.0.0.1:${GCS_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[gcs-e2e] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[gcs-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

json_value() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "${expression}"
}

write_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: gcs-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}

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
    location: US
  dynamodb:
    enabled: false
  bigquery:
    enabled: false
  redshift:
    enabled: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
EOF
}

gcs_create_bucket() {
  curl -fsS -X POST \
    "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${BUCKET}\",\"location\":\"US\",\"storageClass\":\"STANDARD\"}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BUCKET}\""
}

gcs_assert_bucket() {
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BUCKET}\""
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BUCKET}\""
}

gcs_upload_media() {
  printf 'hello from devcloud gcs e2e\n' > "${TMP_DIR}/readme.txt"
  curl -fsS \
    -X POST \
    -H 'Content-Type: text/plain' \
    -H 'x-goog-meta-source: gcs-e2e' \
    --data-binary @"${TMP_DIR}/readme.txt" \
    "${GCS_ENDPOINT}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=docs/readme.txt" > "${TMP_DIR}/object.json"
  grep -q "\"name\"[[:space:]]*:[[:space:]]*\"docs/readme.txt\"" "${TMP_DIR}/object.json"
  json_value 'data["generation"]' < "${TMP_DIR}/object.json" > "${TMP_DIR}/generation.txt"
}

gcs_assert_object() {
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt" > "${TMP_DIR}/metadata.json"
  python3 - <<'PY' "${TMP_DIR}/metadata.json"
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
assert data["name"] == "docs/readme.txt"
assert data["bucket"]
assert data["generation"]
assert data["metageneration"]
assert data["contentType"] == "text/plain"
assert data.get("metadata", {}).get("source") == "gcs-e2e"
PY

  curl -fsS "${GCS_ENDPOINT}/download/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt?alt=media" > "${TMP_DIR}/downloaded.txt"
  cmp "${TMP_DIR}/readme.txt" "${TMP_DIR}/downloaded.txt"

  curl -fsS -H 'Range: bytes=0-4' "${GCS_ENDPOINT}/download/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt?alt=media" > "${TMP_DIR}/range.txt"
  grep -q '^hello$' "${TMP_DIR}/range.txt"

  curl -fsS "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o?prefix=docs/" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"docs/readme.txt\""
}

gcs_copy_and_delete_object() {
  curl -fsS -X POST \
    "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt/copyTo/b/${BUCKET}/o/docs%2Fcopy.txt" \
    -H "Content-Type: application/json" \
    -d '{}' |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"docs/copy.txt\""
  curl -fsS "${GCS_ENDPOINT}/download/storage/v1/b/${BUCKET}/o/docs%2Fcopy.txt?alt=media" |
    grep -q 'hello from devcloud gcs e2e'
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Fcopy.txt" >/dev/null
}

gcs_precondition_check() {
  local code
  code="$(printf 'precondition mismatch\n' | curl -sS -o /dev/null -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: text/plain' \
    --data-binary @- \
    "${GCS_ENDPOINT}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=999999")"
  [[ "${code}" == "412" ]]
}

gcs_resumable_upload() {
  local headers location
  headers="${TMP_DIR}/resumable.headers"
  curl -fsS -D "${headers}" -o /dev/null \
    -X POST \
    -H 'Content-Type: application/json' \
    -H 'X-Upload-Content-Type: text/plain' \
    "${GCS_ENDPOINT}/upload/storage/v1/b/${BUCKET}/o?uploadType=resumable&name=docs/resumable.txt" \
    -d '{"name":"docs/resumable.txt","contentType":"text/plain"}'
  location="$(awk 'BEGIN{IGNORECASE=1} /^Location:/ {sub(/\r$/,"",$2); print $2}' "${headers}" | tail -1)"
  [[ -n "${location}" ]]
  printf 'resumable body' | curl -fsS \
    -X PUT \
    -H 'Content-Type: text/plain' \
    -H 'Content-Range: bytes 0-13/14' \
    --data-binary @- \
    "${location}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"docs/resumable.txt\""
  curl -fsS "${GCS_ENDPOINT}/download/storage/v1/b/${BUCKET}/o/docs%2Fresumable.txt?alt=media" |
    grep -q 'resumable body'
}

assert_dashboard_api() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id":"gcs"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/gcs/status" | grep -q '"running"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/gcs/buckets" | grep -q "${BUCKET}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/gcs/buckets/${BUCKET}/objects?prefix=docs/" | grep -q 'docs/readme.txt'
  curl -fsS "${DASHBOARD_ENDPOINT}/gcs" | grep -qi 'devcloud GCS'
}

delete_data() {
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Fresumable.txt" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Fprecondition.txt" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}" >/dev/null 2>&1 || true
}

run_gcs_journey() {
  gcs_create_bucket
  gcs_assert_bucket
  gcs_upload_media
  gcs_assert_object
  gcs_copy_and_delete_object
  gcs_precondition_check
  gcs_resumable_upload
  assert_dashboard_api

  if [[ "${DELETE_DATA}" == "true" ]]; then
    delete_data
  fi
}

show_interactive_hint() {
  cat <<EOF
[gcs-e2e] browser check:
[gcs-e2e]   Dashboard: ${DASHBOARD_ENDPOINT}/gcs
[gcs-e2e]   GCS endpoint: ${GCS_ENDPOINT}
[gcs-e2e]   Project: ${PROJECT}
[gcs-e2e]   Bucket used: ${BUCKET}
[gcs-e2e] curl examples:
[gcs-e2e]   curl -sS ${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}
[gcs-e2e]   curl -sS ${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o
EOF
}

show_retained_data() {
  if [[ "${DELETE_DATA}" != "false" ]]; then
    return 0
  fi
  log "retained GCS data in bucket: ${BUCKET}"
  log "retained workspace: ${WORKSPACE}"
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o" || true
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

log "starting devcloud up on gcs=${GCS_PORT}, dashboard=${DASHBOARD_PORT}, smtp=${SMTP_PORT}, s3=${S3_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for GCS endpoint"
wait_for_http "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}"
log "waiting for dashboard"
wait_for_http "${DASHBOARD_ENDPOINT}/"

log "running GCS JSON API journey"
run_gcs_journey
show_retained_data

log "passed"

if [[ "${INTERACTIVE}" == "true" ]]; then
  show_interactive_hint
  log "press Ctrl-C to stop devcloud"
  wait "${DEV_PID}"
fi
