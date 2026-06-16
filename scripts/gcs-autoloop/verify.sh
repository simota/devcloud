#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"

free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

GCS_VERIFY_PORT="${GCS_VERIFY_PORT:-4443}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-4566}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-8025}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-1025}"
MAIL_HTTP_VERIFY_PORT="${MAIL_HTTP_VERIFY_PORT:-$(free_port)}"
EVENT_RELAY_VERIFY_PORT="${EVENT_RELAY_VERIFY_PORT:-$(free_port)}"
APP_AUTOSCALING_VERIFY_PORT="${APP_AUTOSCALING_VERIFY_PORT:-$(free_port)}"
REDIS_HTTP_VERIFY_PORT="${REDIS_HTTP_VERIFY_PORT:-$(free_port)}"
VERIFY_HOST="127.0.0.1"
GCS_ENDPOINT="http://${VERIFY_HOST}:${GCS_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"


PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-gcs-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-gcs-verify.err"
PROJECT="devcloud"
BUCKET="devcloud-gcs-loop-demo"

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
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[gcs-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[gcs-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    sleep 0.2
  done
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: gcs-e2e

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  mailHttpPort: ${MAIL_HTTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  eventRelayPort: ${EVENT_RELAY_VERIFY_PORT}
  appAutoScalingPort: ${APP_AUTOSCALING_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}
  gcsPort: ${GCS_VERIFY_PORT}
  redisHttpPort: ${REDIS_HTTP_VERIFY_PORT}

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

assert_gcs_design_contract() {
  test -f docs/design-gcs-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'GCS Compatibility Design|buckets.insert|objects.insert|resumable upload|generation|metageneration' docs/design-gcs-compat.md
}

assert_gcs_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'gcsPort|services\\.gcs|auth\\.gcs|GCS|gcs' rust docs &&
    cargo test --workspace
}

gcs_endpoint_starts() {
  start_devcloud || return 1
  wait_for_http "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}"
}

dashboard_starts() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
  fi
  wait_for_http "${DASHBOARD_ENDPOINT}/"
}

create_bucket() {
  curl -fsS -X POST \
    "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${BUCKET}\",\"location\":\"US\",\"storageClass\":\"STANDARD\"}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BUCKET}\""
}

get_bucket() {
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BUCKET}\""
}

list_buckets() {
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BUCKET}\""
}

upload_object_media() {
  printf 'hello from devcloud gcs\n' | curl -fsS \
    -X POST \
    -H 'Content-Type: text/plain' \
    -H 'x-goog-meta-source: gcs-autoloop' \
    --data-binary @- \
    "${GCS_ENDPOINT}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=docs/readme.txt" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"docs/readme.txt\""
}

get_object_metadata() {
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt" |
    grep -q '"generation"'
}

download_object_media() {
  curl -fsS "${GCS_ENDPOINT}/download/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt?alt=media" |
    grep -q 'hello from devcloud gcs'
}

range_download_object() {
  curl -fsS -H 'Range: bytes=0-4' "${GCS_ENDPOINT}/download/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt?alt=media" |
    grep -q '^hello$'
}

list_objects() {
  curl -fsS "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o?prefix=docs/" |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"docs/readme.txt\""
}

copy_object() {
  curl -fsS -X POST \
    "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt/copyTo/b/${BUCKET}/o/docs%2Fcopy.txt" \
    -H "Content-Type: application/json" \
    -d '{}' |
    grep -q "\"name\"[[:space:]]*:[[:space:]]*\"docs/copy.txt\""
}

delete_object() {
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Fcopy.txt" >/dev/null
}

precondition_rejects_mismatch() {
  local code
  code="$(printf 'mismatch\n' | curl -sS -o /dev/null -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: text/plain' \
    --data-binary @- \
    "${GCS_ENDPOINT}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=999999")"
  [[ "${code}" == "412" ]]
}

resumable_upload_flow() {
  local headers location
  headers="$(mktemp)"
  curl -fsS -D "${headers}" -o /dev/null \
    -X POST \
    -H 'Content-Type: application/json' \
    -H 'X-Upload-Content-Type: text/plain' \
    "${GCS_ENDPOINT}/upload/storage/v1/b/${BUCKET}/o?uploadType=resumable&name=docs/resumable.txt" \
    -d '{"name":"docs/resumable.txt","contentType":"text/plain"}'
  location="$(awk 'BEGIN{IGNORECASE=1} /^Location:/ {sub(/\r$/,"",$2); print $2}' "${headers}" | tail -1)"
  rm -f "${headers}"
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

dashboard_gcs_api() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id":"gcs"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/gcs/status" | grep -q '"running"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/gcs/buckets" | grep -q "${BUCKET}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/gcs/buckets/${BUCKET}/objects?prefix=docs/" | grep -q 'docs/readme.txt'
}

dashboard_gcs_page() {
  curl -fsSL "${DASHBOARD_ENDPOINT}/dashboard/gcs" | grep -q '<div id="root"></div>'
}

delete_bucket() {
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Freadme.txt" >/dev/null
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Fresumable.txt" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}/o/docs%2Fprecondition.txt" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "${GCS_ENDPOINT}/storage/v1/b/${BUCKET}" >/dev/null
}

run_gcs_core_checks() {
  run_check "GCS endpoint starts" gcs_endpoint_starts
  run_check "Dashboard HTTP starts" dashboard_starts
  run_check "buckets.insert works" create_bucket
  run_check "buckets.get works" get_bucket
  run_check "buckets.list works" list_buckets
  run_check "objects.insert media works" upload_object_media
  run_check "objects.get metadata exposes generation" get_object_metadata
  run_check "objects.get media returns body" download_object_media
  run_check "Range media download returns partial body" range_download_object
  run_check "objects.list returns object name" list_objects
  run_check "objects.copy copies existing object" copy_object
  run_check "objects.delete removes copied object" delete_object
  run_check "GCS precondition mismatch returns 412" precondition_rejects_mismatch
}

echo "=== Verification stage: ${VERIFY_STAGE} ==="

run_check "Rust tests pass" cargo test --workspace

case "${VERIFY_STAGE}" in
  foundation)
    run_check "GCS design contract exists" assert_gcs_design_contract
    run_check "devcloud help works" cargo run -p devcloud-orchestrator -- help
    TMP_DIR="$(mktemp -d)"
    run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
    ;;
  config)
    run_check "GCS config tests pass" assert_gcs_config_shape
    ;;
  gcs|gcs-core)
    run_gcs_core_checks
    ;;
  resumable|gcs-resumable)
    run_gcs_core_checks
    run_check "Resumable upload flow works" resumable_upload_flow
    ;;
  dashboard|dashboard-static)
    run_gcs_core_checks
    run_check "GCS dashboard API exposes buckets and objects" dashboard_gcs_api
    run_check "GCS dashboard page renders" dashboard_gcs_page
    ;;
  hardening|full)
    run_check "GCS config tests pass" assert_gcs_config_shape
    run_check "devcloud help works" cargo run -p devcloud-orchestrator -- help
    run_gcs_core_checks
    run_check "Resumable upload flow works" resumable_upload_flow
    run_check "GCS dashboard API exposes buckets and objects" dashboard_gcs_api
    run_check "GCS dashboard page renders" dashboard_gcs_page
    run_check "buckets.delete removes empty bucket" delete_bucket
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
