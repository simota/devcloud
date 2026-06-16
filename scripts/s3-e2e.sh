#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_DEFAULT_OUTPUT="${AWS_DEFAULT_OUTPUT:-json}"

AWSLOCAL_BIN="${AWSLOCAL_BIN:-awslocal}"
S3_PORT="${E2E_S3_PORT:-14566}"
SMTP_PORT="${E2E_SMTP_PORT:-11025}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-18025}"
S3_ENDPOINT="http://127.0.0.1:${S3_PORT}"
DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"
BUCKET="${E2E_BUCKET:-devcloud-e2e-$(date +%s)}"

usage() {
  cat <<'EOF'
Usage:
  scripts/s3-e2e.sh

Environment:
  AWSLOCAL_BIN=awslocal       awscli-local executable. Defaults to awslocal.
  E2E_S3_PORT=14566           Override the S3 endpoint port.
  E2E_SMTP_PORT=11025         Override the Mail SMTP port used by devcloud.
  E2E_DASHBOARD_PORT=18025    Override the dashboard port.
  E2E_BUCKET=devcloud-e2e     Override the test bucket name.
  E2E_DELETE_DATA=false       Keep the bucket and objects after assertions.
  E2E_KEEP_WORKDIR=true       Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true        Keep devcloud running and keep S3 data after assertions.

Examples:
  scripts/s3-e2e.sh
  E2E_S3_PORT=14566 E2E_DASHBOARD_PORT=18025 E2E_SMTP_PORT=11025 scripts/s3-e2e.sh
  AWSLOCAL_BIN=/opt/homebrew/bin/awslocal scripts/s3-e2e.sh
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

log() {
  printf '[s3-e2e] %s\n' "$1"
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
    echo "[s3-e2e] missing command: ${name}" >&2
    echo "[s3-e2e] install awscli-local, for example: pipx install awscli-local" >&2
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

awslocal_cmd() {
  "${AWSLOCAL_BIN}" --endpoint-url="${S3_ENDPOINT}" "$@"
}

json_value() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "${expression}"
}

write_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: s3-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
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
    region: ${AWS_DEFAULT_REGION}
    pathStyle: true
    virtualHostStyle: false
    maxObjectBytes: 5368709120
    multipart:
      minPartBytes: 5242880
EOF
}

assert_dashboard_api() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/status" | grep -q '"running":true'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/buckets" | grep -q "${BUCKET}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/buckets/${BUCKET}/objects?prefix=docs/" | grep -q 'docs/readme.txt'
  # Compatibility /s3 must 301-redirect to /dashboard/s3.
  curl -fsS -o /dev/null -w '%{http_code} %{redirect_url}\n' "${DASHBOARD_ENDPOINT}/s3" \
    | grep -q '^301 .*/dashboard/s3$'
  # React shell serves the S3 dashboard at /dashboard/s3.
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/s3" | grep -q 'devcloud Dashboard'
}

run_s3_journey() {
  local source_file="${TMP_DIR}/readme.txt"
  local download_file="${TMP_DIR}/downloaded.txt"
  local range_file="${TMP_DIR}/range.txt"
  local copy_file="${TMP_DIR}/copy.txt"
  local part1_file="${TMP_DIR}/part1.bin"
  local part2_file="${TMP_DIR}/part2.bin"
  local create_json="${TMP_DIR}/create-multipart.json"
  local part1_json="${TMP_DIR}/part1.json"
  local part2_json="${TMP_DIR}/part2.json"
  local complete_json="${TMP_DIR}/complete-multipart.json"
  local complete_request="${TMP_DIR}/complete-request.json"
  local multipart_file="${TMP_DIR}/multipart.bin"

  printf 'hello from devcloud s3 e2e\n' > "${source_file}"
  printf 'part-one-' > "${part1_file}"
  printf 'part-two' > "${part2_file}"

  awslocal_cmd s3api create-bucket --bucket "${BUCKET}" >/dev/null
  awslocal_cmd s3api list-buckets | grep -q "${BUCKET}"

  awslocal_cmd s3api put-object \
    --bucket "${BUCKET}" \
    --key docs/readme.txt \
    --body "${source_file}" \
    --content-type text/plain \
    --metadata source=awscli-local-e2e >/dev/null

  awslocal_cmd s3api head-object --bucket "${BUCKET}" --key docs/readme.txt |
    python3 -c 'import json,sys; d=json.load(sys.stdin); metadata={k.lower(): v for k, v in d.get("Metadata", {}).items()}; assert d["ContentType"] == "text/plain"; assert metadata["source"] == "awscli-local-e2e"'

  awslocal_cmd s3api get-object --bucket "${BUCKET}" --key docs/readme.txt "${download_file}" >/dev/null
  cmp "${source_file}" "${download_file}"

  awslocal_cmd s3api get-object --bucket "${BUCKET}" --key docs/readme.txt --range bytes=0-4 "${range_file}" >/dev/null
  grep -q '^hello$' "${range_file}"

  awslocal_cmd s3 ls "s3://${BUCKET}/docs/" | grep -q 'readme.txt'

  awslocal_cmd s3 cp "${source_file}" "s3://${BUCKET}/docs/high-level.txt" >/dev/null
  awslocal_cmd s3 cp "s3://${BUCKET}/docs/high-level.txt" "${copy_file}" >/dev/null
  cmp "${source_file}" "${copy_file}"

  awslocal_cmd s3api copy-object \
    --bucket "${BUCKET}" \
    --key docs/readme-copy.txt \
    --copy-source "/${BUCKET}/docs/readme.txt" >/dev/null
  awslocal_cmd s3api delete-object --bucket "${BUCKET}" --key docs/readme-copy.txt >/dev/null

  awslocal_cmd s3api create-multipart-upload \
    --bucket "${BUCKET}" \
    --key large.bin \
    --content-type application/octet-stream > "${create_json}"
  local upload_id
  upload_id="$(json_value 'data["UploadId"]' < "${create_json}")"

  awslocal_cmd s3api upload-part \
    --bucket "${BUCKET}" \
    --key large.bin \
    --part-number 1 \
    --upload-id "${upload_id}" \
    --body "${part1_file}" > "${part1_json}"
  awslocal_cmd s3api upload-part \
    --bucket "${BUCKET}" \
    --key large.bin \
    --part-number 2 \
    --upload-id "${upload_id}" \
    --body "${part2_file}" > "${part2_json}"

  awslocal_cmd s3api list-parts --bucket "${BUCKET}" --key large.bin --upload-id "${upload_id}" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); assert [p["PartNumber"] for p in d["Parts"]] == [1, 2]'

  local etag1 etag2
  etag1="$(json_value 'data["ETag"]' < "${part1_json}")"
  etag2="$(json_value 'data["ETag"]' < "${part2_json}")"
  cat > "${complete_request}" <<EOF
{
  "Parts": [
    {"PartNumber": 1, "ETag": ${etag1}},
    {"PartNumber": 2, "ETag": ${etag2}}
  ]
}
EOF
  awslocal_cmd s3api complete-multipart-upload \
    --bucket "${BUCKET}" \
    --key large.bin \
    --upload-id "${upload_id}" \
    --multipart-upload "file://${complete_request}" > "${complete_json}"
  awslocal_cmd s3api get-object --bucket "${BUCKET}" --key large.bin "${multipart_file}" >/dev/null
  grep -q 'part-one-part-two' "${multipart_file}"

  local abort_json abort_upload_id
  abort_json="${TMP_DIR}/abort-multipart.json"
  awslocal_cmd s3api create-multipart-upload --bucket "${BUCKET}" --key aborted.bin > "${abort_json}"
  abort_upload_id="$(json_value 'data["UploadId"]' < "${abort_json}")"
  awslocal_cmd s3api abort-multipart-upload --bucket "${BUCKET}" --key aborted.bin --upload-id "${abort_upload_id}" >/dev/null

  assert_dashboard_api

  if [[ "${DELETE_DATA}" == "true" ]]; then
    awslocal_cmd s3 rm "s3://${BUCKET}/docs/readme.txt" >/dev/null
    awslocal_cmd s3 rm "s3://${BUCKET}/docs/high-level.txt" >/dev/null
    awslocal_cmd s3 rm "s3://${BUCKET}/large.bin" >/dev/null
    awslocal_cmd s3api delete-bucket --bucket "${BUCKET}" >/dev/null
  fi
}

show_interactive_hint() {
  cat <<EOF
[s3-e2e] browser check:
[s3-e2e]   Dashboard: ${DASHBOARD_ENDPOINT}/dashboard/s3
[s3-e2e]   S3 endpoint: ${S3_ENDPOINT}
[s3-e2e]   Bucket used: ${BUCKET}
[s3-e2e] awscli-local examples:
[s3-e2e]   AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} ${AWSLOCAL_BIN} --endpoint-url=${S3_ENDPOINT} s3 ls s3://${BUCKET}/
[s3-e2e]   AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} ${AWSLOCAL_BIN} --endpoint-url=${S3_ENDPOINT} s3api list-objects-v2 --bucket ${BUCKET}
EOF
}

show_retained_data() {
  if [[ "${DELETE_DATA}" != "false" ]]; then
    return 0
  fi
  log "retained S3 data in bucket: ${BUCKET}"
  log "retained workspace: ${WORKSPACE}"
  log "current bucket contents:"
  awslocal_cmd s3 ls "s3://${BUCKET}/" --recursive || true
}

require_command "${AWSLOCAL_BIN}"
require_command python3
require_command curl

TMP_DIR="$(mktemp -d)"
BIN="${TMP_DIR}/devcloud"
WORKSPACE="${TMP_DIR}/workspace"
mkdir -p "${WORKSPACE}"

log "building devcloud"
devcloud_build "${BIN}"

write_config "${WORKSPACE}"

log "starting devcloud up on s3=${S3_PORT}, dashboard=${DASHBOARD_PORT}, smtp=${SMTP_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for S3 endpoint"
wait_for_http "${S3_ENDPOINT}/"
log "waiting for dashboard"
wait_for_http "${DASHBOARD_ENDPOINT}/"

log "running awscli-local S3 journey"
run_s3_journey
show_retained_data

log "passed"

if [[ "${INTERACTIVE}" == "true" ]]; then
  show_interactive_hint
  log "press Ctrl-C to stop devcloud"
  wait "${DEV_PID}"
fi
