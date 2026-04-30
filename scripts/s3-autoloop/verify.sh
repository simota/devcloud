#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-4566}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-8025}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-1025}"
VERIFY_HOST="127.0.0.1"
S3_ENDPOINT="http://${VERIFY_HOST}:${S3_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-s3-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-s3-verify.err"
BUCKET="devcloud-loop-demo"

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

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: dev

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}

auth:
  smtp:
    mode: off
  s3:
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

assert_s3_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'S3|s3Port|services\.s3|server\.s3Port|auth\.s3' internal/app &&
    go test ./internal/app -run 'Test.*S3|TestDefaultConfig|TestInitWorkspace|TestLoadConfig' -count=1
}

s3_endpoint_starts() {
  start_devcloud || return 1
  wait_for_http "${S3_ENDPOINT}/"
}

dashboard_starts() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
  fi
  wait_for_http "${DASHBOARD_ENDPOINT}/"
}

put_bucket() {
  curl -fsS -X PUT "${S3_ENDPOINT}/${BUCKET}" >/dev/null
}

head_bucket() {
  curl -fsSI "${S3_ENDPOINT}/${BUCKET}" >/dev/null
}

put_object() {
  printf 'hello from devcloud s3\n' | curl -fsS \
    -X PUT \
    -H 'Content-Type: text/plain' \
    -H 'x-amz-meta-source: s3-autoloop' \
    --data-binary @- \
    "${S3_ENDPOINT}/${BUCKET}/docs/readme.txt" >/dev/null
}

head_object() {
  curl -fsSI "${S3_ENDPOINT}/${BUCKET}/docs/readme.txt" | grep -qi 'content-type: text/plain'
}

get_object() {
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/readme.txt" | grep -q 'hello from devcloud s3'
}

range_get_object() {
  curl -fsS -H 'Range: bytes=0-4' "${S3_ENDPOINT}/${BUCKET}/docs/readme.txt" | grep -q '^hello$'
}

list_objects_v2() {
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?list-type=2&prefix=docs/" | grep -q 'docs/readme.txt'
}

copy_object() {
  curl -fsS \
    -X PUT \
    -H "x-amz-copy-source: /${BUCKET}/docs/readme.txt" \
    "${S3_ENDPOINT}/${BUCKET}/docs/readme-copy.txt" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/readme-copy.txt" | grep -q 'hello from devcloud s3'
}

delete_object() {
  curl -fsS -X DELETE "${S3_ENDPOINT}/${BUCKET}/docs/readme-copy.txt" >/dev/null
}

list_buckets() {
  curl -fsS "${S3_ENDPOINT}/" | grep -q "${BUCKET}"
}

presigned_url_get() {
  local url
  url="$(python3 - "${S3_ENDPOINT}" "${BUCKET}" <<'PY'
import datetime
import hashlib
import hmac
import sys
import urllib.parse

endpoint = sys.argv[1]
bucket = sys.argv[2]
access_key = "dev"
secret_key = "dev"
region = "us-east-1"
service = "s3"
method = "GET"
key = "docs/readme.txt"
parsed = urllib.parse.urlparse(endpoint)
host = parsed.netloc
path = f"/{bucket}/{key}"
now = datetime.datetime.utcnow()
amz_date = now.strftime("%Y%m%dT%H%M%SZ")
date_stamp = now.strftime("%Y%m%d")
scope = f"{date_stamp}/{region}/{service}/aws4_request"
credential = f"{access_key}/{scope}"
query = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": credential,
    "X-Amz-Date": amz_date,
    "X-Amz-Expires": "300",
    "X-Amz-SignedHeaders": "host",
}
canonical_query = "&".join(
    f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='~-_')}"
    for k, v in sorted(query.items())
)
canonical_request = "\n".join([
    method,
    urllib.parse.quote(path, safe="/~"),
    canonical_query,
    f"host:{host}\n",
    "host",
    "UNSIGNED-PAYLOAD",
])
string_to_sign = "\n".join([
    "AWS4-HMAC-SHA256",
    amz_date,
    scope,
    hashlib.sha256(canonical_request.encode()).hexdigest(),
])
def sign(key_bytes, message):
    return hmac.new(key_bytes, message.encode(), hashlib.sha256).digest()
signing_key = sign(sign(sign(sign(("AWS4" + secret_key).encode(), date_stamp), region), service), "aws4_request")
signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
print(f"{endpoint}{path}?{canonical_query}&X-Amz-Signature={signature}")
PY
)"
  curl -fsS "${url}" | grep -q 'hello from devcloud s3'
}

multipart_flow() {
  local create_xml upload_id complete_xml
  create_xml="$(curl -fsS -X POST "${S3_ENDPOINT}/${BUCKET}/large.bin?uploads")"
  upload_id="$(printf '%s' "${create_xml}" | python3 -c 'import re,sys; m=re.search(r"<UploadId>([^<]+)</UploadId>", sys.stdin.read()); print(m.group(1) if m else "")')"
  if [[ -z "${upload_id}" ]]; then
    echo "missing upload id" >&2
    return 1
  fi

  printf 'part-one-' | curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/large.bin?partNumber=1&uploadId=${upload_id}" >/dev/null
  printf 'part-two' | curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/large.bin?partNumber=2&uploadId=${upload_id}" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/large.bin?uploadId=${upload_id}" | grep -q '<PartNumber>1</PartNumber>'

  complete_xml='<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>ignored</ETag></Part><Part><PartNumber>2</PartNumber><ETag>ignored</ETag></Part></CompleteMultipartUpload>'
  printf '%s' "${complete_xml}" | curl -fsS -X POST --data-binary @- "${S3_ENDPOINT}/${BUCKET}/large.bin?uploadId=${upload_id}" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/large.bin" | grep -q 'part-one-part-two'

  create_xml="$(curl -fsS -X POST "${S3_ENDPOINT}/${BUCKET}/aborted.bin?uploads")"
  upload_id="$(printf '%s' "${create_xml}" | python3 -c 'import re,sys; m=re.search(r"<UploadId>([^<]+)</UploadId>", sys.stdin.read()); print(m.group(1) if m else "")')"
  [[ -n "${upload_id}" ]]
  curl -fsS -X DELETE "${S3_ENDPOINT}/${BUCKET}/aborted.bin?uploadId=${upload_id}" >/dev/null
}

dashboard_s3_api() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/status" | grep -q '"running"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/buckets" | grep -q "${BUCKET}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/buckets/${BUCKET}/objects?prefix=docs/" | grep -q 'docs/readme.txt'
}

dashboard_s3_page() {
  curl -fsS "${DASHBOARD_ENDPOINT}/s3" | grep -qi 'devcloud S3'
}

run_s3_core_checks() {
  run_check "S3 endpoint starts" s3_endpoint_starts
  run_check "Dashboard HTTP starts" dashboard_starts
  run_check "CreateBucket works" put_bucket
  run_check "HeadBucket works" head_bucket
  run_check "PutObject stores body and metadata" put_object
  run_check "HeadObject exposes content headers" head_object
  run_check "GetObject returns body" get_object
  run_check "Range GET returns partial body" range_get_object
  run_check "ListObjectsV2 returns object key" list_objects_v2
  run_check "CopyObject copies existing object" copy_object
  run_check "DeleteObject removes copied object" delete_object
  run_check "ListBuckets returns created bucket" list_buckets
}

echo "=== Verification stage: ${VERIFY_STAGE} ==="

run_check "Go tests pass" go test ./...

case "${VERIFY_STAGE}" in
  foundation)
    run_check "devcloud help works" go run ./cmd/devcloud help
    run_check "devcloudd help works" go run ./cmd/devcloudd -h
    TMP_DIR="$(mktemp -d)"
    run_check "devcloud binary builds" go build -o "${TMP_DIR}/devcloud" ./cmd/devcloud
    ;;
  config)
    run_check "S3 config tests pass" assert_s3_config_shape
    ;;
  s3|s3-core|sigv4)
    run_s3_core_checks
    run_check "Presigned URL validates" presigned_url_get
    ;;
  multipart|s3-multipart)
    run_s3_core_checks
    run_check "Multipart upload flow works" multipart_flow
    ;;
  dashboard|dashboard-static)
    run_s3_core_checks
    run_check "S3 dashboard API exposes buckets and objects" dashboard_s3_api
    run_check "S3 dashboard page renders" dashboard_s3_page
    ;;
  hardening|full)
    run_check "devcloud help works" go run ./cmd/devcloud help
    run_check "devcloudd help works" go run ./cmd/devcloudd -h
    run_s3_core_checks
    run_check "Presigned URL validates" presigned_url_get
    run_check "Multipart upload flow works" multipart_flow
    run_check "S3 dashboard API exposes buckets and objects" dashboard_s3_api
    run_check "S3 dashboard page renders" dashboard_s3_page
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
