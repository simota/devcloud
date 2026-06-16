#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-4566}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-8025}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-1025}"
VERIFY_HOST="127.0.0.1"
S3_ENDPOINT="http://${VERIFY_HOST}:${S3_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"


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
  local deadline=$((SECONDS + 30))
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
  gcs:
    enabled: false
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

  run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
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
  env -u RIPGREP_CONFIG_PATH rg -q 'S3|s3Port|services\.s3|server\.s3Port|auth\.s3' orchestrator &&
    cargo test --workspace
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

header_value() {
  local name="$1"
  awk -F': ' -v name="${name}" 'tolower($1) == tolower(name) { gsub(/\r/, "", $2); print $2; exit }'
}

versioning_flow() {
  local version1 version2 marker_version headers

  printf '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}?versioning" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?versioning" | grep -q '<Status>Enabled</Status>'

  headers="$(printf 'version-one' | curl -fsS -D - -o /dev/null -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt")"
  version1="$(printf '%s' "${headers}" | header_value 'x-amz-version-id')"
  [[ -n "${version1}" ]]

  headers="$(printf 'version-two' | curl -fsS -D - -o /dev/null -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt")"
  version2="$(printf '%s' "${headers}" | header_value 'x-amz-version-id')"
  [[ -n "${version2}" && "${version2}" != "${version1}" ]]

  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt" | grep -q '^version-two$'
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt?versionId=${version1}" | grep -q '^version-one$'
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?versions&prefix=docs/" | grep -q "${version1}"
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?versions&prefix=docs/" | grep -q "${version2}"

  headers="$(curl -fsS -D - -o /dev/null -X DELETE "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt")"
  marker_version="$(printf '%s' "${headers}" | header_value 'x-amz-version-id')"
  [[ -n "${marker_version}" ]]
  printf '%s' "${headers}" | grep -qi 'x-amz-delete-marker: true'
  if curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt" >/dev/null 2>&1; then
    echo "latest object should be hidden by delete marker" >&2
    return 1
  fi
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt?versionId=${version2}" | grep -q '^version-two$'
  curl -fsS -X DELETE "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt?versionId=${marker_version}" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/versioned.txt" | grep -q '^version-two$'
}

policy_acl_flow() {
  local policy
  policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::devcloud-s3-verify/*"}]}'

  printf '%s' "${policy}" |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}?policy" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?policy" | grep -q '"Action":"s3:GetObject"'
  curl -fsS -X DELETE "${S3_ENDPOINT}/${BUCKET}?policy" >/dev/null
  if curl -fsS "${S3_ENDPOINT}/${BUCKET}?policy" >/dev/null 2>&1; then
    echo "deleted bucket policy should not be returned" >&2
    return 1
  fi

  curl -fsS -X PUT -H 'x-amz-acl: public-read' "${S3_ENDPOINT}/${BUCKET}?acl" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?acl" | grep -q '<CannedACL>public-read</CannedACL>'

  printf 'acl-body' | curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/docs/acl.txt" >/dev/null
  curl -fsS -X PUT -H 'x-amz-acl: bucket-owner-full-control' "${S3_ENDPOINT}/${BUCKET}/docs/acl.txt?acl" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/acl.txt?acl" | grep -q '<CannedACL>bucket-owner-full-control</CannedACL>'
}

lifecycle_flow() {
  local config unsupported
  config='<LifecycleConfiguration><Rule><ID>expire-logs-now</ID><Prefix>logs/</Prefix><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>'

  printf 'expired-log' |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/logs/expired.txt" >/dev/null
  printf 'kept-doc' |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/docs/lifecycle-keep.txt" >/dev/null

  printf '%s' "${config}" |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}?lifecycle" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?lifecycle" | grep -q '<ID>expire-logs-now</ID>'

  if curl -fsS "${S3_ENDPOINT}/${BUCKET}/logs/expired.txt" >/dev/null 2>&1; then
    echo "expired lifecycle object should not be returned" >&2
    return 1
  fi
  curl -fsS "${S3_ENDPOINT}/${BUCKET}/docs/lifecycle-keep.txt" | grep -q '^kept-doc$'
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?list-type=2" | grep -q 'docs/lifecycle-keep.txt'
  if curl -fsS "${S3_ENDPOINT}/${BUCKET}?list-type=2" | grep -q 'logs/expired.txt'; then
    echo "expired lifecycle object should not be listed" >&2
    return 1
  fi

  unsupported='<LifecycleConfiguration><Rule><ID>transition</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>'
  if printf '%s' "${unsupported}" |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}?lifecycle" >/dev/null 2>&1; then
    echo "unsupported lifecycle transition should fail" >&2
    return 1
  fi

  curl -fsS -X DELETE "${S3_ENDPOINT}/${BUCKET}?lifecycle" >/dev/null
  if curl -fsS "${S3_ENDPOINT}/${BUCKET}?lifecycle" >/dev/null 2>&1; then
    echo "deleted lifecycle configuration should not be returned" >&2
    return 1
  fi
}

notification_flow() {
  local config unsupported
  config='<NotificationConfiguration><QueueConfiguration><Id>docs-created</Id><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>docs/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>'

  printf '%s' "${config}" |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}?notification" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?notification" | grep -q '<Id>docs-created</Id>'
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?notification" | grep -q '<Event>s3:ObjectCreated:\*</Event>'

  printf 'notify-body' |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/docs/notify.txt" >/dev/null

  unsupported='<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ReducedRedundancyLostObject</Event></QueueConfiguration></NotificationConfiguration>'
  if printf '%s' "${unsupported}" |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}?notification" >/dev/null 2>&1; then
    echo "unsupported notification event should fail" >&2
    return 1
  fi
}

s3_select_flow() {
  local request
  printf 'name,age\nalice,31\nbob,28\n' |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/reports/users.csv" >/dev/null
  request='<SelectObjectContentRequest><Expression>SELECT * FROM S3Object</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization><OutputSerialization><CSV /></OutputSerialization></SelectObjectContentRequest>'
  printf '%s' "${request}" |
    curl -fsS -X POST --data-binary @- "${S3_ENDPOINT}/${BUCKET}/reports/users.csv?select&select-type=2" |
    grep -a -q 'alice,31'

  request='<SelectObjectContentRequest><Expression>SELECT name FROM S3Object</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><CSV /></InputSerialization><OutputSerialization><CSV /></OutputSerialization></SelectObjectContentRequest>'
  if printf '%s' "${request}" |
    curl -fsS -X POST --data-binary @- "${S3_ENDPOINT}/${BUCKET}/reports/users.csv?select&select-type=2" >/dev/null 2>&1; then
    echo "unsupported S3 Select SQL should fail" >&2
    return 1
  fi
}

replication_flow() {
  local replica_bucket config
  replica_bucket="${BUCKET}-replica"
  curl -fsS -X PUT "${S3_ENDPOINT}/${replica_bucket}" >/dev/null
  config="<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/devcloud</Role><Rule><ID>docs-replica</ID><Status>Enabled</Status><Filter><Prefix>docs/</Prefix></Filter><Destination><Bucket>arn:aws:s3:::${replica_bucket}</Bucket><StorageClass>STANDARD</StorageClass></Destination></Rule></ReplicationConfiguration>"

  printf '%s' "${config}" |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}?replication" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${BUCKET}?replication" | grep -q '<ID>docs-replica</ID>'

  printf 'replicated-body' |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/docs/replicated.txt" >/dev/null
  curl -fsS "${S3_ENDPOINT}/${replica_bucket}/docs/replicated.txt" | grep -q '^replicated-body$'

  printf 'ignored-body' |
    curl -fsS -X PUT --data-binary @- "${S3_ENDPOINT}/${BUCKET}/logs/not-replicated.txt" >/dev/null
  if curl -fsS "${S3_ENDPOINT}/${replica_bucket}/logs/not-replicated.txt" >/dev/null 2>&1; then
    echo "non-matching replication prefix should not create destination object" >&2
    return 1
  fi
}

object_lock_flow() {
  local retain_until status
  retain_until='2099-01-01T00:00:00Z'

  printf 'governance-body' |
    curl -fsS -X PUT \
      -H 'x-amz-object-lock-mode: GOVERNANCE' \
      -H "x-amz-object-lock-retain-until-date: ${retain_until}" \
      --data-binary @- "${S3_ENDPOINT}/${BUCKET}/locks/governance.txt" >/dev/null

  status="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "${S3_ENDPOINT}/${BUCKET}/locks/governance.txt")"
  [[ "${status}" == "403" ]]

  status="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H 'x-amz-bypass-governance-retention: not-bool' "${S3_ENDPOINT}/${BUCKET}/locks/governance.txt")"
  [[ "${status}" == "400" ]]

  curl -fsS -X DELETE -H 'x-amz-bypass-governance-retention: true' "${S3_ENDPOINT}/${BUCKET}/locks/governance.txt" >/dev/null

  printf 'compliance-body' |
    curl -fsS -X PUT \
      -H 'x-amz-object-lock-mode: COMPLIANCE' \
      -H "x-amz-object-lock-retain-until-date: ${retain_until}" \
      --data-binary @- "${S3_ENDPOINT}/${BUCKET}/locks/compliance.txt" >/dev/null
  status="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H 'x-amz-bypass-governance-retention: true' "${S3_ENDPOINT}/${BUCKET}/locks/compliance.txt")"
  [[ "${status}" == "403" ]]

  printf 'hold-body' |
    curl -fsS -X PUT -H 'x-amz-object-lock-legal-hold: ON' --data-binary @- "${S3_ENDPOINT}/${BUCKET}/locks/legal-hold.txt" >/dev/null
  status="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H 'x-amz-bypass-governance-retention: true' "${S3_ENDPOINT}/${BUCKET}/locks/legal-hold.txt")"
  [[ "${status}" == "403" ]]
}

dashboard_s3_api() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/status" | grep -q '"running"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/buckets" | grep -q "${BUCKET}"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/s3/buckets/${BUCKET}/objects?prefix=docs/" | grep -q 'docs/readme.txt'
}

dashboard_s3_page() {
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/s3" | grep -qi 'devcloud Dashboard'
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

run_check "Rust tests pass" cargo test --workspace

case "${VERIFY_STAGE}" in
  foundation)
    run_check "devcloud help works" cargo run -p devcloud-orchestrator -- help
    TMP_DIR="$(mktemp -d)"
    run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
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
    run_check "devcloud help works" cargo run -p devcloud-orchestrator -- help
    run_s3_core_checks
    run_check "Presigned URL validates" presigned_url_get
    run_check "Multipart upload flow works" multipart_flow
    run_check "S3 versioning flow works" versioning_flow
    run_check "S3 policy and ACL metadata flow works" policy_acl_flow
    run_check "S3 lifecycle expiration flow works" lifecycle_flow
    run_check "S3 notification metadata flow works" notification_flow
    run_check "S3 Select narrow CSV flow works" s3_select_flow
    run_check "S3 replication metadata and local copy flow works" replication_flow
    run_check "S3 Object Lock governance bypass flow works" object_lock_flow
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
