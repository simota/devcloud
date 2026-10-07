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

LAMBDA_VERIFY_PORT="${LAMBDA_VERIFY_PORT:-$(free_port)}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-$(free_port)}"
EVENT_RELAY_VERIFY_PORT="${EVENT_RELAY_VERIFY_PORT:-$(free_port)}"
LAMBDA_ENDPOINT="http://127.0.0.1:${LAMBDA_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_VERIFY_PORT}"

REGION="us-east-1"
ACCOUNT_ID="000000000000"
FUNCTION_NAME="devcloud-lambda-loop"
NODE_FUNCTION_NAME="devcloud-lambda-loop-node"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-lambda-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-lambda-verify.err"
STATUS_OUT="${TMPDIR:-/tmp}/devcloud-lambda-verify.status"
HEADERS_OUT="${TMPDIR:-/tmp}/devcloud-lambda-verify.headers"

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

json_assert() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data}), data' "$1"
}

lambda_call() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "${body}" ]]; then
    curl -fsS -D "${HEADERS_OUT}" -X "${method}" -H 'Content-Type: application/json' --data-binary "${body}" "${LAMBDA_ENDPOINT}${path}"
  else
    curl -fsS -D "${HEADERS_OUT}" -X "${method}" "${LAMBDA_ENDPOINT}${path}"
  fi
}

# Raw status for error-path checks (curl -f would hide the body).
lambda_status() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  curl -sS -o "${STATUS_OUT}" -D "${HEADERS_OUT}" -w '%{http_code}' -X "${method}" \
    -H 'Content-Type: application/json' --data-binary "${body}" "${LAMBDA_ENDPOINT}${path}"
}

header_value() {
  grep -i "^$1:" "${HEADERS_OUT}" | head -1 | cut -d: -f2- | tr -d '\r' | sed 's/^ *//'
}

# package_b64 FILENAME SOURCE
package_b64() {
  python3 - "$1" "$2" <<'PY'
import base64, io, sys, zipfile
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr(sys.argv[1], sys.argv[2])
print(base64.b64encode(buf.getvalue()).decode())
PY
}

PY_HANDLER='def handler(event, context):
    if event.get("fail"):
        raise KeyError("missing")
    return {"ok": True, "input": event}
'
NODE_HANDLER='exports.handler = async (event) => ({ node: true, doubled: (event.n || 0) * 2 });
'

wait_for_lambda() {
  local deadline=$((SECONDS + 30))
  until curl -fsS "${LAMBDA_ENDPOINT}/2015-03-31/functions/" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      sed 's/^/[lambda-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2 || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

wait_for_http() {
  local deadline=$((SECONDS + 30))
  until curl -fsS "$1" >/dev/null 2>&1; do
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
project: lambda-autoloop

server:
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  eventRelayPort: ${EVENT_RELAY_VERIFY_PORT}
  lambdaPort: ${LAMBDA_VERIFY_PORT}

storage:
  path: .devcloud/data

services:
  mail:
    enabled: false
  s3:
    enabled: false
  gcs:
    enabled: false
  dynamodb:
    enabled: false
  bigquery:
    enabled: false
  redshift:
    enabled: false
  redis:
    enabled: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
  appAutoScaling:
    enabled: false
  cloudRun:
    enabled: false
  lambda:
    enabled: true
    region: ${REGION}
EOF

  run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
  if [[ "${FAIL}" -gt 0 ]]; then
    return 1
  fi
  # `exec` keeps $! pointing at the devcloud process itself (bash 3.2 orphan
  # leak otherwise; see scripts/applicationautoscaling-autoloop/verify.sh).
  (
    cd "${TMP_DIR}"
    exec "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

ensure_started() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
    wait_for_lambda
  fi
}

# --- foundation / config ------------------------------------------------------

assert_script_contract() {
  bash -n scripts/lambda-autoloop/verify.sh &&
    bash -n scripts/lambda-e2e.sh
}

assert_config_shape() {
  grep -q 'lambdaPort' orchestrator/src/config.rs &&
    grep -q 'services::lambda::run' orchestrator/src/supervisor.rs &&
    grep -q '"lambda"' services/dashboard/src/services.rs
}

# --- provider protocol (lambda-core) ----------------------------------------

create_python_function() {
  local zip
  zip="$(package_b64 app.py "${PY_HANDLER}")"
  lambda_call POST /2015-03-31/functions "{
    \"FunctionName\":\"${FUNCTION_NAME}\",
    \"Runtime\":\"python3.12\",
    \"Role\":\"arn:aws:iam::${ACCOUNT_ID}:role/devcloud-lambda-loop\",
    \"Handler\":\"app.handler\",
    \"Code\":{\"ZipFile\":\"${zip}\"}
  }" | json_assert 'data["FunctionName"] == "'"${FUNCTION_NAME}"'" and data["Runtime"] == "python3.12"'
}

create_node_function() {
  local zip
  zip="$(package_b64 index.js "${NODE_HANDLER}")"
  lambda_call POST /2015-03-31/functions "{
    \"FunctionName\":\"${NODE_FUNCTION_NAME}\",
    \"Runtime\":\"nodejs20.x\",
    \"Role\":\"arn:aws:iam::${ACCOUNT_ID}:role/devcloud-lambda-loop\",
    \"Handler\":\"index.handler\",
    \"Code\":{\"ZipFile\":\"${zip}\"}
  }" | json_assert 'data["Runtime"] == "nodejs20.x"'
}

list_functions() {
  lambda_call GET /2015-03-31/functions/ | json_assert 'len(data["Functions"]) == 2'
}

get_function_by_arn() {
  lambda_call GET "/2015-03-31/functions/arn%3Aaws%3Alambda%3A${REGION}%3A${ACCOUNT_ID}%3Afunction%3A${FUNCTION_NAME}" |
    json_assert 'data["Configuration"]["FunctionName"] == "'"${FUNCTION_NAME}"'"'
}

invoke_python() {
  lambda_call POST "/2015-03-31/functions/${FUNCTION_NAME}/invocations" '{"k":"v"}' |
    json_assert 'data == {"ok": True, "input": {"k": "v"}}'
}

invoke_python_function_error() {
  lambda_call POST "/2015-03-31/functions/${FUNCTION_NAME}/invocations" '{"fail":true}' |
    json_assert 'data["errorType"] == "KeyError"' &&
    [[ "$(header_value X-Amz-Function-Error)" == "Unhandled" ]]
}

invoke_node() {
  if ! command -v node >/dev/null 2>&1; then
    echo "node not installed; skipping node invoke" >&2
    return 0
  fi
  lambda_call POST "/2015-03-31/functions/${NODE_FUNCTION_NAME}/invocations" '{"n":21}' |
    json_assert 'data == {"node": True, "doubled": 42}'
}

update_configuration() {
  lambda_call PUT "/2015-03-31/functions/${FUNCTION_NAME}/configuration" '{"Timeout":15,"Environment":{"Variables":{"A":"1"}}}' |
    json_assert 'data["Timeout"] == 15 and data["Environment"]["Variables"] == {"A": "1"}'
}

tag_lifecycle() {
  local arn="arn%3Aaws%3Alambda%3A${REGION}%3A${ACCOUNT_ID}%3Afunction%3A${FUNCTION_NAME}"
  curl -fsS -X POST --data '{"Tags":{"suite":"lambda-autoloop"}}' "${LAMBDA_ENDPOINT}/2017-03-31/tags/${arn}" >/dev/null &&
    lambda_call GET "/2017-03-31/tags/${arn}" | json_assert 'data["Tags"] == {"suite": "lambda-autoloop"}' &&
    curl -fsS -X DELETE "${LAMBDA_ENDPOINT}/2017-03-31/tags/${arn}?tagKeys=suite" >/dev/null &&
    lambda_call GET "/2017-03-31/tags/${arn}" | json_assert 'data["Tags"] == {}'
}

function_url_lifecycle() {
  local url host id
  url="$(lambda_call POST "/2021-10-31/functions/${FUNCTION_NAME}/url" '{"AuthType":"NONE"}' |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["FunctionUrl"])')" || return 1
  host="${url#http://}"
  host="${host%/}"
  id="${host%%.*}"
  # Host form (the FunctionUrl) and the /_url/ path form reach the handler
  # with a payload 2.0 event.
  curl -fsS -H "Host: ${host}" "${LAMBDA_ENDPOINT}/hello?x=1" |
    json_assert 'data["ok"] and data["input"]["version"] == "2.0" and data["input"]["rawPath"] == "/hello" and data["input"]["queryStringParameters"] == {"x": "1"}' &&
    curl -fsS "${LAMBDA_ENDPOINT}/_url/${id}/path-form" |
    json_assert 'data["input"]["rawPath"] == "/path-form"' &&
    lambda_call PUT "/2021-10-31/functions/${FUNCTION_NAME}/url" '{"AuthType":"AWS_IAM"}' >/dev/null &&
    [[ "$(curl -sS -o /dev/null -w '%{http_code}' "${LAMBDA_ENDPOINT}/_url/${id}/")" == "403" ]] &&
    curl -fsS -X DELETE "${LAMBDA_ENDPOINT}/2021-10-31/functions/${FUNCTION_NAME}/url" >/dev/null &&
    lambda_call GET "/2021-10-31/functions/${FUNCTION_NAME}/urls" | json_assert 'data["FunctionUrlConfigs"] == []'
}

# --- dashboard ----------------------------------------------------------------

dashboard_starts() {
  ensure_started && wait_for_http "${DASHBOARD_ENDPOINT}/"
}

dashboard_registry_has_lambda() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" |
    json_assert 'any(s["id"] == "lambda" and s["status"] == "running" for s in data["services"])'
}

dashboard_status() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/lambda/status" |
    json_assert 'data["service"] == "lambda" and data["running"] == True and data["functionCount"] == 2'
}

dashboard_functions() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/lambda/functions" |
    json_assert 'sorted(f["FunctionName"] for f in data["functions"]) == ["'"${FUNCTION_NAME}"'", "'"${NODE_FUNCTION_NAME}"'"]'
}

dashboard_invoke() {
  curl -fsS -X POST --data '{"from":"dashboard"}' "${DASHBOARD_ENDPOINT}/api/lambda/functions/${FUNCTION_NAME}/invoke" |
    json_assert 'data["functionError"] is None and "dashboard" in data["payload"] and "REPORT RequestId:" in data["log"]'
}

dashboard_invocations() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/lambda/invocations" |
    json_assert 'len(data["invocations"]) >= 3 and data["invocations"][0]["functionName"] == "'"${FUNCTION_NAME}"'"'
}

dashboard_page_loads() {
  curl -fsSL "${DASHBOARD_ENDPOINT}/dashboard/lambda" | grep -q '<div id="root"></div>'
}

# --- hardening ----------------------------------------------------------------

rejects_duplicate_function() {
  local zip code
  zip="$(package_b64 app.py "${PY_HANDLER}")"
  code="$(lambda_status POST /2015-03-31/functions "{\"FunctionName\":\"${FUNCTION_NAME}\",\"Runtime\":\"python3.12\",\"Role\":\"r\",\"Handler\":\"app.handler\",\"Code\":{\"ZipFile\":\"${zip}\"}}")"
  [[ "${code}" == "409" && "$(header_value X-Amzn-ErrorType)" == "ResourceConflictException" ]]
}

rejects_invalid_package() {
  local code
  code="$(lambda_status POST /2015-03-31/functions '{"FunctionName":"bad-zip","Runtime":"python3.12","Role":"r","Handler":"a.b","Code":{"ZipFile":"bm90IGEgemlw"}}')"
  [[ "${code}" == "400" && "$(header_value X-Amzn-ErrorType)" == "InvalidParameterValueException" ]]
}

rejects_invalid_payload() {
  local code
  code="$(lambda_status POST "/2015-03-31/functions/${FUNCTION_NAME}/invocations" '{not json')"
  [[ "${code}" == "400" && "$(header_value X-Amzn-ErrorType)" == "InvalidRequestContentException" ]]
}

missing_function_is_404() {
  local code
  code="$(lambda_status GET /2015-03-31/functions/does-not-exist '')"
  [[ "${code}" == "404" && "$(header_value X-Amzn-ErrorType)" == "ResourceNotFoundException" ]]
}

timeout_reports_function_error() {
  local zip
  zip="$(package_b64 slow.py 'import time
def handler(event, context):
    time.sleep(5)
')"
  lambda_call POST /2015-03-31/functions "{\"FunctionName\":\"slow-loop\",\"Runtime\":\"python3.12\",\"Role\":\"r\",\"Handler\":\"slow.handler\",\"Timeout\":1,\"Code\":{\"ZipFile\":\"${zip}\"}}" >/dev/null &&
    lambda_call POST /2015-03-31/functions/slow-loop/invocations '{}' |
    json_assert '"Task timed out after 1.00 seconds" in data["errorMessage"]' &&
    [[ "$(header_value X-Amz-Function-Error)" == "Unhandled" ]]
}

dashboard_rejects_wrong_method() {
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${DASHBOARD_ENDPOINT}/api/lambda/status")"
  [[ "${code}" == "405" ]]
}

delete_functions() {
  local f code
  for f in "${FUNCTION_NAME}" "${NODE_FUNCTION_NAME}" slow-loop; do
    code="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "${LAMBDA_ENDPOINT}/2015-03-31/functions/${f}")"
    [[ "${code}" == "204" ]] || return 1
  done
  lambda_call GET /2015-03-31/functions/ | json_assert 'data["Functions"] == []'
}

# --- stage runners ------------------------------------------------------------

run_foundation_checks() {
  run_check "autoloop script contract" assert_script_contract
  run_check "lambda crate tests pass" cargo test -p devcloud-lambda
}

run_config_checks() {
  run_check "orchestrator/dashboard wire lambda" assert_config_shape
}

run_core_checks() {
  run_check "Lambda endpoint starts" ensure_started
  run_check "CreateFunction (python3.12) works" create_python_function
  run_check "CreateFunction (nodejs20.x) works" create_node_function
  run_check "ListFunctions works" list_functions
  run_check "GetFunction by ARN works" get_function_by_arn
  run_check "Invoke python handler works" invoke_python
  run_check "Invoke surfaces function errors" invoke_python_function_error
  run_check "Invoke node handler works" invoke_node
  run_check "UpdateFunctionConfiguration works" update_configuration
  run_check "Tag/Untag lifecycle works" tag_lifecycle
  run_check "Function URL lifecycle works" function_url_lifecycle
}

run_dashboard_checks() {
  run_check "dashboard starts" dashboard_starts
  run_check "dashboard registry has Lambda" dashboard_registry_has_lambda
  run_check "dashboard status reports running" dashboard_status
  run_check "dashboard lists functions" dashboard_functions
  run_check "dashboard test invoke works" dashboard_invoke
  run_check "dashboard lists invocations" dashboard_invocations
  run_check "dashboard SPA page loads" dashboard_page_loads
}

run_hardening_checks() {
  run_check "duplicate function rejected" rejects_duplicate_function
  run_check "invalid deployment package rejected" rejects_invalid_package
  run_check "invalid invoke payload rejected" rejects_invalid_payload
  run_check "missing function is 404" missing_function_is_404
  run_check "handler timeout reports function error" timeout_reports_function_error
  run_check "dashboard API rejects wrong method" dashboard_rejects_wrong_method
  run_check "DeleteFunction removes functions" delete_functions
}

run_e2e_checks() {
  run_check "Lambda standalone E2E script passes" bash scripts/lambda-e2e.sh
}

echo "=== Lambda autoloop verification: ${VERIFY_STAGE} ==="

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  config)
    run_foundation_checks
    run_config_checks
    ;;
  lambda|lambda-core)
    run_foundation_checks
    run_config_checks
    run_core_checks
    ;;
  dashboard|dashboard-static)
    run_foundation_checks
    run_config_checks
    run_core_checks
    run_dashboard_checks
    ;;
  hardening|full)
    run_foundation_checks
    run_config_checks
    run_core_checks
    run_dashboard_checks
    run_hardening_checks
    run_e2e_checks
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE: ${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Lambda autoloop verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
