#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

LAMBDA_PORT="${E2E_LAMBDA_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
EVENT_RELAY_PORT="${E2E_EVENT_RELAY_PORT:-}"
LAMBDA_ENDPOINT=""
DASHBOARD_ENDPOINT=""
REGION="${E2E_LAMBDA_REGION:-us-east-1}"
ACCOUNT_ID="${E2E_LAMBDA_ACCOUNT_ID:-000000000000}"
FUNCTION_NAME="${E2E_LAMBDA_FUNCTION_NAME:-devcloud-lambda-e2e-$(date +%s)}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

TMP_DIR=""
DEV_PID=""
WORKSPACE=""
HEADERS_OUT=""

usage() {
  cat <<'EOF'
Usage:
  scripts/lambda-e2e.sh

Environment:
  E2E_LAMBDA_PORT=19010              Override the Lambda endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025           Override the dashboard port. Defaults to an available port.
  E2E_EVENT_RELAY_PORT=18027         Override the dashboard event relay port. Defaults to an available port.
  E2E_LAMBDA_REGION=us-east-1        Override the configured region.
  E2E_LAMBDA_ACCOUNT_ID=000000000000 Override the configured account id.
  E2E_LAMBDA_FUNCTION_NAME=...       Override the function name.
  E2E_DELETE_DATA=false              Skip DeleteFunction so the function stays behind for inspection.
  E2E_KEEP_WORKDIR=true              Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true               Keep devcloud running after assertions (and keep the function).

Requires python3 (handler runtime + JSON assertions) and curl.

Examples:
  scripts/lambda-e2e.sh
  E2E_INTERACTIVE=true scripts/lambda-e2e.sh
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

log() {
  printf '[lambda-e2e] %s\n' "$1"
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
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[lambda-e2e] missing command: $1" >&2
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
  [[ -n "${LAMBDA_PORT}" ]] || LAMBDA_PORT="$(find_free_port)"
  [[ -n "${DASHBOARD_PORT}" ]] || DASHBOARD_PORT="$(find_free_port)"
  [[ -n "${EVENT_RELAY_PORT}" ]] || EVENT_RELAY_PORT="$(find_free_port)"
  LAMBDA_ENDPOINT="http://127.0.0.1:${LAMBDA_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

json_value() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "$1"
}

json_assert() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data}), data' "$1"
}

# lambda_call METHOD PATH [BODY] — provider-protocol call; response headers land
# in ${HEADERS_OUT}.
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

header_value() {
  grep -i "^$1:" "${HEADERS_OUT}" | head -1 | cut -d: -f2- | tr -d '\r' | sed 's/^ *//'
}

# package_b64 SOURCE — base64 of a zip holding app.py with SOURCE.
package_b64() {
  python3 - "$1" <<'PY'
import base64, io, sys, zipfile
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("app.py", sys.argv[1])
print(base64.b64encode(buf.getvalue()).decode())
PY
}

HANDLER_V1='import os
def handler(event, context):
    print("e2e log line")
    if event.get("fail"):
        raise RuntimeError("requested failure")
    return {"echo": event, "function": context.function_name, "stage": os.environ.get("STAGE")}
'
HANDLER_V2='def handler(event, context):
    return {"version": 2}
'

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 30))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[lambda-e2e] devcloud exited while waiting for ${url}" >&2
      sed 's/^/[lambda-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2 || true
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
project: lambda-e2e

server:
  dashboardPort: ${DASHBOARD_PORT}
  eventRelayPort: ${EVENT_RELAY_PORT}
  lambdaPort: ${LAMBDA_PORT}

auth:
  lambda:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "${ACCOUNT_ID}"

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
}

create_function() {
  local zip
  zip="$(package_b64 "${HANDLER_V1}")"
  lambda_call POST /2015-03-31/functions "{
    \"FunctionName\":\"${FUNCTION_NAME}\",
    \"Runtime\":\"python3.12\",
    \"Role\":\"arn:aws:iam::${ACCOUNT_ID}:role/devcloud-lambda-e2e\",
    \"Handler\":\"app.handler\",
    \"Timeout\":10,
    \"Environment\":{\"Variables\":{\"STAGE\":\"e2e\"}},
    \"Tags\":{\"suite\":\"lambda-e2e\"},
    \"Code\":{\"ZipFile\":\"${zip}\"}
  }" | json_assert 'data["FunctionArn"] == "arn:aws:lambda:'"${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"'" and data["State"] == "Active"'
}

exercise_reads() {
  lambda_call GET /2015-03-31/functions/ |
    json_assert 'any(f["FunctionName"] == "'"${FUNCTION_NAME}"'" for f in data["Functions"])'
  lambda_call GET "/2015-03-31/functions/${FUNCTION_NAME}" |
    json_assert 'data["Configuration"]["Handler"] == "app.handler" and data["Tags"] == {"suite": "lambda-e2e"}'
}

exercise_invoke() {
  lambda_call POST "/2015-03-31/functions/${FUNCTION_NAME}/invocations" '{"hello":"world"}' |
    json_assert 'data == {"echo": {"hello": "world"}, "function": "'"${FUNCTION_NAME}"'", "stage": "e2e"}'
  [[ -z "$(header_value X-Amz-Function-Error)" ]]

  lambda_call POST "/2015-03-31/functions/${FUNCTION_NAME}/invocations" '{"fail":true}' |
    json_assert 'data["errorType"] == "RuntimeError" and data["errorMessage"] == "requested failure"'
  [[ "$(header_value X-Amz-Function-Error)" == "Unhandled" ]]

  curl -fsS -D "${HEADERS_OUT}" -o /dev/null -X POST -H 'X-Amz-Log-Type: Tail' --data '{}' \
    "${LAMBDA_ENDPOINT}/2015-03-31/functions/${FUNCTION_NAME}/invocations"
  header_value X-Amz-Log-Result | python3 -c 'import base64,sys; log=base64.b64decode(sys.stdin.read()).decode(); assert "e2e log line" in log and "REPORT RequestId:" in log, log'

  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'X-Amz-Invocation-Type: Event' --data '{}' \
    "${LAMBDA_ENDPOINT}/2015-03-31/functions/${FUNCTION_NAME}/invocations")"
  [[ "${code}" == "202" ]]
}

exercise_updates() {
  lambda_call PUT "/2015-03-31/functions/${FUNCTION_NAME}/configuration" '{"Timeout":20,"MemorySize":256}' |
    json_assert 'data["Timeout"] == 20 and data["MemorySize"] == 256'
  local zip
  zip="$(package_b64 "${HANDLER_V2}")"
  lambda_call PUT "/2015-03-31/functions/${FUNCTION_NAME}/code" "{\"ZipFile\":\"${zip}\"}" |
    json_assert 'data["CodeSize"] > 0'
  lambda_call POST "/2015-03-31/functions/${FUNCTION_NAME}/invocations" '{}' |
    json_assert 'data == {"version": 2}'
}

exercise_dashboard() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/lambda/status" |
    json_assert 'data["running"] == True and data["functionCount"] >= 1'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/lambda/functions" |
    json_assert 'any(f["FunctionName"] == "'"${FUNCTION_NAME}"'" for f in data["functions"])'
  curl -fsS -X POST --data '{}' "${DASHBOARD_ENDPOINT}/api/lambda/functions/${FUNCTION_NAME}/invoke" |
    json_assert 'data["functionError"] is None and data["payload"] == "{\"version\": 2}" and "START RequestId:" in data["log"]'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/lambda/invocations" |
    json_assert 'len(data["invocations"]) >= 4'
  curl -fsSL "${DASHBOARD_ENDPOINT}/dashboard/lambda" | grep -q '<div id="root"></div>'
}

delete_function_and_assert() {
  if [[ "${DELETE_DATA}" != "true" ]]; then
    log "E2E_DELETE_DATA=false: leaving ${FUNCTION_NAME} in place"
    return
  fi
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "${LAMBDA_ENDPOINT}/2015-03-31/functions/${FUNCTION_NAME}")"
  [[ "${code}" == "204" ]]
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${LAMBDA_ENDPOINT}/2015-03-31/functions/${FUNCTION_NAME}")"
  [[ "${code}" == "404" ]]
}

print_interactive_info() {
  cat <<EOF

[lambda-e2e] interactive mode
  Lambda endpoint: ${LAMBDA_ENDPOINT}
  Dashboard:       ${DASHBOARD_ENDPOINT}/dashboard/lambda
  Workspace:       ${WORKSPACE}
  Function:        ${FUNCTION_NAME}

Example:
  aws --endpoint-url ${LAMBDA_ENDPOINT} lambda invoke --function-name ${FUNCTION_NAME} --payload '{}' --cli-binary-format raw-in-base64-out out.json

Press Ctrl-C to stop devcloud.
EOF
  while kill -0 "${DEV_PID}" 2>/dev/null; do
    sleep 3600
  done
}

main() {
  require_command curl
  require_command python3
  assign_ports

  TMP_DIR="$(mktemp -d)"
  WORKSPACE="${TMP_DIR}/workspace"
  HEADERS_OUT="${TMP_DIR}/headers.txt"
  mkdir -p "${WORKSPACE}"
  write_config "${WORKSPACE}"

  log "building devcloud"
  devcloud_build "${TMP_DIR}/devcloud"

  log "starting devcloud"
  # `exec` keeps $! pointing at the devcloud process itself; without it,
  # bash 3.2 leaves the binary running as an orphan after cleanup kills the
  # subshell wrapper.
  (
    cd "${WORKSPACE}"
    exec "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"

  wait_for_http "${LAMBDA_ENDPOINT}/2015-03-31/functions/"
  wait_for_http "${DASHBOARD_ENDPOINT}/"

  log "creating function"
  create_function
  exercise_reads

  log "invoking function"
  exercise_invoke

  log "updating configuration and code"
  exercise_updates

  log "checking dashboard forwarding"
  exercise_dashboard

  log "deleting function"
  delete_function_and_assert

  if [[ "${INTERACTIVE}" == "true" ]]; then
    print_interactive_info
  fi

  log "Lambda E2E passed"
}

main "$@"
