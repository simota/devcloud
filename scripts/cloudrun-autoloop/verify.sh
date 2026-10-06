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

CLOUDRUN_VERIFY_PORT="${CLOUDRUN_VERIFY_PORT:-$(free_port)}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-$(free_port)}"
EVENT_RELAY_VERIFY_PORT="${EVENT_RELAY_VERIFY_PORT:-$(free_port)}"
CLOUDRUN_ENDPOINT="http://127.0.0.1:${CLOUDRUN_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_VERIFY_PORT}"

PROJECT="devcloud"
REGION="us-central1"
SERVICE_ID="loop-web"
PARENT="projects/${PROJECT}/locations/${REGION}"
SERVICE_NAME="${PARENT}/services/${SERVICE_ID}"
SERVICE_HOST="${SERVICE_ID}.${REGION}.${PROJECT}.run.localhost:${CLOUDRUN_VERIFY_PORT}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-cloudrun-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-cloudrun-verify.err"
STATUS_OUT="${TMPDIR:-/tmp}/devcloud-cloudrun-verify.status"

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

run_api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "${body}" ]]; then
    curl -fsS -X "${method}" -H 'Content-Type: application/json' --data-binary "${body}" "${CLOUDRUN_ENDPOINT}${path}"
  else
    curl -fsS -X "${method}" "${CLOUDRUN_ENDPOINT}${path}"
  fi
}

api_status() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  curl -sS -o "${STATUS_OUT}" -w '%{http_code}' -X "${method}" -H 'Content-Type: application/json' \
    --data-binary "${body}" "${CLOUDRUN_ENDPOINT}${path}"
}

service_get() {
  curl -fsS -H "Host: ${SERVICE_HOST}" "${CLOUDRUN_ENDPOINT}$1"
}

service_body() {
  cat <<EOF
{
  "labels": {"suite": "cloudrun-autoloop"},
  "template": {
    "containers": [{
      "image": "us-docker.pkg.dev/cloudrun/container/hello",
      "command": ["python3", "${TMP_DIR}/app.py"],
      "env": [{"name": "GREETING", "value": "$1"}]
    }]
  }
}
EOF
}

wait_for_http() {
  local deadline=$((SECONDS + 30))
  until curl -fsS "$1" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      sed 's/^/[cloudrun-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2 || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/app.py" <<'PY'
import http.server, json, os

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"path": self.path, "greeting": os.environ.get("GREETING"), "revision": os.environ.get("K_REVISION")}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print("handled", self.path, flush=True)

http.server.HTTPServer(("127.0.0.1", int(os.environ["PORT"])), Handler).serve_forever()
PY
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: cloudrun-autoloop

server:
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  eventRelayPort: ${EVENT_RELAY_VERIFY_PORT}
  cloudRunPort: ${CLOUDRUN_VERIFY_PORT}

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
  lambda:
    enabled: false
  cloudRun:
    enabled: true
    project: ${PROJECT}
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
    wait_for_http "${CLOUDRUN_ENDPOINT}/v2/${PARENT}/services"
  fi
}

# --- foundation / config ------------------------------------------------------

assert_script_contract() {
  bash -n scripts/cloudrun-autoloop/verify.sh &&
    bash -n scripts/cloudrun-e2e.sh
}

assert_config_shape() {
  grep -q 'cloudRunPort' orchestrator/src/config.rs &&
    grep -q 'services::cloudrun::run' orchestrator/src/supervisor.rs &&
    grep -q '"cloudrun"' services/dashboard/src/services.rs
}

# --- Admin API v2 + data plane (cloudrun-core) ------------------------------

create_service() {
  run_api POST "/v2/${PARENT}/services?serviceId=${SERVICE_ID}" "$(service_body v1)" |
    json_assert 'data["done"] == True and data["response"]["generation"] == "1" and data["response"]["latestReadyRevision"].startswith("'"${SERVICE_NAME}"'/revisions/'"${SERVICE_ID}"'-00001-")'
}

get_service() {
  run_api GET "/v2/${SERVICE_NAME}" |
    json_assert 'data["uri"] == "http://'"${SERVICE_HOST}"'" and data["terminalCondition"]["state"] == "CONDITION_SUCCEEDED"'
}

list_services() {
  run_api GET "/v2/${PARENT}/services" | json_assert 'len(data["services"]) == 1'
}

host_routed_request() {
  service_get "/hello?x=1" | json_assert 'data["path"] == "/hello?x=1" and data["greeting"] == "v1"'
}

path_routed_request() {
  curl -fsS "${CLOUDRUN_ENDPOINT}/_run/${PROJECT}/${REGION}/${SERVICE_ID}/p" | json_assert 'data["path"] == "/p"'
}

update_creates_revision() {
  run_api PATCH "/v2/${SERVICE_NAME}" "$(service_body v2)" | json_assert 'data["response"]["generation"] == "2"' &&
    service_get "/" | json_assert 'data["greeting"] == "v2" and data["revision"].startswith("'"${SERVICE_ID}"'-00002-")'
}

label_update_keeps_revision() {
  local before
  before="$(run_api GET "/v2/${SERVICE_NAME}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["latestReadyRevision"])')"
  run_api PATCH "/v2/${SERVICE_NAME}?updateMask=labels" '{"labels":{"suite":"relabelled"}}' |
    json_assert 'data["response"]["latestReadyRevision"] == "'"${before}"'" and data["response"]["labels"] == {"suite": "relabelled"}'
}

list_revisions() {
  run_api GET "/v2/${SERVICE_NAME}/revisions" | json_assert 'len(data["revisions"]) == 2'
}

iam_policy_round_trip() {
  run_api POST "/v2/${SERVICE_NAME}:setIamPolicy" '{"policy":{"bindings":[{"role":"roles/run.invoker","members":["allUsers"]}]}}' >/dev/null &&
    run_api GET "/v2/${SERVICE_NAME}:getIamPolicy" | json_assert 'data["bindings"][0]["members"] == ["allUsers"]'
}

# --- dashboard ----------------------------------------------------------------

dashboard_starts() {
  ensure_started && wait_for_http "${DASHBOARD_ENDPOINT}/"
}

dashboard_registry_has_cloudrun() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" |
    json_assert 'any(s["id"] == "cloudrun" and s["status"] == "running" for s in data["services"])'
}

dashboard_status() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/status" |
    json_assert 'data["service"] == "cloudrun" and data["serviceCount"] == 1 and data["instanceCount"] == 1'
}

dashboard_services() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/services" | json_assert 'data["services"][0]["name"] == "'"${SERVICE_NAME}"'"'
}

dashboard_instances() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/instances" |
    json_assert 'data["instances"][0]["service"] == "'"${SERVICE_NAME}"'" and data["instances"][0]["requestCount"] >= 1'
}

dashboard_revisions_and_logs() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/services/${PROJECT}/${REGION}/${SERVICE_ID}/revisions" |
    json_assert 'len(data["revisions"]) == 2' &&
    curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/services/${PROJECT}/${REGION}/${SERVICE_ID}/logs" |
    json_assert 'any("handled" in l for l in data["lines"])'
}

dashboard_page_loads() {
  curl -fsSL "${DASHBOARD_ENDPOINT}/dashboard/cloudrun" | grep -q '<div id="root"></div>'
}

# --- hardening ----------------------------------------------------------------

rejects_duplicate_service() {
  local code
  code="$(api_status POST "/v2/${PARENT}/services?serviceId=${SERVICE_ID}" "$(service_body dup)")"
  [[ "${code}" == "409" ]] && grep -q 'ALREADY_EXISTS' "${STATUS_OUT}"
}

rejects_invalid_service_id() {
  local code
  code="$(api_status POST "/v2/${PARENT}/services?serviceId=Bad_Id" "$(service_body x)")"
  [[ "${code}" == "400" ]] && grep -q 'INVALID_ARGUMENT' "${STATUS_OUT}"
}

rejects_missing_image() {
  local code
  code="$(api_status POST "/v2/${PARENT}/services?serviceId=no-image" '{"template":{"containers":[{}]}}')"
  [[ "${code}" == "400" ]]
}

image_only_service_is_unavailable() {
  run_api POST "/v2/${PARENT}/services?serviceId=image-only" '{"template":{"containers":[{"image":"gcr.io/x/y"}]}}' >/dev/null &&
    [[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "Host: image-only.${REGION}.${PROJECT}.run.localhost" "${CLOUDRUN_ENDPOINT}/")" == "503" ]]
}

unknown_service_is_404() {
  [[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "Host: nope.${REGION}.${PROJECT}.run.localhost" "${CLOUDRUN_ENDPOINT}/")" == "404" ]]
}

dashboard_rejects_wrong_method() {
  [[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${DASHBOARD_ENDPOINT}/api/cloudrun/services")" == "405" ]]
}

delete_services() {
  run_api DELETE "/v2/${SERVICE_NAME}" | json_assert 'data["done"] == True' &&
    run_api DELETE "/v2/${PARENT}/services/image-only" >/dev/null &&
    run_api GET "/v2/${PARENT}/services" | json_assert 'data["services"] == []' &&
    curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/instances" | json_assert 'data["instances"] == []'
}

# --- stage runners ------------------------------------------------------------

run_foundation_checks() {
  run_check "autoloop script contract" assert_script_contract
  run_check "cloudrun crate tests pass" cargo test -p devcloud-cloudrun
}

run_config_checks() {
  run_check "orchestrator/dashboard wire cloudrun" assert_config_shape
}

run_core_checks() {
  run_check "Cloud Run endpoint starts" ensure_started
  run_check "CreateService works" create_service
  run_check "GetService works" get_service
  run_check "ListServices works" list_services
  run_check "Host-routed request reaches the instance" host_routed_request
  run_check "Path-routed request reaches the instance" path_routed_request
  run_check "Template update rolls a new revision" update_creates_revision
  run_check "Label-only update keeps the revision" label_update_keeps_revision
  run_check "ListRevisions works" list_revisions
  run_check "IAM policy round trip works" iam_policy_round_trip
}

run_dashboard_checks() {
  run_check "dashboard starts" dashboard_starts
  run_check "dashboard registry has Cloud Run" dashboard_registry_has_cloudrun
  run_check "dashboard status reports running" dashboard_status
  run_check "dashboard lists services" dashboard_services
  run_check "dashboard lists instances" dashboard_instances
  run_check "dashboard shows revisions and logs" dashboard_revisions_and_logs
  run_check "dashboard SPA page loads" dashboard_page_loads
}

run_hardening_checks() {
  run_check "duplicate service rejected" rejects_duplicate_service
  run_check "invalid service id rejected" rejects_invalid_service_id
  run_check "missing image rejected" rejects_missing_image
  run_check "image-only service without docker is 503" image_only_service_is_unavailable
  run_check "unknown service host is 404" unknown_service_is_404
  run_check "dashboard API rejects wrong method" dashboard_rejects_wrong_method
  run_check "DeleteService removes services and instances" delete_services
}

run_e2e_checks() {
  run_check "Cloud Run standalone E2E script passes" bash scripts/cloudrun-e2e.sh
}

echo "=== Cloud Run autoloop verification: ${VERIFY_STAGE} ==="

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  config)
    run_foundation_checks
    run_config_checks
    ;;
  cloudrun|cloudrun-core)
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

echo "=== Cloud Run autoloop verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
