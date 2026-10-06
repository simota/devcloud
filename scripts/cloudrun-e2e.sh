#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

CLOUDRUN_PORT="${E2E_CLOUDRUN_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
EVENT_RELAY_PORT="${E2E_EVENT_RELAY_PORT:-}"
CLOUDRUN_ENDPOINT=""
DASHBOARD_ENDPOINT=""
PROJECT="${E2E_CLOUDRUN_PROJECT:-devcloud}"
REGION="${E2E_CLOUDRUN_REGION:-us-central1}"
SERVICE_ID="${E2E_CLOUDRUN_SERVICE:-hello-e2e}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

TMP_DIR=""
DEV_PID=""
WORKSPACE=""
PARENT=""
SERVICE_NAME=""

usage() {
  cat <<'EOF'
Usage:
  scripts/cloudrun-e2e.sh

Environment:
  E2E_CLOUDRUN_PORT=18095        Override the Cloud Run endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025       Override the dashboard port. Defaults to an available port.
  E2E_EVENT_RELAY_PORT=18027     Override the dashboard event relay port. Defaults to an available port.
  E2E_CLOUDRUN_PROJECT=devcloud  Override the project id.
  E2E_CLOUDRUN_REGION=us-central1 Override the location.
  E2E_CLOUDRUN_SERVICE=hello-e2e Override the service id.
  E2E_DELETE_DATA=false          Skip DeleteService so the service stays behind for inspection.
  E2E_KEEP_WORKDIR=true          Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true           Keep devcloud running after assertions (and keep the service).

Requires python3 (the sample service + JSON assertions) and curl.

Examples:
  scripts/cloudrun-e2e.sh
  E2E_INTERACTIVE=true scripts/cloudrun-e2e.sh
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
  printf '[cloudrun-e2e] %s\n' "$1"
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
    echo "[cloudrun-e2e] missing command: $1" >&2
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
  [[ -n "${CLOUDRUN_PORT}" ]] || CLOUDRUN_PORT="$(find_free_port)"
  [[ -n "${DASHBOARD_PORT}" ]] || DASHBOARD_PORT="$(find_free_port)"
  [[ -n "${EVENT_RELAY_PORT}" ]] || EVENT_RELAY_PORT="$(find_free_port)"
  CLOUDRUN_ENDPOINT="http://127.0.0.1:${CLOUDRUN_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
  PARENT="projects/${PROJECT}/locations/${REGION}"
  SERVICE_NAME="${PARENT}/services/${SERVICE_ID}"
}

json_value() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "$1"
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

# Requests to the deployed service via Host routing (curl sends the Host as-is).
service_get() {
  curl -fsS -H "Host: ${SERVICE_ID}.${REGION}.${PROJECT}.run.localhost:${CLOUDRUN_PORT}" "${CLOUDRUN_ENDPOINT}$1"
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 30))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[cloudrun-e2e] devcloud exited while waiting for ${url}" >&2
      sed 's/^/[cloudrun-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2 || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

write_app() {
  cat > "${WORKSPACE}/app.py" <<'PY'
import http.server, json, os

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            "path": self.path,
            "greeting": os.environ.get("GREETING"),
            "service": os.environ.get("K_SERVICE"),
            "revision": os.environ.get("K_REVISION"),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print("handled", self.path, flush=True)

http.server.HTTPServer(("127.0.0.1", int(os.environ["PORT"])), Handler).serve_forever()
PY
}

write_config() {
  mkdir -p "${WORKSPACE}/.devcloud"
  cat > "${WORKSPACE}/.devcloud/config.yaml" <<EOF
project: cloudrun-e2e

server:
  dashboardPort: ${DASHBOARD_PORT}
  eventRelayPort: ${EVENT_RELAY_PORT}
  cloudRunPort: ${CLOUDRUN_PORT}

auth:
  cloudRun:
    mode: relaxed

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
}

service_body() {
  cat <<EOF
{
  "labels": {"suite": "cloudrun-e2e"},
  "template": {
    "containers": [{
      "image": "us-docker.pkg.dev/cloudrun/container/hello",
      "command": ["python3", "${WORKSPACE}/app.py"],
      "env": [{"name": "GREETING", "value": "$1"}]
    }]
  }
}
EOF
}

create_service() {
  local op
  op="$(run_api POST "/v2/${PARENT}/services?serviceId=${SERVICE_ID}" "$(service_body v1)")"
  echo "${op}" | json_assert 'data["done"] == True and data["response"]["name"] == "'"${SERVICE_NAME}"'" and data["response"]["terminalCondition"]["state"] == "CONDITION_SUCCEEDED"'
  local op_name
  op_name="$(echo "${op}" | json_value 'data["name"]')"
  run_api GET "/v2/${op_name}" | json_assert 'data["done"] == True'
}

exercise_reads() {
  run_api GET "/v2/${SERVICE_NAME}" |
    json_assert 'data["uri"] == "http://'"${SERVICE_ID}.${REGION}.${PROJECT}"'.run.localhost:'"${CLOUDRUN_PORT}"'" and data["labels"] == {"suite": "cloudrun-e2e"}'
  run_api GET "/v2/${PARENT}/services" |
    json_assert 'any(s["name"] == "'"${SERVICE_NAME}"'" for s in data["services"])'
}

exercise_data_plane() {
  service_get "/hello?x=1" |
    json_assert 'data["path"] == "/hello?x=1" and data["greeting"] == "v1" and data["service"] == "'"${SERVICE_ID}"'"'
  curl -fsS "${CLOUDRUN_ENDPOINT}/_run/${PROJECT}/${REGION}/${SERVICE_ID}/via-path" |
    json_assert 'data["path"] == "/via-path"'
}

exercise_new_revision() {
  run_api PATCH "/v2/${SERVICE_NAME}" "$(service_body v2)" |
    json_assert 'data["response"]["generation"] == "2"'
  service_get "/" | json_assert 'data["greeting"] == "v2"'
  run_api GET "/v2/${SERVICE_NAME}/revisions" | json_assert 'len(data["revisions"]) == 2'
}

exercise_iam() {
  run_api POST "/v2/${SERVICE_NAME}:setIamPolicy" '{"policy":{"bindings":[{"role":"roles/run.invoker","members":["allUsers"]}]}}' |
    json_assert 'data["bindings"][0]["members"] == ["allUsers"]'
  run_api GET "/v2/${SERVICE_NAME}:getIamPolicy" | json_assert 'data["bindings"][0]["role"] == "roles/run.invoker"'
}

exercise_dashboard() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/status" |
    json_assert 'data["running"] == True and data["serviceCount"] == 1 and data["instanceCount"] == 1'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/services" |
    json_assert 'data["services"][0]["name"] == "'"${SERVICE_NAME}"'"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/instances" |
    json_assert 'data["instances"][0]["service"] == "'"${SERVICE_NAME}"'" and data["instances"][0]["mode"] == "process"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/services/${PROJECT}/${REGION}/${SERVICE_ID}/revisions" |
    json_assert 'len(data["revisions"]) == 2'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/cloudrun/services/${PROJECT}/${REGION}/${SERVICE_ID}/logs" |
    json_assert 'any("handled" in line for line in data["lines"])'
  curl -fsSL "${DASHBOARD_ENDPOINT}/dashboard/cloudrun" | grep -q '<div id="root"></div>'
}

delete_service_and_assert() {
  if [[ "${DELETE_DATA}" != "true" ]]; then
    log "E2E_DELETE_DATA=false: leaving ${SERVICE_NAME} in place"
    return
  fi
  run_api DELETE "/v2/${SERVICE_NAME}" | json_assert 'data["done"] == True'
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${CLOUDRUN_ENDPOINT}/v2/${SERVICE_NAME}")"
  [[ "${code}" == "404" ]]
  code="$(curl -sS -o /dev/null -w '%{http_code}' -H "Host: ${SERVICE_ID}.${REGION}.${PROJECT}.run.localhost" "${CLOUDRUN_ENDPOINT}/")"
  [[ "${code}" == "404" ]]
}

print_interactive_info() {
  cat <<EOF

[cloudrun-e2e] interactive mode
  Cloud Run API: ${CLOUDRUN_ENDPOINT}/v2/${PARENT}/services
  Service URL:   http://${SERVICE_ID}.${REGION}.${PROJECT}.run.localhost:${CLOUDRUN_PORT}/
  Path URL:      ${CLOUDRUN_ENDPOINT}/_run/${PROJECT}/${REGION}/${SERVICE_ID}/
  Dashboard:     ${DASHBOARD_ENDPOINT}/dashboard/cloudrun
  Workspace:     ${WORKSPACE}

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
  mkdir -p "${WORKSPACE}"
  write_app
  write_config

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

  wait_for_http "${CLOUDRUN_ENDPOINT}/v2/${PARENT}/services"
  wait_for_http "${DASHBOARD_ENDPOINT}/"

  log "creating service"
  create_service
  exercise_reads

  log "routing requests to the local instance"
  exercise_data_plane

  log "rolling a new revision"
  exercise_new_revision
  exercise_iam

  log "checking dashboard forwarding"
  exercise_dashboard

  log "deleting service"
  delete_service_and_assert

  if [[ "${INTERACTIVE}" == "true" ]]; then
    print_interactive_info
  fi

  log "Cloud Run E2E passed"
}

main "$@"
