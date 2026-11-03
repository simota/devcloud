#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

SVC_PORT="${E2E_SSM_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
EVENT_RELAY_PORT="${E2E_EVENT_RELAY_PORT:-}"
SVC_ENDPOINT=""
DASHBOARD_ENDPOINT=""
REGION="${E2E_SSM_REGION:-us-east-1}"
ACCOUNT_ID="${E2E_SSM_ACCOUNT_ID:-000000000000}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

TMP_DIR=""
DEV_PID=""
WORKSPACE=""

usage() {
  cat <<'EOF'
Usage:
  scripts/ssm-e2e.sh

Environment:
  E2E_SSM_PORT=18031              Override the SSM endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025          Override the dashboard port. Defaults to an available port.
  E2E_EVENT_RELAY_PORT=18027        Override the dashboard event relay port. Defaults to an available port.
  E2E_SSM_REGION=us-east-1         Override the configured region.
  E2E_SSM_ACCOUNT_ID=000000000000  Override the configured account id.
  E2E_DELETE_DATA=false             Skip cleanup steps so created data stays behind for inspection.
  E2E_KEEP_WORKDIR=true             Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true              Keep devcloud running after assertions.
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
  printf '[ssm-e2e] %s\n' "$1"
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
    echo "[ssm-e2e] missing command: $1" >&2
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
  [[ -n "${SVC_PORT}" ]] || SVC_PORT="$(find_free_port)"
  [[ -n "${DASHBOARD_PORT}" ]] || DASHBOARD_PORT="$(find_free_port)"
  [[ -n "${EVENT_RELAY_PORT}" ]] || EVENT_RELAY_PORT="$(find_free_port)"
  SVC_ENDPOINT="http://127.0.0.1:${SVC_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

json_assert() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {"json": json}, {"data": data}), data' "$1"
}

wait_until() {
  local what="$1"
  shift
  local deadline=$((SECONDS + 20))
  until "$@" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[ssm-e2e] devcloud exited while waiting for ${what}" >&2
      sed 's/^/[ssm-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2 || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      echo "[ssm-e2e] timed out waiting for ${what}" >&2
      return 1
    fi
    sleep 0.2
  done
}

write_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: ssm-e2e

server:
  dashboardPort: ${DASHBOARD_PORT}
  eventRelayPort: ${EVENT_RELAY_PORT}
  ssmPort: ${SVC_PORT}

auth:
  ssm:
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
  lambda:
    enabled: false
  cloudRun:
    enabled: false
  sts:
    enabled: false
  ssm:
    enabled: true
    region: ${REGION}
EOF
}

launch_devcloud() {
  # `exec` keeps $! pointing at the devcloud process itself; without it,
  # bash 3.2 leaves the binary running as an orphan after cleanup kills the
  # subshell wrapper.
  (
    cd "${WORKSPACE}"
    exec "${TMP_DIR}/devcloud" up
  ) >>"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  WORKSPACE="${TMP_DIR}/workspace"
  mkdir -p "${WORKSPACE}"
  write_config "${WORKSPACE}"

  log "building devcloud"
  devcloud_build "${TMP_DIR}/devcloud"

  log "starting devcloud"
  launch_devcloud
}

hold_if_interactive() {
  if [[ "${INTERACTIVE}" == "true" ]]; then
    printf '\n[ssm-e2e] interactive mode\n  SSM endpoint: %s\n  Dashboard: %s/dashboard/ssm\n  Workspace: %s\n\nPress Ctrl-C to stop devcloud.\n' \
      "${SVC_ENDPOINT}" "${DASHBOARD_ENDPOINT}" "${WORKSPACE}"
    while kill -0 "${DEV_PID}" 2>/dev/null; do
      sleep 3600
    done
  fi
}

ssm() {
  curl -sS \
    -X POST \
    -H 'Content-Type: application/x-amz-json-1.1' \
    -H "X-Amz-Target: AmazonSSM.$1" \
    --data "$2" \
    "${SVC_ENDPOINT}/"
}

ssm_ok() {
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/x-amz-json-1.1' \
    -H "X-Amz-Target: AmazonSSM.$1" \
    --data "$2" \
    "${SVC_ENDPOINT}/"
}

main() {
  require_command curl
  require_command python3
  assign_ports
  start_devcloud

  wait_until "ssm" ssm_ok DescribeParameters '{}'
  wait_until "dashboard" curl -fsS "${DASHBOARD_ENDPOINT}/"

  log "put / get String and SecureString"
  ssm_ok PutParameter '{"Name":"/e2e/dwh/user","Value":"loader","Type":"String"}' |
    json_assert 'data == {"Version": 1, "Tier": "Standard"}'
  ssm_ok PutParameter '{"Name":"/e2e/dwh/password","Value":"e2e-s3cret","Type":"SecureString"}' |
    json_assert 'data["Version"] == 1'
  ssm_ok GetParameter '{"Name":"/e2e/dwh/user"}' |
    json_assert 'data["Parameter"]["Value"] == "loader" and data["Parameter"]["ARN"] == "arn:aws:ssm:'"${REGION}"':'"${ACCOUNT_ID}"':parameter/e2e/dwh/user"'
  ssm_ok GetParameter '{"Name":"/e2e/dwh/password","WithDecryption":true}' |
    json_assert 'data["Parameter"]["Value"] == "e2e-s3cret" and data["Parameter"]["Type"] == "SecureString"'
  ssm_ok GetParameter '{"Name":"/e2e/dwh/password"}' |
    json_assert '"e2e-s3cret" not in data["Parameter"]["Value"]'

  log "overwrite rules"
  ssm PutParameter '{"Name":"/e2e/dwh/user","Value":"other","Type":"String"}' |
    json_assert 'data["__type"] == "ParameterAlreadyExists"'
  ssm_ok PutParameter '{"Name":"/e2e/dwh/user","Value":"other","Overwrite":true}' |
    json_assert 'data["Version"] == 2'
  ssm GetParameter '{"Name":"/e2e/missing"}' |
    json_assert 'data["__type"] == "ParameterNotFound"'

  log "dashboard lists metadata without values"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/ssm/parameters" |
    json_assert 'sorted(p["Name"] for p in data["parameters"]) == ["/e2e/dwh/password", "/e2e/dwh/user"] and "e2e-s3cret" not in json.dumps(data) and "Value" not in json.dumps(data)'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/ssm/status" |
    json_assert 'data["status"] == "running" and data["parameterCount"] == 2'

  log "restart keeps parameters"
  kill "${DEV_PID}"
  wait "${DEV_PID}" 2>/dev/null || true
  launch_devcloud
  wait_until "ssm after restart" ssm_ok DescribeParameters '{}'
  ssm_ok GetParameter '{"Name":"/e2e/dwh/password","WithDecryption":true}' |
    json_assert 'data["Parameter"]["Value"] == "e2e-s3cret"'

  if [[ "${DELETE_DATA}" == "true" ]]; then
    log "delete"
    ssm_ok DeleteParameter '{"Name":"/e2e/dwh/user"}' | json_assert 'data == {}'
    ssm_ok DeleteParameter '{"Name":"/e2e/dwh/password"}' | json_assert 'data == {}'
    ssm_ok DescribeParameters '{}' | json_assert 'data["Parameters"] == []'
  fi

  hold_if_interactive
  log "SSM E2E passed"
}

main "$@"
