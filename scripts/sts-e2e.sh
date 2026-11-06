#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

SVC_PORT="${E2E_STS_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
EVENT_RELAY_PORT="${E2E_EVENT_RELAY_PORT:-}"
SVC_ENDPOINT=""
DASHBOARD_ENDPOINT=""
REGION="${E2E_STS_REGION:-us-east-1}"
ACCOUNT_ID="${E2E_STS_ACCOUNT_ID:-000000000000}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

TMP_DIR=""
DEV_PID=""
WORKSPACE=""

usage() {
  cat <<'EOF'
Usage:
  scripts/sts-e2e.sh

Environment:
  E2E_STS_PORT=18032              Override the STS endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025          Override the dashboard port. Defaults to an available port.
  E2E_EVENT_RELAY_PORT=18027        Override the dashboard event relay port. Defaults to an available port.
  E2E_STS_REGION=us-east-1         Override the configured region.
  E2E_STS_ACCOUNT_ID=000000000000  Override the configured account id.
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
  printf '[sts-e2e] %s\n' "$1"
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
    echo "[sts-e2e] missing command: $1" >&2
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
      echo "[sts-e2e] devcloud exited while waiting for ${what}" >&2
      sed 's/^/[sts-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2 || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      echo "[sts-e2e] timed out waiting for ${what}" >&2
      return 1
    fi
    sleep 0.2
  done
}

write_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: sts-e2e

server:
  dashboardPort: ${DASHBOARD_PORT}
  eventRelayPort: ${EVENT_RELAY_PORT}
  stsPort: ${SVC_PORT}

auth:
  sts:
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
  ssm:
    enabled: false
  sts:
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
    printf '\n[sts-e2e] interactive mode\n  STS endpoint: %s\n  Dashboard: %s/dashboard/sts\n  Workspace: %s\n\nPress Ctrl-C to stop devcloud.\n' \
      "${SVC_ENDPOINT}" "${DASHBOARD_ENDPOINT}" "${WORKSPACE}"
    while kill -0 "${DEV_PID}" 2>/dev/null; do
      sleep 3600
    done
  fi
}

sts() {
  curl -sS \
    -X POST \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data "$1" \
    "${SVC_ENDPOINT}/"
}

sts_ok() {
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data "$1" \
    "${SVC_ENDPOINT}/"
}

xml_assert() {
  python3 -c '
import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read())
ns = {"s": "https://sts.amazonaws.com/doc/2011-06-15/"}
def text(path):
    node = root.find(path, ns)
    return None if node is None else node.text
assert eval(sys.argv[1], {}, {"root": root, "text": text}), ET.tostring(root)
' "$1"
}

main() {
  require_command curl
  require_command python3
  assign_ports
  start_devcloud

  wait_until "sts" sts_ok 'Action=GetCallerIdentity&Version=2011-06-15'
  wait_until "dashboard" curl -fsS "${DASHBOARD_ENDPOINT}/"

  log "GetCallerIdentity"
  sts_ok 'Action=GetCallerIdentity&Version=2011-06-15' |
    xml_assert 'text("s:GetCallerIdentityResult/s:Account") == "'"${ACCOUNT_ID}"'"'

  log "AssumeRole with an inline session policy"
  local policy
  policy="$(python3 -c 'import urllib.parse; print(urllib.parse.quote("{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:GetObject\",\"Resource\":\"*\"}]}", safe=""))')"
  sts_ok "Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A${ACCOUNT_ID}%3Arole%2Fe2e-runner&RoleSessionName=site-a&DurationSeconds=900&Policy=${policy}" |
    xml_assert 'text("s:AssumeRoleResult/s:Credentials/s:AccessKeyId").startswith("ASIA") and text("s:AssumeRoleResult/s:Credentials/s:SessionToken") and text("s:AssumeRoleResult/s:AssumedRoleUser/s:Arn") == "arn:aws:sts::'"${ACCOUNT_ID}"':assumed-role/e2e-runner/site-a"'

  log "AssumeRole validation"
  sts 'Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr&RoleSessionName=s1&Policy=not-json' |
    xml_assert 'text("s:Error/s:Code") == "MalformedPolicyDocument"'
  sts 'Action=AssumeRole&Version=2011-06-15&RoleSessionName=s1' |
    xml_assert 'text("s:Error/s:Code") == "ValidationError"'

  log "dashboard"
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sts/status" | json_assert 'data["status"] == "running"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/sts/caller-identity" |
    json_assert 'data["account"] == "'"${ACCOUNT_ID}"'"'

  hold_if_interactive
  log "STS E2E passed"
}

main "$@"
