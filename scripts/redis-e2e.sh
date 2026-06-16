#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

REDIS_PORT="${E2E_REDIS_PORT:-$(free_port)}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-$(free_port)}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"

TMP_DIR=""
DEV_PID=""

log() {
  printf '[redis-e2e] %s\n' "$1"
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

usage() {
  cat <<'EOF'
Usage:
  scripts/redis-e2e.sh

Environment:
  E2E_INTERACTIVE=true       Keep devcloud running after assertions.
  E2E_KEEP_WORKDIR=true      Keep the temporary workspace for debugging.
  E2E_REDIS_PORT=16379       Override the Redis port.
  E2E_DASHBOARD_PORT=18026   Override the dashboard port.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v redis-server >/dev/null 2>&1; then
  log "[SKIP] redis-server binary not in PATH"
  exit 0
fi

wait_for_tcp() {
  local port="$1"
  local deadline=$((SECONDS + 20))
  until python3 - "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

port = int(sys.argv[1])
with socket.create_connection(("127.0.0.1", port), timeout=0.5):
    pass
PY
  do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 20))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
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
project: redis-e2e

server:
  dashboardPort: ${DASHBOARD_PORT}
  redisPort: ${REDIS_PORT}

auth:
  redis:
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
    enabled: true
    mode: managed
    dataDir: redis
    appendOnly: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
EOF
}

run_redis_cli_probe() {
  if ! command -v redis-cli >/dev/null 2>&1; then
    log "[SKIP] redis-cli data-plane probe (redis-cli binary not in PATH)"
    return
  fi
  local pong
  pong="$(redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" ping)"
  if [[ "${pong}" != "PONG" ]]; then
    log "redis-cli ping returned ${pong}"
    return 1
  fi
  redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" FLUSHDB >/dev/null
  redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" SET devcloud:e2e:string value >/dev/null
  [[ "$(redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" GET devcloud:e2e:string)" == "value" ]]
  [[ "$(redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" EXPIRE devcloud:e2e:string 60)" == "1" ]]
  [[ "$(redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" HSET devcloud:e2e:hash field hash-value)" == "1" ]]
  [[ "$(redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" HGET devcloud:e2e:hash field)" == "hash-value" ]]
}

assert_dashboard_api() {
  DASHBOARD_PORT="${DASHBOARD_PORT}" python3 - <<'PY'
import json
import os
import time
import urllib.parse
import urllib.request

base_url = f"http://127.0.0.1:{int(os.environ['DASHBOARD_PORT'])}"

def request(path, method="GET", body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(f"{base_url}{path}", data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=5) as response:
        payload = response.read().decode("utf-8")
        return json.loads(payload) if payload else {}

deadline = time.time() + 10
status = {}
while time.time() < deadline:
    status = request("/api/redis/status")
    if status.get("running") is True and status.get("serverVersion"):
        break
    time.sleep(0.2)
else:
    raise SystemExit(f"redis dashboard status was not ready: {status!r}")

if status.get("service") != "redis":
    raise SystemExit(f"unexpected redis service status: {status!r}")
if "password" in json.dumps(status).lower():
    raise SystemExit("status response leaked password fields")

keys = request("/api/redis/keys?cursor=0&count=100&match=devcloud:e2e:*")
if keys.get("keys") is None:
    raise SystemExit("keys field is null")
key_names = {item.get("key") for item in keys["keys"]}
if "devcloud:e2e:string" not in key_names or "devcloud:e2e:hash" not in key_names:
    raise SystemExit(f"expected keys were not listed: {keys!r}")

encoded_key = urllib.parse.quote("devcloud:e2e:string", safe="")
detail = request(f"/api/redis/keys/{encoded_key}")
if detail.get("preview") is None:
    raise SystemExit("preview field is null")
if detail.get("type") != "string" or detail.get("ttlSeconds", 0) <= 0:
    raise SystemExit("unexpected string detail metadata")

command = request("/api/redis/command", method="POST", body={"command": "GET", "args": ["devcloud:e2e:string"]})
if command.get("rows") is None:
    raise SystemExit("command rows field is null")
if command.get("command") != "GET" or command.get("class") != "read":
    raise SystemExit("unexpected command response metadata")

flush = request("/api/redis/keys?confirm=FLUSHDB", method="DELETE")
if flush.get("result") != "OK":
    raise SystemExit(f"unexpected flush response: {flush!r}")

empty = request("/api/redis/keys?cursor=0&count=100&match=devcloud:e2e:*")
if empty.get("keys") is None or len(empty["keys"]) != 0:
    raise SystemExit(f"keyspace was not empty after FLUSHDB: {empty!r}")
PY
}

show_interactive_hint() {
  cat <<EOF
[redis-e2e] browser check:
[redis-e2e]   Dashboard: http://127.0.0.1:${DASHBOARD_PORT}/dashboard/redis
[redis-e2e]   Redis: redis://127.0.0.1:${REDIS_PORT}
EOF
}

TMP_DIR="$(mktemp -d)"
BIN="${TMP_DIR}/devcloud"
WORKSPACE="${TMP_DIR}/workspace"
mkdir -p "${WORKSPACE}"

log "building devcloud"
devcloud_build "${BIN}"

write_config "${WORKSPACE}"

log "starting devcloud up on redis=${REDIS_PORT}, dashboard=${DASHBOARD_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for Redis endpoint"
wait_for_tcp "${REDIS_PORT}"
log "waiting for dashboard"
wait_for_http "http://127.0.0.1:${DASHBOARD_PORT}/"

log "checking redis-cli data-plane journey"
run_redis_cli_probe

log "checking Redis dashboard API"
assert_dashboard_api

log "passed"

if [[ "${INTERACTIVE}" == "true" ]]; then
  show_interactive_hint
  log "press Ctrl-C to stop devcloud"
  wait "${DEV_PID}"
fi
