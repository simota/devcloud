#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
LOOP_DIR="${ROOT_DIR}/scripts/dashboard-design-renewal-autoloop"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
VERIFY_HOST="127.0.0.1"


pick_free_port() {
  local fallback="$1"
  python3 - <<'PY' 2>/dev/null || printf '%s\n' "${fallback}"
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-$(pick_free_port 18125)}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-$(pick_free_port 18825)}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-$(pick_free_port 18466)}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-dashboard-renewal-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-dashboard-renewal-verify.err"

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
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -40
    FAIL=$((FAIL + 1))
  fi
}

finish() {
  if [[ "${FAIL}" -gt 0 ]]; then
    echo "[SUMMARY] ${PASS} passed, ${FAIL} failed"
    exit 1
  fi
  echo "[SUMMARY] ${PASS} passed, ${FAIL} failed"
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

assert_no_unsafe_react_rendering() {
  if env -u RIPGREP_CONFIG_PATH rg -n 'dangerouslySetInnerHTML|innerHTML' web/dashboard/src; then
    return 1
  fi
}

assert_loop_scripts_parse() {
  bash -n "${LOOP_DIR}/bootstrap.sh"
  bash -n "${LOOP_DIR}/run-loop.sh"
  bash -n "${LOOP_DIR}/recover.sh"
  bash -n "${LOOP_DIR}/verify.sh"
}

assert_footer_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS: READY' "${LOOP_DIR}/goal.md" &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_SUMMARY:' "${LOOP_DIR}/goal.md" &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS:' "${LOOP_DIR}/run-loop.sh"
}

rust_foundation() {
  cargo test --workspace
}

dashboard_registry_tests() {
  cargo test --workspace
}

react_build() {
  if [[ ! -d web/dashboard/node_modules ]]; then
    npm --prefix web/dashboard install
  fi
  npm --prefix web/dashboard run build
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: dashboard-renewal-verify

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyID: dev
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
    pathStyle: true
    virtualHostStyle: false
    maxObjectBytes: 5368709120
    multipart:
      minPartBytes: 5242880
  gcs:
    enabled: false
  dynamodb:
    enabled: false
  bigquery:
    enabled: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
  redshift:
    enabled: false
EOF

  devcloud_build "${TMP_DIR}/devcloud"
  (
    cd "${TMP_DIR}"
    "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

api_smoke() {
  start_devcloud
  wait_for_http "${DASHBOARD_ENDPOINT}/api/dashboard/services"
  curl -fsS "${DASHBOARD_ENDPOINT}/" | grep -q 'devcloud Services'
  # Compatibility compatibility paths must 301-redirect to /dashboard/<svc>.
  curl -fsS -o /dev/null -w '%{http_code} %{redirect_url}\n' "${DASHBOARD_ENDPOINT}/mail" | grep -q '^301 .*/dashboard/mail$'
  curl -fsS -o /dev/null -w '%{http_code} %{redirect_url}\n' "${DASHBOARD_ENDPOINT}/s3" | grep -q '^301 .*/dashboard/s3$'
  # React shell serves all service pages under /dashboard/<svc>.
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/mail" | grep -q 'devcloud Dashboard'
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/s3" | grep -q 'devcloud Dashboard'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id":"mail"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id":"s3"'
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"status":"running"'
}

case "${VERIFY_STAGE}" in
  foundation)
    run_check "Loop scripts parse" assert_loop_scripts_parse
    run_check "Footer contract is present" assert_footer_contract
    run_check "compatibility foundation passes" rust_foundation
    run_check "Dashboard registry tests pass" dashboard_registry_tests
    ;;
  react-build)
    run_check "React dashboard builds" react_build
    run_check "No unsafe React rendering helpers" assert_no_unsafe_react_rendering
    ;;
  api-smoke)
    run_check "Dashboard API smoke passes" api_smoke
    ;;
  full)
    run_check "Loop scripts parse" assert_loop_scripts_parse
    run_check "Footer contract is present" assert_footer_contract
    run_check "compatibility foundation passes" rust_foundation
    run_check "Dashboard registry tests pass" dashboard_registry_tests
    run_check "React dashboard builds" react_build
    run_check "No unsafe React rendering helpers" assert_no_unsafe_react_rendering
    run_check "Dashboard API smoke passes" api_smoke
    ;;
  *)
    echo "Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 2
    ;;
esac

finish
