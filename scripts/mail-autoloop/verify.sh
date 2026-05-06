#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-12025}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-18025}"
PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-verify.err"

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
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -20
    FAIL=$((FAIL + 1))
  fi
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 10))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

write_mail_only_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: mail-verify

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}

auth:
  smtp:
    mode: off

storage:
  path: .devcloud/data

services:
  mail:
    enabled: true
    maxMessageBytes: 10485760
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
  sqs:
    enabled: false
  pubsub:
    enabled: false
EOF
}

send_smtp_smoke() {
  SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT}" python3 - <<'PY'
import smtplib
import os
from email.message import EmailMessage

msg = EmailMessage()
msg["From"] = "sender@example.com"
msg["To"] = "user@example.com"
msg["Subject"] = "Autoloop smoke"
msg.set_content("hello from autoloop")

with smtplib.SMTP("127.0.0.1", int(os.environ["SMTP_VERIFY_PORT"]), timeout=5) as smtp:
    smtp.send_message(msg)
PY
}

api_contains_smoke_message() {
  DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT}" python3 - <<'PY'
import json
import os
import urllib.request

with urllib.request.urlopen(f"http://127.0.0.1:{os.environ['DASHBOARD_VERIFY_PORT']}/api/messages", timeout=5) as response:
    data = json.load(response)

messages = data.get("messages") or []
if not any(m.get("subject") == "Autoloop smoke" for m in messages):
    raise SystemExit("smoke message not found")
PY
}

raw_source_available() {
  DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT}" python3 - <<'PY'
import json
import os
import urllib.request

base_url = f"http://127.0.0.1:{os.environ['DASHBOARD_VERIFY_PORT']}"
with urllib.request.urlopen(f"{base_url}/api/messages", timeout=5) as response:
    data = json.load(response)

messages = data.get("messages") or []
message_id = next((m["id"] for m in messages if m.get("subject") == "Autoloop smoke"), "")
if not message_id:
    raise SystemExit("smoke message not found")
with urllib.request.urlopen(f"{base_url}/api/messages/{message_id}/raw", timeout=5) as response:
    raw = response.read().decode("utf-8", errors="replace")

if "Subject: Autoloop smoke" not in raw:
    raise SystemExit("raw source missing subject")
PY
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  run_check "devcloud binary builds" go build -o "${TMP_DIR}/devcloud" ./cmd/devcloud
  if [[ "${FAIL}" -gt 0 ]]; then
    return 1
  fi
  write_mail_only_config "${TMP_DIR}"

  (
    cd "${TMP_DIR}"
    "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

run_runtime_smoke() {
  start_devcloud || return 0
  run_check "Dashboard HTTP starts" wait_for_http "http://127.0.0.1:${DASHBOARD_VERIFY_PORT}/"
  run_check "SMTP accepts message" send_smtp_smoke
}

run_api_smoke() {
  run_runtime_smoke
  run_check "Mail API lists message" api_contains_smoke_message
  run_check "Raw source API returns RFC 5322 source" raw_source_available
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
  smtp|smtp-protocol|smtp-persist)
    run_runtime_smoke
    ;;
  api|api-smoke|dashboard-static|hardening|full)
    run_check "devcloud help works" go run ./cmd/devcloud help
    run_check "devcloudd help works" go run ./cmd/devcloudd -h
    run_api_smoke
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
