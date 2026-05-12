#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

SMTP_PORT="${E2E_SMTP_PORT:-12026}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-18026}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_MESSAGE="${E2E_DELETE_MESSAGE:-true}"

usage() {
  cat <<'EOF'
Usage:
  scripts/mail-e2e.sh

Environment:
  E2E_INTERACTIVE=true       Keep devcloud running and keep the smoke mail visible in the Web UI.
  E2E_DELETE_MESSAGE=false   Keep the smoke mail after assertions.
  E2E_KEEP_WORKDIR=true      Keep the temporary workspace for debugging.
  E2E_SMTP_PORT=1125         Override the SMTP port.
  E2E_DASHBOARD_PORT=8125    Override the dashboard port.

Examples:
  scripts/mail-e2e.sh
  E2E_INTERACTIVE=true scripts/mail-e2e.sh
  E2E_SMTP_PORT=1125 E2E_DASHBOARD_PORT=8125 scripts/mail-e2e.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${INTERACTIVE}" == "true" && -z "${E2E_DELETE_MESSAGE+x}" ]]; then
  DELETE_MESSAGE="false"
fi

TMP_DIR=""
DEV_PID=""

log() {
  printf '[e2e] %s\n' "$1"
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

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
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
project: e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}

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

assert_dashboard_shell() {
  local html_file="${TMP_DIR}/dashboard.html"
  # Legacy /mail must 301-redirect to /dashboard/mail.
  curl -fsS -o /dev/null -w '%{http_code} %{redirect_url}\n' "http://127.0.0.1:${DASHBOARD_PORT}/mail" \
    | grep -q '^301 .*/dashboard/mail$'
  # React shell serves the Mail dashboard at /dashboard/mail.
  curl -fsS "http://127.0.0.1:${DASHBOARD_PORT}/dashboard/mail" > "${html_file}"
  grep -q '<title>devcloud Dashboard</title>' "${html_file}"
  grep -q '<div id="root"></div>' "${html_file}"
}

run_mail_journey() {
  E2E_SMTP_PORT="${SMTP_PORT}" E2E_DASHBOARD_PORT="${DASHBOARD_PORT}" E2E_DELETE_MESSAGE="${DELETE_MESSAGE}" python3 - <<'PY'
import json
import os
import smtplib
import time
import urllib.error
import urllib.request
from email.message import EmailMessage

smtp_port = int(os.environ["E2E_SMTP_PORT"])
dashboard_port = int(os.environ["E2E_DASHBOARD_PORT"])
delete_message = os.environ.get("E2E_DELETE_MESSAGE", "true") == "true"
base_url = f"http://127.0.0.1:{dashboard_port}"
subject = f"E2E smoke {int(time.time() * 1000)}"
body = "hello from devcloud mail e2e"

msg = EmailMessage()
msg["From"] = "sender@example.com"
msg["To"] = "user@example.com"
msg["Subject"] = subject
msg.set_content(body)

with smtplib.SMTP("127.0.0.1", smtp_port, timeout=5) as smtp:
    smtp.send_message(msg)

def request(path, method="GET"):
    req = urllib.request.Request(f"{base_url}{path}", method=method)
    with urllib.request.urlopen(req, timeout=5) as response:
        data = response.read()
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return json.loads(data.decode("utf-8"))
        return data.decode("utf-8", errors="replace")

deadline = time.time() + 10
message = None
while time.time() < deadline:
    payload = request("/api/messages")
    messages = payload.get("messages") or []
    message = next((item for item in messages if item.get("subject") == subject), None)
    if message:
        break
    time.sleep(0.2)

if not message:
    raise SystemExit("message was not listed by /api/messages")

message_id = message["id"]
if message.get("from") != "sender@example.com":
    raise SystemExit(f"unexpected sender: {message.get('from')!r}")
if "user@example.com" not in (message.get("to") or []):
    raise SystemExit(f"unexpected recipients: {message.get('to')!r}")
if body not in (message.get("textBody") or ""):
    raise SystemExit("text body missing from list metadata")

detail = request(f"/api/messages/{message_id}")
if detail.get("id") != message_id:
    raise SystemExit("detail API returned a different message")
if detail.get("subject") != subject:
    raise SystemExit("detail API subject mismatch")

raw = request(f"/api/messages/{message_id}/raw")
if f"Subject: {subject}" not in raw or body not in raw:
    raise SystemExit("raw API response did not include expected message source")

if delete_message:
    request(f"/api/messages/{message_id}", method="DELETE")
    try:
        request(f"/api/messages/{message_id}")
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    else:
        raise SystemExit("deleted message was still retrievable")

print(f"subject={subject}")
PY
}

show_interactive_hint() {
  cat <<EOF
[e2e] browser check:
[e2e]   URL: http://127.0.0.1:${DASHBOARD_PORT}/dashboard/mail
[e2e]   Expected message subject is printed above.
[e2e]   Use Refresh if the inbox is already open.
EOF
}

TMP_DIR="$(mktemp -d)"
BIN="${TMP_DIR}/devcloud"
WORKSPACE="${TMP_DIR}/workspace"
mkdir -p "${WORKSPACE}"

log "building devcloud"
go build -o "${BIN}" ./cmd/devcloud

write_config "${WORKSPACE}"

log "starting devcloud up on smtp=${SMTP_PORT}, dashboard=${DASHBOARD_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for dashboard"
wait_for_http "http://127.0.0.1:${DASHBOARD_PORT}/"

log "checking Web UI shell"
assert_dashboard_shell

if [[ "${DELETE_MESSAGE}" == "true" ]]; then
  log "running SMTP -> API -> raw -> delete journey"
else
  log "running SMTP -> API -> raw journey without deleting the message"
fi
run_mail_journey

log "passed"

if [[ "${INTERACTIVE}" == "true" ]]; then
  show_interactive_hint
  log "press Ctrl-C to stop devcloud"
  wait "${DEV_PID}"
fi
