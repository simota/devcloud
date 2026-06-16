#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

HOST="127.0.0.1"

log() {
  printf '[redshift-e2e] %s\n' "$1"
}

loopback_sockets_available() {
  python3 - <<'PY'
import socket

try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
except PermissionError:
    raise SystemExit(1)
PY
}

run_dashboard_contract_fallback() {
  log "loopback sockets unavailable; running Rust in-process Redshift dashboard contract"
  cargo test -p devcloud-redshift --test dashboard_parity
}

if ! loopback_sockets_available; then
  run_dashboard_contract_fallback
  exit 0
fi

free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

REDSHIFT_SQL_PORT="${REDSHIFT_SQL_PORT:-$(free_port)}"
REDSHIFT_API_PORT="${REDSHIFT_API_PORT:-$(free_port)}"
DASHBOARD_PORT="${DASHBOARD_PORT:-$(free_port)}"
EVENT_RELAY_PORT="${EVENT_RELAY_PORT:-$(free_port)}"
APP_AUTOSCALING_PORT="${APP_AUTOSCALING_PORT:-$(free_port)}"
REDIS_HTTP_PORT="${REDIS_HTTP_PORT:-$(free_port)}"
SMTP_PORT="${SMTP_PORT:-$(free_port)}"
S3_PORT="${S3_PORT:-$(free_port)}"
GCS_PORT="${GCS_PORT:-$(free_port)}"
DYNAMODB_PORT="${DYNAMODB_PORT:-$(free_port)}"
BIGQUERY_PORT="${BIGQUERY_PORT:-$(free_port)}"
SQS_PORT="${SQS_PORT:-$(free_port)}"
PUBSUB_GRPC_PORT="${PUBSUB_GRPC_PORT:-$(free_port)}"
PUBSUB_REST_PORT="${PUBSUB_REST_PORT:-$(free_port)}"
CLUSTER_ID="${REDSHIFT_CLUSTER_ID:-devcloud}"
DATABASE="${REDSHIFT_DATABASE:-dev}"
DB_USER="${REDSHIFT_USER:-dev}"
DB_PASSWORD="${REDSHIFT_PASSWORD:-dev}"
REDSHIFT_BACKEND_KIND="${REDSHIFT_BACKEND_KIND:-postgres}"
REDSHIFT_BACKEND_MODE="${REDSHIFT_BACKEND_MODE:-managed}"
REDSHIFT_BACKEND_EXTERNAL_DSN="${REDSHIFT_BACKEND_EXTERNAL_DSN:-}"
REDSHIFT_BACKEND_MANAGED="${REDSHIFT_BACKEND_MANAGED:-true}"
API_ENDPOINT="http://${HOST}:${REDSHIFT_API_PORT}"
DASHBOARD_ENDPOINT="http://${HOST}:${DASHBOARD_PORT}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"

TMP_DIR=""
DEV_PID=""
STATEMENT_ID=""

usage() {
  cat <<'EOF'
Usage:
  scripts/redshift-e2e.sh

Environment:
  REDSHIFT_SQL_PORT=15439             Override the Redshift SQL port.
  REDSHIFT_API_PORT=19099             Override the Redshift HTTP API port.
  DASHBOARD_PORT=18025                Override the dashboard port.
  EVENT_RELAY_PORT=18027              Override the dashboard event relay port.
  APP_AUTOSCALING_PORT=18030          Override the Application Auto Scaling port.
  REDIS_HTTP_PORT=16380               Override the Redis control HTTP port.
  REDSHIFT_CLUSTER_ID=devcloud        Override the local cluster identifier.
  REDSHIFT_DATABASE=dev               Override the database name.
  REDSHIFT_USER=dev                   Override the database user.
  REDSHIFT_PASSWORD=dev               Override the database password.
  REDSHIFT_BACKEND_KIND=postgres      Override the SQL backend kind. Use memory for explicit fallback.
  REDSHIFT_BACKEND_MODE=managed       Override the SQL backend mode. Use external with external DSN.
  REDSHIFT_BACKEND_EXTERNAL_DSN=      External PostgreSQL DSN for backend.kind=postgres.
  REDSHIFT_BACKEND_MANAGED=true       Toggle managed PostgreSQL backend config.
  E2E_KEEP_WORKDIR=true               Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true                Keep devcloud running after assertions.

Examples:
  scripts/redshift-e2e.sh
  E2E_KEEP_WORKDIR=true scripts/redshift-e2e.sh
  REDSHIFT_SQL_PORT=15439 REDSHIFT_API_PORT=19099 scripts/redshift-e2e.sh
  REDSHIFT_BACKEND_KIND=memory REDSHIFT_BACKEND_MODE=external REDSHIFT_BACKEND_MANAGED=false scripts/redshift-e2e.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${INTERACTIVE}" == "true" && -z "${E2E_KEEP_WORKDIR+x}" ]]; then
  KEEP_WORKDIR="true"
fi

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "[redshift-e2e] missing command: ${name}" >&2
    exit 1
  fi
}

cleanup() {
  if [[ -n "${DEV_PID}" ]]; then
    if [[ "${INTERACTIVE}" == "true" ]]; then
      log "devcloud still running: pid=${DEV_PID}"
    else
      kill "${DEV_PID}" >/dev/null 2>&1 || true
      wait "${DEV_PID}" >/dev/null 2>&1 || true
    fi
  fi
  if [[ "${KEEP_WORKDIR}" != "true" && "${INTERACTIVE}" != "true" && -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  elif [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    log "kept workdir: ${TMP_DIR}"
  fi
}
trap cleanup EXIT

print_devcloud_log() {
  if [[ -z "${TMP_DIR}" || ! -f "${TMP_DIR}/devcloud-up.log" ]]; then
    return
  fi
  python3 - "${TMP_DIR}/devcloud-up.log" "${REDSHIFT_BACKEND_EXTERNAL_DSN}" <<'PY' >&2
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
secrets = [secret for secret in sys.argv[2:] if "://" in secret or "@" in secret]
text = path.read_text(errors="replace")
for secret in secrets:
    text = text.replace(secret, "redacted")
text = re.sub(r"(?i)(postgres(?:ql)?://[^:\s/@]+:)[^@\s]+(@)", r"\1redacted\2", text)
text = re.sub(r"(?i)(password[^\s:=]*[\s:=]+)\S+", r"\1redacted", text)
for line in text.splitlines():
    print(f"[redshift-e2e] devcloud: {line}")
PY
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[FAIL] devcloud exited while waiting for ${url}" >&2
      print_devcloud_log
      return 1
    fi
    if (( SECONDS >= deadline )); then
      echo "[FAIL] timed out waiting for ${url}" >&2
      return 1
    fi
    sleep 0.2
  done
}

wait_for_tcp() {
  local port="$1"
  local deadline=$((SECONDS + 15))
  until python3 - "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

port = int(sys.argv[1])
with socket.create_connection(("127.0.0.1", port), timeout=0.5):
    pass
PY
  do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[FAIL] devcloud exited while waiting for tcp/${port}" >&2
      print_devcloud_log
      return 1
    fi
    if (( SECONDS >= deadline )); then
      echo "[FAIL] timed out waiting for tcp/${port}" >&2
      return 1
    fi
    sleep 0.2
  done
}

for command in curl cargo grep python3; do
  require_command "${command}"
done

if ! command -v psql >/dev/null 2>&1; then
  echo "[redshift-e2e] missing command: psql" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "[redshift-e2e] missing command: aws" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
mkdir -p "${TMP_DIR}/.devcloud"
cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: redshift-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  eventRelayPort: ${EVENT_RELAY_PORT}
  appAutoScalingPort: ${APP_AUTOSCALING_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}
  redisHttpPort: ${REDIS_HTTP_PORT}
  dynamodbPort: ${DYNAMODB_PORT}
  bigQueryPort: ${BIGQUERY_PORT}
  sqsPort: ${SQS_PORT}
  pubsubGrpcPort: ${PUBSUB_GRPC_PORT}
  pubsubRestPort: ${PUBSUB_REST_PORT}
  redshiftPort: ${REDSHIFT_SQL_PORT}
  redshiftAPIPort: ${REDSHIFT_API_PORT}

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  gcs:
    mode: relaxed
    project: devcloud
  dynamodb:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  bigquery:
    mode: relaxed
    project: devcloud
    bearerToken: dev
  sqs:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"
  pubsub:
    mode: relaxed
    projectID: devcloud
  redshift:
    mode: relaxed
    user: ${DB_USER}
    password: ${DB_PASSWORD}
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"

storage:
  path: .devcloud/data

services:
  mail:
    enabled: false
    maxMessageBytes: 10485760
  s3:
    enabled: true
    region: us-east-1
  gcs:
    enabled: false
    project: devcloud
    location: US
  dynamodb:
    enabled: false
    region: us-east-1
  bigquery:
    enabled: false
    project: devcloud
    location: US
  sqs:
    enabled: false
    region: us-east-1
    queueUrlHost: 127.0.0.1
  pubsub:
    enabled: false
    project: devcloud
  redshift:
    enabled: true
    region: us-east-1
    clusterIdentifier: ${CLUSTER_ID}
    database: ${DATABASE}
    dataDir: redshift
    nodeType: dc2.large
    numberOfNodes: 1
    backend:
      kind: ${REDSHIFT_BACKEND_KIND}
      mode: ${REDSHIFT_BACKEND_MODE}
      externalDsn: ${REDSHIFT_BACKEND_EXTERNAL_DSN}
      managed: ${REDSHIFT_BACKEND_MANAGED}
EOF

log "building devcloud"
devcloud_build "${TMP_DIR}/devcloud"

log "starting devcloud in ${TMP_DIR}"
(
  cd "${TMP_DIR}"
  "${TMP_DIR}/devcloud" up
) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

wait_for_tcp "${REDSHIFT_SQL_PORT}"
wait_for_http "${API_ENDPOINT}/health"
wait_for_http "${DASHBOARD_ENDPOINT}/"

log "running SQL smoke"
PGPASSWORD="${DB_PASSWORD}" psql "host=${HOST} port=${REDSHIFT_SQL_PORT} dbname=${DATABASE} user=${DB_USER} sslmode=disable" \
  -v ON_ERROR_STOP=1 <<'SQL'
select 1;
create schema if not exists e2e;
drop table if exists e2e.events;
create table e2e.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id);
insert into e2e.events values (1, 'created');
select id, payload from e2e.events where id = 1;
SQL

log "running Data API smoke"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
STATEMENT_ID="$(aws redshift-data execute-statement \
  --endpoint-url "${API_ENDPOINT}" \
  --region "${AWS_DEFAULT_REGION}" \
  --cluster-identifier "${CLUSTER_ID}" \
  --database "${DATABASE}" \
  --db-user "${DB_USER}" \
  --sql 'select 1' \
  --query Id \
  --output text)"
test -n "${STATEMENT_ID}"
aws redshift-data describe-statement --endpoint-url "${API_ENDPOINT}" --region "${AWS_DEFAULT_REGION}" --id "${STATEMENT_ID}" >/dev/null
aws redshift-data get-statement-result --endpoint-url "${API_ENDPOINT}" --region "${AWS_DEFAULT_REGION}" --id "${STATEMENT_ID}" | grep -q 'Records'

log "running management API smoke"
aws redshift describe-clusters --endpoint-url "${API_ENDPOINT}" --region "${AWS_DEFAULT_REGION}" | grep -q "${CLUSTER_ID}"

log "running dashboard smoke"
curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" | grep -q '"id"[[:space:]]*:[[:space:]]*"redshift"'
curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/redshift" | grep -q 'devcloud Dashboard'

log "running dashboard Redshift API journey"
curl -fsS "${DASHBOARD_ENDPOINT}/api/redshift/status" >"${TMP_DIR}/redshift-status.json"
python3 - "${TMP_DIR}/redshift-status.json" "${REDSHIFT_SQL_PORT}" "${REDSHIFT_API_PORT}" "${REDSHIFT_BACKEND_KIND}" "${REDSHIFT_BACKEND_MODE}" <<'PY'
import json
import sys

path, sql_port, api_port, backend_kind, backend_mode = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    payload = json.load(handle)

assert payload["service"] == "redshift", payload
assert payload["status"] == "running", payload
assert payload["running"] is True, payload
assert payload["clusterCount"] == 1, payload
assert payload["backendKind"] == backend_kind, payload
assert payload["backendMode"] == backend_mode, payload
assert payload["sqlEndpoint"].endswith(":" + sql_port), payload
assert payload["apiEndpoint"].endswith(":" + api_port), payload
PY

curl -fsS "${DASHBOARD_ENDPOINT}/api/redshift/clusters" >"${TMP_DIR}/redshift-clusters.json"
python3 - "${TMP_DIR}/redshift-clusters.json" "${CLUSTER_ID}" "${REDSHIFT_SQL_PORT}" <<'PY'
import json
import sys

path, cluster_id, sql_port = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    payload = json.load(handle)

clusters = payload.get("clusters", [])
assert len(clusters) == 1, payload
cluster = clusters[0]
assert cluster["clusterIdentifier"] == cluster_id, payload
assert cluster["clusterStatus"] == "available", payload
assert cluster["endpoint"]["port"] == int(sql_port), payload
PY

curl -fsS "${DASHBOARD_ENDPOINT}/api/redshift/catalog" >"${TMP_DIR}/redshift-catalog.json"
python3 - "${TMP_DIR}/redshift-catalog.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)

catalog = payload.get("catalog", {})
schemas = {schema["name"] for schema in catalog.get("schemas", [])}
tables = {(table["schema"], table["name"]) for table in catalog.get("tables", [])}
columns = {(column["schema"], column["table"], column["name"]) for column in catalog.get("columns", [])}
assert "e2e" in schemas, payload
assert ("e2e", "events") in tables, payload
assert ("e2e", "events", "id") in columns, payload
assert ("e2e", "events", "payload") in columns, payload
PY

curl -fsS "${DASHBOARD_ENDPOINT}/api/redshift/tables/e2e/events?limit=5" >"${TMP_DIR}/redshift-table.json"
python3 - "${TMP_DIR}/redshift-table.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)

assert payload["schema"] == "e2e", payload
assert payload["table"] == "events", payload
column_names = [column["name"] for column in payload.get("columns", [])]
assert column_names == ["id", "payload"], payload
rows = payload.get("rows") or []
if rows:
    assert any(len(row) >= 2 and row[1] == "created" for row in rows), payload
PY

curl -fsS -X POST "${DASHBOARD_ENDPOINT}/api/redshift/query" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"select id, payload from e2e.events where id = 1","maxRows":5}' \
  >"${TMP_DIR}/redshift-query.json"
python3 - "${TMP_DIR}/redshift-query.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)

result = payload.get("result", {})
statement = result.get("statement", {})
assert statement.get("status") == "FINISHED", payload
assert result.get("commandTag", "").startswith("SELECT"), payload
assert result.get("rowCount") == 1, payload
column_names = [column["name"] for column in result.get("columns", [])]
assert column_names == ["id", "payload"], payload
assert any(len(row) >= 2 and row[1] == "created" for row in result.get("rows", [])), payload
PY

curl -fsS "${DASHBOARD_ENDPOINT}/api/redshift/statements" >"${TMP_DIR}/redshift-statements.json"
python3 - "${TMP_DIR}/redshift-statements.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)

statements = payload.get("statements", [])
assert statements, payload
assert any(statement.get("status") == "FINISHED" for statement in statements), payload
assert any("e2e.events" in statement.get("queryPreview", "") for statement in statements), payload
PY

log "Redshift E2E passed"
