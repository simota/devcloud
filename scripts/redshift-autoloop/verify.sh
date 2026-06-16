#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
VERIFY_HOST="127.0.0.1"

free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    try:
        sock.bind(("127.0.0.1", 0))
        print(sock.getsockname()[1])
    except PermissionError:
        print(0)
PY
}

REDSHIFT_SQL_VERIFY_PORT="${REDSHIFT_SQL_VERIFY_PORT:-$(free_port)}"
REDSHIFT_API_VERIFY_PORT="${REDSHIFT_API_VERIFY_PORT:-$(free_port)}"
S3_VERIFY_PORT="${S3_VERIFY_PORT:-$(free_port)}"
GCS_VERIFY_PORT="${GCS_VERIFY_PORT:-$(free_port)}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-$(free_port)}"
SMTP_VERIFY_PORT="${SMTP_VERIFY_PORT:-$(free_port)}"
EVENT_RELAY_VERIFY_PORT="${EVENT_RELAY_VERIFY_PORT:-$(free_port)}"
APP_AUTOSCALING_VERIFY_PORT="${APP_AUTOSCALING_VERIFY_PORT:-$(free_port)}"
REDIS_HTTP_VERIFY_PORT="${REDIS_HTTP_VERIFY_PORT:-$(free_port)}"
DYNAMODB_VERIFY_PORT="${DYNAMODB_VERIFY_PORT:-$(free_port)}"
BIGQUERY_VERIFY_PORT="${BIGQUERY_VERIFY_PORT:-$(free_port)}"
SQS_VERIFY_PORT="${SQS_VERIFY_PORT:-$(free_port)}"
PUBSUB_GRPC_VERIFY_PORT="${PUBSUB_GRPC_VERIFY_PORT:-$(free_port)}"
PUBSUB_REST_VERIFY_PORT="${PUBSUB_REST_VERIFY_PORT:-$(free_port)}"
REDSHIFT_API_ENDPOINT="http://${VERIFY_HOST}:${REDSHIFT_API_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"
CLUSTER_ID="${REDSHIFT_VERIFY_CLUSTER:-devcloud}"
DATABASE="${REDSHIFT_VERIFY_DATABASE:-dev}"
DB_USER="${REDSHIFT_VERIFY_USER:-dev}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redshift-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redshift-verify.err"
STATEMENT_ID=""

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
    tail -40 "${VERIFY_ERR}" | sed 's/^/  stderr: /'
    FAIL=$((FAIL + 1))
  fi
}

loopback_bind_available() {
  python3 - <<'PY' >/dev/null 2>&1
import socket

sock = socket.socket()
try:
    sock.bind(("127.0.0.1", 0))
finally:
    sock.close()
PY
}

skip_runtime_checks_without_loopback() {
  if loopback_bind_available; then
    return 1
  fi
  echo "[SKIP] loopback TCP bind unavailable; skipping Redshift runtime smoke checks"
  return 0
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[redshift-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[redshift-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
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
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[redshift-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[redshift-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    sleep 0.2
  done
}

require_aws_cli() {
  command -v aws >/dev/null 2>&1
}

require_psql() {
  command -v psql >/dev/null 2>&1
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: redshift-e2e

server:
  smtpPort: ${SMTP_VERIFY_PORT}
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  eventRelayPort: ${EVENT_RELAY_VERIFY_PORT}
  appAutoScalingPort: ${APP_AUTOSCALING_VERIFY_PORT}
  s3Port: ${S3_VERIFY_PORT}
  gcsPort: ${GCS_VERIFY_PORT}
  redisHttpPort: ${REDIS_HTTP_VERIFY_PORT}
  dynamodbPort: ${DYNAMODB_VERIFY_PORT}
  bigQueryPort: ${BIGQUERY_VERIFY_PORT}
  sqsPort: ${SQS_VERIFY_PORT}
  pubsubGrpcPort: ${PUBSUB_GRPC_VERIFY_PORT}
  pubsubRestPort: ${PUBSUB_REST_VERIFY_PORT}
  redshiftPort: ${REDSHIFT_SQL_VERIFY_PORT}
  redshiftAPIPort: ${REDSHIFT_API_VERIFY_PORT}

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
    password: dev
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
      kind: memory
      mode: memory
      externalDsn:
      managed: false
EOF

  local failures_before_build="${FAIL}"
  run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
  if [[ "${FAIL}" -gt "${failures_before_build}" ]]; then
    return 1
  fi

  (
    cd "${TMP_DIR}"
    "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

ensure_started() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
    wait_for_tcp "${REDSHIFT_SQL_VERIFY_PORT}"
    wait_for_http "${REDSHIFT_API_ENDPOINT}/health"
  fi
}

assert_redshift_design_contract() {
  test -f docs/design-redshift-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Amazon Redshift Compatibility Design|15439|ExecuteStatement|DescribeClusters|COPY|UNLOAD|AC-001' docs/design-redshift-compat.md
}

assert_script_contract() {
  bash -n scripts/redshift-autoloop/bootstrap.sh &&
    bash -n scripts/redshift-autoloop/run-loop.sh &&
    bash -n scripts/redshift-autoloop/recover.sh &&
    bash -n scripts/redshift-autoloop/verify.sh &&
    bash -n scripts/redshift-e2e.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS: READY' scripts/redshift-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY' scripts/redshift-autoloop/run-loop.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'mktemp .*state.env|mv .*state.env|shasum -a 256' scripts/redshift-autoloop/bootstrap.sh scripts/redshift-autoloop/recover.sh scripts/redshift-autoloop/run-loop.sh
}

assert_redshift_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'redshiftPort|redshiftAPIPort|services\\.redshift|auth\\.redshift|Redshift|redshift' orchestrator services/dashboard &&
    cargo test --workspace
}

assert_postgres_backend_shape() {
  test -f services/redshift/src/backend.rs &&
    test -f services/redshift/src/backend_postgres.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'SqlBackend|backend_kind|postgres' services/redshift orchestrator
}

assert_translator_shape() {
  test -f services/redshift/src/translator.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'trait RedshiftTranslator|translate\(|GETDATE|SYSDATE|NVL|DECODE|DATEADD|DATEDIFF|LISTAGG|DISTKEY|SORTKEY|ENCODE' services/redshift
}

redshift_endpoints_start() {
  ensure_started
}

psql_select_one() {
  require_psql || {
    echo "[SKIP] psql is not installed"
    return 0
  }
  PGPASSWORD=dev psql "host=${VERIFY_HOST} port=${REDSHIFT_SQL_VERIFY_PORT} dbname=${DATABASE} user=${DB_USER} sslmode=disable" \
    -v ON_ERROR_STOP=1 -Atc 'select 1;' | grep -qx '1'
}

psql_sql_core_workflow() {
  require_psql || {
    echo "[SKIP] psql is not installed"
    return 0
  }
  PGPASSWORD=dev psql "host=${VERIFY_HOST} port=${REDSHIFT_SQL_VERIFY_PORT} dbname=${DATABASE} user=${DB_USER} sslmode=disable" \
    -v ON_ERROR_STOP=1 <<'SQL'
create schema if not exists loop;
drop table if exists loop.events;
create table loop.events(
  id integer encode raw,
  payload varchar(64)
)
diststyle key
distkey(id)
sortkey(id);
insert into loop.events values (1, 'created');
select id, payload from loop.events where id = 1;
SQL
}

data_api_execute_statement() {
  require_aws_cli || {
    echo "[SKIP] aws CLI is not installed"
    return 0
  }
  STATEMENT_ID="$(aws redshift-data execute-statement \
    --endpoint-url "${REDSHIFT_API_ENDPOINT}" \
    --region "${AWS_DEFAULT_REGION}" \
    --cluster-identifier "${CLUSTER_ID}" \
    --database "${DATABASE}" \
    --db-user "${DB_USER}" \
    --sql 'select 1' \
    --query Id \
    --output text)"
  test -n "${STATEMENT_ID}"
}

data_api_get_result() {
  require_aws_cli || {
    echo "[SKIP] aws CLI is not installed"
    return 0
  }
  if [[ -z "${STATEMENT_ID}" ]]; then
    data_api_execute_statement >/dev/null
  fi
  aws redshift-data describe-statement \
    --endpoint-url "${REDSHIFT_API_ENDPOINT}" \
    --region "${AWS_DEFAULT_REGION}" \
    --id "${STATEMENT_ID}" >/dev/null
  aws redshift-data get-statement-result \
    --endpoint-url "${REDSHIFT_API_ENDPOINT}" \
    --region "${AWS_DEFAULT_REGION}" \
    --id "${STATEMENT_ID}" | grep -q 'Records'
}

data_api_metadata_lists() {
  require_aws_cli || {
    echo "[SKIP] aws CLI is not installed"
    return 0
  }
  aws redshift-data list-databases \
    --endpoint-url "${REDSHIFT_API_ENDPOINT}" \
    --region "${AWS_DEFAULT_REGION}" \
    --cluster-identifier "${CLUSTER_ID}" \
    --database "${DATABASE}" \
    --db-user "${DB_USER}" | grep -q "${DATABASE}"
}

management_describe_clusters() {
  require_aws_cli || {
    echo "[SKIP] aws CLI is not installed"
    return 0
  }
  aws redshift describe-clusters \
    --endpoint-url "${REDSHIFT_API_ENDPOINT}" \
    --region "${AWS_DEFAULT_REGION}" | grep -q "${CLUSTER_ID}"
}

copy_unload_workflow() {
  require_psql || {
    echo "[SKIP] psql is not installed"
    return 0
  }
  ensure_started
  printf '1,copy\n2,unload\n' > "${TMP_DIR}/events.csv"
  PGPASSWORD=dev psql "host=${VERIFY_HOST} port=${REDSHIFT_SQL_VERIFY_PORT} dbname=${DATABASE} user=${DB_USER} sslmode=disable" \
    -v ON_ERROR_STOP=1 <<SQL
drop table if exists public.copy_events;
create table public.copy_events(id integer, payload varchar(64));
copy public.copy_events from '${TMP_DIR}/events.csv' csv;
unload ('select * from public.copy_events order by id') to '${TMP_DIR}/exports/events_' csv allowoverwrite;
SQL
  find "${TMP_DIR}/exports" -type f | grep -q .
}

dashboard_starts() {
  ensure_started &&
    wait_for_http "${DASHBOARD_ENDPOINT}/"
}

dashboard_service_registry_has_redshift() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" |
    grep -q '"id"[[:space:]]*:[[:space:]]*"redshift"'
}

dashboard_redshift_page_loads() {
  curl -fsS "${DASHBOARD_ENDPOINT}/dashboard/redshift" |
    grep -q 'devcloud Dashboard'
}

dashboard_redshift_api_lists_clusters() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/redshift/clusters" |
    grep -q "${CLUSTER_ID}"
}

run_foundation_checks() {
  run_check "Redshift design contract exists" assert_redshift_design_contract
  run_check "Redshift autoloop script contract" assert_script_contract
  run_check "repository tests pass" cargo test --workspace
  run_check "devcloud help works" cargo run -p devcloud-orchestrator -- help
}

run_config_checks() {
  run_check "Redshift config shape" assert_redshift_config_shape
}

run_pgwire_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Redshift endpoints start" redshift_endpoints_start
  run_check "psql select 1 works" psql_select_one
}

run_sql_core_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "SQL core workflow works" psql_sql_core_workflow
}

run_postgres_backend_checks() {
  run_check "PostgreSQL backend shape exists" assert_postgres_backend_shape
}

run_translator_checks() {
  run_check "Redshift translator shape exists" assert_translator_shape
}

run_data_api_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Data API ExecuteStatement works" data_api_execute_statement
  run_check "Data API statement result works" data_api_get_result
  run_check "Data API metadata lists work" data_api_metadata_lists
}

run_management_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Management API DescribeClusters works" management_describe_clusters
}

run_copy_unload_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "COPY and UNLOAD workflow works" copy_unload_workflow
}

run_dashboard_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "dashboard starts" dashboard_starts
  run_check "dashboard service registry has Redshift" dashboard_service_registry_has_redshift
  run_check "dashboard Redshift page loads" dashboard_redshift_page_loads
  run_check "dashboard Redshift API lists clusters" dashboard_redshift_api_lists_clusters
}

run_e2e_checks() {
  if skip_runtime_checks_without_loopback; then
    return
  fi
  run_check "Redshift standalone E2E script passes" env REDSHIFT_BACKEND_KIND=memory REDSHIFT_BACKEND_MODE=memory REDSHIFT_BACKEND_MANAGED=false bash scripts/redshift-e2e.sh
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  config)
    run_foundation_checks
    run_config_checks
    ;;
  pgwire)
    run_foundation_checks
    run_config_checks
    run_pgwire_checks
    ;;
  sql-core)
    run_foundation_checks
    run_config_checks
    run_pgwire_checks
    run_sql_core_checks
    ;;
  postgres-backend)
    run_foundation_checks
    run_config_checks
    run_postgres_backend_checks
    ;;
  translator)
    run_foundation_checks
    run_config_checks
    run_translator_checks
    ;;
  data-api)
    run_foundation_checks
    run_config_checks
    run_pgwire_checks
    run_sql_core_checks
    run_postgres_backend_checks
    run_translator_checks
    run_data_api_checks
    ;;
  management)
    run_foundation_checks
    run_config_checks
    run_pgwire_checks
    run_postgres_backend_checks
    run_translator_checks
    run_data_api_checks
    run_management_checks
    ;;
  copy-unload)
    run_foundation_checks
    run_config_checks
    run_pgwire_checks
    run_sql_core_checks
    run_postgres_backend_checks
    run_translator_checks
    run_copy_unload_checks
    ;;
  dashboard)
    run_foundation_checks
    run_config_checks
    run_pgwire_checks
    run_sql_core_checks
    run_postgres_backend_checks
    run_translator_checks
    run_data_api_checks
    run_management_checks
    run_dashboard_checks
    ;;
  e2e)
    run_foundation_checks
    run_e2e_checks
    ;;
  full)
    run_foundation_checks
    run_config_checks
    run_pgwire_checks
    run_sql_core_checks
    run_postgres_backend_checks
    run_translator_checks
    run_data_api_checks
    run_management_checks
    run_copy_unload_checks
    run_dashboard_checks
    run_e2e_checks
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Redshift autoloop verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
