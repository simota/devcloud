#!/usr/bin/env bash
# End-to-end smoke for the Redshift→PostgreSQL translator.
# Boots devcloud, opens a psql session, and exercises a representative
# sample of the rewrite rules accumulated under
# internal/services/redshift/translator/translator.go. Each statement runs
# in its own psql invocation so a single failure does not abort the suite;
# the final report lists per-test PASS/FAIL with the captured stderr.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

HOST="127.0.0.1"
DB_USER="dev"
DB_PASSWORD="dev"
DATABASE="dev"

log() { printf '[redshift-translator-e2e] %s\n' "$1"; }

free_port() {
  # Sweep a low-numbered range so daemon code paths that compute
  # RedshiftPort+10000 (managed PostgreSQL backend) stay below the
  # 65535 TCP ceiling. macOS ephemeral allocation starts at 49152 and
  # often returns ports too high for that offset.
  python3 - <<'PY'
import random
import socket

attempts = list(range(15000, 35000))
random.shuffle(attempts)
for port in attempts:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            sock.bind(("127.0.0.1", port))
        print(port)
        break
    except OSError:
        continue
else:
    raise SystemExit("could not allocate a port in 15000-35000")
PY
}

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "[FAIL] missing command: $1" >&2; exit 1; }
}
for cmd in go psql python3; do require "${cmd}"; done

REDSHIFT_SQL_PORT="${REDSHIFT_SQL_PORT:-$(free_port)}"
REDSHIFT_API_PORT="${REDSHIFT_API_PORT:-$(free_port)}"
DASHBOARD_PORT="${DASHBOARD_PORT:-$(free_port)}"
SMTP_PORT="${SMTP_PORT:-$(free_port)}"
S3_PORT="${S3_PORT:-$(free_port)}"
GCS_PORT="${GCS_PORT:-$(free_port)}"
DYNAMODB_PORT="${DYNAMODB_PORT:-$(free_port)}"
BIGQUERY_PORT="${BIGQUERY_PORT:-$(free_port)}"
SQS_PORT="${SQS_PORT:-$(free_port)}"
PUBSUB_GRPC_PORT="${PUBSUB_GRPC_PORT:-$(free_port)}"
PUBSUB_REST_PORT="${PUBSUB_REST_PORT:-$(free_port)}"
APP_AUTOSCALING_PORT="${APP_AUTOSCALING_PORT:-$(free_port)}"

TMP_DIR="$(mktemp -d)"
DEV_PID=""
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"

cleanup() {
  if [[ -n "${DEV_PID}" ]]; then
    kill "${DEV_PID}" >/dev/null 2>&1 || true
    wait "${DEV_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_WORKDIR}" == "true" ]]; then
    log "kept workdir: ${TMP_DIR}"
  elif [[ -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT

mkdir -p "${TMP_DIR}/.devcloud"
cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: redshift-translator-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}
  dynamodbPort: ${DYNAMODB_PORT}
  bigQueryPort: ${BIGQUERY_PORT}
  sqsPort: ${SQS_PORT}
  pubsubGrpcPort: ${PUBSUB_GRPC_PORT}
  pubsubRestPort: ${PUBSUB_REST_PORT}
  redshiftPort: ${REDSHIFT_SQL_PORT}
  redshiftAPIPort: ${REDSHIFT_API_PORT}
  appAutoScalingPort: ${APP_AUTOSCALING_PORT}

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
    enabled: false
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
  appAutoScaling:
    enabled: false
    region: us-east-1
  redshift:
    enabled: true
    region: us-east-1
    clusterIdentifier: devcloud
    database: ${DATABASE}
    dataDir: redshift
    nodeType: dc2.large
    numberOfNodes: 1
    backend:
      kind: postgres
      mode: managed
      externalDsn: ""
      managed: true
EOF

log "building devcloud"
go build -o "${TMP_DIR}/devcloud" ./cmd/devcloud

log "starting devcloud in ${TMP_DIR}"
( cd "${TMP_DIR}" && "${TMP_DIR}/devcloud" up ) >"${TMP_DIR}/devcloud.log" 2>&1 &
DEV_PID="$!"

# Wait for the Redshift SQL port to accept connections.
deadline=$((SECONDS + 30))
until python3 - "${REDSHIFT_SQL_PORT}" <<'PY' >/dev/null 2>&1
import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.5):
    pass
PY
do
  if ! kill -0 "${DEV_PID}" 2>/dev/null; then
    log "devcloud exited unexpectedly; tail of log:"
    tail -30 "${TMP_DIR}/devcloud.log" >&2
    exit 1
  fi
  (( SECONDS < deadline )) || { log "timed out waiting for redshift port"; tail -30 "${TMP_DIR}/devcloud.log" >&2; exit 1; }
  sleep 0.3
done

log "redshift port up on ${REDSHIFT_SQL_PORT}"

PSQL_CONN="host=${HOST} port=${REDSHIFT_SQL_PORT} dbname=${DATABASE} user=${DB_USER} sslmode=disable"
PASS=0
FAIL=0
FAILED_TESTS=()

run_sql() {
  local label="$1"; shift
  local sql="$1"; shift
  local expect_substr="${1:-}"

  local stdout_file="${TMP_DIR}/${label//[^A-Za-z0-9_-]/_}.out"
  if PGPASSWORD="${DB_PASSWORD}" psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -c "${sql}" >"${stdout_file}" 2>&1; then
    if [[ -n "${expect_substr}" ]] && ! grep -qF -- "${expect_substr}" "${stdout_file}"; then
      log "  FAIL ${label}: expected '${expect_substr}' missing"
      FAIL=$((FAIL + 1))
      FAILED_TESTS+=("${label}: missing expect '${expect_substr}'")
    else
      log "  PASS ${label}"
      PASS=$((PASS + 1))
    fi
  else
    local err
    err=$(tail -2 "${stdout_file}" | tr '\n' ' ')
    log "  FAIL ${label}: ${err}"
    FAIL=$((FAIL + 1))
    FAILED_TESTS+=("${label}: ${err}")
  fi
}

log "==> setup: schema + table"
run_sql setup_schema   "create schema if not exists e2e;"
run_sql setup_drop     "drop table if exists e2e.events;"
run_sql setup_create   "create table e2e.events(id integer identity(1,1), tenant varchar(32) encode raw, ts timestamp, payload varchar(64), score integer) diststyle key distkey(tenant) sortkey(ts);"
run_sql setup_seed     "insert into e2e.events (tenant, ts, payload, score) values
                          ('acme','2026-01-01'::timestamp,'a',10),
                          ('acme','2026-01-02'::timestamp,'b',20),
                          ('zeta','2026-01-03'::timestamp,'c',30),
                          ('zeta','2026-01-04'::timestamp,'d',null);"

log "==> scalar functions"
run_sql getdate        "select getdate() as now;"      "now"
run_sql sysdate        "select sysdate as now;"        "now"
run_sql nvl            "select nvl(null::int, 1);"     "1"
run_sql nvl2           "select nvl2(null::int, 'a', 'b');"   "b"
run_sql decode         "select decode('x','x','match','other') as v;" "match"
run_sql greatest_null  "select greatest(null::int, 1, 2);"          "2"
run_sql least_null     "select least(null::int, 1, 2);"             "1"
run_sql len            "select len('hello');"          "5"
run_sql charindex      "select charindex('l','hello');"             "3"
run_sql rand_in_range  "select rand() between 0 and 1;"             "t"
run_sql round_neg      "select round(123.456::numeric, -1);"        "120"

log "==> date/time functions"
run_sql dateadd_day    "select dateadd(day, 3, '2026-01-01'::date);" "2026-01-04"
run_sql datediff_day   "select datediff(day, '2026-01-01'::date, '2026-01-04'::date);" "3"
run_sql convert_tz     "select convert_timezone('UTC','America/New_York', '2026-01-01 12:00:00'::timestamp);" "2026-01-01 07:00:00"
run_sql last_day       "select last_day('2026-02-10'::date);" "2026-02-28"
run_sql months_between "select months_between('2026-04-01'::date, '2026-01-01'::date);" "3"
run_sql add_months     "select add_months('2026-01-31'::date, 1);" "2026-02-28"
run_sql to_char_ts     "select to_char('2026-05-18 12:34:56'::timestamp, 'YYYY-MM-DD');" "2026-05-18"

log "==> aggregates"
run_sql listagg        "select listagg(payload, ',') within group (order by ts) from e2e.events where tenant='acme';" "a,b"
run_sql median         "select median(score) from e2e.events where tenant='acme';" "15"
run_sql approx_count   "select approximate count(distinct tenant) from e2e.events;" "2"
run_sql ratio_to_rep   "select tenant, ratio_to_report(score) over () from e2e.events where score is not null order by tenant, score limit 1;"

log "==> SELECT shapes"
run_sql top_n          "select top 2 tenant, payload from e2e.events order by id;" "acme"
run_sql qualify        "select tenant, payload, row_number() over (partition by tenant order by ts) rn from e2e.events qualify rn=1 order by tenant;" "acme"
run_sql like_escape    "select 'a_b' like 'a\\_b';" "t"

log "==> JSON / PartiQL"
run_sql json_parse_text "select json_extract_path_text('{\"k\":\"v\"}','k');" "v"
run_sql json_arr_len   "select json_array_length('[1,2,3,4]');" "4"

log "==> DML"
run_sql truncate_ok    "truncate table e2e.events;"
run_sql reseed         "insert into e2e.events (tenant, ts, payload, score) values ('acme','2026-01-01'::timestamp,'r',1);"
run_sql merge_into     "merge into e2e.events tgt using (select 'acme' as tenant, '2026-01-01'::timestamp as ts, 'r2' as payload, 7 as score) src on tgt.tenant = src.tenant when matched then update set payload = src.payload, score = src.score when not matched then insert (tenant, ts, payload, score) values (src.tenant, src.ts, src.payload, src.score);"
run_sql post_merge     "select payload, score from e2e.events where tenant='acme';" "r2"

log "==> teardown"
run_sql teardown       "drop schema e2e cascade;"

log ""
log "summary: ${PASS} pass / ${FAIL} fail"
if (( FAIL > 0 )); then
  log "failed tests:"
  for entry in "${FAILED_TESTS[@]}"; do log "  - ${entry}"; done
  exit 1
fi
log "all translator rewrites passed end-to-end"
