#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

BIGQUERY_PORT="${E2E_BIGQUERY_PORT:-}"
S3_PORT="${E2E_S3_PORT:-}"
SMTP_PORT="${E2E_SMTP_PORT:-}"
DYNAMODB_PORT="${E2E_DYNAMODB_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
BIGQUERY_ENDPOINT=""
BIGQUERY_SDK_ENDPOINT=""
DASHBOARD_ENDPOINT=""
PROJECT="${E2E_BIGQUERY_PROJECT:-devcloud}"
LOCATION="${E2E_BIGQUERY_LOCATION:-US}"
DATASET="${E2E_BIGQUERY_DATASET:-devcloud_sdk_e2e_$(date +%s)}"
TABLE="${E2E_BIGQUERY_TABLE:-people}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"

usage() {
  cat <<'EOF'
Usage:
  scripts/bigquery-sdk-e2e.sh

Environment:
  E2E_BIGQUERY_PORT=19050             Override the BigQuery endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025            Override the dashboard port. Defaults to an available port.
  E2E_S3_PORT=14566                   Override the S3 endpoint port used by devcloud. Defaults to an available port.
  E2E_DYNAMODB_PORT=18000             Override the DynamoDB endpoint port used by devcloud. Defaults to an available port.
  E2E_SMTP_PORT=11025                 Override the Mail SMTP port used by devcloud. Defaults to an available port.
  E2E_BIGQUERY_PROJECT=devcloud       Override the BigQuery project id.
  E2E_BIGQUERY_LOCATION=US            Override the BigQuery location.
  E2E_BIGQUERY_DATASET=devcloud_sdk   Override the test dataset id.
  E2E_BIGQUERY_TABLE=people           Override the test table id.
  E2E_KEEP_WORKDIR=true               Keep the temporary workspace for debugging.

Examples:
  scripts/bigquery-sdk-e2e.sh
  E2E_BIGQUERY_PORT=19050 E2E_DASHBOARD_PORT=18025 scripts/bigquery-sdk-e2e.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TMP_DIR=""
DEV_PID=""
WORKSPACE=""

log() {
  printf '[bigquery-sdk-e2e] %s\n' "$1"
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
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "[bigquery-sdk-e2e] missing command: ${name}" >&2
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
  if [[ -z "${BIGQUERY_PORT}" ]]; then
    BIGQUERY_PORT="$(find_free_port)"
  fi
  if [[ -z "${S3_PORT}" ]]; then
    S3_PORT="$(find_free_port)"
  fi
  if [[ -z "${SMTP_PORT}" ]]; then
    SMTP_PORT="$(find_free_port)"
  fi
  if [[ -z "${DYNAMODB_PORT}" ]]; then
    DYNAMODB_PORT="$(find_free_port)"
  fi
  if [[ -z "${DASHBOARD_PORT}" ]]; then
    DASHBOARD_PORT="$(find_free_port)"
  fi
  BIGQUERY_ENDPOINT="http://127.0.0.1:${BIGQUERY_PORT}"
  BIGQUERY_SDK_ENDPOINT="${BIGQUERY_ENDPOINT}/bigquery/v2/"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 20))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[bigquery-sdk-e2e] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[bigquery-sdk-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
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
project: bigquery-sdk-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}
  dynamodbPort: ${DYNAMODB_PORT}
  bigqueryPort: ${BIGQUERY_PORT}

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  dynamodb:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  bigquery:
    mode: relaxed
    project: ${PROJECT}

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
  dynamodb:
    enabled: true
    region: us-east-1
  bigquery:
    enabled: true
    project: ${PROJECT}
    location: ${LOCATION}
    maxRowsPerTable: 1000000
    maxRequestBytes: 10485760
    query:
      maxResultRows: 10000
      maxExecutionSeconds: 30
      defaultUseLegacySql: false
  redshift:
    enabled: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
EOF
}

write_sdk_smoke() {
  local sdk_dir="$1"
  mkdir -p "${sdk_dir}"
  cat > "${sdk_dir}/go.mod" <<'EOF'
module devcloud-bigquery-sdk-smoke

go 1.22

require cloud.google.com/go/bigquery v1.66.2
EOF
  cat > "${sdk_dir}/main.go" <<'EOF'
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type person struct {
	ID     string `bigquery:"id"`
	Name   string `bigquery:"name"`
	Age    int64  `bigquery:"age"`
	Active bool   `bigquery:"active"`
}

func main() {
	endpoint := requiredEnv("DEVCLOUD_BIGQUERY_ENDPOINT")
	project := requiredEnv("DEVCLOUD_BIGQUERY_PROJECT")
	location := requiredEnv("DEVCLOUD_BIGQUERY_LOCATION")
	datasetID := requiredEnv("DEVCLOUD_BIGQUERY_DATASET")
	tableID := requiredEnv("DEVCLOUD_BIGQUERY_TABLE")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := bigquery.NewClient(
		ctx,
		project,
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
	)
	if err != nil {
		fail("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	dataset := client.Dataset(datasetID)
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{Location: location, Name: "BigQuery SDK E2E Dataset"}); err != nil {
		fail("Dataset.Create: %v", err)
	}

	datasetMeta, err := dataset.Metadata(ctx)
	if err != nil {
		fail("Dataset.Metadata: %v", err)
	}
	if datasetMeta.Location != location {
		fail("Dataset.Metadata location = %q, want %q", datasetMeta.Location, location)
	}

	if !datasetListed(ctx, client, datasetID) {
		fail("Dataset Iterator did not include %q", datasetID)
	}

	table := dataset.Table(tableID)
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
		{Name: "active", Type: bigquery.BooleanFieldType},
	}
	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema, Name: "People"}); err != nil {
		fail("Table.Create: %v", err)
	}

	tableMeta, err := table.Metadata(ctx)
	if err != nil {
		fail("Table.Metadata: %v", err)
	}
	if len(tableMeta.Schema) != len(schema) {
		fail("Table.Metadata schema length = %d, want %d", len(tableMeta.Schema), len(schema))
	}

	rows := []*person{
		{ID: "1", Name: "Ada", Age: 37, Active: true},
		{ID: "2", Name: "Grace", Age: 31, Active: true},
	}
	if err := table.Inserter().Put(ctx, rows); err != nil {
		fail("Inserter.Put: %v", err)
	}

	if got := readTableRows(ctx, table); got != 2 {
		fail("Table.Read row count = %d, want 2", got)
	}

	query := client.Query(fmt.Sprintf("SELECT id, age FROM `%s.%s.%s` WHERE age >= 30 ORDER BY id", project, datasetID, tableID))
	query.Location = location
	query.UseLegacySQL = false
	job, err := query.Run(ctx)
	if err != nil {
		fail("Query.Run: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		fail("Job.Wait: %v", err)
	}
	if err := status.Err(); err != nil {
		fail("Job status: %v", err)
	}
	if job.ID() == "" {
		fail("Job.ID is empty")
	}

	iter, err := job.Read(ctx)
	if err != nil {
		fail("Job.Read: %v", err)
	}
	if got := countQueryRows(iter); got != 2 {
		fail("Job.Read row count = %d, want 2", got)
	}

	if err := table.Delete(ctx); err != nil {
		fail("Table.Delete: %v", err)
	}
	if _, err := table.Metadata(ctx); err == nil {
		fail("Table.Metadata succeeded after Table.Delete")
	}

	if err := dataset.Delete(ctx); err != nil {
		fail("Dataset.Delete: %v", err)
	}
	if _, err := dataset.Metadata(ctx); err == nil {
		fail("Dataset.Metadata succeeded after Dataset.Delete")
	}

	fmt.Println("sdk smoke passed")
}

func datasetListed(ctx context.Context, client *bigquery.Client, datasetID string) bool {
	iter := client.Datasets(ctx)
	for {
		ds, err := iter.Next()
		if err == iterator.Done {
			return false
		}
		if err != nil {
			fail("Dataset Iterator: %v", err)
		}
		if ds.DatasetID == datasetID {
			return true
		}
	}
}

func readTableRows(ctx context.Context, table *bigquery.Table) int {
	iter := table.Read(ctx)
	count := 0
	for {
		var values []bigquery.Value
		err := iter.Next(&values)
		if err == iterator.Done {
			return count
		}
		if err != nil {
			fail("Table.Read Iterator: %v", err)
		}
		if len(values) == 0 {
			fail("Table.Read returned an empty row")
		}
		count++
	}
}

func countQueryRows(iter *bigquery.RowIterator) int {
	count := 0
	for {
		var values []bigquery.Value
		err := iter.Next(&values)
		if err == iterator.Done {
			return count
		}
		if err != nil {
			fail("Query RowIterator: %v", err)
		}
		if len(values) != 2 {
			fail("Query row width = %d, want 2", len(values))
		}
		count++
	}
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		fail("%s is required", name)
	}
	return value
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
EOF
}

run_sdk_smoke() {
  local sdk_dir="${TMP_DIR}/sdk-smoke"
  write_sdk_smoke "${sdk_dir}"
  (
    cd "${sdk_dir}"
    DEVCLOUD_BIGQUERY_ENDPOINT="${BIGQUERY_SDK_ENDPOINT}" \
      DEVCLOUD_BIGQUERY_PROJECT="${PROJECT}" \
      DEVCLOUD_BIGQUERY_LOCATION="${LOCATION}" \
      DEVCLOUD_BIGQUERY_DATASET="${DATASET}" \
      DEVCLOUD_BIGQUERY_TABLE="${TABLE}" \
      go mod tidy
    DEVCLOUD_BIGQUERY_ENDPOINT="${BIGQUERY_SDK_ENDPOINT}" \
      DEVCLOUD_BIGQUERY_PROJECT="${PROJECT}" \
      DEVCLOUD_BIGQUERY_LOCATION="${LOCATION}" \
      DEVCLOUD_BIGQUERY_DATASET="${DATASET}" \
      DEVCLOUD_BIGQUERY_TABLE="${TABLE}" \
      go run .
  )
}

require_command curl
require_command go
require_command python3

assign_ports

TMP_DIR="$(mktemp -d)"
BIN="${TMP_DIR}/devcloud"
WORKSPACE="${TMP_DIR}/workspace"
mkdir -p "${WORKSPACE}"

log "building devcloud"
go build -o "${BIN}" ./cmd/devcloud

write_config "${WORKSPACE}"

log "starting devcloud up on bigquery=${BIGQUERY_PORT}, dashboard=${DASHBOARD_PORT}, smtp=${SMTP_PORT}, s3=${S3_PORT}, dynamodb=${DYNAMODB_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for BigQuery endpoint"
wait_for_http "${BIGQUERY_ENDPOINT}/bigquery/v2/projects"
log "waiting for dashboard"
wait_for_http "${DASHBOARD_ENDPOINT}/"

log "running Google BigQuery SDK journey"
run_sdk_smoke

log "passed"
