#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GOCACHE="${GOCACHE:-${ROOT_DIR}/.devcloud/go-build}"

GCS_PORT="${E2E_GCS_PORT:-}"
S3_PORT="${E2E_S3_PORT:-}"
SMTP_PORT="${E2E_SMTP_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
GCS_ENDPOINT=""
GCS_SDK_ENDPOINT=""
DASHBOARD_ENDPOINT=""
PROJECT="${E2E_GCS_PROJECT:-devcloud}"
BUCKET="${E2E_BUCKET:-devcloud-gcs-sdk-e2e-$(date +%s)}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"

usage() {
  cat <<'EOF'
Usage:
  scripts/gcs-sdk-e2e.sh

Environment:
  E2E_GCS_PORT=14443          Override the GCS endpoint port. Defaults to an available port.
  E2E_S3_PORT=14566           Override the S3 endpoint port used by devcloud. Defaults to an available port.
  E2E_SMTP_PORT=11025         Override the Mail SMTP port used by devcloud. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025    Override the dashboard port. Defaults to an available port.
  E2E_GCS_PROJECT=devcloud    Override the GCS project id.
  E2E_BUCKET=devcloud-gcs-sdk Override the test bucket name.
  E2E_KEEP_WORKDIR=true       Keep the temporary workspace for debugging.

Examples:
  scripts/gcs-sdk-e2e.sh
  E2E_GCS_PORT=14443 E2E_DASHBOARD_PORT=18025 scripts/gcs-sdk-e2e.sh
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
  printf '[gcs-sdk-e2e] %s\n' "$1"
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
    echo "[gcs-sdk-e2e] missing command: ${name}" >&2
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
  if [[ -z "${GCS_PORT}" ]]; then
    GCS_PORT="$(find_free_port)"
  fi
  if [[ -z "${S3_PORT}" ]]; then
    S3_PORT="$(find_free_port)"
  fi
  if [[ -z "${SMTP_PORT}" ]]; then
    SMTP_PORT="$(find_free_port)"
  fi
  if [[ -z "${DASHBOARD_PORT}" ]]; then
    DASHBOARD_PORT="$(find_free_port)"
  fi
  GCS_ENDPOINT="http://127.0.0.1:${GCS_PORT}"
  GCS_SDK_ENDPOINT="${GCS_ENDPOINT}/storage/v1/"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[gcs-sdk-e2e] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[gcs-sdk-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
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
project: gcs-sdk-e2e

server:
  smtpPort: ${SMTP_PORT}
  dashboardPort: ${DASHBOARD_PORT}
  s3Port: ${S3_PORT}
  gcsPort: ${GCS_PORT}

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  gcs:
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
    enabled: true
    project: ${PROJECT}
    location: US
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

write_sdk_smoke() {
  local sdk_dir="$1"
  mkdir -p "${sdk_dir}"
  cat > "${sdk_dir}/go.mod" <<'EOF'
module devcloud-gcs-sdk-smoke

go 1.22

require cloud.google.com/go/storage v1.43.0
EOF
  cat > "${sdk_dir}/main.go" <<'EOF'
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const objectName = "docs/sdk-smoke.txt"
const objectBody = "hello from devcloud gcs sdk e2e\n"

func main() {
	endpoint := requiredEnv("DEVCLOUD_GCS_ENDPOINT")
	project := requiredEnv("DEVCLOUD_GCS_PROJECT")
	bucketName := requiredEnv("DEVCLOUD_GCS_BUCKET")

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
		storage.WithJSONReads(),
	)
	if err != nil {
		fail("storage.NewClient: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	if err := bucket.Create(ctx, project, &storage.BucketAttrs{
		Location:     "US",
		StorageClass: "STANDARD",
	}); err != nil {
		fail("Bucket.Create: %v", err)
	}

	bucketAttrs, err := bucket.Attrs(ctx)
	if err != nil {
		fail("Bucket.Attrs: %v", err)
	}
	if bucketAttrs.Name != bucketName {
		fail("Bucket.Attrs name = %q, want %q", bucketAttrs.Name, bucketName)
	}

	if !bucketListed(ctx, client, project, bucketName) {
		fail("Buckets iterator did not include %q", bucketName)
	}

	object := bucket.Object(objectName)
	writer := object.NewWriter(ctx)
	writer.ContentType = "text/plain"
	writer.Metadata = map[string]string{"source": "gcs-sdk-e2e"}
	writer.ChunkSize = 0
	if _, err := io.WriteString(writer, objectBody); err != nil {
		fail("Object.NewWriter Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		fail("Object.NewWriter Close: %v", err)
	}

	objectAttrs, err := object.Attrs(ctx)
	if err != nil {
		fail("Object.Attrs: %v", err)
	}
	if objectAttrs.Name != objectName || objectAttrs.Bucket != bucketName {
		fail("Object.Attrs identity = %s/%s, want %s/%s", objectAttrs.Bucket, objectAttrs.Name, bucketName, objectName)
	}
	if objectAttrs.ContentType != "text/plain" || objectAttrs.Metadata["source"] != "gcs-sdk-e2e" {
		fail("Object.Attrs metadata/content type mismatch")
	}
	if objectAttrs.Generation == 0 || objectAttrs.Metageneration == 0 {
		fail("Object.Attrs missing generation metadata")
	}

	reader, err := object.NewReader(ctx)
	if err != nil {
		fail("Object.NewReader: %v", err)
	}
	body, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		fail("Object.NewReader ReadAll: %v", err)
	}
	if closeErr != nil {
		fail("Object.NewReader Close: %v", closeErr)
	}
	if string(body) != objectBody {
		fail("Object.NewReader body mismatch")
	}

	if !objectListed(ctx, bucket, objectName) {
		fail("Objects iterator did not include %q", objectName)
	}

	if err := object.Delete(ctx); err != nil {
		fail("Object.Delete: %v", err)
	}
	if _, err := object.Attrs(ctx); err == nil {
		fail("Object.Attrs succeeded after Object.Delete")
	}

	if err := bucket.Delete(ctx); err != nil {
		fail("Bucket.Delete: %v", err)
	}

	fmt.Println("sdk smoke passed")
}

func bucketListed(ctx context.Context, client *storage.Client, project string, bucketName string) bool {
	iter := client.Buckets(ctx, project)
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			return false
		}
		if err != nil {
			fail("Buckets iterator: %v", err)
		}
		if attrs.Name == bucketName {
			return true
		}
	}
}

func objectListed(ctx context.Context, bucket *storage.BucketHandle, objectName string) bool {
	iter := bucket.Objects(ctx, &storage.Query{Prefix: "docs/"})
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			return false
		}
		if err != nil {
			fail("Objects iterator: %v", err)
		}
		if attrs.Name == objectName {
			return true
		}
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
    DEVCLOUD_GCS_ENDPOINT="${GCS_SDK_ENDPOINT}" \
      DEVCLOUD_GCS_PROJECT="${PROJECT}" \
      DEVCLOUD_GCS_BUCKET="${BUCKET}" \
      go mod tidy
    DEVCLOUD_GCS_ENDPOINT="${GCS_SDK_ENDPOINT}" \
      DEVCLOUD_GCS_PROJECT="${PROJECT}" \
      DEVCLOUD_GCS_BUCKET="${BUCKET}" \
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

log "starting devcloud up on gcs=${GCS_PORT}, dashboard=${DASHBOARD_PORT}, smtp=${SMTP_PORT}, s3=${S3_PORT}"
(
  cd "${WORKSPACE}"
  "${BIN}" up
) > "${TMP_DIR}/devcloud-up.log" 2>&1 &
DEV_PID="$!"

log "waiting for GCS endpoint"
wait_for_http "${GCS_ENDPOINT}/storage/v1/b?project=${PROJECT}"
log "waiting for dashboard"
wait_for_http "${DASHBOARD_ENDPOINT}/"

log "running Google Cloud Storage SDK journey"
run_sdk_smoke

log "passed"
