#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

for command in initdb postgres psql; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "[redshift-managed-postgres-e2e] skip: missing command ${command}; install PostgreSQL binaries or run external DSN mode" >&2
    exit 0
  fi
done

PREFLIGHT_DIR="$(mktemp -d)"
PREFLIGHT_PW="$(mktemp)"
PREFLIGHT_LOG="$(mktemp)"
cleanup_preflight() {
  rm -rf "${PREFLIGHT_DIR}" "${PREFLIGHT_PW}" "${PREFLIGHT_LOG}"
}
trap cleanup_preflight EXIT

printf 'devcloud-redshift-managed-postgres-preflight\n' > "${PREFLIGHT_PW}"
if ! initdb -D "${PREFLIGHT_DIR}" -U devcloud --auth-host=scram-sha-256 --auth-local=trust --pwfile "${PREFLIGHT_PW}" >"${PREFLIGHT_LOG}" 2>&1; then
  echo "[redshift-managed-postgres-e2e] skip: PostgreSQL initdb is present but cannot initialize a local cluster in this environment" >&2
  sed 's/^/[redshift-managed-postgres-e2e] initdb: /' "${PREFLIGHT_LOG}" | tail -20 >&2
  exit 0
fi
cleanup_preflight
trap - EXIT

PORT_BASE="${REDSHIFT_MANAGED_E2E_PORT_BASE:-$((20000 + ($$ % 10000)))}"

export REDSHIFT_SQL_PORT="${REDSHIFT_SQL_PORT:-$((PORT_BASE + 0))}"
export REDSHIFT_API_PORT="${REDSHIFT_API_PORT:-$((PORT_BASE + 1))}"
export DASHBOARD_PORT="${DASHBOARD_PORT:-$((PORT_BASE + 2))}"
export SMTP_PORT="${SMTP_PORT:-$((PORT_BASE + 3))}"
export S3_PORT="${S3_PORT:-$((PORT_BASE + 4))}"
export GCS_PORT="${GCS_PORT:-$((PORT_BASE + 5))}"
export DYNAMODB_PORT="${DYNAMODB_PORT:-$((PORT_BASE + 6))}"
export BIGQUERY_PORT="${BIGQUERY_PORT:-$((PORT_BASE + 7))}"
export SQS_PORT="${SQS_PORT:-$((PORT_BASE + 8))}"
export PUBSUB_GRPC_PORT="${PUBSUB_GRPC_PORT:-$((PORT_BASE + 9))}"
export PUBSUB_REST_PORT="${PUBSUB_REST_PORT:-$((PORT_BASE + 10))}"

REDSHIFT_BACKEND_KIND=postgres \
REDSHIFT_BACKEND_MODE=managed \
REDSHIFT_BACKEND_MANAGED=true \
bash scripts/redshift-e2e.sh
