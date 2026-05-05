#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-bigquery-dashboard-management-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-bigquery-dashboard-management-verify.err"

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -60
    FAIL=$((FAIL + 1))
  fi
}

assert_loop_contract() {
  bash -n scripts/bigquery-dashboard-management-autoloop/bootstrap.sh &&
    bash -n scripts/bigquery-dashboard-management-autoloop/run-loop.sh &&
    bash -n scripts/bigquery-dashboard-management-autoloop/recover.sh &&
    bash -n scripts/bigquery-dashboard-management-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'query runner|datasets.insert|tables.insert|tabledata.insertAll|job detail|full-management-ui|NEXUS_LOOP_STATUS: READY' scripts/bigquery-dashboard-management-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/bigquery-dashboard-management-autoloop/run-loop.sh
}

assert_existing_bigquery_gate() {
  BIGQUERY_VERIFY_PORT="${BIGQUERY_DASHBOARD_BIGQUERY_VERIFY_PORT:-19050}" \
    GCS_VERIFY_PORT="${BIGQUERY_DASHBOARD_GCS_VERIFY_PORT:-14443}" \
    S3_VERIFY_PORT="${BIGQUERY_DASHBOARD_S3_VERIFY_PORT:-14566}" \
    DASHBOARD_VERIFY_PORT="${BIGQUERY_DASHBOARD_DASHBOARD_VERIFY_PORT:-18025}" \
    SMTP_VERIFY_PORT="${BIGQUERY_DASHBOARD_SMTP_VERIFY_PORT:-11025}" \
    DYNAMODB_VERIFY_PORT="${BIGQUERY_DASHBOARD_DYNAMODB_VERIFY_PORT:-18000}" \
    VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh
}

assert_query_runner_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'runBigQueryQuery|BigQueryQueryRunner|jobs.query|queryResult|queryJob|dryRun|useLegacySql' web/dashboard/src/app/services/bigquery internal/dashboard README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q '/api/bigquery/projects/.*/queries|/queries|query runner|SQL query' web/dashboard/src/app/services/bigquery internal/dashboard README.md docs &&
    go test ./internal/dashboard -run 'TestBigQuery.*Query|TestBigQuery.*Dashboard' -count=1 &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_management_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'createBigQueryDataset|createBigQueryTable|insertBigQueryRows|datasets.insert|tables.insert|tabledata.insertAll' web/dashboard/src/app/services/bigquery internal/dashboard README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Create dataset|Create table|Insert row|raw JSON|schema|fields' web/dashboard/src/app/services/bigquery &&
    env -u RIPGREP_CONFIG_PATH rg -q "method: 'POST'|/datasets|/tables|/insertAll" web/dashboard/src/app/services/bigquery/api.ts &&
    go test ./internal/dashboard -run 'TestBigQuery.*Dataset|TestBigQuery.*Table|TestBigQuery.*Insert|TestBigQuery.*Dashboard' -count=1 &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_jobs_validation_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'job detail|selected job|recent quer|query history|jobReference|jobId|localStorage' web/dashboard/src/app/services/bigquery README.md docs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'validateJSON|JSON validation|validation error|partial insert|insertErrors|InsertErrors' web/dashboard/src/app/services/bigquery README.md docs &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_e2e_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'query runner|Create dataset|Create table|Insert row|job detail|validation|BigQuery dashboard management' README.md docs scripts/bigquery-e2e.sh scripts/bigquery-dashboard-management-autoloop 2>/dev/null &&
    go test ./internal/dashboard ./internal/services/bigquery -count=1
}

run_foundation_checks() {
  run_check "BigQuery dashboard management loop contract exists" assert_loop_contract
  run_check "existing BigQuery compatibility gate remains green" assert_existing_bigquery_gate
}

run_query_runner_checks() {
  run_foundation_checks
  run_check "BigQuery query runner contract passes" assert_query_runner_contract
}

run_management_checks() {
  run_query_runner_checks
  run_check "BigQuery management UI contract passes" assert_management_contract
}

run_jobs_validation_checks() {
  run_management_checks
  run_check "BigQuery jobs and validation contract passes" assert_jobs_validation_contract
}

run_e2e_docs_checks() {
  run_jobs_validation_checks
  run_check "BigQuery dashboard E2E/docs contract passes" assert_e2e_docs_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  query-runner)
    run_query_runner_checks
    ;;
  management)
    run_management_checks
    ;;
  jobs-validation)
    run_jobs_validation_checks
    ;;
  e2e-docs)
    run_e2e_docs_checks
    ;;
  full-management-ui)
    run_e2e_docs_checks
    run_check "repository tests pass" go test ./...
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== BigQuery dashboard management verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
