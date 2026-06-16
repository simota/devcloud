#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redshift-advanced-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redshift-advanced-verify.err"

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
  bash -n scripts/redshift-advanced-compat-autoloop/bootstrap.sh &&
    bash -n scripts/redshift-advanced-compat-autoloop/run-loop.sh &&
    bash -n scripts/redshift-advanced-compat-autoloop/recover.sh &&
    bash -n scripts/redshift-advanced-compat-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'extended query protocol|serverless|snapshots|full-advanced|NEXUS_LOOP_STATUS: READY' scripts/redshift-advanced-compat-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/redshift-advanced-compat-autoloop/run-loop.sh
}

assert_existing_redshift_gates() {
  VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh &&
    VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh &&
    VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/verify.sh &&
    VERIFY_STAGE=full-remaining bash scripts/redshift-remaining-autoloop/verify.sh &&
    VERIFY_STAGE=full-query-runner-ui bash scripts/redshift-query-runner-ui-autoloop/verify.sh
}

assert_extended_protocol_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Parse|Bind|Describe|Execute|Sync|Close|prepared statement|extended query' services/redshift &&
    cargo test --workspace
}

assert_sql_advanced_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'CTAS|CREATE TABLE AS|CREATE VIEW|MATERIALIZED VIEW|UPDATE|DELETE|MERGE' services/redshift orchestrator docs &&
    cargo test --workspace
}

assert_serverless_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'redshift-serverless|Workgroup|Namespace|GetCredentials|ListWorkgroups|ListNamespaces' services/redshift services/dashboard scripts docs &&
    cargo test --workspace
}

assert_snapshots_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Snapshot|CreateClusterSnapshot|DescribeClusterSnapshots|DeleteClusterSnapshot|RestoreFromClusterSnapshot|snapshot metadata' services/redshift services/dashboard docs &&
    cargo test --workspace
}

assert_introspection_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'wlm|workload|stl_|stv_|svv_|pg_catalog|information_schema|JDBC|ODBC|BI' services/redshift services/dashboard docs &&
    cargo test --workspace
}

assert_procedures_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'PROCEDURE|CREATE PROCEDURE|UDF|FUNCTION|stored procedure|unsupported SQLSTATE' services/redshift docs &&
    cargo test --workspace
}

assert_dashboard_e2e_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'serverless|snapshot|workload|advanced|extended' scripts/redshift-e2e.sh services/dashboard web/dashboard/src/app/services/redshift &&
    VERIFY_STAGE=full-query-runner-ui bash scripts/redshift-query-runner-ui-autoloop/verify.sh
}

run_foundation_checks() {
  run_check "Redshift advanced loop contract exists" assert_loop_contract
  run_check "existing Redshift completed gates remain green" assert_existing_redshift_gates
}

run_extended_protocol_checks() {
  run_foundation_checks
  run_check "Redshift extended query protocol contract passes" assert_extended_protocol_contract
}

run_sql_advanced_checks() {
  run_extended_protocol_checks
  run_check "Redshift advanced SQL contract passes" assert_sql_advanced_contract
}

run_serverless_checks() {
  run_sql_advanced_checks
  run_check "Redshift Serverless metadata contract passes" assert_serverless_contract
}

run_snapshots_checks() {
  run_serverless_checks
  run_check "Redshift snapshot metadata contract passes" assert_snapshots_contract
}

run_introspection_checks() {
  run_snapshots_checks
  run_check "Redshift introspection contract passes" assert_introspection_contract
}

run_procedures_checks() {
  run_introspection_checks
  run_check "Redshift procedure/UDF metadata contract passes" assert_procedures_contract
}

run_dashboard_e2e_checks() {
  run_procedures_checks
  run_check "Redshift advanced dashboard/E2E contract passes" assert_dashboard_e2e_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  extended-protocol)
    run_extended_protocol_checks
    ;;
  sql-advanced)
    run_sql_advanced_checks
    ;;
  serverless)
    run_serverless_checks
    ;;
  snapshots)
    run_snapshots_checks
    ;;
  introspection)
    run_introspection_checks
    ;;
  procedures)
    run_procedures_checks
    ;;
  dashboard-e2e)
    run_dashboard_e2e_checks
    ;;
  full-advanced)
    run_dashboard_e2e_checks
    run_check "repository tests pass" cargo test --workspace
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Redshift advanced compatibility verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
