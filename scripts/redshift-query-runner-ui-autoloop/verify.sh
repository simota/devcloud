#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redshift-query-runner-ui-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redshift-query-runner-ui-verify.err"

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    tail -50 "${VERIFY_ERR}" | sed 's/^/  stderr: /'
    FAIL=$((FAIL + 1))
  fi
}

assert_loop_contract() {
  bash -n scripts/redshift-query-runner-ui-autoloop/bootstrap.sh &&
    bash -n scripts/redshift-query-runner-ui-autoloop/run-loop.sh &&
    bash -n scripts/redshift-query-runner-ui-autoloop/recover.sh &&
    bash -n scripts/redshift-query-runner-ui-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'query runner|full-query-runner-ui|NEXUS_LOOP_STATUS: READY' scripts/redshift-query-runner-ui-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/redshift-query-runner-ui-autoloop/run-loop.sh
}

assert_existing_redshift_gates() {
  VERIFY_STAGE=full-remaining bash scripts/redshift-remaining-autoloop/verify.sh
}

assert_frontend_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'runRedshiftQuery' web/dashboard/src/app/services/redshift/api.ts &&
    env -u RIPGREP_CONFIG_PATH rg -q 'RedshiftQueryResult|RedshiftQueryResponse|commandTag|rowCount' web/dashboard/src/app/services/redshift/types.ts &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Query runner|Run query|textarea|queryResult|commandTag|rowCount|setQuerySql' web/dashboard/src/app/services/redshift/RedshiftDashboard.tsx &&
    (cd web/dashboard && npm run typecheck && npm run build)
}

assert_dashboard_api_contract() {
  cargo test --workspace
}

assert_e2e_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q '/api/redshift/query|redshift-query.json|commandTag|rowCount' scripts/redshift-e2e.sh &&
    env REDSHIFT_BACKEND_KIND=memory REDSHIFT_BACKEND_MODE=memory REDSHIFT_BACKEND_MANAGED=false bash scripts/redshift-e2e.sh
}

run_foundation_checks() {
  run_check "Redshift query runner UI loop contract exists" assert_loop_contract
  run_check "existing Redshift remaining gate remains green" assert_existing_redshift_gates
}

run_frontend_checks() {
  run_foundation_checks
  run_check "Redshift query runner frontend contract passes" assert_frontend_contract
}

run_dashboard_api_checks() {
  run_frontend_checks
  run_check "Redshift query runner dashboard API contract passes" assert_dashboard_api_contract
}

run_e2e_checks() {
  run_dashboard_api_checks
  run_check "Redshift query runner E2E contract passes" assert_e2e_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  frontend)
    run_frontend_checks
    ;;
  dashboard-api)
    run_dashboard_api_checks
    ;;
  e2e)
    run_e2e_checks
    ;;
  full-query-runner-ui)
    run_e2e_checks
    run_check "repository tests pass" cargo test --workspace
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Redshift query runner UI verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
