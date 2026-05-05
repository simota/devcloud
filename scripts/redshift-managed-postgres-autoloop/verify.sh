#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redshift-managed-postgres-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redshift-managed-postgres-verify.err"

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -50
    FAIL=$((FAIL + 1))
  fi
}

assert_loop_contract() {
  bash -n scripts/redshift-managed-postgres-autoloop/bootstrap.sh &&
    bash -n scripts/redshift-managed-postgres-autoloop/run-loop.sh &&
    bash -n scripts/redshift-managed-postgres-autoloop/recover.sh &&
    bash -n scripts/redshift-managed-postgres-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'managed PostgreSQL mode|full-managed|NEXUS_LOOP_STATUS: READY' scripts/redshift-managed-postgres-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/redshift-managed-postgres-autoloop/run-loop.sh
}

assert_design_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'managed PostgreSQL|MIG-003|services\\.redshift\\.backend\\.kind|services\\.redshift\\.backend\\.postgres|external DSN' docs/design-redshift-compat.md
}

assert_existing_postgres_backend_gate() {
  VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh
}

assert_managed_config_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'Backend.*Managed|services\\.redshift\\.backend\\.managed|backend\\.managed|managed' internal/app internal/services/redshift &&
    go test ./internal/app -run 'Test.*Redshift.*Backend|Test.*Redshift.*Managed|Test.*Postgres.*Managed' -count=1
}

assert_managed_lifecycle_contract() {
  ! env -u RIPGREP_CONFIG_PATH rg -q 'managed backend is not implemented yet' internal/app internal/services/redshift &&
    env -u RIPGREP_CONFIG_PATH rg -q 'initdb|pg_ctl|postgres.*-D|ManagedPostgres|managed.*postgres|PostgresProcess|Shutdown|Close' internal/app internal/services/redshift &&
    go test ./internal/app ./internal/services/redshift -run 'Test.*Managed.*Postgres|Test.*Postgres.*Managed|Test.*Managed.*Lifecycle|Test.*Managed.*Shutdown|Test.*Redact.*DSN' -count=1
}

assert_managed_e2e_contract() {
  test -x scripts/redshift-managed-postgres-e2e.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'REDSHIFT_BACKEND_KIND=postgres|REDSHIFT_BACKEND_MODE=managed|psql|redshift-e2e' scripts/redshift-managed-postgres-e2e.sh &&
    bash scripts/redshift-managed-postgres-e2e.sh
}

run_foundation_checks() {
  run_check "managed PostgreSQL loop contract exists" assert_loop_contract
  run_check "Redshift design includes managed PostgreSQL contract" assert_design_contract
  run_check "existing PostgreSQL backend migration gate remains green" assert_existing_postgres_backend_gate
}

run_managed_config_checks() {
  run_foundation_checks
  run_check "managed PostgreSQL config contract passes" assert_managed_config_contract
}

run_managed_lifecycle_checks() {
  run_managed_config_checks
  run_check "managed PostgreSQL lifecycle contract passes" assert_managed_lifecycle_contract
}

run_managed_e2e_checks() {
  run_managed_lifecycle_checks
  run_check "managed PostgreSQL E2E contract passes" assert_managed_e2e_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  managed-config)
    run_managed_config_checks
    ;;
  managed-lifecycle)
    run_managed_lifecycle_checks
    ;;
  managed-e2e)
    run_managed_e2e_checks
    ;;
  full-managed)
    run_managed_e2e_checks
    run_check "repository tests pass" go test ./...
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Redshift managed PostgreSQL verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
