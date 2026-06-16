#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redshift-postgres-backend-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redshift-postgres-backend-verify.err"

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
  bash -n scripts/redshift-postgres-backend-autoloop/bootstrap.sh &&
    bash -n scripts/redshift-postgres-backend-autoloop/run-loop.sh &&
    bash -n scripts/redshift-postgres-backend-autoloop/recover.sh &&
    bash -n scripts/redshift-postgres-backend-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'PostgreSQL backend|SQLBackend|RedshiftTranslator|full-postgres|NEXUS_LOOP_STATUS: READY' scripts/redshift-postgres-backend-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY' scripts/redshift-postgres-backend-autoloop/run-loop.sh
}

assert_design_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'PostgreSQL backend|SQLBackend|RedshiftTranslator|Function Rewrite|PostgreSQL Backend Migration Plan|MIG-001|COPY.*STDIN|UNLOAD.*PostgreSQL' docs/design-redshift-compat.md
}

assert_redshift_mvp_still_passes() {
  VERIFY_STAGE=foundation bash scripts/redshift-autoloop/verify.sh &&
    cargo test --workspace
}

assert_backend_interface_contract() {
  test -f services/redshift/src/backend.rs &&
    test -f services/redshift/src/backend_memory.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'trait SqlBackend|fn exec|fn begin|fn catalog|fn close' services/redshift &&
    cargo test --workspace
}

assert_postgres_backend_contract() {
  test -f services/redshift/src/backend_postgres.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'postgres|BEGIN|COMMIT|ROLLBACK' services/redshift/src/backend_postgres.rs orchestrator &&
    env -u RIPGREP_CONFIG_PATH rg -q 'backend.*kind|redshift.*backend|postgres.*dsn|managed|external' orchestrator services/redshift &&
    cargo test --workspace
}

assert_translator_contract() {
  test -f services/redshift/src/translator.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'trait RedshiftTranslator|translate\(|TranslationResult|SideEffect' services/redshift &&
    env -u RIPGREP_CONFIG_PATH rg -q 'DISTSTYLE|DISTKEY|SORTKEY|ENCODE|IDENTITY|GETDATE|SYSDATE|NVL|DECODE|DATEADD|DATEDIFF|LISTAGG|COALESCE|CURRENT_TIMESTAMP|string_agg' services/redshift &&
    cargo test --workspace
}

assert_copy_unload_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'COPY.*STDIN|CopyFrom|copy.*postgres|UNLOAD.*postgres|SideEffect|s3://' services/redshift &&
    cargo test --workspace
}

assert_system_views_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'pg_catalog|information_schema|stl_|stv_|svv_|system.*view' services/redshift &&
    cargo test --workspace
}

assert_dashboard_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'backendMode|postgres|redshift.*backend|Backend' services/dashboard web/dashboard/src/app/services/redshift &&
    cargo test --workspace
}

assert_e2e_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'backend.*postgres|REDSHIFT_BACKEND|full-postgres|postgres' scripts/redshift-e2e.sh scripts/redshift-postgres-backend-autoloop &&
    VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh
}

run_foundation_checks() {
  run_check "PostgreSQL backend loop contract exists" assert_loop_contract
  run_check "Redshift design has PostgreSQL backend contract" assert_design_contract
  run_check "Redshift MVP gate still passes" assert_redshift_mvp_still_passes
}

run_backend_interface_checks() {
  run_foundation_checks
  run_check "SQLBackend interface and memory fallback contract pass" assert_backend_interface_contract
}

run_postgres_backend_checks() {
  run_backend_interface_checks
  run_check "PostgreSQL backend contract passes" assert_postgres_backend_contract
}

run_translator_checks() {
  run_postgres_backend_checks
  run_check "Redshift translator contract passes" assert_translator_contract
}

run_copy_unload_checks() {
  run_translator_checks
  run_check "COPY/UNLOAD PostgreSQL side-effect contract passes" assert_copy_unload_contract
}

run_system_views_checks() {
  run_copy_unload_checks
  run_check "PostgreSQL-backed Redshift system views contract passes" assert_system_views_contract
}

run_dashboard_checks() {
  run_system_views_checks
  run_check "dashboard backend mode contract passes" assert_dashboard_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  backend-interface)
    run_backend_interface_checks
    ;;
  postgres-backend)
    run_postgres_backend_checks
    ;;
  translator)
    run_translator_checks
    ;;
  copy-unload)
    run_copy_unload_checks
    ;;
  system-views)
    run_system_views_checks
    ;;
  dashboard)
    run_dashboard_checks
    ;;
  e2e)
    run_dashboard_checks
    run_check "PostgreSQL backend E2E contract passes" assert_e2e_contract
    ;;
  full-postgres)
    run_dashboard_checks
    run_check "repository tests pass" cargo test --workspace
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Redshift PostgreSQL backend verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
