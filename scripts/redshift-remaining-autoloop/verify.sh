#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redshift-remaining-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redshift-remaining-verify.err"

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
  bash -n scripts/redshift-remaining-autoloop/bootstrap.sh &&
    bash -n scripts/redshift-remaining-autoloop/run-loop.sh &&
    bash -n scripts/redshift-remaining-autoloop/recover.sh &&
    bash -n scripts/redshift-remaining-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'default.*postgres|full-remaining|NEXUS_LOOP_STATUS: READY' scripts/redshift-remaining-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY|SUCCESS|FAILED|atomic_write_state' scripts/redshift-remaining-autoloop/run-loop.sh
}

assert_existing_gates() {
  VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh &&
    VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/verify.sh
}

assert_default_backend_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'backend_kind: "postgres"|backend_mode: "managed"' orchestrator/src/config.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'kind: memory|backend_kind.*memory|memory fallback' orchestrator docs scripts &&
    cargo test --workspace
}

assert_docs_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'system `initdb`|system PostgreSQL|managed PostgreSQL.*system|MIG-009|default backend|Open Questions' docs/design-redshift-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'MIG-001|MIG-002|MIG-003|MIG-004|MIG-005|MIG-006|MIG-007|MIG-008|MIG-009' docs/design-redshift-compat.md
}

run_foundation_checks() {
  run_check "Redshift remaining loop contract exists" assert_loop_contract
  run_check "existing PostgreSQL and managed gates remain green" assert_existing_gates
}

run_default_backend_checks() {
  run_foundation_checks
  run_check "Redshift default backend contract passes" assert_default_backend_contract
}

run_docs_checks() {
  run_default_backend_checks
  run_check "Redshift remaining docs contract passes" assert_docs_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  default-backend)
    run_default_backend_checks
    ;;
  docs)
    run_docs_checks
    ;;
  full-remaining)
    run_docs_checks
    run_check "Redshift full gate passes" env VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh
    run_check "repository tests pass" cargo test --workspace
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Redshift remaining verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
