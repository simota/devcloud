#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
VERIFY_HOST="127.0.0.1"

free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    try:
        sock.bind(("127.0.0.1", 0))
        print(sock.getsockname()[1])
    except PermissionError:
        print(0)
PY
}

REDIS_VERIFY_PORT="${REDIS_VERIFY_PORT:-$(free_port)}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-$(free_port)}"


PASS=0
FAIL=0
SKIP=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-redis-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-redis-verify.err"

cleanup() {
  if [[ -n "${DEV_PID}" ]]; then
    kill "${DEV_PID}" >/dev/null 2>&1 || true
    wait "${DEV_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -40
    FAIL=$((FAIL + 1))
  fi
}

run_skip() {
  local name="$1"
  local reason="$2"
  echo "[SKIP] ${name} (${reason})"
  SKIP=$((SKIP + 1))
}

loopback_bind_available() {
  python3 - <<'PY' >/dev/null 2>&1
import socket

sock = socket.socket()
try:
    sock.bind(("127.0.0.1", 0))
finally:
    sock.close()
PY
}

have_redis_server() {
  command -v redis-server >/dev/null 2>&1
}

have_redis_cli() {
  command -v redis-cli >/dev/null 2>&1
}

wait_for_tcp() {
  local port="$1"
  local deadline=$((SECONDS + 15))
  until python3 - "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

port = int(sys.argv[1])
with socket.create_connection(("127.0.0.1", port), timeout=0.5):
    pass
PY
  do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

# --- foundation assertions: docs + script contract + build + base tests ---

assert_redis_design_contract() {
  test -f docs/design-redis-compat.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'managed|external|allowlist|6379|design-redis' docs/design-redis-compat.md
}

assert_script_contract() {
  local file
  for file in scripts/redis-autoloop/bootstrap.sh \
              scripts/redis-autoloop/run-loop.sh \
              scripts/redis-autoloop/recover.sh \
              scripts/redis-autoloop/verify.sh; do
    bash -n "${file}" || return 1
  done
  grep -q 'NEXUS_LOOP_STATUS' scripts/redis-autoloop/run-loop.sh
}

assert_goal_contract() {
  test -f scripts/redis-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'Acceptance Criteria|NEXUS_LOOP_STATUS|allowlist|managed' scripts/redis-autoloop/goal.md
}

# --- config / service / dashboard shape assertions (run when files exist) ---

assert_redis_config_shape() {
  if [[ ! -f orchestrator/config.rs ]]; then
    return 1
  fi
  env -u RIPGREP_CONFIG_PATH rg -q 'RedisServiceConfig|RedisPort|Redis +RedisAuthConfig' orchestrator/config.rs
}

assert_redis_service_pkg_shape() {
  test -d services/redis &&
    test -f services/redis/server.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'func NewServer' services/redis/server.rs
}

assert_managed_redis_shape() {
  test -f orchestrator/managed_redis.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'redis-server|SIGTERM|--requirepass|--port' orchestrator/managed_redis.rs
}

assert_dashboard_redis_shape() {
  test -f services/dashboard/redis_handlers.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q '/api/redis/status|/api/redis/keys|allowlist|Allowlist|allowedCommands' services/dashboard/redis_handlers.rs &&
    env -u RIPGREP_CONFIG_PATH rg -q 'redis' services/dashboard/services.rs
}

assert_dashboard_redis_ui_shape() {
  test -f web/dashboard/src/app/services/redis/RedisDashboard.tsx &&
    test -f web/dashboard/src/app/services/redis/api.ts &&
    test -f web/dashboard/src/app/services/redis/types.ts
}

# --- check groups ---

run_foundation_checks() {
  run_check "redis design contract" assert_redis_design_contract
  run_check "redis autoloop script contract" assert_script_contract
  run_check "redis goal contract" assert_goal_contract
  run_check "devcloud builds" cargo build --workspace
  run_check "cargo test --workspace" cargo test --workspace
}

run_config_checks() {
  run_check "redis config shape" assert_redis_config_shape
  run_check "orchestrator tests" cargo test --workspace
}

run_service_checks() {
  run_check "redis service package shape" assert_redis_service_pkg_shape
  run_check "managed redis lifecycle shape" assert_managed_redis_shape
  run_check "services/redis tests" cargo test --workspace
}

run_dashboard_static_checks() {
  run_check "dashboard redis handlers shape" assert_dashboard_redis_shape
  run_check "dashboard redis UI files" assert_dashboard_redis_ui_shape
  run_check "services/dashboard tests" cargo test --workspace
}

run_managed_lifecycle_checks() {
  if ! have_redis_server; then
    run_skip "managed redis-server lifecycle" "redis-server binary not in PATH"
    return
  fi
  if ! loopback_bind_available; then
    run_skip "managed redis-server lifecycle" "loopback bind unavailable"
    return
  fi
  run_check "managed redis tests" cargo test --workspace
}

run_e2e_checks() {
  if ! have_redis_server; then
    run_skip "redis e2e" "redis-server binary not in PATH"
    return
  fi
  if ! loopback_bind_available; then
    run_skip "redis e2e" "loopback bind unavailable"
    return
  fi
  if [[ ! -x scripts/redis-e2e.sh ]]; then
    run_skip "redis e2e" "scripts/redis-e2e.sh not present yet"
    return
  fi
  run_check "redis standalone e2e" bash scripts/redis-e2e.sh
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  config)
    run_foundation_checks
    run_config_checks
    ;;
  redis-core)
    run_foundation_checks
    run_config_checks
    run_service_checks
    ;;
  dashboard-static)
    run_foundation_checks
    run_config_checks
    run_service_checks
    run_dashboard_static_checks
    ;;
  hardening)
    run_foundation_checks
    run_config_checks
    run_service_checks
    run_dashboard_static_checks
    run_managed_lifecycle_checks
    ;;
  e2e)
    run_foundation_checks
    run_e2e_checks
    ;;
  full)
    run_foundation_checks
    run_config_checks
    run_service_checks
    run_dashboard_static_checks
    run_managed_lifecycle_checks
    run_e2e_checks
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Redis autoloop verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL} skipped=${SKIP}"

if [[ "${FAIL}" -ne 0 ]]; then
  echo "NEXUS_LOOP_STATUS: CONTINUE"
  echo "NEXUS_LOOP_SUMMARY: Redis verification ${VERIFY_STAGE} failed with ${FAIL} failing check(s)."
  exit 1
fi

if [[ "${VERIFY_STAGE}" == "full" && "${SKIP}" -eq 0 ]]; then
  echo "NEXUS_LOOP_STATUS: DONE"
  echo "NEXUS_LOOP_SUMMARY: Redis verification full passed with all checks executed."
else
  echo "NEXUS_LOOP_STATUS: CONTINUE"
  echo "NEXUS_LOOP_SUMMARY: Redis verification ${VERIFY_STAGE} passed with ${SKIP} skipped check(s)."
fi
