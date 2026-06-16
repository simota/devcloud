#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${LOOP_DIR}/../.." && pwd)"
GOAL_FILE="${LOOP_DIR}/goal.md"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
STATE_FILE="${LOOP_DIR}/state.env"
RUNNER_LOG="${LOOP_DIR}/runner.log"
JSON_LOG="${LOOP_DIR}/runner.jsonl"
LOCK_FILE="${LOOP_DIR}/.run-loop.lock"
CIRCUIT_FILE="${LOOP_DIR}/.circuit-state"

MAX_ITERATIONS="${MAX_ITERATIONS:-30}"
ITER_TIMEOUT="${ITER_TIMEOUT:-1200}"
LOOP_TIMEOUT="${LOOP_TIMEOUT:-0}"
CIRCUIT_THRESHOLD="${CIRCUIT_THRESHOLD:-3}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_ARGS="${CODEX_ARGS:---full-auto}"
AUTOCOMMIT="${AUTOCOMMIT:-false}"
STRUCTURED_LOG="${STRUCTURED_LOG:-true}"
MAX_LOG_SIZE="${MAX_LOG_SIZE:-5242880}"
REQUESTED_VERIFY_STAGE="${VERIFY_STAGE-}"
REQUESTED_DONE_VERIFY_STAGE="${DONE_VERIFY_STAGE-}"
VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
DONE_VERIFY_STAGE="${DONE_VERIFY_STAGE:-full}"

log() {
  local message="$1"
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "${message}" | tee -a "${RUNNER_LOG}"
}

emit_json() {
  [[ "${STRUCTURED_LOG}" == "true" ]] || return 0
  local event="$1"
  local status="$2"
  printf '{"timestamp":"%s","event":"%s","status":"%s","iteration":%s}\n' \
    "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "${event}" "${status}" "${ITERATION:-0}" >> "${JSON_LOG}"
}

portable_timeout() {
  local secs="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "${secs}" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "${secs}" "$@"
  else
    perl -e '
      use POSIX ":sys_wait_h";
      my $timeout = shift @ARGV;
      my $pid = fork // die "fork: $!";
      if ($pid == 0) { exec @ARGV; die "exec: $!" }
      local $SIG{ALRM} = sub { kill "TERM", $pid; waitpid($pid, 0); exit 124 };
      alarm $timeout;
      waitpid($pid, 0);
      alarm 0;
      exit($? >> 8);
    ' "${secs}" "$@"
  fi
}

rotate_logs() {
  if [[ -f "${RUNNER_LOG}" ]] && [[ "$(wc -c < "${RUNNER_LOG}")" -gt "${MAX_LOG_SIZE}" ]]; then
    mv "${RUNNER_LOG}" "${RUNNER_LOG}.prev"
  fi
  if [[ -f "${JSON_LOG}" ]] && [[ "$(wc -c < "${JSON_LOG}")" -gt "${MAX_LOG_SIZE}" ]]; then
    mv "${JSON_LOG}" "${JSON_LOG}.prev"
  fi
}

atomic_write_state() {
  local next_iteration="$1"
  local status="$2"
  local tmp
  tmp="$(mktemp "${STATE_FILE}.XXXXXX")"
  cat > "${tmp}" <<EOF
CONTRACT_VERSION=1.1.0
NEXT_ITERATION=${next_iteration}
LAST_STATUS=${status}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TOTAL_TOKENS=0
TOTAL_API_CALLS=0
ESTIMATED_COST_USD=0
ITER_TOKENS=0
ITER_API_CALLS=0
VERIFY_STAGE=${VERIFY_STAGE}
DONE_VERIFY_STAGE=${DONE_VERIFY_STAGE}
CIRCUIT_STATE=${CIRCUIT_STATE:-CLOSED}
CIRCUIT_FAIL_COUNT=${CIRCUIT_FAIL_COUNT:-0}
EOF
  mv "${tmp}" "${STATE_FILE}"
  shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"
}

cleanup() {
  rm -f "${LOCK_FILE}"
}
trap 'cleanup' EXIT

preflight() {
  cd "${ROOT_DIR}"
  rotate_logs

  if ! command -v "${CODEX_BIN}" >/dev/null 2>&1; then
    log "[PREFLIGHT:FAIL] Codex binary not found: ${CODEX_BIN}"
    exit 1
  fi
  if [[ ! -f "${GOAL_FILE}" ]]; then
    log "[PREFLIGHT:FAIL] Missing goal file: ${GOAL_FILE}"
    exit 1
  fi
  if [[ ! -f "docs/design-redis-compat.md" ]]; then
    log "[PREFLIGHT:FAIL] Missing docs/design-redis-compat.md"
    exit 1
  fi

  if [[ -f "${LOCK_FILE}" ]]; then
    local pid
    pid="$(cat "${LOCK_FILE}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      log "[PREFLIGHT:FAIL] Active lock: PID ${pid}"
      exit 1
    fi
    rm -f "${LOCK_FILE}"
  fi
  echo $$ > "${LOCK_FILE}"

  if [[ -d .git/rebase-merge || -d .git/rebase-apply ]]; then
    log "[PREFLIGHT:FAIL] Git rebase in progress"
    exit 1
  fi

  local avail_kb
  avail_kb="$(df -k "${ROOT_DIR}" | awk 'NR==2{print $4}')"
  if [[ "${avail_kb}" -lt 102400 ]]; then
    log "[PREFLIGHT:FAIL] Less than 100MB disk space available"
    exit 1
  fi

  if [[ -f "${STATE_FILE}" && -f "${STATE_FILE}.sha256" ]]; then
    local expected actual
    expected="$(cat "${STATE_FILE}.sha256")"
    actual="$(shasum -a 256 "${STATE_FILE}" | awk '{print $1}')"
    if [[ "${expected}" != "${actual}" ]]; then
      log "[PREFLIGHT:FAIL] state.env checksum mismatch; run scripts/redis-autoloop/recover.sh"
      exit 1
    fi
  fi

  log "[PREFLIGHT] OK"
}

load_state() {
  if [[ -f "${STATE_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${STATE_FILE}"
  fi
  NEXT_ITERATION="${NEXT_ITERATION:-1}"
  LAST_STATUS="${LAST_STATUS:-READY}"
  VERIFY_STAGE="${REQUESTED_VERIFY_STAGE:-${VERIFY_STAGE:-foundation}}"
  DONE_VERIFY_STAGE="${REQUESTED_DONE_VERIFY_STAGE:-${DONE_VERIFY_STAGE:-full}}"
  CIRCUIT_STATE="${CIRCUIT_STATE:-CLOSED}"
  CIRCUIT_FAIL_COUNT="${CIRCUIT_FAIL_COUNT:-0}"
}

save_circuit() {
  local tmp
  tmp="$(mktemp "${CIRCUIT_FILE}.XXXXXX")"
  cat > "${tmp}" <<EOF
CIRCUIT_STATE=${CIRCUIT_STATE}
CIRCUIT_FAIL_COUNT=${CIRCUIT_FAIL_COUNT}
LAST_FAILURE_SIGNATURE=${LAST_FAILURE_SIGNATURE:-}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
  mv "${tmp}" "${CIRCUIT_FILE}"
}

build_prompt() {
  local prompt_file="$1"
  cat > "${prompt_file}" <<'PROMPT'
You are Codex running inside the devcloud repository.

Goal:
- Read scripts/redis-autoloop/goal.md and docs/design-redis-compat.md.
- Implement the next smallest safe slice toward adding a managed-or-external Redis service to devcloud with a /dashboard/redis management UI.
- Preserve existing user changes.
- Do not modify scripts/redis-autoloop/progress.md, state.env, state.env.sha256, runner.log, runner.jsonl, iteration outputs, or done.md; the runner owns loop state.
- Do not re-implement RESP or the Redis protocol; the daemon either manages a `redis-server` child process or proxies to an external Redis URL.
- Use Rust standard library unless a dependency is unavoidable and justified. The dashboard's in-process client may depend on `Redis client crate`.
- Keep generated runtime data under .devcloud/data/redis/.
- Preserve existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, Redshift, and dashboard behavior.
- Prefer tests for each behavior you add.
- Never log passwords, AUTH headers, RESP command argument payloads, or value bodies. Redact in handler logs.

Current repository context:
- docs/design-redis-compat.md defines the Redis compatibility target (managed/external modes, allowlist, dashboard contract).
- orchestrator/config.rs and orchestrator/daemon.rs are the expected config and lifecycle integration points; mirror the Redshift `managed`/`external` shape.
- orchestrator/managed_postgres.rs shows the existing managed child-process pattern (used by Redshift). Copy this shape for `managed_redis.rs`.
- services/dashboard/services.rs and web/dashboard/src/app/services/* show dashboard registration patterns; `RedshiftDashboard.tsx` is the closest 3-pane layout reference (replace clusters→keys, catalog→inspector, query runner→command runner).
- services/dashboard/redshift_handlers.rs shows the in-process service wiring style; never call raw storage from dashboard handlers.
- Recent commits established a JSON contract: every array field must be initialized to []T{} on the compatibility side so it serializes as [] (not null) for the React client.
- The current fast gate is: VERIFY_STAGE=foundation bash scripts/redis-autoloop/verify.sh
- The final acceptance gate is: VERIFY_STAGE=full bash scripts/redis-autoloop/verify.sh

Recommended implementation order:
1. Config and daemon wiring for services.redis, auth.redis, server.redisPort. Mirror the Redshift managed/external switch.
2. services/redis/{server,config}.rs with Run(ctx) that either spawns redis-server (managed) or validates external connectivity (external) and exposes a typed in-process client.
3. orchestrator/managed_redis.rs that owns the redis-server child process, propagates --port/--dir/--save/--requirepass, and shuts down with SIGTERM→timeout→SIGKILL.
4. Dashboard backend: services/dashboard/redis_handlers.rs with /api/redis/status, /api/redis/keys (SCAN paging), /api/redis/keys/{key}, /api/redis/command (allowlist), DELETE /api/redis/keys/{key}, POST /api/redis/keys/{key}/expire, DELETE /api/redis/keys (FLUSHDB only).
5. Dashboard service registry entry in services/dashboard/services.rs and shared SetRedis on *Server.
6. Frontend web/dashboard/src/app/services/redis/{RedisDashboard.tsx,api.ts,types.ts} with 3-pane layout (Keys / Inspector / Command runner).
7. Tests: services/redis/*_test.rs (config validation, allowlist), services/dashboard/redis_test.rs (every endpoint returns [] not null on empty), orchestrator/managed_redis_test.rs (graceful shutdown, binary-missing error message).
8. scripts/redis-e2e.sh that boots devcloud up, exercises SET/GET/EXPIRE through the daemon port, hits /api/redis/*, then tears down.
9. Documentation: README service section, config sample line, dashboard screenshot reference.

Required workflow for this iteration:
1. Inspect the current implementation.
2. Choose the next smallest coherent slice from the order above.
3. Edit files directly.
4. Run focused tests and then cargo test --workspace when possible.
5. End your final response with the exact footer:

NEXUS_LOOP_STATUS: CONTINUE or DONE
NEXUS_LOOP_SUMMARY: <single-line summary>

Use DONE only when VERIFY_STAGE=full bash scripts/redis-autoloop/verify.sh passes completely.
PROMPT
}

parse_status() {
  local output_file="$1"
  if grep -q 'NEXUS_LOOP_STATUS: DONE' "${output_file}"; then
    echo "DONE"
  elif grep -q 'NEXUS_LOOP_STATUS: CONTINUE' "${output_file}"; then
    echo "CONTINUE"
  else
    echo "CONTINUE"
  fi
}

append_progress() {
  local iteration="$1"
  local status="$2"
  local verify_result="$3"
  local output_file="$4"
  local verify_stage="$5"
  {
    echo ""
    echo "## Iteration ${iteration} - $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "- Codex status: ${status}"
    echo "- Verification: ${verify_result} (${verify_stage})"
    echo "- Changed files:"
    git status --short | sed 's/^/  - /' || true
    echo "- Output reference: ${output_file}"
    echo "- Decision: ${status}"
    echo ""
    echo "NEXUS_LOOP_STATUS: ${status}"
    echo "NEXUS_LOOP_SUMMARY: Iteration ${iteration} ended with ${status}; ${verify_stage} verification ${verify_result}."
  } >> "${PROGRESS_FILE}"
}

maybe_commit() {
  local iteration="$1"
  local status="$2"
  [[ "${AUTOCOMMIT}" == "true" ]] || return 0
  if git diff --quiet && git diff --cached --quiet && [[ -z "$(git ls-files --others --exclude-standard)" ]]; then
    return 0
  fi
  {
    git diff --name-only
    git diff --cached --name-only
    git ls-files --others --exclude-standard
  } | sort -u | git add --pathspec-from-file=-
  git commit -m "feat(redis): advance implementation loop iteration ${iteration}" -m "Loop status: ${status}"
}

main() {
  preflight
  load_state

  local started_at
  started_at="$(date +%s)"
  local max_end=0
  if [[ "${LOOP_TIMEOUT}" != "0" ]]; then
    max_end=$((started_at + LOOP_TIMEOUT))
  fi

  local end_iteration=$((NEXT_ITERATION + MAX_ITERATIONS - 1))
  for ((ITERATION=NEXT_ITERATION; ITERATION<=end_iteration; ITERATION++)); do
    if [[ "${max_end}" != "0" && "$(date +%s)" -ge "${max_end}" ]]; then
      log "[LOOP:TIMEOUT] Loop timeout reached"
      atomic_write_state "${ITERATION}" "CONTINUE"
      exit 1
    fi

    if [[ "${CIRCUIT_STATE}" == "OPEN" ]]; then
      log "[CIRCUIT:OPEN] Blocking execution after repeated failures"
      atomic_write_state "${ITERATION}" "BLOCKED"
      exit 1
    fi

    log "[ITER:${ITERATION}] Starting Codex execution"
    emit_json "iteration_start" "RUNNING"

    prompt_file="$(mktemp "${LOOP_DIR}/prompt-${ITERATION}.XXXXXX")"
    output_file="${LOOP_DIR}/iteration-${ITERATION}.out"
    build_prompt "${prompt_file}"

    read -r -a codex_args <<< "${CODEX_ARGS}"

    set +e
    portable_timeout "${ITER_TIMEOUT}" "${CODEX_BIN}" exec "${codex_args[@]}" "$(cat "${prompt_file}")" > "${output_file}" 2>&1
    exec_code=$?
    set -e
    rm -f "${prompt_file}"

    if [[ "${exec_code}" -ne 0 ]]; then
      CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
      LAST_FAILURE_SIGNATURE="codex_exit_${exec_code}"
      log "[ITER:${ITERATION}] Codex failed with exit ${exec_code}"
      if [[ "${CIRCUIT_FAIL_COUNT}" -ge "${CIRCUIT_THRESHOLD}" ]]; then
        CIRCUIT_STATE="OPEN"
      fi
      save_circuit
      append_progress "${ITERATION}" "CONTINUE" "codex_failed_${exec_code}" "${output_file}" "not_run"
      atomic_write_state "$((ITERATION + 1))" "CONTINUE"
      continue
    fi

    status="$(parse_status "${output_file}")"
    verify_result="not_run"
    active_verify_stage="${VERIFY_STAGE}"
    if [[ "${status}" == "DONE" ]]; then
      active_verify_stage="${DONE_VERIFY_STAGE}"
    fi
    if VERIFY_STAGE="${active_verify_stage}" bash "${LOOP_DIR}/verify.sh" >> "${RUNNER_LOG}" 2>&1; then
      verify_result="passed"
      CIRCUIT_STATE="CLOSED"
      CIRCUIT_FAIL_COUNT=0
    else
      verify_result="failed"
      if [[ "${status}" == "DONE" ]]; then
        status="CONTINUE"
      fi
    fi

    save_circuit
    append_progress "${ITERATION}" "${status}" "${verify_result}" "${output_file}" "${active_verify_stage}"
    maybe_commit "${ITERATION}" "${status}"
    atomic_write_state "$((ITERATION + 1))" "${status}"
    emit_json "iteration_end" "${status}"

    if [[ "${status}" == "DONE" && "${verify_result}" == "passed" ]]; then
      cat > "${LOOP_DIR}/done.md" <<EOF
# Done

- Completed at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
- Final iteration: ${ITERATION}
- Verification: VERIFY_STAGE=${DONE_VERIFY_STAGE} bash scripts/redis-autoloop/verify.sh passed
- Rollback: revert the implementation commits or restore from git history.

NEXUS_LOOP_STATUS: DONE
NEXUS_LOOP_SUMMARY: Redis service loop completed with verification evidence.
EOF
      log "[DONE] Redis service loop completed"
      exit 0
    fi
  done

  log "[MAX_ITER] Reached MAX_ITERATIONS=${MAX_ITERATIONS}"
  exit 1
}

main "$@"
