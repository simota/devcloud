#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${LOOP_DIR}/../.." && pwd)"
GOAL_FILE="${LOOP_DIR}/goal.md"
PROGRESS_FILE="${LOOP_DIR}/progress.md"
STATE_FILE="${LOOP_DIR}/state.env"
STATE_SHA_FILE="${STATE_FILE}.sha256"
RUNNER_LOG="${LOOP_DIR}/runner.log"
JSON_LOG="${LOOP_DIR}/runner.jsonl"
LOCK_FILE="${LOOP_DIR}/.run-loop.lock"
CIRCUIT_FILE="${LOOP_DIR}/.circuit-state"

MAX_ITERATIONS="${MAX_ITERATIONS:-10}"
ITER_TIMEOUT="${ITER_TIMEOUT:-900}"
LOOP_TIMEOUT="${LOOP_TIMEOUT:-0}"
CIRCUIT_THRESHOLD="${CIRCUIT_THRESHOLD:-3}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_ARGS="${CODEX_ARGS:-exec --dangerously-bypass-approvals-and-sandbox -C ${ROOT_DIR} -}"
AUTOCOMMIT="${AUTOCOMMIT:-false}"
STRUCTURED_LOG="${STRUCTURED_LOG:-true}"
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

atomic_write_state() {
  local next_iteration="$1"
  local status="$2"
  local tmp
  tmp="$(mktemp "${STATE_FILE}.XXXXXX")"
  cat > "${tmp}" <<EOF
CONTRACT_VERSION=1.0.0
NEXT_ITERATION=${next_iteration}
LAST_STATUS=${status}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VERIFY_STAGE=${VERIFY_STAGE}
DONE_VERIFY_STAGE=${DONE_VERIFY_STAGE}
CIRCUIT_STATE=${CIRCUIT_STATE:-CLOSED}
CIRCUIT_FAIL_COUNT=${CIRCUIT_FAIL_COUNT:-0}
EOF
  mv "${tmp}" "${STATE_FILE}"
  shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_SHA_FILE}"
}

cleanup() {
  rm -f "${LOCK_FILE}"
}
trap 'cleanup' EXIT

preflight() {
  cd "${ROOT_DIR}"
  if ! command -v "${CODEX_BIN}" >/dev/null 2>&1; then
    log "[PREFLIGHT:FAIL] Codex binary not found: ${CODEX_BIN}"
    exit 1
  fi
  if [[ ! -f "${GOAL_FILE}" ]]; then
    log "[PREFLIGHT:FAIL] Missing goal file: ${GOAL_FILE}"
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
  log "[PREFLIGHT] OK"
}

load_state() {
  if [[ -f "${STATE_FILE}" && -f "${STATE_SHA_FILE}" ]]; then
    local expected actual
    expected="$(cat "${STATE_SHA_FILE}")"
    actual="$(shasum -a 256 "${STATE_FILE}" | awk '{print $1}')"
    if [[ "${expected}" != "${actual}" ]]; then
      log "[PREFLIGHT:FAIL] state.env checksum mismatch; run recover.sh"
      exit 1
    fi
  fi
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
  cat > "${CIRCUIT_FILE}" <<EOF
CIRCUIT_STATE=${CIRCUIT_STATE}
CIRCUIT_FAIL_COUNT=${CIRCUIT_FAIL_COUNT}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
}

build_prompt() {
  local prompt_file="$1"
  cat > "${prompt_file}" <<'PROMPT'
You are Codex running inside the devcloud repository.

Goal:
- Read scripts/redshift-pgwire-refactor-autoloop/goal.md.
- Refactor internal/services/redshift/pgwire.go into smaller files without changing behavior.
- Preserve existing user changes and avoid feature work.
- Keep all moved code in package redshift.
- Prefer mechanical movement over renaming or logic cleanup.
- Do not modify scripts/redshift-pgwire-refactor-autoloop/progress.md, state.env, runner.log, runner.jsonl, or done.md; the runner owns loop state.

Suggested order:
1. Move pure codec/types helpers.
2. Move catalog and SQL parsing helpers.
3. Move predicate/assignment/selection helpers.
4. Move COPY/UNLOAD and S3 helpers.
5. Move SQL memory execution and backend dispatch.
6. Move extended query protocol.
7. Split tests only after source movement is stable.

Required checks:
- Run gofmt on changed Go files.
- Run go test ./internal/services/redshift.
- Keep VERIFY_STAGE=foundation bash scripts/redshift-pgwire-refactor-autoloop/verify.sh passing.
- Use VERIFY_STAGE=full bash scripts/redshift-pgwire-refactor-autoloop/verify.sh only before claiming DONE.

End your final response with the exact footer:

NEXUS_LOOP_STATUS: CONTINUE or DONE
NEXUS_LOOP_SUMMARY: <single-line summary>

Use DONE only when VERIFY_STAGE=full bash scripts/redshift-pgwire-refactor-autoloop/verify.sh passes completely.
PROMPT
}

parse_status() {
  local output_file="$1"
  if grep -q 'NEXUS_LOOP_STATUS: DONE' "${output_file}"; then
    echo "DONE"
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
  if [[ -z "$(git status --short -- . ':!scripts/redshift-pgwire-refactor-autoloop/runner.log' ':!scripts/redshift-pgwire-refactor-autoloop/runner.jsonl' ':!scripts/redshift-pgwire-refactor-autoloop/state.env' ':!scripts/redshift-pgwire-refactor-autoloop/state.env.sha256' ':!scripts/redshift-pgwire-refactor-autoloop/progress.md' ':!scripts/redshift-pgwire-refactor-autoloop/done.md')" ]]; then
    return 0
  fi
  git add . \
    ':!scripts/redshift-pgwire-refactor-autoloop/runner.log' \
    ':!scripts/redshift-pgwire-refactor-autoloop/runner.jsonl' \
    ':!scripts/redshift-pgwire-refactor-autoloop/state.env' \
    ':!scripts/redshift-pgwire-refactor-autoloop/state.env.sha256' \
    ':!scripts/redshift-pgwire-refactor-autoloop/progress.md' \
    ':!scripts/redshift-pgwire-refactor-autoloop/done.md'
  git commit -m "refactor(redshift): split pgwire implementation iteration ${iteration}" -m "Loop status: ${status}"
}

main() {
  preflight
  load_state

  local started_at max_end end_iteration
  started_at="$(date +%s)"
  max_end=0
  if [[ "${LOOP_TIMEOUT}" != "0" ]]; then
    max_end=$((started_at + LOOP_TIMEOUT))
  fi

  end_iteration=$((NEXT_ITERATION + MAX_ITERATIONS - 1))
  for ((ITERATION=NEXT_ITERATION; ITERATION<=end_iteration; ITERATION++)); do
    if [[ "${max_end}" != "0" && "$(date +%s)" -ge "${max_end}" ]]; then
      log "[LOOP:TIMEOUT] Loop timeout reached"
      atomic_write_state "${ITERATION}" "CONTINUE"
      exit 1
    fi

    if [[ "${CIRCUIT_STATE}" == "OPEN" ]]; then
      log "[CIRCUIT:OPEN] Stop; run recover.sh or inspect previous failures"
      exit 1
    fi

    local prompt_file output_file status verify_result verify_stage
    prompt_file="${LOOP_DIR}/prompt-${ITERATION}.md"
    output_file="${LOOP_DIR}/iteration-${ITERATION}.out"
    build_prompt "${prompt_file}"

    log "[ITER:${ITERATION}] Starting Codex execution"
    emit_json "iteration_start" "RUNNING"
    if portable_timeout "${ITER_TIMEOUT}" "${CODEX_BIN}" ${CODEX_ARGS} < "${prompt_file}" > "${output_file}" 2>&1; then
      status="$(parse_status "${output_file}")"
      CIRCUIT_FAIL_COUNT=0
      CIRCUIT_STATE=CLOSED
    else
      status="CONTINUE"
      CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
      log "[ITER:${ITERATION}] Codex execution failed"
      if [[ "${CIRCUIT_FAIL_COUNT}" -ge "${CIRCUIT_THRESHOLD}" ]]; then
        CIRCUIT_STATE=OPEN
      fi
    fi

    verify_stage="${VERIFY_STAGE}"
    if [[ "${status}" == "DONE" ]]; then
      verify_stage="${DONE_VERIFY_STAGE}"
    fi

    if VERIFY_STAGE="${verify_stage}" bash "${LOOP_DIR}/verify.sh" > "${LOOP_DIR}/verify-${ITERATION}.out" 2>&1; then
      verify_result="passed"
    else
      verify_result="failed"
      status="CONTINUE"
      CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
      if [[ "${CIRCUIT_FAIL_COUNT}" -ge "${CIRCUIT_THRESHOLD}" ]]; then
        CIRCUIT_STATE=OPEN
      fi
    fi

    append_progress "${ITERATION}" "${status}" "${verify_result}" "${output_file}" "${verify_stage}"
    maybe_commit "${ITERATION}" "${status}"
    save_circuit

    if [[ "${status}" == "DONE" && "${verify_result}" == "passed" ]]; then
      cp "${LOOP_DIR}/verify-${ITERATION}.out" "${LOOP_DIR}/done.md"
      atomic_write_state $((ITERATION + 1)) "DONE"
      log "[DONE] Redshift pgwire refactor completed"
      emit_json "loop_done" "DONE"
      exit 0
    fi

    atomic_write_state $((ITERATION + 1)) "CONTINUE"
  done

  log "[MAX_ITER] Reached MAX_ITERATIONS=${MAX_ITERATIONS}"
  emit_json "max_iterations" "CONTINUE"
  echo "NEXUS_LOOP_STATUS: CONTINUE"
  echo "NEXUS_LOOP_SUMMARY: Reached MAX_ITERATIONS=${MAX_ITERATIONS}; inspect ${PROGRESS_FILE} and continue."
}

main "$@"
