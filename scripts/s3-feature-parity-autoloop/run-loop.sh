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

MAX_ITERATIONS="${MAX_ITERATIONS:-20}"
ITER_TIMEOUT="${ITER_TIMEOUT:-1200}"
LOOP_TIMEOUT="${LOOP_TIMEOUT:-0}"
CIRCUIT_THRESHOLD="${CIRCUIT_THRESHOLD:-3}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_ARGS="${CODEX_ARGS:---full-auto}"
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
LAST_FAILURE_SIGNATURE=${LAST_FAILURE_SIGNATURE:-}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
}

build_prompt() {
  local prompt_file="$1"
  cat > "${prompt_file}" <<'PROMPT'
You are Codex running inside the devcloud repository.

Goal:
- Read scripts/s3-feature-parity-autoloop/goal.md.
- Implement the next smallest safe S3 feature parity slice from the 1-10 target list.
- Preserve existing user changes and avoid broad refactors.
- Do not modify scripts/s3-feature-parity-autoloop/progress.md, state.env, runner.log, runner.jsonl, or done.md; the runner owns loop state.
- Keep runtime data under .devcloud/.
- Add focused Go tests and E2E/script coverage when the slice changes public behavior.
- Update README S3 compatibility notes when a feature moves from No/Partial to Partial/Yes.

Implementation order:
1. Versioning.
2. Bucket policy and ACL metadata.
3. Lifecycle expiration.
4. Event notifications.
5. SSE/KMS metadata.
6. Virtual-host style routes.
7. Object Lock.
8. S3 Select.
9. Inventory and analytics.
10. Replication.

Required checks:
- Run focused tests for changed packages.
- Run go test ./... when feasible.
- Keep VERIFY_STAGE=foundation bash scripts/s3-feature-parity-autoloop/verify.sh passing.
- Use VERIFY_STAGE=full bash scripts/s3-feature-parity-autoloop/verify.sh only before claiming DONE.

End your final response with the exact footer:

NEXUS_LOOP_STATUS: CONTINUE or DONE
NEXUS_LOOP_SUMMARY: <single-line summary>

Use DONE only when VERIFY_STAGE=full bash scripts/s3-feature-parity-autoloop/verify.sh passes completely.
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
  if [[ -z "$(git status --short -- . ':!scripts/s3-feature-parity-autoloop/runner.log' ':!scripts/s3-feature-parity-autoloop/runner.jsonl' ':!scripts/s3-feature-parity-autoloop/state.env' ':!scripts/s3-feature-parity-autoloop/state.env.sha256' ':!scripts/s3-feature-parity-autoloop/progress.md')" ]]; then
    return 0
  fi
  git add . \
    ':!scripts/s3-feature-parity-autoloop/runner.log' \
    ':!scripts/s3-feature-parity-autoloop/runner.jsonl' \
    ':!scripts/s3-feature-parity-autoloop/state.env' \
    ':!scripts/s3-feature-parity-autoloop/state.env.sha256' \
    ':!scripts/s3-feature-parity-autoloop/progress.md'
  git commit -m "feat(s3): advance feature parity loop iteration ${iteration}" -m "Loop status: ${status}"
}

main() {
  preflight
  load_state

  local started_at max_end
  started_at="$(date +%s)"
  max_end=0
  if [[ "${LOOP_TIMEOUT}" != "0" ]]; then
    max_end=$((started_at + LOOP_TIMEOUT))
  fi

  local end_iteration
  end_iteration=$((NEXT_ITERATION + MAX_ITERATIONS - 1))
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
    active_verify_stage="${VERIFY_STAGE}"
    if [[ "${status}" == "DONE" ]]; then
      active_verify_stage="${DONE_VERIFY_STAGE}"
    fi
    verify_result="failed"
    if VERIFY_STAGE="${active_verify_stage}" bash "${LOOP_DIR}/verify.sh" >> "${RUNNER_LOG}" 2>&1; then
      verify_result="passed"
      CIRCUIT_STATE="CLOSED"
      CIRCUIT_FAIL_COUNT=0
    else
      CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
      LAST_FAILURE_SIGNATURE="verify_${active_verify_stage}_failed"
      if [[ "${status}" == "DONE" ]]; then
        status="CONTINUE"
      fi
      if [[ "${CIRCUIT_FAIL_COUNT}" -ge "${CIRCUIT_THRESHOLD}" ]]; then
        CIRCUIT_STATE="OPEN"
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
- Verification: VERIFY_STAGE=${DONE_VERIFY_STAGE} bash scripts/s3-feature-parity-autoloop/verify.sh passed
- Rollback: revert the implementation commits or restore from git history.

NEXUS_LOOP_STATUS: DONE
NEXUS_LOOP_SUMMARY: S3 feature parity loop completed with verification evidence.
EOF
      log "[DONE] S3 feature parity loop completed"
      exit 0
    fi
  done

  log "[MAX_ITER] Reached MAX_ITERATIONS=${MAX_ITERATIONS}"
  echo "NEXUS_LOOP_STATUS: CONTINUE"
  echo "NEXUS_LOOP_SUMMARY: Reached MAX_ITERATIONS=${MAX_ITERATIONS}; continue after reviewing progress."
  exit 1
}

main "$@"
