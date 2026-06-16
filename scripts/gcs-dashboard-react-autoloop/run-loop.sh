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

MAX_ITERATIONS="${MAX_ITERATIONS:-40}"
ITER_TIMEOUT="${ITER_TIMEOUT:-1200}"
CIRCUIT_THRESHOLD="${CIRCUIT_THRESHOLD:-3}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_ARGS="${CODEX_ARGS:-exec --dangerously-bypass-approvals-and-sandbox -C ${ROOT_DIR} -}"
AUTOCOMMIT="${AUTOCOMMIT:-false}"
STRUCTURED_LOG="${STRUCTURED_LOG:-true}"
REQUESTED_VERIFY_STAGE="${VERIFY_STAGE-}"
REQUESTED_DONE_VERIFY_STAGE="${DONE_VERIFY_STAGE-}"
VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
DONE_VERIFY_STAGE="${DONE_VERIFY_STAGE:-full-react-gcs}"

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
  if [[ -f "${STATE_FILE}" && -f "${STATE_FILE}.sha256" ]]; then
    local expected actual
    expected="$(cat "${STATE_FILE}.sha256")"
    actual="$(shasum -a 256 "${STATE_FILE}" | awk '{print $1}')"
    if [[ "${expected}" != "${actual}" ]]; then
      log "[PREFLIGHT:FAIL] state.env checksum mismatch; run scripts/gcs-dashboard-react-autoloop/recover.sh"
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
  VERIFY_STAGE="${REQUESTED_VERIFY_STAGE:-${VERIFY_STAGE:-foundation}}"
  DONE_VERIFY_STAGE="${REQUESTED_DONE_VERIFY_STAGE:-${DONE_VERIFY_STAGE:-full-react-gcs}}"
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
- Read scripts/gcs-dashboard-react-autoloop/goal.md.
- Implement the next smallest safe slice toward moving the GCS dashboard into the shared React dashboard shell.
- Preserve existing user changes and existing GCS API/static dashboard behavior until the React route is verified.
- Do not modify progress.md, state.env, state.env.sha256, runner.log, runner.jsonl, iteration outputs, or done.md; the runner owns loop state.
- Treat tool results as SUCCESS or FAILED.

Current context:
- GCS dashboard API exists in services/dashboard/server.rs under /api/gcs/*.
- Compatibility /gcs static HTML exists in services/dashboard/gcs_static.rs.
- Shared React routes live under web/dashboard/src/app/routes.tsx.
- Existing service UIs live under web/dashboard/src/app/services/{s3,dynamodb,bigquery,sqs,pubsub,redshift,mail}.

Recommended order:
1. Add typed GCS React service module and /gcs route.
2. Render status, buckets, objects, metadata, download links, and upload sessions.
3. Add guarded create/delete bucket, object, and upload-session management flows.
4. Update dashboard tests/E2E and docs.
5. Remove or bypass static /gcs only after React route is covered.

Safety:
- Mutations must go through /api/gcs/*.
- Do not persist object bodies, credentials, Authorization headers, bearer tokens, or full payloads.
- Destructive actions must require explicit confirmation.
- Disabled GCS state must not expose active mutation controls.

Verification:
- Fast gate: VERIFY_STAGE=foundation bash scripts/gcs-dashboard-react-autoloop/verify.sh
- Final gate: VERIFY_STAGE=full-react-gcs bash scripts/gcs-dashboard-react-autoloop/verify.sh

End your final response with the exact footer:

NEXUS_LOOP_STATUS: CONTINUE or DONE
NEXUS_LOOP_SUMMARY: <single-line summary>

Use DONE only when the final full-react-gcs gate passes completely.
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
  local verify_status="$3"
  local output_file="$4"
  {
    echo ""
    echo "## Iteration ${iteration} - $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "- Codex status: ${status}"
    echo "- Verification: ${verify_status}"
    echo "- Changed files:"
    git status --short | sed 's/^/  - /'
    echo "- Output reference: ${output_file}"
    echo "- Decision: ${status}"
    echo ""
    echo "NEXUS_LOOP_STATUS: ${status}"
    echo "NEXUS_LOOP_SUMMARY: Iteration ${iteration} ended with ${status}; verification ${verify_status}."
  } >> "${PROGRESS_FILE}"
}

autocommit_if_requested() {
  local iteration="$1"
  [[ "${AUTOCOMMIT}" == "true" ]] || return 0
  if [[ -z "$(git status --short)" ]]; then
    return 0
  fi
  git add .agents/orbit.md .gitignore services/dashboard web/dashboard README.md docs scripts/gcs-e2e.sh scripts/gcs-dashboard-react-autoloop 2>/dev/null || true
  if git diff --cached --quiet; then
    return 0
  fi
  git commit -m "feat(gcs): advance React dashboard loop iteration ${iteration}"
}

mark_done() {
  local iteration="$1"
  cat > "${LOOP_DIR}/done.md" <<EOF
# Done

- Completed at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
- Final iteration: ${iteration}
- Verification: VERIFY_STAGE=${DONE_VERIFY_STAGE} bash scripts/gcs-dashboard-react-autoloop/verify.sh passed
- Rollback: revert the implementation commits or restore from git history.

NEXUS_LOOP_STATUS: DONE
NEXUS_LOOP_SUMMARY: GCS React dashboard integration loop completed with verification evidence.
EOF
}

run_iteration() {
  ITERATION="$1"
  local prompt_file="${LOOP_DIR}/prompt-${ITERATION}.md"
  local output_file="${LOOP_DIR}/iteration-${ITERATION}.out"
  build_prompt "${prompt_file}"
  log "[ITER:${ITERATION}] Starting Codex execution"
  emit_json "iteration_start" "READY"
  set +e
  portable_timeout "${ITER_TIMEOUT}" "${CODEX_BIN}" ${CODEX_ARGS} < "${prompt_file}" > "${output_file}" 2>&1
  local codex_status=$?
  set -e
  if [[ "${codex_status}" -ne 0 ]]; then
    LAST_FAILURE_SIGNATURE="codex_exit_${codex_status}"
    CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
    if [[ "${CIRCUIT_FAIL_COUNT}" -ge "${CIRCUIT_THRESHOLD}" ]]; then
      CIRCUIT_STATE="OPEN"
      save_circuit
      log "[CIRCUIT:OPEN] Codex failed ${CIRCUIT_FAIL_COUNT} consecutive times"
      append_progress "${ITERATION}" "CONTINUE" "codex failed" "${output_file}"
      atomic_write_state "$((ITERATION + 1))" "CONTINUE"
      exit 1
    fi
    save_circuit
    append_progress "${ITERATION}" "CONTINUE" "codex failed" "${output_file}"
    atomic_write_state "$((ITERATION + 1))" "CONTINUE"
    return 0
  fi

  local status
  status="$(parse_status "${output_file}")"
  local verify_stage="${VERIFY_STAGE}"
  if [[ "${status}" == "DONE" ]]; then
    verify_stage="${DONE_VERIFY_STAGE}"
  fi

  set +e
  VERIFY_STAGE="${verify_stage}" bash scripts/gcs-dashboard-react-autoloop/verify.sh > "${LOOP_DIR}/verify-${ITERATION}.out" 2>&1
  local verify_exit=$?
  set -e

  if [[ "${verify_exit}" -eq 0 ]]; then
    CIRCUIT_FAIL_COUNT=0
    CIRCUIT_STATE="CLOSED"
    save_circuit
    autocommit_if_requested "${ITERATION}"
    append_progress "${ITERATION}" "${status}" "passed (${verify_stage})" "${output_file}"
    atomic_write_state "$((ITERATION + 1))" "${status}"
    emit_json "iteration_complete" "${status}"
    if [[ "${status}" == "DONE" ]]; then
      mark_done "${ITERATION}"
      log "[DONE] GCS React dashboard integration completed"
      exit 0
    fi
  else
    CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
    LAST_FAILURE_SIGNATURE="verify_${verify_stage}_failed"
    save_circuit
    append_progress "${ITERATION}" "CONTINUE" "failed (${verify_stage})" "${output_file}"
    atomic_write_state "$((ITERATION + 1))" "CONTINUE"
    emit_json "iteration_verify_failed" "FAILED"
  fi
}

main() {
  preflight
  load_state
  if [[ "${CIRCUIT_STATE}" == "OPEN" ]]; then
    log "[CIRCUIT:OPEN] Run recover.sh before continuing"
    exit 1
  fi
  if [[ ! -f "${PROGRESS_FILE}" ]]; then
    bash "${LOOP_DIR}/bootstrap.sh" >/dev/null
    load_state
  fi
  local start="${NEXT_ITERATION}"
  local end=$((start + MAX_ITERATIONS - 1))
  for ((i = start; i <= end; i++)); do
    run_iteration "${i}"
  done
  log "[MAX_ITER] Reached MAX_ITERATIONS=${MAX_ITERATIONS}"
}

main "$@"
