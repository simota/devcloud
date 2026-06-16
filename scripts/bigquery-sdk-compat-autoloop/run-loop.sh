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

MAX_ITERATIONS="${MAX_ITERATIONS:-20}"
ITER_TIMEOUT="${ITER_TIMEOUT:-1200}"
CIRCUIT_THRESHOLD="${CIRCUIT_THRESHOLD:-3}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_ARGS="${CODEX_ARGS:-exec --dangerously-bypass-approvals-and-sandbox -C ${ROOT_DIR} -}"
AUTOCOMMIT="${AUTOCOMMIT:-false}"
STRUCTURED_LOG="${STRUCTURED_LOG:-true}"
REQUESTED_VERIFY_STAGE="${VERIFY_STAGE-}"
REQUESTED_DONE_VERIFY_STAGE="${DONE_VERIFY_STAGE-}"
VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
DONE_VERIFY_STAGE="${DONE_VERIFY_STAGE:-full-sdk-compat}"

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
  shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"
}

cleanup() {
  rm -f "${LOCK_FILE}"
}
trap cleanup EXIT

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
      log "[PREFLIGHT:FAIL] state.env checksum mismatch; run scripts/bigquery-sdk-compat-autoloop/recover.sh"
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
  DONE_VERIFY_STAGE="${REQUESTED_DONE_VERIFY_STAGE:-${DONE_VERIFY_STAGE:-full-sdk-compat}}"
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
- Read scripts/bigquery-sdk-compat-autoloop/goal.md.
- Implement the next smallest safe slice toward BigQuery SDK compatibility E2E coverage.
- Preserve existing user changes and existing BigQuery REST/dashboard behavior.
- Do not modify progress.md, state.env, state.env.sha256, runner.log, runner.jsonl, iteration outputs, or done.md; the runner owns loop state.
- Treat tool results as SUCCESS or FAILED.

Current context:
- Existing REST BigQuery E2E is scripts/bigquery-e2e.sh.
- BigQuery service implementation is services/bigquery.
- The new target should prove official Google BigQuery SDK client compatibility via local endpoint override only.

Recommended order:
1. Add scripts/bigquery-sdk-e2e.sh that starts devcloud in a temp workspace with isolated ports and BigQuery enabled.
2. Add a temporary first-party SDK smoke program inside the script using first-party BigQuery SDK.
3. Verify dataset create/list/get, table create/get, row insert/read, query execution, job/result handling where supported, table delete, and dataset delete.
4. Update README or docs with the SDK E2E command and compatibility target.
5. Keep scripts/bigquery-e2e.sh, VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh, and cargo test --workspace green.

Safety:
- Do not call real Google Cloud.
- Do not require gcloud auth, production credentials, or external emulators.
- Do not log credentials, Authorization headers, bearer tokens, or sensitive row/query payloads.
- Keep all temp files isolated and clean up the devcloud process.

Verification:
- Fast gate: VERIFY_STAGE=foundation bash scripts/bigquery-sdk-compat-autoloop/verify.sh
- SDK gate: VERIFY_STAGE=sdk-e2e bash scripts/bigquery-sdk-compat-autoloop/verify.sh
- Final gate: VERIFY_STAGE=full-sdk-compat bash scripts/bigquery-sdk-compat-autoloop/verify.sh

End your final response with the exact footer:

NEXUS_LOOP_STATUS: CONTINUE or DONE
NEXUS_LOOP_SUMMARY: <single-line summary>

Use DONE only when the final full-sdk-compat gate passes completely.
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
  git add README.md docs scripts/bigquery-sdk-e2e.sh scripts/bigquery-sdk-compat-autoloop scripts/bigquery-e2e.sh 2>/dev/null || true
  if git diff --cached --quiet; then
    return 0
  fi
  git commit -m "test(bigquery): advance SDK compatibility loop iteration ${iteration}"
}

run_verify() {
  local stage="$1"
  VERIFY_STAGE="${stage}" bash "${LOOP_DIR}/verify.sh"
}

main() {
  preflight
  load_state
  : > "${JSON_LOG}"
  if [[ ! -f "${PROGRESS_FILE}" ]]; then
    printf '# BigQuery SDK Compatibility Progress\n' > "${PROGRESS_FILE}"
  fi
  ITERATION="${NEXT_ITERATION}"
  while (( ITERATION <= MAX_ITERATIONS )); do
    if [[ "${CIRCUIT_STATE}" == "OPEN" ]]; then
      log "[CIRCUIT] OPEN; stopping loop"
      exit 1
    fi

    log "[ITER:${ITERATION}] Starting Codex execution"
    emit_json "iteration_start" "READY"
    prompt_file="${LOOP_DIR}/prompt-${ITERATION}.md"
    output_file="${LOOP_DIR}/iteration-${ITERATION}.out"
    build_prompt "${prompt_file}"

    set +e
    portable_timeout "${ITER_TIMEOUT}" "${CODEX_BIN}" ${CODEX_ARGS} < "${prompt_file}" > "${output_file}" 2>&1
    codex_status=$?
    set -e

    loop_status="CONTINUE"
    verify_status="FAILED"
    if [[ "${codex_status}" -eq 0 ]]; then
      loop_status="$(parse_status "${output_file}")"
      if run_verify "${VERIFY_STAGE}" > "${LOOP_DIR}/verify-${ITERATION}.out" 2>&1; then
        verify_status="SUCCESS"
        CIRCUIT_FAIL_COUNT=0
      else
        verify_status="FAILED"
        CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
      fi
    else
      log "[ITER:${ITERATION}] Codex failed with exit=${codex_status}"
      CIRCUIT_FAIL_COUNT=$((CIRCUIT_FAIL_COUNT + 1))
    fi

    if (( CIRCUIT_FAIL_COUNT >= CIRCUIT_THRESHOLD )); then
      CIRCUIT_STATE="OPEN"
    fi
    save_circuit
    autocommit_if_requested "${ITERATION}"
    append_progress "${ITERATION}" "${loop_status}" "${verify_status}" "${output_file}"

    if [[ "${loop_status}" == "DONE" ]]; then
      if run_verify "${DONE_VERIFY_STAGE}" > "${LOOP_DIR}/verify-done.out" 2>&1; then
        log "[DONE] BigQuery SDK compatibility E2E completed"
        atomic_write_state "$((ITERATION + 1))" "DONE"
        echo "NEXUS_LOOP_STATUS: DONE"
        echo "NEXUS_LOOP_SUMMARY: BigQuery SDK compatibility E2E completed."
        exit 0
      fi
      log "[DONE-GATE:FAILED] DONE footer present but final verification failed"
    fi

    atomic_write_state "$((ITERATION + 1))" "${loop_status}"
    ITERATION=$((ITERATION + 1))
  done

  log "[MAX_ITER] Reached MAX_ITERATIONS=${MAX_ITERATIONS}"
  echo "NEXUS_LOOP_STATUS: CONTINUE"
  echo "NEXUS_LOOP_SUMMARY: Reached MAX_ITERATIONS=${MAX_ITERATIONS}; continue after reviewing progress."
}

main "$@"
