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
LOOP_TIMEOUT="${LOOP_TIMEOUT:-0}"
RETRY_LIMIT="${RETRY_LIMIT:-3}"
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
  if [[ ! -f "docs/design-sqs-compat.md" ]]; then
    log "[PREFLIGHT:FAIL] Missing docs/design-sqs-compat.md"
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
      log "[PREFLIGHT:FAIL] state.env checksum mismatch; run scripts/sqs-autoloop/recover.sh"
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
- Read scripts/sqs-autoloop/goal.md and docs/design-sqs-compat.md.
- Implement the next smallest safe slice toward completing the SQS-compatible server.
- Preserve existing user changes.
- Do not modify scripts/sqs-autoloop/progress.md, state.env, state.env.sha256, runner.log, runner.jsonl, iteration outputs, or done.md; the runner owns loop state.
- Use Go standard library unless a dependency is unavoidable and justified.
- Keep generated runtime data under .devcloud/.
- Preserve existing Mail, S3, GCS, and dashboard behavior.
- Prefer tests for each behavior you add.
- Never log credentials, Authorization headers, signatures, canonical requests, message bodies, message attributes, receipt handles, or queue policy payloads.

Current repository context:
- docs/design-sqs-compat.md defines the SQS compatibility target.
- docs/spec-v0.md defines the Message Core direction shared by SQS and future Pub/Sub.
- docs/design-s3-compat.md and docs/design-dynamodb-compat.md show AWS protocol, SigV4, daemon, and dashboard integration patterns.
- internal/app/config.go and internal/app/daemon.go are the expected config and lifecycle integration points.
- internal/services/s3 and internal/services/gcs show the closest HTTP service lifecycle examples.
- docs/design-dashboard-shell.md defines the shared dashboard shell direction.
- The current fast gate is: VERIFY_STAGE=foundation bash scripts/sqs-autoloop/verify.sh
- The final acceptance gate is: VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh

Recommended implementation order:
1. Config and daemon wiring for services.sqs, auth.sqs, and server.sqsPort.
2. Minimal SQS endpoint with AWS JSON protocol: POST /, Content-Type application/x-amz-json-1.0, and X-Amz-Target AmazonSQS.{Operation}.
3. Query protocol support: Action, Version=2012-11-05, form decoding, XML success/error responses.
4. Queue core: CreateQueue, ListQueues, GetQueueUrl, GetQueueAttributes, SetQueueAttributes, DeleteQueue, and PurgeQueue.
5. Message core: SendMessage, ReceiveMessage, DeleteMessage, ChangeMessageVisibility, receipt leases, MD5OfMessageBody, and system attributes.
6. Scheduler behavior: DelaySeconds, VisibilityTimeout, MessageRetentionPeriod, long polling, and deterministic test hooks.
7. Batch/message attributes: SendMessageBatch, DeleteMessageBatch, ChangeMessageVisibilityBatch, MessageAttributes, and MD5OfMessageAttributes.
8. FIFO and DLQ: .fifo validation, MessageGroupId ordering, deduplication window, RedrivePolicy, and DLQ move behavior.
9. Strict SigV4 mode, safe request logging, dashboard service registry, /dashboard/sqs page, and /api/sqs visibility.

Required workflow for this iteration:
1. Inspect the current implementation.
2. Choose the next smallest coherent phase/slice.
3. Edit files directly.
4. Run focused tests and then go test ./... when possible.
5. End your final response with the exact footer:

NEXUS_LOOP_STATUS: CONTINUE or DONE
NEXUS_LOOP_SUMMARY: <single-line summary>

Use DONE only when VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh passes completely.
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
    echo "## Iteration ${iteration} — $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
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
  git commit -m "feat(sqs): advance implementation loop iteration ${iteration}" -m "Loop status: ${status}"
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
- Verification: VERIFY_STAGE=${DONE_VERIFY_STAGE} bash scripts/sqs-autoloop/verify.sh passed
- Rollback: revert the implementation commits or restore from git history.

NEXUS_LOOP_STATUS: DONE
NEXUS_LOOP_SUMMARY: SQS-compatible server loop completed with verification evidence.
EOF
      log "[DONE] SQS-compatible server loop completed"
      exit 0
    fi
  done

  log "[MAX_ITER] Reached MAX_ITERATIONS=${MAX_ITERATIONS}"
  exit 1
}

main "$@"
