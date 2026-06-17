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
ACTION_SIG_LOG="${LOOP_DIR}/.action-sig.log"
COST_FILE="${LOOP_DIR}/.cost-usd"
GOAL_HASH_FILE="${LOOP_DIR}/.goal.sha256"

MAX_ITERATIONS="${MAX_ITERATIONS:-5}"
ITER_TIMEOUT="${ITER_TIMEOUT:-1200}"
LOOP_TIMEOUT="${LOOP_TIMEOUT:-7200}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"
REASONING_EFFORT="${REASONING_EFFORT:-high}"
SANDBOX_MODE="${SANDBOX_MODE:-workspace-write}"
ALLOW_DIRTY="${ALLOW_DIRTY:-false}"
USD_PER_RUN_CAP="${USD_PER_RUN_CAP:-0}"

log() {
  local message="$1"
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "${message}" | tee -a "${RUNNER_LOG}"
}

emit_json() {
  local event="$1"
  local status="$2"
  local iteration="${3:-0}"
  printf '{"timestamp":"%s","event":"%s","status":"%s","iteration":%s}\n' \
    "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "${event}" "${status}" "${iteration}" >> "${JSON_LOG}"
}

portable_timeout() {
  local secs="$1"
  shift
  if [[ "${secs}" == "0" ]]; then
    "$@"
  elif command -v timeout >/dev/null 2>&1; then
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
CONTRACT_VERSION=1.2.0
NEXT_ITERATION=${next_iteration}
LAST_STATUS=${status}
LAST_UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
MAX_ITERATIONS=${MAX_ITERATIONS}
ITER_TIMEOUT=${ITER_TIMEOUT}
LOOP_TIMEOUT=${LOOP_TIMEOUT}
TOTAL_TOKENS=0
TOTAL_API_CALLS=0
ESTIMATED_COST_USD=$(sum_costs)
EOF
  mv "${tmp}" "${STATE_FILE}"
  shasum -a 256 "${STATE_FILE}" | awk '{print $1}' > "${STATE_FILE}.sha256"
}

sum_costs() {
  if [[ ! -f "${COST_FILE}" ]]; then
    printf '0'
    return 0
  fi
  awk '{sum += $1} END {printf "%.6f", sum + 0}' "${COST_FILE}"
}

check_cost_cap() {
  if [[ "${USD_PER_RUN_CAP}" == "0" ]]; then
    return 1
  fi
  awk -v sum="$(sum_costs)" -v cap="${USD_PER_RUN_CAP}" 'BEGIN { exit !(sum > cap) }'
}

cleanup() {
  rm -f "${LOCK_FILE}"
}
trap cleanup EXIT

preflight() {
  cd "${ROOT_DIR}"
  mkdir -p "${LOOP_DIR}"
  touch "${RUNNER_LOG}" "${JSON_LOG}" "${PROGRESS_FILE}" "${ACTION_SIG_LOG}" "${COST_FILE}"

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
  if [[ "${ALLOW_DIRTY}" != "true" && -n "$(git status --short --untracked-files=no)" ]]; then
    log "[PREFLIGHT:FAIL] Dirty tracked worktree. Re-run with ALLOW_DIRTY=true if intentional."
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
      log "[PREFLIGHT:FAIL] state.env checksum mismatch; run ${LOOP_DIR}/recover.sh --rebuild-state"
      exit 1
    fi
  fi

  shasum -a 256 "${GOAL_FILE}" | awk '{print $1}' > "${GOAL_HASH_FILE}"
  log "[PREFLIGHT] OK"
}

load_state() {
  if [[ -f "${STATE_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${STATE_FILE}"
  fi
  NEXT_ITERATION="${NEXT_ITERATION:-1}"
  LAST_STATUS="${LAST_STATUS:-READY}"
}

assert_goal_unchanged() {
  local expected actual
  expected="$(cat "${GOAL_HASH_FILE}")"
  actual="$(shasum -a 256 "${GOAL_FILE}" | awk '{print $1}')"
  if [[ "${expected}" != "${actual}" ]]; then
    log "[ABORT] goal.md changed during loop"
    atomic_write_state "${NEXT_ITERATION}" "FAILED"
    exit 1
  fi
}

build_prompt() {
  local iteration="$1"
  local prompt_file="$2"
  cat > "${prompt_file}" <<PROMPT
\$nexus kaizen

Run one bounded Kaizen iteration in this repository.

Iteration: ${iteration} of ${MAX_ITERATIONS}
Mode: AUTORUN_FULL

Hard requirements:
- Use the nexus skill for this turn.
- Choose the next smallest safe improvement from local repository context.
- Preserve user work and do not edit scripts/nexus-kaizen-5x/*; the runner owns loop state.
- Keep changes focused and reviewable.
- Run relevant verification.
- Commit completed source/test changes with the repository's Conventional Commit style when verification passes.
- If no safe improvement is available, report that clearly without inventing work.
- Respond in Japanese and include the Nexus execution footer required by the skill.

Loop status footer for the runner:
NEXUS_LOOP_STATUS: CONTINUE
NEXUS_LOOP_SUMMARY: one Kaizen iteration attempted
PROMPT
}

record_progress() {
  local iteration="$1"
  local status="$2"
  local output_file="$3"
  {
    printf '## Iteration %s\n\n' "${iteration}"
    printf '%s\n' "- UTC: \`$(date -u +"%Y-%m-%dT%H:%M:%SZ")\`"
    printf '%s\n' "- Status: \`${status}\`"
    printf '%s\n' "- Output: \`${output_file}\`"
    printf '%s\n' "- HEAD: \`$(git rev-parse --short HEAD 2>/dev/null || echo unknown)\`"
    printf '\n'
  } >> "${PROGRESS_FILE}"
}

main() {
  preflight
  load_state
  atomic_write_state "${NEXT_ITERATION}" "${LAST_STATUS}"

  local loop_start
  loop_start="$(date +%s)"

  while (( NEXT_ITERATION <= MAX_ITERATIONS )); do
    if [[ "${LOOP_TIMEOUT}" != "0" ]]; then
      local now elapsed
      now="$(date +%s)"
      elapsed=$((now - loop_start))
      if (( elapsed >= LOOP_TIMEOUT )); then
        log "[ABORT] LOOP_TIMEOUT reached after ${elapsed}s"
        atomic_write_state "${NEXT_ITERATION}" "FAILED"
        exit 124
      fi
    fi
    if check_cost_cap; then
      log "[ABORT] USD_PER_RUN_CAP exceeded: $(sum_costs) > ${USD_PER_RUN_CAP}"
      atomic_write_state "${NEXT_ITERATION}" "FAILED"
      exit 1
    fi

    assert_goal_unchanged
    local iteration prompt_file output_file status
    iteration="${NEXT_ITERATION}"
    prompt_file="${LOOP_DIR}/iteration-${iteration}.prompt"
    output_file="${LOOP_DIR}/iteration-${iteration}.out"
    build_prompt "${iteration}" "${prompt_file}"

    log "[ITERATION:${iteration}] start"
    emit_json "iteration_start" "RUNNING" "${iteration}"

    if portable_timeout "${ITER_TIMEOUT}" \
      "${CODEX_BIN}" exec \
        -m "${CODEX_MODEL}" \
        -C "${ROOT_DIR}" \
        -s "${SANDBOX_MODE}" \
        -c "model_reasoning_effort=\"${REASONING_EFFORT}\"" \
        --json \
        -o "${output_file}" \
        - < "${prompt_file}" >> "${JSON_LOG}" 2>> "${RUNNER_LOG}"; then
      status="CONTINUE"
      log "[ITERATION:${iteration}] codex exit 0"
    else
      status="FAILED"
      log "[ITERATION:${iteration}] codex failed"
      record_progress "${iteration}" "${status}" "${output_file}"
      emit_json "iteration_done" "${status}" "${iteration}"
      atomic_write_state "${iteration}" "${status}"
      printf 'NEXUS_LOOP_STATUS: CONTINUE\n'
      printf 'NEXUS_LOOP_SUMMARY: iteration %s failed; inspect %s\n' "${iteration}" "${RUNNER_LOG}"
      exit 1
    fi

    printf '%s %s %s\n' "${iteration}" "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" "$(git status --short --untracked-files=no | wc -l | tr -d ' ')" >> "${ACTION_SIG_LOG}"
    record_progress "${iteration}" "${status}" "${output_file}"
    emit_json "iteration_done" "${status}" "${iteration}"

    NEXT_ITERATION=$((NEXT_ITERATION + 1))
    atomic_write_state "${NEXT_ITERATION}" "${status}"
  done

  log "[DONE] completed ${MAX_ITERATIONS} iterations"
  atomic_write_state "${NEXT_ITERATION}" "DONE"
  printf 'NEXUS_LOOP_STATUS: DONE\n'
  printf 'NEXUS_LOOP_SUMMARY: completed %s Codex Nexus Kaizen iterations\n' "${MAX_ITERATIONS}"
}

main "$@"
