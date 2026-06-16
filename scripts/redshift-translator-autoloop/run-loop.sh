#!/usr/bin/env bash
# Drive Codex CLI to consume COMPATIBILITY.md one item per iteration.
# Each iteration: pick → codex exec → verify.sh → (commit | rollback).
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
CHECKLIST_FILE="${ROOT_DIR}/services/redshift/translator/COMPATIBILITY.md"
VERIFY_SCRIPT="${LOOP_DIR}/verify.sh"

# --- defaults (override via env) ---
: "${MAX_ITERATIONS:=5}"
: "${ITER_TIMEOUT:=900}"
: "${CIRCUIT_THRESHOLD:=3}"
: "${CODEX_BIN:=codex}"
: "${CODEX_ARGS:=exec --full-auto --skip-git-repo-check}"
: "${AUTOCOMMIT:=true}"
: "${STRUCTURED_LOG:=true}"
: "${DRY_RUN:=false}"

cd "${ROOT_DIR}"

# --- helpers ---
log() {
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$1" | tee -a "${RUNNER_LOG}"
}
emit_json() {
  [[ "${STRUCTURED_LOG}" == "true" ]] || return 0
  printf '{"timestamp":"%s","event":"%s","status":"%s","iteration":%s,"item":%s}\n' \
    "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$1" "$2" "${ITERATION:-0}" \
    "$(printf '%s' "${3:-}" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))' 2>/dev/null || printf '"%s"' "${3:-}")" \
    >> "${JSON_LOG}"
}
atomic_write() {
  local target="$1"
  local tmp="${target}.tmp.$$"
  cat > "${tmp}"
  mv "${tmp}" "${target}"
}
portable_timeout() {
  local secs="$1"; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "${secs}" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "${secs}" "$@"
  else
    "$@"
  fi
}
acquire_lock() {
  if [[ -f "${LOCK_FILE}" ]]; then
    local pid; pid="$(cat "${LOCK_FILE}" 2>/dev/null || echo 0)"
    if kill -0 "${pid}" 2>/dev/null; then
      echo "another run-loop is active (pid=${pid})" >&2
      exit 1
    fi
    rm -f "${LOCK_FILE}"
  fi
  echo "$$" > "${LOCK_FILE}"
}
release_lock() { rm -f "${LOCK_FILE}"; }
trap release_lock EXIT

# --- pre-flight ---
[[ -f "${GOAL_FILE}" ]] || { echo "missing goal.md" >&2; exit 1; }
[[ -f "${STATE_FILE}" ]] || { echo "state.env missing — run bootstrap.sh first" >&2; exit 1; }
[[ -f "${CHECKLIST_FILE}" ]] || { echo "checklist missing" >&2; exit 1; }
[[ -f "${VERIFY_SCRIPT}" ]] || { echo "verify.sh missing" >&2; exit 1; }
command -v "${CODEX_BIN}" >/dev/null || { echo "codex CLI not found" >&2; exit 1; }

acquire_lock
# shellcheck disable=SC1090
source "${STATE_FILE}"
: "${NEXT_ITERATION:=1}"
: "${LAST_STATUS:=INIT}"

# --- pick_next_item: first unchecked R-only / R≠P line ---
pick_next_item() {
  grep -nE '^- \[ \] \*\*(R-only|R≠P)\*\*' "${CHECKLIST_FILE}" | head -1
}

# --- circuit breaker ---
circuit_state() { [[ -f "${CIRCUIT_FILE}" ]] && cat "${CIRCUIT_FILE}" || echo "CLOSED|0"; }
record_failure() {
  IFS='|' read -r state count <<<"$(circuit_state)"
  count=$((count + 1))
  if (( count >= CIRCUIT_THRESHOLD )); then
    printf 'OPEN|%d' "${count}" | atomic_write "${CIRCUIT_FILE}"
  else
    printf '%s|%d' "${state}" "${count}" | atomic_write "${CIRCUIT_FILE}"
  fi
}
reset_circuit() { printf 'CLOSED|0' | atomic_write "${CIRCUIT_FILE}"; }

IFS='|' read -r CIRCUIT_NOW CIRCUIT_COUNT <<<"$(circuit_state)"
if [[ "${CIRCUIT_NOW}" == "OPEN" ]]; then
  log "circuit OPEN (${CIRCUIT_COUNT} consecutive failures) — delete ${CIRCUIT_FILE} to reset"
  echo "NEXUS_LOOP_STATUS: BLOCKED"
  echo "NEXUS_LOOP_SUMMARY: circuit OPEN, ${CIRCUIT_COUNT} consecutive failures"
  exit 1
fi

# --- main loop ---
ITERATION="${NEXT_ITERATION}"
END_ITERATION=$((NEXT_ITERATION + MAX_ITERATIONS - 1))
FINAL_STATUS="${LAST_STATUS}"
FINAL_ITEM=""

while (( ITERATION <= END_ITERATION )); do
  log "=== iteration ${ITERATION} ==="
  emit_json "iter_start" "RUNNING" ""

  raw="$(pick_next_item || true)"
  if [[ -z "${raw}" ]]; then
    log "no unchecked R-only / R≠P items remaining"
    FINAL_STATUS="DONE"
    break
  fi
  ITEM_LINE="${raw%%:*}"
  ITEM_TEXT="${raw#*:}"
  FINAL_ITEM="${ITEM_TEXT}"
  log "target: line=${ITEM_LINE} text=${ITEM_TEXT}"

  pre_sha="$(git rev-parse HEAD)"
  pre_dirty="$(git status --porcelain | wc -l | tr -d ' ')"
  if (( pre_dirty > 0 )); then
    log "WARNING: working tree dirty before iteration (${pre_dirty} entries); aborting to preserve baseline"
    FINAL_STATUS="BLOCKED"
    break
  fi

  prompt=$(cat <<PROMPT
You are implementing one item from the Redshift→PostgreSQL translator compatibility checklist for the devcloud project.

Repository: ${ROOT_DIR}
Checklist: services/redshift/translator/COMPATIBILITY.md
Target line ${ITEM_LINE}: ${ITEM_TEXT}

Tasks:
1. Read services/redshift/translator/translator.rs and translator_test.rs to understand existing patterns (rewriteRedshiftFunctions, translateCreateTable, rewriteParenFunction, postgresDatePart, etc.).
2. Implement a translation rule for the target construct so that the Redshift form is rewritten to a PostgreSQL-equivalent form.
3. Add at least one unit test in translator_test.rs in the existing table-driven style covering the new rule.
4. Update COMPATIBILITY.md: on the target line only, change "- [ ]" to "- [x]". Do not modify any other line.
5. Verify locally before finishing:
   - cargo test --workspace   (must PASS)
   - cargo test --workspace              (must PASS, regression check)
   - cargo build -p devcloud-orchestrator  (must succeed)

Constraints (strict):
- compatibility 1.22 standard library only; no new external dependencies.
- rustfmt clean; match existing import grouping.
- Modify ONLY:
  * services/redshift/translator/translator.rs
  * services/redshift/translator/translator_test.rs
  * services/redshift/translator/COMPATIBILITY.md
- Do not create new files.
- Do not run git commit or git push — the runner handles version control.
- Do not modify goal.md, verify.sh, run-loop.sh, or any file outside the three listed above.
- If the chosen construct turns out to require a new abstraction layer (parser rewrite, new package, schema changes), STOP after analysis, leave a one-line // TODO(translator): <reason> at the relevant existing site in translator.rs but do NOT flip the checkbox; the runner will rollback non-flipping iterations.

When finished, print a short summary:
  - Target construct
  - Translator function extended (or noted unsupported with reason)
  - Test name(s) added
  - Whether the checklist line was flipped (yes/no)
PROMPT
)

  if [[ "${DRY_RUN}" == "true" ]]; then
    log "[DRY_RUN] would invoke: ${CODEX_BIN} ${CODEX_ARGS}"
    log "[DRY_RUN] prompt length=${#prompt} bytes"
    exec_rc=0
  else
    set +e
    portable_timeout "${ITER_TIMEOUT}" \
      "${CODEX_BIN}" ${CODEX_ARGS} "${prompt}" \
      >> "${RUNNER_LOG}" 2>&1
    exec_rc=$?
    set -e
    log "codex exit_rc=${exec_rc}"
  fi

  if (( exec_rc != 0 )); then
    log "codex failed; rolling back to ${pre_sha}"
    git reset --hard "${pre_sha}" >/dev/null 2>&1 || true
    emit_json "iter_done" "EXEC_FAIL" "${ITEM_TEXT}"
    printf '| %d | EXEC_FAIL | %s |\n' "${ITERATION}" "${ITEM_TEXT}" >> "${PROGRESS_FILE}"
    record_failure
    FINAL_STATUS="EXEC_FAIL"
    ITERATION=$((ITERATION + 1))
    {
      printf 'NEXT_ITERATION=%d\n' "${ITERATION}"
      printf 'LAST_STATUS=%s\n' "${FINAL_STATUS}"
      printf 'LAST_ITEM=%q\n' "${ITEM_TEXT}"
    } | atomic_write "${STATE_FILE}"
    IFS='|' read -r CIRCUIT_NOW _ <<<"$(circuit_state)"
    [[ "${CIRCUIT_NOW}" == "OPEN" ]] && { log "circuit tripped — aborting loop"; FINAL_STATUS="CIRCUIT_OPEN"; break; }
    continue
  fi

  # verify
  if PRE_ITEM_LINE="${ITEM_LINE}" bash "${VERIFY_SCRIPT}" >> "${RUNNER_LOG}" 2>&1; then
    if [[ "${AUTOCOMMIT}" == "true" ]] && [[ "${DRY_RUN}" != "true" ]]; then
      git add services/redshift/translator/translator.rs \
              services/redshift/translator/translator_test.rs \
              services/redshift/translator/COMPATIBILITY.md
      if git diff --cached --quiet; then
        log "no changes staged after verify pass (unexpected); marking NO_CHANGE"
        FINAL_STATUS="NO_CHANGE"
      else
        construct="$(printf '%s' "${ITEM_TEXT}" | sed -E 's/^- \[ \] \*\*[^*]+\*\* `?([^`]+)`?.*$/\1/' | head -c 80)"
        git commit -m "feat(redshift/translator): translate ${construct}" >/dev/null
        FINAL_STATUS="SUCCESS"
        log "committed iteration ${ITERATION}: ${construct}"
      fi
    else
      FINAL_STATUS="SUCCESS"
      log "verify PASS (autocommit=${AUTOCOMMIT}, dry_run=${DRY_RUN})"
    fi
    reset_circuit
    emit_json "iter_done" "${FINAL_STATUS}" "${ITEM_TEXT}"
    printf '| %d | %s | %s |\n' "${ITERATION}" "${FINAL_STATUS}" "${ITEM_TEXT}" >> "${PROGRESS_FILE}"
  else
    log "verify FAIL; rolling back to ${pre_sha}"
    git reset --hard "${pre_sha}" >/dev/null 2>&1 || true
    emit_json "iter_done" "VERIFY_FAIL" "${ITEM_TEXT}"
    printf '| %d | VERIFY_FAIL | %s |\n' "${ITERATION}" "${ITEM_TEXT}" >> "${PROGRESS_FILE}"
    record_failure
    FINAL_STATUS="VERIFY_FAIL"
  fi

  ITERATION=$((ITERATION + 1))
  {
    printf 'NEXT_ITERATION=%d\n' "${ITERATION}"
    printf 'LAST_STATUS=%s\n' "${FINAL_STATUS}"
    printf 'LAST_ITEM=%q\n' "${ITEM_TEXT}"
  } | atomic_write "${STATE_FILE}"
  IFS='|' read -r CIRCUIT_NOW _ <<<"$(circuit_state)"
  [[ "${CIRCUIT_NOW}" == "OPEN" ]] && { log "circuit tripped — aborting loop"; FINAL_STATUS="CIRCUIT_OPEN"; break; }
done

remaining="$(grep -cE '^- \[ \] \*\*(R-only|R≠P)\*\*' "${CHECKLIST_FILE}" || true)"
log "loop end: final_status=${FINAL_STATUS} remaining_items=${remaining}"

if [[ "${FINAL_STATUS}" == "DONE" ]]; then
  echo "NEXUS_LOOP_STATUS: DONE"
elif [[ "${FINAL_STATUS}" == "SUCCESS" ]] && (( remaining > 0 )); then
  echo "NEXUS_LOOP_STATUS: CONTINUE"
elif [[ "${FINAL_STATUS}" == "CIRCUIT_OPEN" || "${FINAL_STATUS}" == "BLOCKED" ]]; then
  echo "NEXUS_LOOP_STATUS: BLOCKED"
else
  echo "NEXUS_LOOP_STATUS: CONTINUE"
fi
echo "NEXUS_LOOP_SUMMARY: iter=$((ITERATION - 1)) status=${FINAL_STATUS} remaining=${remaining} last_item=\"${FINAL_ITEM}\""
