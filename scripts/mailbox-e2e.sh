#!/usr/bin/env bash
# Black-box acceptance gate for the standalone devcloud-mailbox binary.
# DIFF oracle: mailhog/mailhog:v1.0.1 (docker). See scripts/mailbox-e2e/README.md.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

ORACLE_IMAGE="mailhog/mailhog:v1.0.1"
PROFILE="${E2E_CARGO_PROFILE:-release}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
BODY_MARKER="SECRET-BODY-MARKER-91c2"

usage() {
  cat <<'EOF'
Usage:
  scripts/mailbox-e2e.sh

Environment:
  CARGO_OFFLINE=1            cargo build --offline
  E2E_CARGO_PROFILE=debug    build profile for the binary under test (default: release; AC-19 timings assume release)
  E2E_SLOW=1                 run slow checks (SSE heartbeat, HTTP header timeout, SMTP 300s idle, SSE slot release)
  E2E_DOCKER=1               build services/mailbox/Dockerfile and run the container checks (AC-01/02/18/25)
  E2E_DOCKER_MULTIARCH=1     also run docker buildx for linux/amd64,linux/arm64 (with E2E_DOCKER=1)
  E2E_ASSETS=1               npm ci && npm run build in web/mailbox, then assert no drift (AC-27)
  E2E_ALLOW_SKIP=1           do not fail when the MailHog oracle (docker) is unavailable
  E2E_KEEP_WORKDIR=true      keep the temporary workspace (logs, storage) for debugging
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

# Never inherit mailbox runtime configuration from the caller's shell.
while IFS= read -r var; do
  unset "${var}"
done < <(compgen -e | grep '^DEVCLOUD_MAILBOX_' || true)

PASS=0
FAIL=0
SKIP=0
result() {
  local status="$1"
  shift
  printf '[%s] %s\n' "${status}" "$*"
  case "${status}" in
    PASS) PASS=$((PASS + 1)) ;;
    FAIL) FAIL=$((FAIL + 1)) ;;
    SKIP) SKIP=$((SKIP + 1)) ;;
  esac
}

log() {
  printf '[e2e] %s\n' "$*"
}

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mailbox-e2e.XXXXXX")"
MBX_PID=""
DOCKER_OK=0
ORACLE_SKIPPED=0
DOCKER_NAMES=()
DOCKER_VOLUMES=()
DOCKER_IMAGES=()
BG_PIDS=()

cleanup() {
  local rc=$?
  local item
  for item in ${BG_PIDS[@]+"${BG_PIDS[@]}"}; do
    kill "${item}" >/dev/null 2>&1 || true
  done
  if [[ -n "${MBX_PID}" ]]; then
    kill "${MBX_PID}" >/dev/null 2>&1 || true
    wait "${MBX_PID}" >/dev/null 2>&1 || true
  fi
  for item in ${DOCKER_NAMES[@]+"${DOCKER_NAMES[@]}"}; do
    docker rm -f "${item}" >/dev/null 2>&1 || true
  done
  for item in ${DOCKER_VOLUMES[@]+"${DOCKER_VOLUMES[@]}"}; do
    docker volume rm -f "${item}" >/dev/null 2>&1 || true
  done
  for item in ${DOCKER_IMAGES[@]+"${DOCKER_IMAGES[@]}"}; do
    docker image rm -f "${item}" >/dev/null 2>&1 || true
  done
  if [[ "${KEEP_WORKDIR}" == "true" ]]; then
    log "kept workdir: ${TMP_DIR}"
  else
    chmod -R u+w "${TMP_DIR}" >/dev/null 2>&1 || true
    rm -rf "${TMP_DIR}"
  fi
  exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

http_code() {
  curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$@" 2>/dev/null || true
}

wait_http() {
  local url="$1"
  local timeout="${2:-20}"
  local deadline=$((SECONDS + timeout))
  until [[ "$(http_code "${url}")" == "200" ]]; do
    if ((SECONDS >= deadline)); then
      return 1
    fi
    sleep 0.2
  done
}

v2_total() {
  curl -fsS --max-time 10 "$@" | python3 -c 'import json, sys; print(json.load(sys.stdin)["total"])'
}

# ---------------------------------------------------------------- build
CARGO_ARGS=(build -p devcloud-mailbox --bin devcloud-mailbox)
if [[ "${CARGO_OFFLINE:-0}" == "1" ]]; then
  CARGO_ARGS+=(--offline)
fi
if [[ "${PROFILE}" == "release" ]]; then
  CARGO_ARGS+=(--release)
fi
log "cargo ${CARGO_ARGS[*]}"
if cargo "${CARGO_ARGS[@]}"; then
  result PASS "BUILD cargo ${CARGO_ARGS[*]}"
else
  result FAIL "BUILD cargo ${CARGO_ARGS[*]}"
  exit 1
fi
BIN="${CARGO_TARGET_DIR:-${ROOT_DIR}/target}/${PROFILE}/devcloud-mailbox"

# ---------------------------------------------------------------- binary under test
SMTP_PORT="$(free_port)"
HTTP_PORT="$(free_port)"
STORAGE="${TMP_DIR}/storage"
MBX_LOG="${TMP_DIR}/mailbox.log"
mkdir -p "${STORAGE}"
log "starting devcloud-mailbox smtp=127.0.0.1:${SMTP_PORT} http=127.0.0.1:${HTTP_PORT}"
DEVCLOUD_MAILBOX_SMTP_ADDR="127.0.0.1:${SMTP_PORT}" \
  DEVCLOUD_MAILBOX_HTTP_ADDR="127.0.0.1:${HTTP_PORT}" \
  DEVCLOUD_MAILBOX_STORAGE="${STORAGE}" \
  "${BIN}" >"${MBX_LOG}" 2>&1 &
MBX_PID=$!
if wait_http "http://127.0.0.1:${HTTP_PORT}/api/v2/messages" 20; then
  result PASS "START devcloud-mailbox answers on its HTTP port"
else
  result FAIL "START devcloud-mailbox did not become ready (log: ${MBX_LOG})"
  KEEP_WORKDIR=true
  exit 1
fi

# ---------------------------------------------------------------- MailHog oracle
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  DOCKER_OK=1
fi
ORACLE_ARGS=()
if [[ "${DOCKER_OK}" == "1" ]]; then
  O_SMTP="$(free_port)"
  O_HTTP="$(free_port)"
  ORACLE_NAME="mailbox-e2e-oracle-$$"
  DOCKER_NAMES+=("${ORACLE_NAME}")
  log "starting ${ORACLE_IMAGE} smtp=127.0.0.1:${O_SMTP} http=127.0.0.1:${O_HTTP}"
  if docker run -d --rm --name "${ORACLE_NAME}" --platform linux/amd64 \
    -p "127.0.0.1:${O_SMTP}:1025" -p "127.0.0.1:${O_HTTP}:8025" "${ORACLE_IMAGE}" >/dev/null &&
    wait_http "http://127.0.0.1:${O_HTTP}/api/v2/messages" 60; then
    result PASS "ORACLE ${ORACLE_IMAGE} running"
    ORACLE_ARGS=(--oracle-smtp "${O_SMTP}" --oracle-http "${O_HTTP}")
  else
    result FAIL "ORACLE ${ORACLE_IMAGE} did not start"
  fi
else
  result SKIP "DIFF oracle unavailable (docker not reachable); every DIFF check is skipped"
  ORACLE_SKIPPED=1
fi

# ---------------------------------------------------------------- harness
HARNESS_ARGS=(--bin "${BIN}" --ours-smtp "${SMTP_PORT}" --ours-http "${HTTP_PORT}"
  --ours-pid "${MBX_PID}" --ours-log "${MBX_LOG}" --workdir "${TMP_DIR}")
if [[ "${E2E_SLOW:-0}" == "1" ]]; then
  HARNESS_ARGS+=(--slow)
fi
HARNESS_RC=0
python3 "${ROOT_DIR}/scripts/mailbox-e2e/harness.py" "${HARNESS_ARGS[@]}" ${ORACLE_ARGS[@]+"${ORACLE_ARGS[@]}"} || HARNESS_RC=$?

if kill -0 "${MBX_PID}" >/dev/null 2>&1; then
  result PASS "AC-21 binary under test still running after the harness"
  t0="$(now_ms)"
  kill -TERM "${MBX_PID}"
  deadline=$((SECONDS + 12))
  while kill -0 "${MBX_PID}" >/dev/null 2>&1 && ((SECONDS < deadline)); do
    sleep 0.1
  done
  t1="$(now_ms)"
  code=0
  if kill -0 "${MBX_PID}" >/dev/null 2>&1; then
    code=timeout
    kill -KILL "${MBX_PID}" >/dev/null 2>&1 || true
  fi
  wait_rc=0
  wait "${MBX_PID}" >/dev/null 2>&1 || wait_rc=$?
  if [[ "${code}" != "timeout" ]]; then
    code="${wait_rc}"
  fi
  MBX_PID=""
  if [[ "${code}" == "0" && $((t1 - t0)) -lt 10000 ]]; then
    result PASS "AC-01 SIGTERM -> exit 0 in $((t1 - t0))ms (local binary)"
  else
    result FAIL "AC-01 SIGTERM -> exit ${code} after $((t1 - t0))ms (want 0 within 10s)"
  fi
else
  result FAIL "AC-21 binary under test died during the harness (log: ${MBX_LOG})"
  KEEP_WORKDIR=true
  MBX_PID=""
fi

# ---------------------------------------------------------------- AC-24 static grep ban (cheap, always)
if [[ -d web/mailbox/src ]]; then
  hits="$(grep -RInE 'dangerouslySetInnerHTML|srcdoc|innerHTML' web/mailbox/src | cut -d: -f1,2 || true)"
  if [[ -z "${hits}" ]]; then
    result PASS "AC-24 web/mailbox/src has no dangerouslySetInnerHTML/srcdoc/innerHTML"
  else
    result FAIL "AC-24 banned DOM sinks in web/mailbox/src at: $(echo "${hits}" | tr '\n' ' ')"
  fi
else
  result FAIL "AC-24 web/mailbox/src not found"
fi

# ---------------------------------------------------------------- Docker stage (AC-01, AC-02, AC-18, AC-25)
docker_stage() {
  local img="devcloud-mailbox:e2e-$$"
  local name="mailbox-e2e-ctr-$$"
  local ro_name="mailbox-e2e-ro-$$"
  local vol="mailbox-e2e-data-$$"
  local d_smtp d_http fake_token t0 t1 elapsed code user exposed vols envs total running out

  if grep -v '^[[:space:]]*#' services/mailbox/Dockerfile | grep -Eiq '(^|[^a-z])(node|npm|pnpm|yarn|npx)([^a-z]|$)'; then
    result FAIL "AC-01 Dockerfile references Node tooling (UI must be prebuilt and embedded)"
  else
    result PASS "AC-01 Dockerfile does not use Node tooling"
  fi
  DOCKER_IMAGES+=("${img}")
  if docker build -f services/mailbox/Dockerfile -t "${img}" .; then
    result PASS "AC-01 docker build -f services/mailbox/Dockerfile ."
  else
    result FAIL "AC-01 docker build -f services/mailbox/Dockerfile ."
    return
  fi
  if [[ "${E2E_DOCKER_MULTIARCH:-0}" == "1" ]]; then
    if docker buildx build --platform linux/amd64,linux/arm64 -f services/mailbox/Dockerfile .; then
      result PASS "AC-01 docker buildx --platform linux/amd64,linux/arm64"
    else
      result FAIL "AC-01 docker buildx --platform linux/amd64,linux/arm64"
    fi
  else
    result SKIP "AC-01 multi-arch buildx (set E2E_DOCKER_MULTIARCH=1)"
  fi

  user="$(docker image inspect -f '{{.Config.User}}' "${img}")"
  if [[ -n "${user}" && "${user}" != "root" && "${user%%:*}" != "0" ]]; then
    result PASS "AC-01 image runs as non-root (USER=${user})"
  else
    result FAIL "AC-01 image runs as root (USER='${user}')"
  fi
  exposed="$(docker image inspect -f '{{json .Config.ExposedPorts}}' "${img}")"
  if [[ "${exposed}" == *'"1025/tcp"'* && "${exposed}" == *'"8025/tcp"'* ]]; then
    result PASS "AC-01 EXPOSE 1025 and 8025"
  else
    result FAIL "AC-01 EXPOSE 1025 and 8025 (got ${exposed})"
  fi
  vols="$(docker image inspect -f '{{json .Config.Volumes}}' "${img}")"
  if [[ "${vols}" == *'"/data"'* ]]; then
    result PASS "AC-01 VOLUME /data"
  else
    result FAIL "AC-01 VOLUME /data (got ${vols})"
  fi
  envs="$(docker image inspect -f '{{json .Config.Env}}' "${img}")"
  if [[ "${envs}" == *'DEVCLOUD_MAILBOX_SMTP_ADDR=0.0.0.0:1025'* && "${envs}" == *'DEVCLOUD_MAILBOX_HTTP_ADDR=0.0.0.0:8025'* ]]; then
    result PASS "AC-01 image binds 0.0.0.0:1025 / 0.0.0.0:8025"
  else
    result FAIL "AC-01 image ENV does not bind 0.0.0.0:1025 / 0.0.0.0:8025"
  fi

  d_smtp="$(free_port)"
  d_http="$(free_port)"
  DOCKER_VOLUMES+=("${vol}")
  DOCKER_NAMES+=("${name}")
  if ! docker volume create "${vol}" >/dev/null ||
    ! docker run -d --name "${name}" -p "127.0.0.1:${d_smtp}:1025" -p "127.0.0.1:${d_http}:8025" \
      -v "${vol}:/data" "${img}" >/dev/null; then
    result FAIL "AC-01 docker run of the built image failed"
    return
  fi
  if ! wait_http "http://127.0.0.1:${d_http}/api/v2/messages" 30; then
    result FAIL "AC-01 container did not answer on published 8025"
    return
  fi
  fake_token="$(python3 -c 'import base64, secrets; print(base64.b64encode(("leak:" + secrets.token_hex(8)).encode()).decode())')"
  python3 - "${d_smtp}" "${BODY_MARKER}" <<'PY' || true
import smtplib
import sys
msg = ("From: Docker <docker@example.test>\r\nTo: b@example.test\r\nSubject: docker-e2e\r\n"
       "X-Fixture: docker-e2e\r\n\r\nbody " + sys.argv[2] + "\r\n").encode()
with smtplib.SMTP("127.0.0.1", int(sys.argv[1]), local_hostname="docker-e2e.test", timeout=10) as s:
    s.sendmail("env@bounce.test", ["b@example.test"], msg)
PY
  total="$(v2_total -H "Authorization: Basic ${fake_token}" "http://127.0.0.1:${d_http}/api/v2/messages" || echo error)"
  if [[ "${total}" == "1" ]]; then
    result PASS "AC-01 container receives SMTP on 1025 and lists it on 8025"
  else
    result FAIL "AC-01 container SMTP->HTTP round trip (total=${total})"
  fi
  http_code -H "Authorization: Basic ${fake_token}" "http://127.0.0.1:${d_http}/" >/dev/null

  curl -sN --max-time 60 "http://127.0.0.1:${d_http}/api/v1/events" >/dev/null 2>&1 &
  BG_PIDS+=($!)
  curl -sN --max-time 60 "http://127.0.0.1:${d_http}/api/v1/events" >/dev/null 2>&1 &
  BG_PIDS+=($!)
  sleep 1
  t0="$(now_ms)"
  docker stop -t 10 "${name}" >/dev/null || true
  t1="$(now_ms)"
  elapsed=$((t1 - t0))
  code="$(docker inspect -f '{{.State.ExitCode}}' "${name}" || echo unknown)"
  if [[ "${code}" == "0" && "${elapsed}" -lt 10000 ]]; then
    result PASS "AC-25 docker stop with 2 SSE clients -> exit 0 in ${elapsed}ms"
  else
    result FAIL "AC-25 docker stop with 2 SSE clients -> exit ${code} in ${elapsed}ms (want 0 within 10s)"
  fi

  if docker start "${name}" >/dev/null && wait_http "http://127.0.0.1:${d_http}/api/v2/messages" 30 &&
    [[ "$(v2_total "http://127.0.0.1:${d_http}/api/v2/messages" || echo error)" == "1" ]]; then
    result PASS "AC-02 message survives container restart"
  else
    result FAIL "AC-02 message survives container restart"
  fi

  out="${TMP_DIR}/docker-logs.txt"
  docker logs "${name}" >"${out}" 2>&1 || true
  if grep -qF "${BODY_MARKER}" "${out}" || grep -qF "${fake_token}" "${out}" || grep -qF "Authorization:" "${out}"; then
    result FAIL "AC-18 container logs contain a mail body, credential or Authorization header"
  else
    result PASS "AC-18 container logs contain no mail body, credential or Authorization header"
  fi
  docker rm -f "${name}" >/dev/null 2>&1 || true

  local ro_dir="${TMP_DIR}/ro-data"
  mkdir -p "${ro_dir}"
  DOCKER_NAMES+=("${ro_name}")
  if ! docker run -d --name "${ro_name}" -v "${ro_dir}:/data:ro" "${img}" >/dev/null; then
    result FAIL "AC-25 docker run with read-only /data could not be started"
    return
  fi
  running=true
  for _ in $(seq 1 75); do
    running="$(docker inspect -f '{{.State.Running}}' "${ro_name}" || echo unknown)"
    if [[ "${running}" == "false" ]]; then
      break
    fi
    sleep 0.2
  done
  code="$(docker inspect -f '{{.State.ExitCode}}' "${ro_name}" || echo unknown)"
  if [[ "${running}" == "false" && "${code}" != "0" ]] && docker logs "${ro_name}" 2>&1 | grep -qi 'uid'; then
    result PASS "AC-25 unwritable /data -> non-zero exit (${code}) naming the uid"
  else
    result FAIL "AC-25 unwritable /data -> running=${running} exit=${code}; want non-zero exit naming the uid"
  fi
  docker rm -f "${ro_name}" >/dev/null 2>&1 || true

  # AC-29: ephemeral mode in the container -> empty inbox after docker restart
  local eph_name="mailbox-e2e-eph-$$"
  local e_smtp e_http
  e_smtp="$(free_port)"
  e_http="$(free_port)"
  DOCKER_NAMES+=("${eph_name}")
  if ! docker run -d --name "${eph_name}" -e DEVCLOUD_MAILBOX_EPHEMERAL=true \
    -p "127.0.0.1:${e_smtp}:1025" -p "127.0.0.1:${e_http}:8025" "${img}" >/dev/null ||
    ! wait_http "http://127.0.0.1:${e_http}/api/v2/messages" 30; then
    result FAIL "AC-29 container with DEVCLOUD_MAILBOX_EPHEMERAL=true did not start"
    return
  fi
  python3 - "${e_smtp}" <<'PY' || true
import smtplib
import sys
msg = b"From: e@example.test\r\nTo: b@example.test\r\nSubject: ephemeral\r\n\r\nephemeral body\r\n"
with smtplib.SMTP("127.0.0.1", int(sys.argv[1]), timeout=10) as s:
    s.sendmail("env@bounce.test", ["b@example.test"], msg)
PY
  total="$(v2_total "http://127.0.0.1:${e_http}/api/v2/messages" || echo error)"
  if [[ "${total}" == "1" ]] && docker restart -t 10 "${eph_name}" >/dev/null &&
    wait_http "http://127.0.0.1:${e_http}/api/v2/messages" 30 &&
    [[ "$(v2_total "http://127.0.0.1:${e_http}/api/v2/messages" || echo error)" == "0" ]]; then
    result PASS "AC-29 -e DEVCLOUD_MAILBOX_EPHEMERAL=true: inbox empty after docker restart"
  else
    result FAIL "AC-29 -e DEVCLOUD_MAILBOX_EPHEMERAL=true: want 1 message before and 0 after docker restart (before=${total})"
  fi
  docker rm -f "${eph_name}" >/dev/null 2>&1 || true
}

if [[ "${E2E_DOCKER:-0}" == "1" ]]; then
  if [[ "${DOCKER_OK}" == "1" ]]; then
    docker_stage
  else
    result FAIL "AC-01 E2E_DOCKER=1 but docker is not reachable"
  fi
else
  result SKIP "AC-01/02/18/25/29 docker image stage (set E2E_DOCKER=1)"
fi

# ---------------------------------------------------------------- asset drift (AC-27)
snapshot_assets() {
  python3 - "$1" <<'PY'
import hashlib
import os
import sys
root = sys.argv[1]
for dirpath, _, files in sorted(os.walk(root)):
    for f in sorted(files):
        p = os.path.join(dirpath, f)
        with open(p, "rb") as fh:
            print(hashlib.sha256(fh.read()).hexdigest(), os.path.relpath(p, root))
PY
}

if [[ "${E2E_ASSETS:-0}" == "1" ]]; then
  before="$(snapshot_assets services/mailbox/assets/ui)"
  if (cd web/mailbox && npm ci && npm run build); then
    after="$(snapshot_assets services/mailbox/assets/ui)"
    if [[ "${before}" == "${after}" ]] && git diff --quiet --exit-code -- services/mailbox/assets/ui; then
      result PASS "AC-27 npm ci && npm run build leaves services/mailbox/assets/ui unchanged"
    else
      result FAIL "AC-27 rebuilt UI differs from the committed/embedded services/mailbox/assets/ui"
    fi
  else
    result FAIL "AC-27 npm ci && npm run build failed in web/mailbox"
  fi
else
  result SKIP "AC-27 asset drift stage (set E2E_ASSETS=1)"
fi

# ---------------------------------------------------------------- summary
RC=0
if [[ "${HARNESS_RC}" != "0" ]]; then
  RC=1
fi
if ((FAIL > 0)); then
  RC=1
fi
if [[ "${ORACLE_SKIPPED}" == "1" && "${E2E_ALLOW_SKIP:-0}" != "1" ]]; then
  result FAIL "DIFF oracle was skipped; set E2E_ALLOW_SKIP=1 to accept a run without MailHog"
  RC=1
fi
log "stage summary: PASS=${PASS} FAIL=${FAIL} SKIP=${SKIP}; harness exit=${HARNESS_RC}"
if [[ "${RC}" == "0" ]]; then
  log "passed"
else
  log "FAILED"
fi
exit "${RC}"
