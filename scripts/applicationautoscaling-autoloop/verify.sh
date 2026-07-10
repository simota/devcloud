#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"

free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

AAS_VERIFY_PORT="${AAS_VERIFY_PORT:-$(free_port)}"
DASHBOARD_VERIFY_PORT="${DASHBOARD_VERIFY_PORT:-$(free_port)}"
EVENT_RELAY_VERIFY_PORT="${EVENT_RELAY_VERIFY_PORT:-$(free_port)}"
VERIFY_HOST="127.0.0.1"
AAS_ENDPOINT="http://${VERIFY_HOST}:${AAS_VERIFY_PORT}"
DASHBOARD_ENDPOINT="http://${VERIFY_HOST}:${DASHBOARD_VERIFY_PORT}"

REGION="us-east-1"
ACCOUNT_ID="000000000000"
NAMESPACE="dynamodb"
RESOURCE_ID="table/devcloud-aas-loop"
DIMENSION="dynamodb:table:WriteCapacityUnits"
POLICY_NAME="devcloud-aas-loop-policy"
SCHEDULE_NAME="devcloud-aas-loop-schedule"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dev}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dev}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

PASS=0
FAIL=0
TMP_DIR=""
DEV_PID=""
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-aas-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-aas-verify.err"
STATUS_OUT="${TMPDIR:-/tmp}/devcloud-aas-verify.status"
POLICY_ARN=""

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
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -30
    FAIL=$((FAIL + 1))
  fi
}

json_assert() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data}), data' "${expression}"
}

json_value() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "${expression}"
}

# Provider protocol call: POST / with the AWS JSON 1.1 X-Amz-Target header,
# mirroring what a real application-autoscaling SDK client sends.
aas_json() {
  local target="$1"
  local payload="$2"
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/x-amz-json-1.1' \
    -H "X-Amz-Target: AnyScaleFrontendService.${target}" \
    --data "${payload}" \
    "${AAS_ENDPOINT}/"
}

# Returns the raw HTTP status for a provider call (used by hardening checks that
# assert on error responses, where curl -f would otherwise mask the body/status).
aas_status() {
  local target="$1"
  local payload="$2"
  curl -sS -o "${STATUS_OUT}" -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: application/x-amz-json-1.1' \
    -H "X-Amz-Target: AnyScaleFrontendService.${target}" \
    --data "${payload}" \
    "${AAS_ENDPOINT}/"
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 20))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[aas-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[aas-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    sleep 0.2
  done
}

wait_for_aas() {
  local deadline=$((SECONDS + 20))
  until aas_json DescribeScalableTargets "{\"ServiceNamespace\":\"${NAMESPACE}\"}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[aas-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[aas-verify] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    sleep 0.2
  done
}

start_devcloud() {
  TMP_DIR="$(mktemp -d)"
  mkdir -p "${TMP_DIR}/.devcloud"
  cat > "${TMP_DIR}/.devcloud/config.yaml" <<EOF
project: applicationautoscaling-e2e

server:
  dashboardPort: ${DASHBOARD_VERIFY_PORT}
  eventRelayPort: ${EVENT_RELAY_VERIFY_PORT}
  appAutoScalingPort: ${AAS_VERIFY_PORT}

auth:
  appAutoScaling:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "${ACCOUNT_ID}"

storage:
  path: .devcloud/data

services:
  mail:
    enabled: false
  s3:
    enabled: false
  gcs:
    enabled: false
  dynamodb:
    enabled: false
  bigquery:
    enabled: false
  redshift:
    enabled: false
  redis:
    enabled: false
  sqs:
    enabled: false
  pubsub:
    enabled: false
  appAutoScaling:
    enabled: true
    region: ${REGION}
EOF

  run_check "devcloud binary builds" devcloud_build "${TMP_DIR}/devcloud"
  if [[ "${FAIL}" -gt 0 ]]; then
    return 1
  fi

  # `exec` keeps $! pointing at the devcloud process itself; without it,
  # bash 3.2 leaves the binary running as an orphan when cleanup kills the
  # subshell, and the orphan keeps serving the verify ports with a deleted
  # working directory (persist fails with 500 on every write).
  (
    cd "${TMP_DIR}"
    exec "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"
}

ensure_started() {
  if [[ -z "${DEV_PID}" ]]; then
    start_devcloud || return 1
    wait_for_aas
  fi
}

# --- foundation / config ------------------------------------------------------

assert_config_shape() {
  env -u RIPGREP_CONFIG_PATH rg -q 'appAutoScalingPort|app_auto_scaling|AppAutoScaling|appAutoScaling' orchestrator services &&
    cargo test -p devcloud-applicationautoscaling
}

assert_script_contract() {
  bash -n scripts/applicationautoscaling-autoloop/verify.sh &&
    bash -n scripts/applicationautoscaling-e2e.sh
}

# --- provider protocol (aas-core) --------------------------------------------

aas_endpoint_starts() {
  ensure_started
}

register_scalable_target() {
  aas_json RegisterScalableTarget "{
    \"ServiceNamespace\":\"${NAMESPACE}\",
    \"ResourceId\":\"${RESOURCE_ID}\",
    \"ScalableDimension\":\"${DIMENSION}\",
    \"MinCapacity\":1,
    \"MaxCapacity\":10,
    \"RoleARN\":\"arn:aws:iam::${ACCOUNT_ID}:role/devcloud-aas-loop\"
  }" | json_assert 'data == {}'
}

describe_scalable_targets() {
  aas_json DescribeScalableTargets "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceIds\":[\"${RESOURCE_ID}\"]}" |
    json_assert 'len(data["ScalableTargets"]) == 1 and data["ScalableTargets"][0]["ResourceId"] == "'"${RESOURCE_ID}"'" and data["ScalableTargets"][0]["MinCapacity"] == 1 and data["ScalableTargets"][0]["MaxCapacity"] == 10'
}

put_scaling_policy() {
  local resp
  resp="$(aas_json PutScalingPolicy "{
    \"PolicyName\":\"${POLICY_NAME}\",
    \"ServiceNamespace\":\"${NAMESPACE}\",
    \"ResourceId\":\"${RESOURCE_ID}\",
    \"ScalableDimension\":\"${DIMENSION}\",
    \"PolicyType\":\"TargetTrackingScaling\",
    \"TargetTrackingScalingPolicyConfiguration\":{
      \"TargetValue\":70.0,
      \"PredefinedMetricSpecification\":{\"PredefinedMetricType\":\"DynamoDBWriteCapacityUtilization\"}
    }
  }")"
  POLICY_ARN="$(echo "${resp}" | json_value 'data["PolicyARN"]')"
  echo "${resp}" | json_assert 'data["PolicyARN"].startswith("arn:aws:autoscaling:") and ":policyName/'"${POLICY_NAME}"'" in data["PolicyARN"] and isinstance(data["Alarms"], list)'
}

describe_scaling_policies() {
  aas_json DescribeScalingPolicies "{\"ServiceNamespace\":\"${NAMESPACE}\",\"PolicyNames\":[\"${POLICY_NAME}\"]}" |
    json_assert 'len(data["ScalingPolicies"]) == 1 and data["ScalingPolicies"][0]["PolicyARN"] == "'"${POLICY_ARN}"'" and data["ScalingPolicies"][0]["PolicyType"] == "TargetTrackingScaling"'
}

describe_scaling_activities() {
  aas_json DescribeScalingActivities "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\"}" |
    json_assert 'data["ScalingActivities"] == []'
}

tag_lifecycle() {
  aas_json TagResource "{\"ResourceARN\":\"${POLICY_ARN}\",\"Tags\":{\"suite\":\"applicationautoscaling-autoloop\"}}" |
    json_assert 'data == {}'
  aas_json ListTagsForResource "{\"ResourceARN\":\"${POLICY_ARN}\"}" |
    json_assert 'data["Tags"] == {"suite": "applicationautoscaling-autoloop"}'
  aas_json UntagResource "{\"ResourceARN\":\"${POLICY_ARN}\",\"TagKeys\":[\"suite\"]}" |
    json_assert 'data == {}'
  aas_json ListTagsForResource "{\"ResourceARN\":\"${POLICY_ARN}\"}" |
    json_assert 'data["Tags"] == {}'
}

put_scheduled_action() {
  aas_json PutScheduledAction "{
    \"ServiceNamespace\":\"${NAMESPACE}\",
    \"ScheduledActionName\":\"${SCHEDULE_NAME}\",
    \"ResourceId\":\"${RESOURCE_ID}\",
    \"ScalableDimension\":\"${DIMENSION}\",
    \"Schedule\":\"at(2030-01-01T00:00:00)\",
    \"ScalableTargetAction\":{\"MinCapacity\":2,\"MaxCapacity\":8}
  }" | json_assert 'data == {}'
}

describe_scheduled_actions() {
  aas_json DescribeScheduledActions "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ScheduledActionNames\":[\"${SCHEDULE_NAME}\"]}" |
    json_assert 'len(data["ScheduledActions"]) == 1 and data["ScheduledActions"][0]["Schedule"] == "at(2030-01-01T00:00:00)" and data["ScheduledActions"][0]["ScalableTargetAction"]["MinCapacity"] == 2 and data["ScheduledActions"][0]["ScalableTargetAction"]["MaxCapacity"] == 8'
}

delete_scaling_policy() {
  aas_json DeleteScalingPolicy "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\",\"PolicyName\":\"${POLICY_NAME}\"}" |
    json_assert 'data == {}'
  aas_json DescribeScalingPolicies "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\"}" |
    json_assert 'not any(p["PolicyName"] == "'"${POLICY_NAME}"'" for p in data["ScalingPolicies"])'
}

delete_scheduled_action() {
  aas_json DeleteScheduledAction "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\",\"ScheduledActionName\":\"${SCHEDULE_NAME}\"}" |
    json_assert 'data == {}'
  aas_json DescribeScheduledActions "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\"}" |
    json_assert 'not any(a["ScheduledActionName"] == "'"${SCHEDULE_NAME}"'" for a in data["ScheduledActions"])'
}

deregister_scalable_target() {
  aas_json DeregisterScalableTarget "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\"}" |
    json_assert 'data == {}'
  aas_json DescribeScalableTargets "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceIds\":[\"${RESOURCE_ID}\"]}" |
    json_assert 'data["ScalableTargets"] == []'
}

# --- dashboard forwarding (dashboard-static) ---------------------------------

dashboard_starts() {
  ensure_started &&
    wait_for_http "${DASHBOARD_ENDPOINT}/"
}

dashboard_service_registry_has_aas() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/dashboard/services" |
    grep -q '"id"[[:space:]]*:[[:space:]]*"applicationautoscaling"'
}

dashboard_status_reports_running() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/applicationautoscaling/status" |
    json_assert 'data["running"] == True and data["service"] == "applicationautoscaling"'
}

dashboard_lists_scalable_targets() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/applicationautoscaling/scalable-targets" |
    json_assert 'any(t["ResourceId"] == "'"${RESOURCE_ID}"'" for t in data["scalableTargets"])'
}

dashboard_lists_scaling_policies() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/applicationautoscaling/scaling-policies" |
    json_assert 'any(p["PolicyName"] == "'"${POLICY_NAME}"'" for p in data["scalingPolicies"])'
}

dashboard_lists_scheduled_actions() {
  curl -fsS "${DASHBOARD_ENDPOINT}/api/applicationautoscaling/scheduled-actions" |
    json_assert 'any(a["ScheduledActionName"] == "'"${SCHEDULE_NAME}"'" for a in data["scheduledActions"])'
}

dashboard_page_loads() {
  curl -fsSL "${DASHBOARD_ENDPOINT}/dashboard/applicationautoscaling" |
    grep -q '<div id="root"></div>'
}

# --- hardening ----------------------------------------------------------------

rejects_unsupported_namespace() {
  local code
  code="$(aas_status DescribeScalableTargets '{"ServiceNamespace":"ecs"}')"
  [[ "${code}" == "400" ]] &&
    grep -q 'ValidationException' "${STATUS_OUT}"
}

rejects_missing_required_field() {
  local code
  code="$(aas_status RegisterScalableTarget "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ScalableDimension\":\"${DIMENSION}\"}")"
  [[ "${code}" == "400" ]] &&
    grep -q 'ValidationException' "${STATUS_OUT}"
}

rejects_unknown_operation() {
  local code
  code="$(aas_status BogusOperation '{}')"
  [[ "${code}" == "400" ]] &&
    grep -q 'UnknownOperationException' "${STATUS_OUT}"
}

dashboard_rejects_non_get() {
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${DASHBOARD_ENDPOINT}/api/applicationautoscaling/status")"
  [[ "${code}" == "405" ]]
}

# --- stage runners ------------------------------------------------------------

run_foundation_checks() {
  run_check "autoloop script contract" assert_script_contract
  run_check "applicationautoscaling crate tests pass" cargo test -p devcloud-applicationautoscaling
}

run_config_checks() {
  run_check "config shape wires appAutoScaling" assert_config_shape
}

run_core_checks() {
  run_check "Application Auto Scaling endpoint starts" aas_endpoint_starts
  run_check "RegisterScalableTarget works" register_scalable_target
  run_check "DescribeScalableTargets works" describe_scalable_targets
  run_check "PutScalingPolicy works" put_scaling_policy
  run_check "DescribeScalingPolicies works" describe_scaling_policies
  run_check "DescribeScalingActivities works" describe_scaling_activities
  run_check "Tag/Untag lifecycle works" tag_lifecycle
  run_check "PutScheduledAction works" put_scheduled_action
  run_check "DescribeScheduledActions works" describe_scheduled_actions
}

run_dashboard_checks() {
  run_check "dashboard starts" dashboard_starts
  run_check "dashboard service registry has Application Auto Scaling" dashboard_service_registry_has_aas
  run_check "dashboard status reports running" dashboard_status_reports_running
  run_check "dashboard lists scalable targets" dashboard_lists_scalable_targets
  run_check "dashboard lists scaling policies" dashboard_lists_scaling_policies
  run_check "dashboard lists scheduled actions" dashboard_lists_scheduled_actions
  run_check "dashboard SPA page loads" dashboard_page_loads
}

run_hardening_checks() {
  run_check "unsupported namespace rejected" rejects_unsupported_namespace
  run_check "missing required field rejected" rejects_missing_required_field
  run_check "unknown operation rejected" rejects_unknown_operation
  run_check "dashboard API rejects non-GET" dashboard_rejects_non_get
  run_check "DeleteScalingPolicy removes policy" delete_scaling_policy
  run_check "DeleteScheduledAction removes action" delete_scheduled_action
  run_check "DeregisterScalableTarget removes target" deregister_scalable_target
}

run_e2e_checks() {
  run_check "Application Auto Scaling standalone E2E script passes" bash scripts/applicationautoscaling-e2e.sh
}

echo "=== Application Auto Scaling autoloop verification: ${VERIFY_STAGE} ==="

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  config)
    run_foundation_checks
    run_config_checks
    ;;
  applicationautoscaling|aas-core)
    run_foundation_checks
    run_config_checks
    run_core_checks
    ;;
  dashboard|dashboard-static)
    run_foundation_checks
    run_config_checks
    run_core_checks
    run_dashboard_checks
    ;;
  hardening|full)
    run_foundation_checks
    run_config_checks
    run_core_checks
    run_dashboard_checks
    run_hardening_checks
    run_e2e_checks
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE: ${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Application Auto Scaling autoloop verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
