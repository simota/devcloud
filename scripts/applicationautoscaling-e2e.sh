#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
cd "${ROOT_DIR}"

AAS_PORT="${E2E_APPLICATIONAUTOSCALING_PORT:-}"
DASHBOARD_PORT="${E2E_DASHBOARD_PORT:-}"
EVENT_RELAY_PORT="${E2E_EVENT_RELAY_PORT:-}"
AAS_ENDPOINT=""
DASHBOARD_ENDPOINT=""
REGION="${E2E_AAS_REGION:-us-east-1}"
ACCOUNT_ID="${E2E_AAS_ACCOUNT_ID:-000000000000}"
NAMESPACE="dynamodb"
RESOURCE_ID="${E2E_AAS_RESOURCE_ID:-table/devcloud-aas-e2e-$(date +%s)}"
DIMENSION="dynamodb:table:WriteCapacityUnits"
POLICY_NAME="${E2E_AAS_POLICY_NAME:-devcloud-aas-policy-e2e-$(date +%s)}"
SCHEDULE_NAME="${E2E_AAS_SCHEDULE_NAME:-devcloud-aas-schedule-e2e-$(date +%s)}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-false}"
INTERACTIVE="${E2E_INTERACTIVE:-false}"
DELETE_DATA="${E2E_DELETE_DATA:-true}"

TMP_DIR=""
DEV_PID=""
WORKSPACE=""
POLICY_ARN=""

usage() {
  cat <<'EOF'
Usage:
  scripts/applicationautoscaling-e2e.sh

Environment:
  E2E_APPLICATIONAUTOSCALING_PORT=18030  Override the Application Auto Scaling endpoint port. Defaults to an available port.
  E2E_DASHBOARD_PORT=18025               Override the dashboard port. Defaults to an available port.
  E2E_EVENT_RELAY_PORT=18027             Override the dashboard event relay port. Defaults to an available port.
  E2E_AAS_REGION=us-east-1               Override the configured region.
  E2E_AAS_ACCOUNT_ID=000000000000        Override the configured account id.
  E2E_AAS_RESOURCE_ID=table/...          Override the scalable target resource id.
  E2E_AAS_POLICY_NAME=...                Override the scaling policy name.
  E2E_AAS_SCHEDULE_NAME=...              Override the scheduled action name.
  E2E_DELETE_DATA=false                  Skip the Delete*/Deregister* steps so resources stay behind for inspection.
  E2E_KEEP_WORKDIR=true                  Keep the temporary workspace for debugging.
  E2E_INTERACTIVE=true                   Keep devcloud running after assertions (and keep resources, like E2E_DELETE_DATA=false).

Examples:
  scripts/applicationautoscaling-e2e.sh
  E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/applicationautoscaling-e2e.sh
  E2E_APPLICATIONAUTOSCALING_PORT=18030 E2E_DASHBOARD_PORT=18025 scripts/applicationautoscaling-e2e.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${INTERACTIVE}" == "true" && -z "${E2E_DELETE_DATA+x}" ]]; then
  DELETE_DATA="false"
fi
if [[ "${INTERACTIVE}" == "true" && -z "${E2E_KEEP_WORKDIR+x}" ]]; then
  KEEP_WORKDIR="true"
fi
if [[ "${DELETE_DATA}" == "false" && -z "${E2E_KEEP_WORKDIR+x}" ]]; then
  KEEP_WORKDIR="true"
fi

log() {
  printf '[applicationautoscaling-e2e] %s\n' "$1"
}

cleanup() {
  if [[ -n "${DEV_PID}" ]]; then
    kill "${DEV_PID}" >/dev/null 2>&1 || true
    wait "${DEV_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_WORKDIR}" != "true" && -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  elif [[ -n "${TMP_DIR}" ]]; then
    log "kept workdir: ${TMP_DIR}"
  fi
}
trap cleanup EXIT

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "[applicationautoscaling-e2e] missing command: ${name}" >&2
    exit 1
  fi
}

find_free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

assign_ports() {
  if [[ -z "${AAS_PORT}" ]]; then
    AAS_PORT="$(find_free_port)"
  fi
  if [[ -z "${DASHBOARD_PORT}" ]]; then
    DASHBOARD_PORT="$(find_free_port)"
  fi
  if [[ -z "${EVENT_RELAY_PORT}" ]]; then
    EVENT_RELAY_PORT="$(find_free_port)"
  fi
  AAS_ENDPOINT="http://127.0.0.1:${AAS_PORT}"
  DASHBOARD_ENDPOINT="http://127.0.0.1:${DASHBOARD_PORT}"
}

json_value() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {}, {"data": data}))' "${expression}"
}

json_assert() {
  local expression="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert eval(sys.argv[1], {}, {"data": data}), data' "${expression}"
}

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

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 20))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[applicationautoscaling-e2e] devcloud exited while waiting for ${url}" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[applicationautoscaling-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

wait_for_aas() {
  local deadline=$((SECONDS + 20))
  until aas_json DescribeScalableTargets '{"ServiceNamespace":"dynamodb"}' >/dev/null 2>&1; do
    if [[ -n "${DEV_PID}" ]] && ! kill -0 "${DEV_PID}" 2>/dev/null; then
      echo "[applicationautoscaling-e2e] devcloud exited while waiting for Application Auto Scaling" >&2
      if [[ -n "${TMP_DIR}" && -f "${TMP_DIR}/devcloud-up.log" ]]; then
        sed 's/^/[applicationautoscaling-e2e] devcloud: /' "${TMP_DIR}/devcloud-up.log" >&2
      fi
      return 1
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

write_config() {
  local workspace="$1"
  mkdir -p "${workspace}/.devcloud"
  cat > "${workspace}/.devcloud/config.yaml" <<EOF
project: applicationautoscaling-e2e

server:
  dashboardPort: ${DASHBOARD_PORT}
  eventRelayPort: ${EVENT_RELAY_PORT}
  appAutoScalingPort: ${AAS_PORT}

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
}

register_scalable_target() {
  aas_json RegisterScalableTarget "{
    \"ServiceNamespace\":\"${NAMESPACE}\",
    \"ResourceId\":\"${RESOURCE_ID}\",
    \"ScalableDimension\":\"${DIMENSION}\",
    \"MinCapacity\":1,
    \"MaxCapacity\":10,
    \"RoleARN\":\"arn:aws:iam::${ACCOUNT_ID}:role/devcloud-aas-e2e\"
  }" | json_assert 'data == {}'
}

exercise_describe_targets() {
  aas_json DescribeScalableTargets "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceIds\":[\"${RESOURCE_ID}\"]}" |
    json_assert 'len(data["ScalableTargets"]) == 1 and data["ScalableTargets"][0]["ResourceId"] == "'"${RESOURCE_ID}"'" and data["ScalableTargets"][0]["MinCapacity"] == 1 and data["ScalableTargets"][0]["MaxCapacity"] == 10'
}

exercise_put_scaling_policy() {
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

exercise_describe_scaling_policies() {
  aas_json DescribeScalingPolicies "{\"ServiceNamespace\":\"${NAMESPACE}\",\"PolicyNames\":[\"${POLICY_NAME}\"]}" |
    json_assert 'len(data["ScalingPolicies"]) == 1 and data["ScalingPolicies"][0]["PolicyARN"] == "'"${POLICY_ARN}"'" and data["ScalingPolicies"][0]["PolicyType"] == "TargetTrackingScaling"'
}

exercise_scaling_activities() {
  aas_json DescribeScalingActivities "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\"}" |
    json_assert 'data["ScalingActivities"] == []'
}

exercise_tags() {
  aas_json TagResource "{\"ResourceARN\":\"${POLICY_ARN}\",\"Tags\":{\"suite\":\"applicationautoscaling-e2e\"}}" |
    json_assert 'data == {}'
  aas_json ListTagsForResource "{\"ResourceARN\":\"${POLICY_ARN}\"}" |
    json_assert 'data["Tags"] == {"suite": "applicationautoscaling-e2e"}'
  aas_json UntagResource "{\"ResourceARN\":\"${POLICY_ARN}\",\"TagKeys\":[\"suite\"]}" |
    json_assert 'data == {}'
  aas_json ListTagsForResource "{\"ResourceARN\":\"${POLICY_ARN}\"}" |
    json_assert 'data["Tags"] == {}'
}

exercise_put_scheduled_action() {
  aas_json PutScheduledAction "{
    \"ServiceNamespace\":\"${NAMESPACE}\",
    \"ScheduledActionName\":\"${SCHEDULE_NAME}\",
    \"ResourceId\":\"${RESOURCE_ID}\",
    \"ScalableDimension\":\"${DIMENSION}\",
    \"Schedule\":\"at(2030-01-01T00:00:00)\",
    \"ScalableTargetAction\":{\"MinCapacity\":2,\"MaxCapacity\":8}
  }" | json_assert 'data == {}'
}

exercise_describe_scheduled_actions() {
  aas_json DescribeScheduledActions "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ScheduledActionNames\":[\"${SCHEDULE_NAME}\"]}" |
    json_assert 'len(data["ScheduledActions"]) == 1 and data["ScheduledActions"][0]["Schedule"] == "at(2030-01-01T00:00:00)" and data["ScheduledActions"][0]["ScalableTargetAction"]["MinCapacity"] == 2 and data["ScheduledActions"][0]["ScalableTargetAction"]["MaxCapacity"] == 8'
}

delete_scaling_policy_and_assert() {
  aas_json DeleteScalingPolicy "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\",\"PolicyName\":\"${POLICY_NAME}\"}" |
    json_assert 'data == {}'
  aas_json DescribeScalingPolicies "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\"}" |
    json_assert 'not any(p["PolicyName"] == "'"${POLICY_NAME}"'" for p in data["ScalingPolicies"])'
}

delete_scheduled_action_and_assert() {
  aas_json DeleteScheduledAction "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\",\"ScheduledActionName\":\"${SCHEDULE_NAME}\"}" |
    json_assert 'data == {}'
  aas_json DescribeScheduledActions "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\"}" |
    json_assert 'not any(a["ScheduledActionName"] == "'"${SCHEDULE_NAME}"'" for a in data["ScheduledActions"])'
}

deregister_scalable_target_and_assert() {
  aas_json DeregisterScalableTarget "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceId\":\"${RESOURCE_ID}\",\"ScalableDimension\":\"${DIMENSION}\"}" |
    json_assert 'data == {}'
  aas_json DescribeScalableTargets "{\"ServiceNamespace\":\"${NAMESPACE}\",\"ResourceIds\":[\"${RESOURCE_ID}\"]}" |
    json_assert 'data["ScalableTargets"] == []'
}

teardown_resources() {
  if [[ "${DELETE_DATA}" == "true" ]]; then
    delete_scaling_policy_and_assert
    delete_scheduled_action_and_assert
    deregister_scalable_target_and_assert
  else
    log "E2E_DELETE_DATA=false: leaving scalable target/policy/scheduled action in place"
  fi
}

print_interactive_info() {
  cat <<EOF

[applicationautoscaling-e2e] interactive mode
  Application Auto Scaling endpoint: ${AAS_ENDPOINT}
  Dashboard:                         ${DASHBOARD_ENDPOINT}/
  Workspace:                         ${WORKSPACE}
  Resource id:                       ${RESOURCE_ID}

Example:
  curl -sS -X POST \\
    -H 'Content-Type: application/x-amz-json-1.1' \\
    -H 'X-Amz-Target: AnyScaleFrontendService.DescribeScalableTargets' \\
    --data '{"ServiceNamespace":"dynamodb"}' \\
    ${AAS_ENDPOINT}/

Press Ctrl-C to stop devcloud.
EOF
  while kill -0 "${DEV_PID}" 2>/dev/null; do
    sleep 3600
  done
}

main() {
  require_command curl
  require_command python3
  assign_ports

  TMP_DIR="$(mktemp -d)"
  WORKSPACE="${TMP_DIR}/workspace"
  mkdir -p "${WORKSPACE}"
  write_config "${WORKSPACE}"

  log "building devcloud"
  devcloud_build "${TMP_DIR}/devcloud"

  log "starting devcloud"
  # `exec` keeps $! pointing at the devcloud process itself; without it,
  # bash 3.2 leaves the binary running as an orphan after cleanup kills the
  # subshell wrapper.
  (
    cd "${WORKSPACE}"
    exec "${TMP_DIR}/devcloud" up
  ) >"${TMP_DIR}/devcloud-up.log" 2>&1 &
  DEV_PID="$!"

  wait_for_aas
  wait_for_http "${DASHBOARD_ENDPOINT}/"

  log "exercising scalable targets"
  register_scalable_target
  exercise_describe_targets

  log "exercising scaling policies"
  exercise_put_scaling_policy
  exercise_describe_scaling_policies
  exercise_scaling_activities
  exercise_tags

  log "exercising scheduled actions"
  exercise_put_scheduled_action
  exercise_describe_scheduled_actions

  log "tearing down and asserting removal"
  teardown_resources

  if [[ "${INTERACTIVE}" == "true" ]]; then
    print_interactive_info
  fi

  log "Application Auto Scaling E2E passed"
}

main "$@"
