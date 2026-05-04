#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

VERIFY_STAGE="${VERIFY_STAGE:-foundation}"
PASS=0
FAIL=0
VERIFY_OUT="${TMPDIR:-/tmp}/devcloud-pubsub-full-compat-verify.out"
VERIFY_ERR="${TMPDIR:-/tmp}/devcloud-pubsub-full-compat-verify.err"

run_check() {
  local name="$1"
  shift
  if "$@" > "${VERIFY_OUT}" 2>"${VERIFY_ERR}"; then
    echo "[PASS] ${name}"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] ${name}"
    sed 's/^/  stderr: /' "${VERIFY_ERR}" | tail -40
    FAIL=$((FAIL + 1))
  fi
}

assert_remaining_contract() {
  bash -n scripts/pubsub-full-compat-autoloop/bootstrap.sh &&
    bash -n scripts/pubsub-full-compat-autoloop/run-loop.sh &&
    bash -n scripts/pubsub-full-compat-autoloop/recover.sh &&
    bash -n scripts/pubsub-full-compat-autoloop/verify.sh &&
    env -u RIPGREP_CONFIG_PATH rg -q 'StreamingPull|SchemaService|full-compat|NEXUS_LOOP_STATUS: READY' scripts/pubsub-full-compat-autoloop/goal.md &&
    env -u RIPGREP_CONFIG_PATH rg -q 'NEXUS_LOOP_STATUS|NEXUS_LOOP_SUMMARY' scripts/pubsub-full-compat-autoloop/run-loop.sh
}

assert_mvp_still_passes() {
  VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh
}

assert_streaming_pull_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'func .*StreamingPull' internal/services/pubsub &&
    go test ./internal/services/pubsub -run 'Test.*StreamingPull' -count=1
}

assert_snapshot_seek_grpc_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'func .*CreateSnapshot' internal/services/pubsub &&
    env -u RIPGREP_CONFIG_PATH rg -q 'func .*GetSnapshot' internal/services/pubsub &&
    env -u RIPGREP_CONFIG_PATH rg -q 'func .*ListSnapshots' internal/services/pubsub &&
    env -u RIPGREP_CONFIG_PATH rg -q 'func .*DeleteSnapshot' internal/services/pubsub &&
    env -u RIPGREP_CONFIG_PATH rg -q 'func .*Seek' internal/services/pubsub &&
    go test ./internal/services/pubsub -run 'Test.*GRPC.*Snapshot|Test.*GRPC.*Seek' -count=1
}

assert_schema_grpc_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'RegisterSchemaServiceServer|SchemaServiceServer' internal/services/pubsub &&
    env -u RIPGREP_CONFIG_PATH rg -q 'func .*CreateSchema' internal/services/pubsub &&
    env -u RIPGREP_CONFIG_PATH rg -q 'func .*ValidateMessage' internal/services/pubsub &&
    go test ./internal/services/pubsub -run 'Test.*GRPC.*Schema' -count=1
}

assert_push_delivery_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'pushWorker|runPush|deliverPush|pushDelivery' internal/services/pubsub &&
    go test ./internal/services/pubsub -run 'Test.*Push.*Delivery|Test.*Push.*Retry' -count=1
}

assert_ordering_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'StreamingPull.*Ordering|Ordering.*StreamingPull' internal/services/pubsub &&
    go test ./internal/services/pubsub -run 'Test.*Ordering|Test.*StreamingPull.*Ordering' -count=1
}

assert_sdk_contract() {
  env -u RIPGREP_CONFIG_PATH rg -q 'cloud.google.com/go/pubsub[^/]|PUBSUB_EMULATOR_HOST' scripts internal &&
    go test ./internal/services/pubsub ./internal/app -run 'Test.*PubSub.*Client|Test.*Emulator|Test.*GRPCPublisherSubscriberWorkflow' -count=1
}

run_foundation_checks() {
  run_check "remaining loop contract exists" assert_remaining_contract
  run_check "Pub/Sub MVP gate still passes" assert_mvp_still_passes
}

run_streaming_checks() {
  run_foundation_checks
  run_check "gRPC StreamingPull contract passes" assert_streaming_pull_contract
}

run_snapshot_checks() {
  run_streaming_checks
  run_check "gRPC snapshot and seek contract passes" assert_snapshot_seek_grpc_contract
}

run_schema_checks() {
  run_snapshot_checks
  run_check "gRPC SchemaService contract passes" assert_schema_grpc_contract
}

run_push_checks() {
  run_schema_checks
  run_check "push delivery contract passes" assert_push_delivery_contract
}

run_ordering_checks() {
  run_push_checks
  run_check "ordering compatibility contract passes" assert_ordering_contract
}

run_sdk_checks() {
  run_ordering_checks
  run_check "Google Pub/Sub client compatibility contract passes" assert_sdk_contract
}

case "${VERIFY_STAGE}" in
  foundation)
    run_foundation_checks
    ;;
  streaming)
    run_streaming_checks
    ;;
  snapshots)
    run_snapshot_checks
    ;;
  schemas)
    run_schema_checks
    ;;
  push)
    run_push_checks
    ;;
  ordering)
    run_ordering_checks
    ;;
  sdk)
    run_sdk_checks
    ;;
  full-compat)
    run_sdk_checks
    run_check "repository tests pass" go test ./...
    ;;
  *)
    echo "[FAIL] Unknown VERIFY_STAGE=${VERIFY_STAGE}" >&2
    exit 1
    ;;
esac

echo "=== Pub/Sub full compatibility verification: ${VERIFY_STAGE} ==="
echo "passed=${PASS} failed=${FAIL}"

if [[ "${FAIL}" -ne 0 ]]; then
  exit 1
fi
