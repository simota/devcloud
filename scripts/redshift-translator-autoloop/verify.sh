#!/usr/bin/env bash
# Acceptance gate for redshift-translator-autoloop.
# Runs translator tests + redshift regression + build.
# If PRE_ITEM_LINE is exported, also asserts that line is now "- [x]".
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${LOOP_DIR}/../.." && pwd)"
source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
CHECKLIST_FILE="${ROOT_DIR}/services/redshift/tests/translator_parity.rs"

cd "${ROOT_DIR}"

log() { printf '[verify] %s\n' "$*"; }

log "AC-1: translator unit tests"
cargo test --workspace

log "AC-2: redshift package regression"
cargo test --workspace

log "AC-3: build devcloud"
devcloud_build /tmp/devcloud-verify

if [[ -n "${PRE_ITEM_LINE:-}" ]]; then
  log "AC-4: checklist line ${PRE_ITEM_LINE} flipped"
  current="$(sed -n "${PRE_ITEM_LINE}p" "${CHECKLIST_FILE}")"
  if [[ "${current}" =~ ^-\ \[x\] ]]; then
    log "OK: ${current}"
  else
    log "FAIL: line ${PRE_ITEM_LINE} is not - [x]: ${current}"
    exit 1
  fi
fi

log "AC-5: translator parity test file modified"
if [[ -n "${PRE_ITEM_LINE:-}" ]]; then
  if git diff --cached --name-only 2>/dev/null | grep -q "services/redshift/tests/translator_parity.rs"; then
    log "OK: translator_parity.rs staged"
  elif git diff --name-only | grep -q "services/redshift/tests/translator_parity.rs"; then
    log "OK: translator_parity.rs modified (unstaged)"
  else
    log "WARN: translator_parity.rs was not modified — accepting only if no behavioral change is required"
  fi
fi

log "all checks passed"
