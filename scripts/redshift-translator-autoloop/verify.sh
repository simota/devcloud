#!/usr/bin/env bash
# Acceptance gate for redshift-translator-autoloop.
# Runs translator tests + redshift regression + build.
# If PRE_ITEM_LINE is exported, also asserts that line is now "- [x]".
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${LOOP_DIR}/../.." && pwd)"
CHECKLIST_FILE="${ROOT_DIR}/internal/services/redshift/translator/COMPATIBILITY.md"

cd "${ROOT_DIR}"

log() { printf '[verify] %s\n' "$*"; }

log "AC-1: translator unit tests"
go test ./internal/services/redshift/translator/...

log "AC-2: redshift package regression"
go test ./internal/services/redshift/...

log "AC-3: build devcloud"
go build -o /tmp/devcloud-verify ./cmd/devcloud

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

log "AC-5: translator test file modified"
if [[ -n "${PRE_ITEM_LINE:-}" ]]; then
  if git diff --cached --name-only 2>/dev/null | grep -q "internal/services/redshift/translator/translator_test.go"; then
    log "OK: translator_test.go staged"
  elif git diff --name-only | grep -q "internal/services/redshift/translator/translator_test.go"; then
    log "OK: translator_test.go modified (unstaged)"
  else
    log "WARN: translator_test.go was not modified — accepting only if no behavioral change is required"
  fi
fi

log "all checks passed"
