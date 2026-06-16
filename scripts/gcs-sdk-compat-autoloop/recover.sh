#!/usr/bin/env bash
set -euo pipefail

LOOP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${LOOP_DIR}/state.env"

if [[ ! -f "${STATE_FILE}" ]]; then
  bash "${LOOP_DIR}/bootstrap.sh"
  exit 0
fi

if [[ -f "${STATE_FILE}.sha256" ]]; then
  expected="$(cat "${STATE_FILE}.sha256")"
  actual="$(shasum -a 256 "${STATE_FILE}" | awk '{print $1}')"
  if [[ "${expected}" == "${actual}" ]]; then
    echo "NEXUS_LOOP_STATUS: READY"
    echo "NEXUS_LOOP_SUMMARY: state.env checksum is valid; no recovery needed."
    exit 0
  fi
fi

backup="${STATE_FILE}.corrupt.$(date -u +"%Y%m%dT%H%M%SZ")"
cp "${STATE_FILE}" "${backup}"
bash "${LOOP_DIR}/bootstrap.sh"

echo "NEXUS_LOOP_STATUS: READY"
echo "NEXUS_LOOP_SUMMARY: Rebuilt state.env and preserved corrupt copy at ${backup}."
