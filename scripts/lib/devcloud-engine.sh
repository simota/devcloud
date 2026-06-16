#!/usr/bin/env bash
# Shared devcloud launcher helper for e2e / autoloop verify scripts.
#
# compatibility-removal roadmap Phase 4 (docs/ROADMAP.md): acceptance gates
# launch the Rust orchestrator binary (single in-process supervisor). The former
# Dashboard fallback paths were removed during Rust workspace consolidation.
#
# Usage (after ROOT_DIR is defined):
#   source "${ROOT_DIR}/scripts/lib/devcloud-engine.sh"
#   devcloud_build "${TMP_DIR}/devcloud"   # build + place the launcher binary at $1
#   "${TMP_DIR}/devcloud" up               # unchanged — same CLI UX as the Rust binary
#
# Resolve the repository root from this file's location (scripts/lib/ -> repo root).
_devcloud_engine_root() {
  ( cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd )
}

# devcloud_build <dest>
#   Builds the Rust devcloud launcher and copies it to <dest>,
#   so callers can keep invoking "<dest> up | reset | dashboard" unchanged.
devcloud_build() {
  local dest="$1"
  local root
  root="$(_devcloud_engine_root)"
  ( cd "${root}" && cargo build --quiet -p devcloud-orchestrator ) || return 1
  cp "${root}/target/debug/devcloud" "${dest}"
}
