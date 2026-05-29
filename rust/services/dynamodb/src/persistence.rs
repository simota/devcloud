//! `state.json` persistence — byte-compatible with the Go service.
//!
//! Mirrors `persistedState` / `persistedTable` in
//! `internal/services/dynamodb/persistence.go`. The Go service writes
//! `state.json` via `json.NewEncoder(file).Encode` — **compact, trailing
//! newline**, struct fields in declaration order, map keys sorted. `tables` is
//! always present; the other three top-level maps and the per-table optional
//! fields all use Go's `omitempty` (an empty map/slice/string/nil pointer is
//! dropped). `to_state_json` routes through `go_json::to_vec`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{BackupDescription, ContinuousBackupsDescription, Item, TableDescription};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistedState {
    #[serde(default)]
    pub tables: BTreeMap<String, PersistedTable>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub backups: BTreeMap<String, BackupDescription>,
    #[serde(
        rename = "backupTables",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub backup_tables: BTreeMap<String, TableDescription>,
    #[serde(
        rename = "backupItems",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub backup_items: BTreeMap<String, BTreeMap<String, Item>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedTable {
    pub description: TableDescription,
    #[serde(default)]
    pub items: BTreeMap<String, Item>,
    #[serde(
        rename = "streamRecords",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub stream_records: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(
        rename = "continuousBackups",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub continuous_backups: Option<ContinuousBackupsDescription>,
    #[serde(
        rename = "resourcePolicy",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub resource_policy: String,
    #[serde(
        rename = "resourcePolicyRevision",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub resource_policy_revision: String,
}

impl PersistedState {
    /// Decodes a `state.json` byte buffer.
    pub fn from_slice(data: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(data)
    }

    /// Encodes to the exact `state.json` byte layout the Go service writes:
    /// compact, struct-field-ordered, sorted map keys, HTML-escaped, trailing
    /// newline.
    pub fn to_state_json(&self) -> Vec<u8> {
        crate::go_json::to_vec(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_state_round_trips_byte_for_byte() {
        // Golden oracle captured from the Go service (CreateTable + PutItem with
        // `<`, `>`, `&`, nested L/M, BOOL, N attributes).
        let fixture = include_bytes!("../tests/fixtures/state_basic.json");
        let state = PersistedState::from_slice(fixture).expect("decode fixture");
        let encoded = state.to_state_json();
        assert_eq!(
            encoded,
            fixture.to_vec(),
            "re-encoded state.json must match the Go oracle byte-for-byte\n--- got ---\n{}\n--- want ---\n{}",
            String::from_utf8_lossy(&encoded),
            String::from_utf8_lossy(fixture),
        );
    }

    #[test]
    fn empty_state_emits_only_tables() {
        // `tables` has no omitempty; the other three top-level maps are dropped
        // when empty, matching Go.
        let state = PersistedState::default();
        assert_eq!(state.to_state_json(), b"{\"tables\":{}}\n".to_vec());
    }
}
