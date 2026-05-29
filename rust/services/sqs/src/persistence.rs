//! Mirrors the persisted schema from `internal/services/sqs/persistence.go`.
//!
//! `state.json` is written with Go's `json.NewEncoder(...).Encode(...)`, which
//! appends a trailing newline. The struct tags below reproduce Go's field
//! names and `omitempty` semantics exactly so the Rust store and the Go
//! dashboard interoperate on the same file during the strangler-fig window.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{DeduplicationState, MessageState, MoveTaskState};

/// Top-level persisted document. `queues` is always emitted; `moveTasks` is
/// omitempty (dropped when empty), matching Go.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PersistedState {
    #[serde(rename = "queues", default)]
    pub queues: BTreeMap<String, PersistedQueue>,
    #[serde(
        rename = "moveTasks",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub move_tasks: BTreeMap<String, MoveTaskState>,
}

/// Mirrors `persistedQueue`. `name`/`url`/`arn`/`attributes`/`createdAt` are
/// always present; `tags`/`modifiedAt`/`messages`/`sequence`/`dedup` are
/// omitempty.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PersistedQueue {
    #[serde(rename = "name", default)]
    pub name: String,
    #[serde(rename = "url", default)]
    pub url: String,
    #[serde(rename = "arn", default)]
    pub arn: String,
    #[serde(rename = "attributes", default)]
    pub attributes: BTreeMap<String, String>,
    #[serde(rename = "tags", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(rename = "createdAt", default)]
    pub created_at: String,
    #[serde(
        rename = "modifiedAt",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub modified_at: String,
    #[serde(rename = "messages", default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<MessageState>,
    // Go: `sequence,omitempty` on a uint64 — dropped only when 0.
    #[serde(rename = "sequence", default, skip_serializing_if = "is_zero_u64")]
    pub sequence: u64,
    #[serde(rename = "dedup", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dedup: BTreeMap<String, DeduplicationState>,
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

impl PersistedState {
    /// Parses `state.json` content. Mirrors the decode half of `Server.load`.
    pub fn from_json(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }

    /// Serializes to the exact bytes Go writes: compact JSON + a trailing
    /// newline (from `json.Encoder.Encode`).
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec(self).expect("serialize persisted state");
        out.push(b'\n');
        out
    }
}
