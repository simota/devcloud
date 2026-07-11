//! Mirrors the persisted schema from `internal/services/sqs/persistence.rs`.
//!
//! `state.json` is written with legacy `json.NewEncoder(...).Encode(...)`, which
//! appends a trailing newline. The struct tags below reproduce legacy field
//! names and `omitempty` semantics exactly so the Rust store and the legacy
//! dashboard interoperate on the same file during the strangler-fig window.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{DeduplicationState, MessageState, MoveTaskState};

/// Top-level persisted document. `queues` is always emitted; `moveTasks` is
/// omitempty (dropped when empty), matching legacy.
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
    // legacy: `sequence,omitempty` on a uint64 — dropped only when 0.
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

    /// Serializes to the exact bytes legacy writes: compact JSON + a trailing
    /// newline (from `json.Encoder.Encode`).
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec(self).expect("serialize persisted state");
        out.push(b'\n');
        out
    }
}

fn map_ref_is_empty<K, V>(m: &&BTreeMap<K, V>) -> bool {
    m.is_empty()
}

fn slice_ref_is_empty<T>(s: &&[T]) -> bool {
    s.is_empty()
}

/// Borrowing mirror of [`PersistedState`] / [`PersistedQueue`] used by
/// `Server::persist` to serialize `state.json` straight from the live queue
/// map. `persist()` runs on every mutating call, so cloning every message,
/// attribute, and dedup entry into an owned `PersistedState` first (as the
/// naive path would) makes each op's cost grow with total queue size; these
/// `Ref` types serialize directly off `&QueueState`/`&Server` fields instead,
/// producing byte-identical output without the intermediate deep clone.
#[derive(Serialize)]
pub struct PersistedStateRef<'a> {
    pub queues: BTreeMap<&'a str, PersistedQueueRef<'a>>,
    #[serde(rename = "moveTasks", skip_serializing_if = "map_ref_is_empty")]
    pub move_tasks: &'a BTreeMap<String, MoveTaskState>,
}

#[derive(Serialize)]
pub struct PersistedQueueRef<'a> {
    pub name: &'a str,
    pub url: &'a str,
    pub arn: &'a str,
    pub attributes: &'a BTreeMap<String, String>,
    #[serde(skip_serializing_if = "map_ref_is_empty")]
    pub tags: &'a BTreeMap<String, String>,
    #[serde(rename = "createdAt")]
    pub created_at: &'a str,
    #[serde(rename = "modifiedAt", skip_serializing_if = "String::is_empty")]
    pub modified_at: String,
    #[serde(skip_serializing_if = "slice_ref_is_empty")]
    pub messages: &'a [MessageState],
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub sequence: u64,
    #[serde(skip_serializing_if = "map_ref_is_empty")]
    pub dedup: &'a BTreeMap<String, DeduplicationState>,
}

impl<'a> PersistedStateRef<'a> {
    /// Same byte layout as [`PersistedState::to_json_bytes`]: compact JSON +
    /// trailing newline.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec(self).expect("serialize persisted state");
        out.push(b'\n');
        out
    }
}
