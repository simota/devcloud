//! Mirrors `internal/services/mail/model.rs`.
//!
//! Serialization is pinned to legacy `encoding/json` output so that, during the
//! strangler-fig window, the legacy dashboard can read the same `messages.jsonl`
//! the Rust store writes:
//!   * field order follows the legacy struct declaration order;
//!   * `omitempty` fields are skipped when empty (`skip_serializing_if`);
//!   * `Headers` uses a `BTreeMap` so keys serialize in sorted order, matching
//!     legacy map marshaling;
//!   * timestamps are RFC 3339 UTC strings (the only form ever observed for the
//!     legacy `time.Time` in this service — no time arithmetic is performed).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Attachment metadata. Mirrors legacy `Attachment`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub id: String,
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "contentType")]
    pub content_type: String,
    pub size: i64,
    pub blob: String,
}

/// A stored mail message. Mirrors legacy `Message`. Field order and `omitempty`
/// semantics match the legacy struct tags exactly.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: Vec<String>,
    #[serde(default)]
    pub subject: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, Vec<String>>,
    /// Blob id of the raw message (filled by the store).
    #[serde(default)]
    pub raw: String,
    #[serde(rename = "textBody", default, skip_serializing_if = "String::is_empty")]
    pub text_body: String,
    #[serde(rename = "htmlBody", default, skip_serializing_if = "String::is_empty")]
    pub html_body: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    /// RFC 3339 UTC. The store guarantees this is set, so it is always present
    /// in persisted records (matching legacy, which always emits `receivedAt`).
    /// `skip_serializing_if` only guards the unreachable zero-value case so we
    /// never emit JSON `null`, which legacy never produces.
    #[serde(
        rename = "receivedAt",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub received_at: Option<String>,
    /// RFC 3339 UTC tombstone marker. Mirrors legacy `*time.Time` with `omitempty`.
    #[serde(rename = "deletedAt", default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,
    #[serde(
        rename = "parseError",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub parse_error: String,
}

/// SMTP envelope (reverse-path + forward-paths). Mirrors legacy `Envelope`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Envelope {
    pub from: String,
    pub to: Vec<String>,
}

/// Pagination input for listing messages. Mirrors legacy `ListMessagesInput`.
#[derive(Clone, Debug, Default)]
pub struct ListMessagesInput {
    pub limit: i32,
    pub cursor: String,
}

/// Result of listing messages. Mirrors legacy `ListMessagesResult`; `messages`
/// always serializes as a JSON array (never `null`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ListMessagesResult {
    #[serde(default)]
    pub messages: Vec<Message>,
    #[serde(rename = "nextCursor", default)]
    pub next_cursor: String,
}
