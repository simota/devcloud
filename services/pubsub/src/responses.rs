//! Typed list-response envelopes.
//!
//! legacy builds list bodies as `map[string]any{"<collection>": [...],
//! "nextPageToken": ...}`. `json.Encoder` sorts the **map** keys but keeps each
//! element struct's fields in declaration order. Routing the element structs
//! through `serde_json::Value` would re-sort their fields, so these envelopes
//! serialize the typed structs directly. `nextPageToken` sorts before every
//! collection name (`snapshots`/`subscriptions`/`topics`), so declaring it first
//! reproduces legacy sorted-key output.

use serde::Serialize;

use crate::model::{Schema, Snapshot, Subscription, Topic};

/// `{"nextPageToken": ..., "topics": [...]}`.
#[derive(Debug, Serialize)]
pub struct ListTopicsResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
    pub topics: Vec<Topic>,
}

/// `{"nextPageToken": ..., "subscriptions": [...]}` of full subscriptions.
#[derive(Debug, Serialize)]
pub struct ListSubscriptionsResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
    pub subscriptions: Vec<Subscription>,
}

/// `{"nextPageToken": ..., "subscriptions": [...]}` of subscription names.
#[derive(Debug, Serialize)]
pub struct ListSubscriptionNamesResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
    pub subscriptions: Vec<String>,
}

/// `{"nextPageToken": ..., "snapshots": [...]}` of snapshot names.
#[derive(Debug, Serialize)]
pub struct ListSnapshotNamesResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
    pub snapshots: Vec<String>,
}

/// `{"nextPageToken": ..., "snapshots": [...]}` of full snapshots.
#[derive(Debug, Serialize)]
pub struct ListSnapshotsResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
    pub snapshots: Vec<Snapshot>,
}

/// `{"nextPageToken": ..., "schemas": [...]}` of full schemas.
#[derive(Debug, Serialize)]
pub struct ListSchemasResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: String,
    pub schemas: Vec<Schema>,
}
