//! `resources.json` + `pubsub.json` persistence — byte-compatible with the legacy
//! service.
//!
//! Mirrors `resourceFile` / `messageStateFile` in
//! `internal/services/pubsub/types.rs`. Both are written with
//! `json.MarshalIndent(v, "", "  ")` (2-space indent, no trailing newline). In
//! `resourceFile`, `topics` and `subscriptions` are always present (empty array
//! when unused); everything else is `omitempty`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{DeliveryRecord, PubsubMessage, Schema, Snapshot, Subscription, Topic};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceFile {
    #[serde(default)]
    pub topics: Vec<Topic>,
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshots: Vec<Snapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<Schema>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<PubsubMessage>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deliveries: BTreeMap<String, Vec<DeliveryRecord>>,
    #[serde(rename = "nextMessageId", default, skip_serializing_if = "is_zero_u64")]
    pub next_message_id: u64,
    #[serde(rename = "nextAckId", default, skip_serializing_if = "is_zero_u64")]
    pub next_ack_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MessageStateFile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<PubsubMessage>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deliveries: BTreeMap<String, Vec<DeliveryRecord>>,
    #[serde(rename = "nextMessageId", default, skip_serializing_if = "is_zero_u64")]
    pub next_message_id: u64,
    #[serde(rename = "nextAckId", default, skip_serializing_if = "is_zero_u64")]
    pub next_ack_id: u64,
}

fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

impl ResourceFile {
    pub fn from_slice(data: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(data)
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        crate::wire_json::to_vec_indent(self)
    }
}

impl MessageStateFile {
    pub fn from_slice(data: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(data)
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        crate::wire_json::to_vec_indent(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_resources_round_trip_byte_for_byte() {
        let fixture = include_bytes!("../tests/fixtures/resources_topic.json");
        let file = ResourceFile::from_slice(fixture).expect("decode");
        assert_eq!(
            file.to_bytes(),
            fixture.to_vec(),
            "re-encoded resources.json must match the legacy oracle byte-for-byte\n--- got ---\n{}\n--- want ---\n{}",
            String::from_utf8_lossy(&file.to_bytes()),
            String::from_utf8_lossy(fixture),
        );
    }

    #[test]
    fn empty_message_state_is_object() {
        let fixture = include_bytes!("../tests/fixtures/msgstate_empty.json");
        let file = MessageStateFile::from_slice(fixture).expect("decode");
        assert_eq!(file.to_bytes(), fixture.to_vec());
    }

    #[test]
    fn empty_resource_file_emits_topics_and_subscriptions() {
        let file = ResourceFile::default();
        assert_eq!(
            String::from_utf8_lossy(&file.to_bytes()),
            "{\n  \"topics\": [],\n  \"subscriptions\": []\n}"
        );
    }
}
