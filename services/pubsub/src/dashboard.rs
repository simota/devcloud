//! Dashboard snapshot shapes and read-only projections.
//!
//! Ports `internal/services/pubsub/dashboard.rs`: the in-memory `Snapshot` /
//! `MessageSnapshot` structs the dashboard browses, plus the `Server::snapshot`
//! / `Server::message_snapshot` projections that build them from the shared
//! resource + delivery state. Field declaration order, JSON tags, and
//! `omitempty` mirror the legacy structs so the encoded bodies are byte-identical
//! (serde sorted keys = legacy map marshaling for the free-form sub-objects).
//!
//! These are read-only metadata views: message *payloads* are never included
//! (only message ids and delivery bookkeeping), matching legacy.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{Map, Value};

use crate::server::Server;

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub status: String,
    pub running: bool,
    pub project: String,
    pub topics: Vec<TopicSnapshot>,
    pub subscriptions: Vec<SubscriptionSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicSnapshot {
    pub name: String,
    #[serde(rename = "subscriptionCount")]
    pub subscription_count: usize,
    #[serde(rename = "createdAt", skip_serializing_if = "String::is_empty")]
    pub created_at: String,
    #[serde(rename = "updatedAt", skip_serializing_if = "String::is_empty")]
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionSnapshot {
    pub name: String,
    pub topic: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(rename = "createdAt", skip_serializing_if = "String::is_empty")]
    pub created_at: String,
    #[serde(rename = "updatedAt", skip_serializing_if = "String::is_empty")]
    pub updated_at: String,
    #[serde(rename = "ackDeadlineSeconds")]
    pub ack_deadline_seconds: i64,
    #[serde(rename = "enableMessageOrdering", skip_serializing_if = "is_false")]
    pub enable_message_ordering: bool,
    #[serde(rename = "enableExactlyOnceDelivery", skip_serializing_if = "is_false")]
    pub enable_exactly_once_delivery: bool,
    #[serde(rename = "retainAckedMessages", skip_serializing_if = "is_false")]
    pub retain_acked_messages: bool,
    #[serde(
        rename = "messageRetentionDuration",
        skip_serializing_if = "String::is_empty"
    )]
    pub message_retention_duration: String,
    #[serde(rename = "expirationPolicy", skip_serializing_if = "Option::is_none")]
    pub expiration_policy: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub filter: String,
    #[serde(rename = "deadLetterPolicy", skip_serializing_if = "Option::is_none")]
    pub dead_letter_policy: Option<Value>,
    #[serde(rename = "retryPolicy", skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<Value>,
    #[serde(rename = "pushConfig", skip_serializing_if = "Option::is_none")]
    pub push_config: Option<Value>,
    #[serde(rename = "backlogMessages")]
    pub backlog_messages: i64,
    #[serde(rename = "inFlightMessages")]
    pub in_flight_messages: i64,
    #[serde(rename = "totalRetainedMessages")]
    pub total_retained_messages: i64,
    #[serde(rename = "maxDeliveryAttemptSeen")]
    pub max_delivery_attempt_seen: i64,
    #[serde(rename = "recentDeliveries", skip_serializing_if = "Vec::is_empty")]
    pub recent_deliveries: Vec<DeliverySnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliverySnapshot {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub subscription: String,
    #[serde(rename = "publishTime", skip_serializing_if = "String::is_empty")]
    pub publish_time: String,
    #[serde(rename = "orderingKey", skip_serializing_if = "String::is_empty")]
    pub ordering_key: String,
    pub state: String,
    #[serde(rename = "leaseDeadline", skip_serializing_if = "String::is_empty")]
    pub lease_deadline: String,
    #[serde(rename = "nextDeliveryTime", skip_serializing_if = "String::is_empty")]
    pub next_delivery_time: String,
    #[serde(rename = "deliveryAttempt")]
    pub delivery_attempt: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageSnapshot {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "publishTime", skip_serializing_if = "String::is_empty")]
    pub publish_time: String,
    #[serde(rename = "orderingKey", skip_serializing_if = "String::is_empty")]
    pub ordering_key: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<DeliverySnapshot>,
}

/// `copyAnyMap` parity: `None` / empty object → `None`, else the object as-is.
fn copy_any_map(value: &Option<Value>) -> Option<Value> {
    match value {
        Some(Value::Object(map)) if !map.is_empty() => Some(Value::Object(map.clone())),
        _ => None,
    }
}

/// `safePushConfigSnapshot` parity: keep only a non-empty `pushEndpoint`,
/// dropping any push credentials/auth from the snapshot.
fn safe_push_config_snapshot(config: &Option<Value>) -> Option<Value> {
    let object = config.as_ref().and_then(Value::as_object)?;
    if object.is_empty() {
        return None;
    }
    let endpoint = object
        .get("pushEndpoint")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if endpoint.is_empty() {
        return None;
    }
    let mut safe = Map::new();
    safe.insert(
        "pushEndpoint".to_string(),
        Value::String(endpoint.to_string()),
    );
    Some(Value::Object(safe))
}

impl Server {
    /// Builds the dashboard snapshot, mirroring `(*Server).Snapshot`. Runs the
    /// same lease-expiry + retention cleanup the legacy method does, then projects
    /// topics (with subscription counts) and subscriptions (with delivery
    /// bookkeeping) in sorted-name order.
    pub fn snapshot(&mut self) -> Snapshot {
        let (now_secs, _) = self.now_parts();
        self.expire_leases(now_secs);
        self.cleanup_retained_messages(now_secs);

        let topics = self
            .topics
            .keys()
            .map(|name| {
                let count = self
                    .subscriptions
                    .values()
                    .filter(|sub| sub.topic == *name)
                    .count();
                let topic = &self.topics[name];
                TopicSnapshot {
                    name: name.clone(),
                    subscription_count: count,
                    created_at: topic.created_at.clone(),
                    updated_at: topic.updated_at.clone(),
                }
            })
            .collect();

        let subscriptions = self
            .subscriptions
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|name| {
                let sub = &self.subscriptions[&name];
                let mut snapshot = SubscriptionSnapshot {
                    name: sub.name.clone(),
                    topic: sub.topic.clone(),
                    labels: sub.labels.clone(),
                    created_at: sub.created_at.clone(),
                    updated_at: sub.updated_at.clone(),
                    ack_deadline_seconds: sub.ack_deadline_seconds,
                    enable_message_ordering: sub.enable_message_ordering,
                    enable_exactly_once_delivery: sub.enable_exactly_once_delivery,
                    retain_acked_messages: sub.retain_acked_messages,
                    message_retention_duration: sub.message_retention_duration.clone(),
                    expiration_policy: copy_any_map(&sub.expiration_policy),
                    filter: sub.filter.clone(),
                    dead_letter_policy: copy_any_map(&sub.dead_letter_policy),
                    retry_policy: copy_any_map(&sub.retry_policy),
                    push_config: safe_push_config_snapshot(&sub.push_config),
                    backlog_messages: 0,
                    in_flight_messages: 0,
                    total_retained_messages: 0,
                    max_delivery_attempt_seen: 0,
                    recent_deliveries: Vec::new(),
                };
                let deliveries = self.deliveries.get(&name).cloned().unwrap_or_default();
                for delivery in &deliveries {
                    if delivery.acked {
                        continue;
                    }
                    snapshot.total_retained_messages += 1;
                    if delivery.delivery_attempt > snapshot.max_delivery_attempt_seen {
                        snapshot.max_delivery_attempt_seen = delivery.delivery_attempt;
                    }
                    if crate::delivery::after(&delivery.lease_deadline, now_secs) {
                        snapshot.in_flight_messages += 1;
                    } else {
                        snapshot.backlog_messages += 1;
                    }
                    snapshot
                        .recent_deliveries
                        .push(self.delivery_snapshot(delivery, now_secs));
                }
                if snapshot.recent_deliveries.len() > 20 {
                    let start = snapshot.recent_deliveries.len() - 20;
                    snapshot.recent_deliveries.drain(0..start);
                }
                snapshot
            })
            .collect();

        Snapshot {
            status: "running".to_string(),
            running: true,
            project: self.snapshot_project(),
            topics,
            subscriptions,
        }
    }

    /// Mirrors `(*Server).deliverySnapshotLocked`.
    fn delivery_snapshot(
        &self,
        delivery: &crate::model::DeliveryRecord,
        now_secs: i64,
    ) -> DeliverySnapshot {
        let mut state = "backlog";
        let mut lease_deadline = String::new();
        let mut next_delivery_time = String::new();
        if crate::delivery::after(&delivery.lease_deadline, now_secs) {
            state = "in-flight";
            lease_deadline = delivery.lease_deadline.clone();
        } else if crate::delivery::after(&delivery.next_delivery_time, now_secs) {
            state = "delayed";
            next_delivery_time = delivery.next_delivery_time.clone();
        }
        let message = self.messages.get(&delivery.message_id);
        DeliverySnapshot {
            message_id: delivery.message_id.clone(),
            subscription: String::new(),
            publish_time: message.map(|m| m.publish_time.clone()).unwrap_or_default(),
            ordering_key: message.map(|m| m.ordering_key.clone()).unwrap_or_default(),
            state: state.to_string(),
            lease_deadline,
            next_delivery_time,
            delivery_attempt: delivery.delivery_attempt,
        }
    }

    /// Builds the per-message delivery snapshot, mirroring
    /// `(*Server).MessageSnapshot`. Returns `None` when the message is absent.
    pub fn message_snapshot(&mut self, message_id: &str) -> Option<MessageSnapshot> {
        let (now_secs, _) = self.now_parts();
        self.cleanup_retained_messages(now_secs);
        let message = self.messages.get(message_id)?.clone();
        let mut snapshot = MessageSnapshot {
            message_id: message.message_id.clone(),
            publish_time: message.publish_time.clone(),
            ordering_key: message.ordering_key.clone(),
            subscriptions: Vec::new(),
        };
        for (sub_name, deliveries) in &self.deliveries {
            for delivery in deliveries {
                if delivery.message_id != message_id || delivery.acked {
                    continue;
                }
                let mut delivery_snapshot = self.delivery_snapshot(delivery, now_secs);
                delivery_snapshot.subscription = sub_name.clone();
                snapshot.subscriptions.push(delivery_snapshot);
            }
        }
        snapshot
            .subscriptions
            .sort_by(|a, b| a.subscription.cmp(&b.subscription));
        Some(snapshot)
    }
}
