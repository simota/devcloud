//! Mirrors the message lifecycle from
//! `internal/services/sqs/{message_handlers,message_core}.rs`:
//! send (+FIFO dedup), receive (visibility + FIFO group blocking + DLQ
//! redrive), delete, change-visibility, their batch variants, retention
//! cleanup, and the `receivedMessage` projection.
//!
//! Receive long-polling is implemented as a bounded `std::thread::sleep` loop
//! (this crate has no async runtime); `wait_time_seconds == 0` returns
//! immediately, matching the legacy fast path. The legacy wait-channel is purely an
//! early-wake optimization and is not behaviorally required.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::hashing::{md5_hex, md5_of_message_attributes, MessageAttributeValue};
use crate::model::{DeduplicationState, MessageState};
use crate::server::{
    is_fifo_queue, queue_name_from_url, QueueState, Server, MAX_DELAY_SECONDS,
    MAX_VISIBILITY_TIMEOUT_SECONDS,
};
use crate::time_fmt::{add_seconds, before, is_zero, now_rfc3339, unix_millis_from_rfc3339};
use crate::validation::{
    valid_batch_entry_id, valid_message_body, validate_message_attribute_name,
    validate_message_attribute_value, validate_message_system_attribute,
};

const FIFO_DEDUPLICATION_WINDOW_SECONDS: i64 = 5 * 60;

// --- request / result DTOs (the subset the logic needs) ---

#[derive(Clone, Debug, Default)]
pub struct SendMessageRequest {
    pub queue_url: String,
    pub message_body: String,
    pub delay_seconds: Option<i64>,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_system_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_group_id: String,
    pub message_deduplication_id: String,
}

#[derive(Clone, Debug, Default)]
pub struct SendMessageBatchEntry {
    pub id: String,
    pub message_body: String,
    pub delay_seconds: Option<i64>,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_system_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_group_id: String,
    pub message_deduplication_id: String,
}

#[derive(Clone, Debug, Default)]
pub struct ReceiveMessageRequest {
    pub queue_url: String,
    pub max_number_of_messages: Option<i64>,
    pub visibility_timeout: Option<i64>,
    pub wait_time_seconds: Option<i64>,
    pub attribute_names: Vec<String>,
    pub message_attribute_names: Vec<String>,
    pub message_system_attribute_names: Vec<String>,
}

/// Mirrors legacy `receivedMessage` (response projection).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub message_id: String,
    pub receipt_handle: String,
    pub md5_of_message_body: String,
    pub md5_of_message_attributes: String,
    pub md5_of_message_system_attributes: String,
    pub body: String,
    pub attributes: BTreeMap<String, String>,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
}

#[derive(Clone, Debug, Default)]
pub struct SendMessageBatchResultEntry {
    pub id: String,
    pub message_id: String,
    pub md5_of_message_body: String,
    pub md5_of_message_attributes: String,
    pub md5_of_message_system_attributes: String,
    pub sequence_number: String,
}

#[derive(Clone, Debug, Default)]
pub struct BatchResultErrorEntry {
    pub id: String,
    pub sender_fault: bool,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Default)]
pub struct SendMessageBatchResult {
    pub successful: Vec<SendMessageBatchResultEntry>,
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Clone, Debug, Default)]
pub struct IdResultEntry {
    pub id: String,
}

#[derive(Clone, Debug, Default)]
pub struct BatchResult {
    pub successful: Vec<IdResultEntry>,
    pub failed: Vec<BatchResultErrorEntry>,
}

#[derive(Clone, Debug, Default)]
pub struct DeleteMessageBatchEntry {
    pub id: String,
    pub receipt_handle: String,
}

#[derive(Clone, Debug, Default)]
pub struct ChangeMessageVisibilityBatchEntry {
    pub id: String,
    pub receipt_handle: String,
    pub visibility_timeout: i64,
}

impl Server {
    // --- send (mirror message_handlers.rs sendMessage) ---

    pub fn send_message(&mut self, input: &SendMessageRequest) -> Result<MessageState, String> {
        if input.queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        if input.message_body.is_empty() {
            return Err("MessageBody is required".into());
        }
        let max_bytes = self.max_message_bytes();
        if max_bytes > 0 && input.message_body.len() as i64 > max_bytes {
            return Err("MessageBody exceeds maximum message size".into());
        }
        if !valid_message_body(&input.message_body) {
            return Err("MessageBody contains invalid characters".into());
        }
        for (name, attr) in &input.message_attributes {
            validate_message_attribute_name(name)?;
            validate_message_attribute_value(name, attr)?;
        }
        for (name, attr) in &input.message_system_attributes {
            validate_message_system_attribute(name, attr)?;
        }
        let name = queue_name_from_url(&input.queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let previous_queue = self.queues.get(&name).cloned().unwrap();

        let fifo = is_fifo_queue(self.queues.get(&name).unwrap());
        if fifo && input.delay_seconds.is_some() {
            return Err("DelaySeconds is not supported for FIFO queue messages".into());
        }
        let now = now_rfc3339();
        cleanup_expired_messages(self.queues.get_mut(&name).unwrap(), &now);

        let queue = self.queues.get(&name).unwrap();
        let mut delay_seconds = int_attribute(&queue.attributes, "DelaySeconds", 0);
        if let Some(d) = input.delay_seconds {
            delay_seconds = d;
        }
        if delay_seconds < 0 {
            return Err("DelaySeconds must be non-negative".into());
        }
        if delay_seconds > MAX_DELAY_SECONDS {
            return Err("DelaySeconds must be no greater than 900".into());
        }

        let mut message = MessageState {
            id: new_opaque_id("msg"),
            body: input.message_body.clone(),
            body_md5: md5_hex(&input.message_body),
            attributes: input.message_attributes.clone(),
            system_attributes: input.message_system_attributes.clone(),
            sent_at: now.clone(),
            available_at: add_seconds(&now, delay_seconds),
            message_group_id: input.message_group_id.clone(),
            ..Default::default()
        };

        if fifo {
            let dedup_id = fifo_deduplication_id(queue, input)?;
            cleanup_expired_deduplication(self.queues.get_mut(&name).unwrap(), &now);
            let queue = self.queues.get(&name).unwrap();
            if let Some(deduped) = queue.dedup.get(&dedup_id) {
                if before(&now, &deduped.expires_at) {
                    if let Some(m) = &deduped.message {
                        return Ok(m.clone());
                    }
                }
            }
            let queue = self.queues.get_mut(&name).unwrap();
            queue.sequence += 1;
            message.deduplication_id = dedup_id.clone();
            message.sequence_number = queue.sequence.to_string();
            queue.dedup.insert(
                dedup_id,
                DeduplicationState {
                    expires_at: add_seconds(&now, FIFO_DEDUPLICATION_WINDOW_SECONDS),
                    message: Some(message.clone()),
                },
            );
        }

        let queue = self.queues.get_mut(&name).unwrap();
        queue.messages.push(message.clone());
        if let Err(e) = self.persist() {
            self.queues.insert(name, previous_queue);
            return Err(e);
        }
        Ok(message)
    }

    pub fn send_message_batch(
        &mut self,
        queue_url: &str,
        entries: &[SendMessageBatchEntry],
    ) -> Result<SendMessageBatchResult, String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        validate_batch_entries(entries.iter().map(|e| e.id.as_str()))?;
        let mut result = SendMessageBatchResult::default();
        for entry in entries {
            let req = SendMessageRequest {
                queue_url: queue_url.to_string(),
                message_body: entry.message_body.clone(),
                delay_seconds: entry.delay_seconds,
                message_attributes: entry.message_attributes.clone(),
                message_system_attributes: entry.message_system_attributes.clone(),
                message_group_id: entry.message_group_id.clone(),
                message_deduplication_id: entry.message_deduplication_id.clone(),
            };
            match self.send_message(&req) {
                Ok(m) => result.successful.push(SendMessageBatchResultEntry {
                    id: entry.id.clone(),
                    message_id: m.id,
                    md5_of_message_body: m.body_md5,
                    md5_of_message_attributes: md5_of_message_attributes(&m.attributes),
                    md5_of_message_system_attributes: md5_of_message_attributes(
                        &m.system_attributes,
                    ),
                    sequence_number: m.sequence_number,
                }),
                Err(e) => result.failed.push(batch_error(&entry.id, &e)),
            }
        }
        Ok(result)
    }

    // --- receive (mirror message_core.rs) ---

    /// Long-poll receive. `wait_time_seconds == 0` returns immediately.
    pub fn receive_messages(
        &mut self,
        input: &ReceiveMessageRequest,
    ) -> Result<Vec<ReceivedMessage>, String> {
        if input.queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        let name = queue_name_from_url(&input.queue_url);
        if name.is_empty() {
            return Err("queue does not exist".into());
        }
        let mut max_messages = input.max_number_of_messages.unwrap_or(1);
        if max_messages < 1 {
            max_messages = 1;
        }
        if max_messages > self.max_receive_batch_size() {
            max_messages = self.max_receive_batch_size();
        }
        let wait_seconds = input
            .wait_time_seconds
            .unwrap_or_else(|| self.default_receive_wait_time_seconds());
        if wait_seconds < 0 {
            return Err("WaitTimeSeconds must be non-negative".into());
        }
        if wait_seconds > 20 {
            return Err("WaitTimeSeconds must be no greater than 20".into());
        }

        let attempts = wait_seconds * 10 + 1; // ~100ms slices, mirroring the legacy loop
        for attempt in 0..attempts.max(1) {
            let messages = self.receive_available_messages(
                &name,
                max_messages,
                input.visibility_timeout,
                &input.message_attribute_names,
                &requested_system_attribute_names(input),
            )?;
            if !messages.is_empty() || attempt == attempts.max(1) - 1 {
                return Ok(messages);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(Vec::new())
    }

    pub(crate) fn receive_available_messages(
        &mut self,
        name: &str,
        max_messages: i64,
        visibility_override: Option<i64>,
        message_attribute_names: &[String],
        system_attribute_names: &[String],
    ) -> Result<Vec<ReceivedMessage>, String> {
        if !self.queues.contains_key(name) {
            return Err("queue does not exist".into());
        }
        let previous_queues = self.queues.clone();
        let now = now_rfc3339();
        cleanup_expired_messages(self.queues.get_mut(name).unwrap(), &now);

        let queue = self.queues.get(name).unwrap();
        let default_vis = self.default_visibility_timeout_seconds();
        let mut visibility = int_attribute(&queue.attributes, "VisibilityTimeout", default_vis);
        if let Some(v) = visibility_override {
            visibility = v;
        }
        if visibility < 0 {
            return Err("VisibilityTimeout must be non-negative".into());
        }
        if visibility > MAX_VISIBILITY_TIMEOUT_SECONDS {
            return Err("VisibilityTimeout must be no greater than 43200".into());
        }

        let fifo = is_fifo_queue(queue);
        let mut messages: Vec<ReceivedMessage> = Vec::new();
        let mut blocked_groups: HashSet<String> = HashSet::new();
        let mut delivered_groups: HashSet<String> = HashSet::new();
        let mut changed = false;
        // Indices of messages to redrive to a DLQ after the scan (need &mut self).
        let count = self.queues.get(name).unwrap().messages.len();
        for i in 0..count {
            if messages.len() as i64 >= max_messages {
                break;
            }
            // Re-borrow per iteration; redrive mutates other queues.
            let (deleted, group, available_at, invisible_until) = {
                let m = &self.queues.get(name).unwrap().messages[i];
                (
                    m.deleted,
                    m.message_group_id.clone(),
                    m.available_at.clone(),
                    m.invisible_until.clone(),
                )
            };
            if deleted {
                continue;
            }
            if fifo
                && !group.is_empty()
                && (blocked_groups.contains(&group) || delivered_groups.contains(&group))
            {
                continue;
            }
            if before(&now, &available_at) || before(&now, &invisible_until) {
                if fifo && !group.is_empty() {
                    blocked_groups.insert(group);
                }
                continue;
            }
            if self.move_to_dlq_if_needed(name, i, &now) {
                changed = true;
                if fifo && !group.is_empty() {
                    blocked_groups.insert(group);
                }
                continue;
            }
            // Deliver.
            let received = {
                let q = self.queues.get_mut(name).unwrap();
                let m = &mut q.messages[i];
                m.receive_count += 1;
                if is_zero(&m.first_receive_at) {
                    m.first_receive_at = now.clone();
                }
                m.receipt_handle = new_opaque_id("rct");
                m.invisible_until = add_seconds(&now, visibility);
                received_message_from_state(m, message_attribute_names, system_attribute_names)
            };
            messages.push(received);
            changed = true;
            if fifo && !group.is_empty() {
                delivered_groups.insert(group);
            }
        }
        if changed {
            if let Err(e) = self.persist() {
                self.queues = previous_queues;
                return Err(e);
            }
        }
        Ok(messages)
    }

    /// Mirrors `moveToDeadLetterQueueIfNeededLocked`. Returns true if message at
    /// `messages[i]` of queue `name` was redriven (and is now tombstoned).
    fn move_to_dlq_if_needed(&mut self, name: &str, i: usize, now: &str) -> bool {
        let (policy, source_arn, receive_count) = {
            let queue = self.queues.get(name).unwrap();
            match redrive_policy_from_queue(queue) {
                Some(p) => (p, queue.arn.clone(), queue.messages[i].receive_count),
                None => return false,
            }
        };
        if receive_count < policy.max_receive_count {
            return false;
        }
        // Find the DLQ by ARN.
        let dlq_name = self
            .queues
            .iter()
            .find(|(_, q)| q.arn == policy.dead_letter_target_arn)
            .map(|(n, _)| n.clone());
        let dlq_name = match dlq_name {
            None => return false,
            Some(n) => n,
        };
        let mut moved = self.queues.get(name).unwrap().messages[i].clone();
        moved.available_at = now.to_string();
        moved.invisible_until = crate::model::ZERO_TIME.to_string();
        moved.receipt_handle = String::new();
        moved.receive_count = 0;
        moved.first_receive_at = crate::model::ZERO_TIME.to_string();
        moved.deleted = false;
        moved.dead_letter_source_arn = source_arn;
        self.queues.get_mut(&dlq_name).unwrap().messages.push(moved);
        let m = &mut self.queues.get_mut(name).unwrap().messages[i];
        m.deleted = true;
        m.receipt_handle = String::new();
        true
    }

    // --- delete / change visibility (mirror message_core.rs) ---

    pub fn delete_message(&mut self, queue_url: &str, receipt_handle: &str) -> Result<(), String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        if receipt_handle.is_empty() {
            return Err("ReceiptHandle is required".into());
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let now = now_rfc3339();
        let idx = self
            .queues
            .get(&name)
            .unwrap()
            .messages
            .iter()
            .position(|m| !m.deleted && m.receipt_handle == receipt_handle);
        let idx = match idx {
            None => return Err("receipt handle is invalid".into()),
            Some(i) => i,
        };
        let expired = !before(
            &now,
            &self.queues.get(&name).unwrap().messages[idx].invisible_until,
        );
        let previous_message = self.queues.get(&name).unwrap().messages[idx].clone();
        if expired {
            self.queues.get_mut(&name).unwrap().messages[idx].receipt_handle = String::new();
            if let Err(e) = self.persist() {
                self.queues.get_mut(&name).unwrap().messages[idx] = previous_message;
                return Err(e);
            }
            return Err("receipt handle is invalid".into());
        }
        let m = &mut self.queues.get_mut(&name).unwrap().messages[idx];
        m.deleted = true;
        m.receipt_handle = String::new();
        if let Err(e) = self.persist() {
            self.queues.get_mut(&name).unwrap().messages[idx] = previous_message;
            return Err(e);
        }
        Ok(())
    }

    pub fn change_message_visibility(
        &mut self,
        queue_url: &str,
        receipt_handle: &str,
        visibility_seconds: i64,
    ) -> Result<(), String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        if receipt_handle.is_empty() {
            return Err("ReceiptHandle is required".into());
        }
        if visibility_seconds < 0 {
            return Err("VisibilityTimeout must be non-negative".into());
        }
        if visibility_seconds > MAX_VISIBILITY_TIMEOUT_SECONDS {
            return Err("VisibilityTimeout must be no greater than 43200".into());
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let now = now_rfc3339();
        let idx = self
            .queues
            .get(&name)
            .unwrap()
            .messages
            .iter()
            .position(|m| !m.deleted && m.receipt_handle == receipt_handle);
        let idx = match idx {
            None => return Err("receipt handle is invalid".into()),
            Some(i) => i,
        };
        let expired = !before(
            &now,
            &self.queues.get(&name).unwrap().messages[idx].invisible_until,
        );
        let previous_message = self.queues.get(&name).unwrap().messages[idx].clone();
        if expired {
            self.queues.get_mut(&name).unwrap().messages[idx].receipt_handle = String::new();
            if let Err(e) = self.persist() {
                self.queues.get_mut(&name).unwrap().messages[idx] = previous_message;
                return Err(e);
            }
            return Err("receipt handle is invalid".into());
        }
        self.queues.get_mut(&name).unwrap().messages[idx].invisible_until =
            add_seconds(&now, visibility_seconds);
        if let Err(e) = self.persist() {
            self.queues.get_mut(&name).unwrap().messages[idx] = previous_message;
            return Err(e);
        }
        Ok(())
    }

    // --- batch delete / change visibility ---

    pub fn delete_message_batch(
        &mut self,
        queue_url: &str,
        entries: &[DeleteMessageBatchEntry],
    ) -> Result<BatchResult, String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        validate_batch_entries(entries.iter().map(|e| e.id.as_str()))?;
        let mut result = BatchResult::default();
        for entry in entries {
            match self.delete_message(queue_url, &entry.receipt_handle) {
                Ok(()) => result.successful.push(IdResultEntry {
                    id: entry.id.clone(),
                }),
                Err(e) => result.failed.push(batch_error(&entry.id, &e)),
            }
        }
        Ok(result)
    }

    pub fn change_message_visibility_batch(
        &mut self,
        queue_url: &str,
        entries: &[ChangeMessageVisibilityBatchEntry],
    ) -> Result<BatchResult, String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        validate_batch_entries(entries.iter().map(|e| e.id.as_str()))?;
        let mut result = BatchResult::default();
        for entry in entries {
            match self.change_message_visibility(
                queue_url,
                &entry.receipt_handle,
                entry.visibility_timeout,
            ) {
                Ok(()) => result.successful.push(IdResultEntry {
                    id: entry.id.clone(),
                }),
                Err(e) => result.failed.push(batch_error(&entry.id, &e)),
            }
        }
        Ok(result)
    }
}

// --- free helpers (mirror message_core.rs + queue_attributes.rs) ---

fn requested_system_attribute_names(input: &ReceiveMessageRequest) -> Vec<String> {
    if !input.message_system_attribute_names.is_empty() {
        input.message_system_attribute_names.clone()
    } else {
        input.attribute_names.clone()
    }
}

/// Mirrors `cleanupExpiredMessagesLocked`: drop tombstoned + retention-expired
/// messages, then expire dedup entries.
pub(crate) fn cleanup_expired_messages(queue: &mut QueueState, now: &str) {
    let retention = int_attribute(&queue.attributes, "MessageRetentionPeriod", 345600);
    queue.messages.retain(|m| {
        if m.deleted {
            return false;
        }
        if retention > 0 {
            let age_nanos = crate::time_fmt::unix_nanos_from_rfc3339(now)
                - crate::time_fmt::unix_nanos_from_rfc3339(&m.sent_at);
            if age_nanos > (retention as i128) * 1_000_000_000 {
                return false;
            }
        }
        true
    });
    cleanup_expired_deduplication(queue, now);
}

fn cleanup_expired_deduplication(queue: &mut QueueState, now: &str) {
    queue
        .dedup
        .retain(|_, state| before(now, &state.expires_at));
}

fn int_attribute(attrs: &BTreeMap<String, String>, key: &str, fallback: i64) -> i64 {
    attrs
        .get(key)
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

/// Mirrors `receivedMessageFromState`.
fn received_message_from_state(
    message: &MessageState,
    message_attribute_names: &[String],
    system_attribute_names: &[String],
) -> ReceivedMessage {
    let mut attrs = BTreeMap::new();
    attrs.insert(
        "ApproximateReceiveCount".to_string(),
        message.receive_count.to_string(),
    );
    attrs.insert(
        "SentTimestamp".to_string(),
        unix_millis_from_rfc3339(&message.sent_at).to_string(),
    );
    if !is_zero(&message.first_receive_at) {
        attrs.insert(
            "ApproximateFirstReceiveTimestamp".to_string(),
            unix_millis_from_rfc3339(&message.first_receive_at).to_string(),
        );
    }
    if wants_any(system_attribute_names, "AWSTraceHeader") {
        if let Some(v) = message.system_attributes.get("AWSTraceHeader") {
            if !v.string_value.is_empty() {
                attrs.insert("AWSTraceHeader".to_string(), v.string_value.clone());
            }
        }
    }
    let mut response = ReceivedMessage {
        message_id: message.id.clone(),
        receipt_handle: message.receipt_handle.clone(),
        md5_of_message_body: message.body_md5.clone(),
        body: message.body.clone(),
        attributes: attrs,
        ..Default::default()
    };
    if wants_all(message_attribute_names) {
        response.message_attributes = message.attributes.clone();
        response.md5_of_message_attributes =
            md5_of_message_attributes(&response.message_attributes);
    } else {
        let filtered = filter_message_attributes(&message.attributes, message_attribute_names);
        if !filtered.is_empty() {
            response.md5_of_message_attributes = md5_of_message_attributes(&filtered);
            response.message_attributes = filtered;
        }
    }
    if wants_any(system_attribute_names, "AWSTraceHeader") {
        response.md5_of_message_system_attributes =
            md5_of_message_attributes(&message.system_attributes);
    }
    response
}

fn wants_all(names: &[String]) -> bool {
    names.iter().any(|n| n == "All" || n == ".*")
}

fn wants_any(names: &[String], target: &str) -> bool {
    wants_all(names) || names.iter().any(|n| n == target)
}

fn filter_message_attributes(
    attrs: &BTreeMap<String, MessageAttributeValue>,
    names: &[String],
) -> BTreeMap<String, MessageAttributeValue> {
    let mut filtered = BTreeMap::new();
    if attrs.is_empty() || names.is_empty() {
        return filtered;
    }
    for requested in names {
        if requested.is_empty() {
            continue;
        }
        if let Some(prefix) = requested.strip_suffix(".*") {
            for (name, value) in attrs {
                if name.starts_with(prefix) {
                    filtered.insert(name.clone(), value.clone());
                }
            }
            continue;
        }
        if let Some(value) = attrs.get(requested) {
            filtered.insert(requested.clone(), value.clone());
        }
    }
    filtered
}

/// Mirrors `redrivePolicyFromQueue`: returns the policy only if valid.
fn redrive_policy_from_queue(queue: &QueueState) -> Option<crate::policy::RedrivePolicy> {
    let raw = queue.attributes.get("RedrivePolicy")?;
    if raw.is_empty() {
        return None;
    }
    let policy = crate::policy::parse_redrive_policy(raw).ok()?;
    if policy.max_receive_count < 1 || policy.dead_letter_target_arn.is_empty() {
        return None;
    }
    Some(policy)
}

/// Mirrors `fifoDeduplicationID`.
fn fifo_deduplication_id(queue: &QueueState, input: &SendMessageRequest) -> Result<String, String> {
    if input.message_group_id.is_empty() {
        return Err("MessageGroupId is required for FIFO queues".into());
    }
    if !input.message_deduplication_id.is_empty() {
        return Ok(input.message_deduplication_id.clone());
    }
    let content_based = queue
        .attributes
        .get("ContentBasedDeduplication")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if content_based {
        let mut hasher = Sha256::new();
        hasher.update(input.message_body.as_bytes());
        return Ok(hex_lower(&hasher.finalize()));
    }
    Err("MessageDeduplicationId is required for FIFO queues unless ContentBasedDeduplication is enabled".into())
}

/// Shared batch-entry preconditions (id required, valid, unique, ≤10 entries).
fn validate_batch_entries<'a>(ids: impl Iterator<Item = &'a str>) -> Result<(), String> {
    let ids: Vec<&str> = ids.collect();
    if ids.is_empty() {
        return Err("Entries is required".into());
    }
    if ids.len() > 10 {
        return Err("Entries must contain no more than 10 entries".into());
    }
    let mut seen = HashSet::new();
    for id in ids {
        if id.is_empty() {
            return Err("batch entry Id is required".into());
        }
        if !valid_batch_entry_id(id) {
            return Err("batch entry Id may contain only alphanumeric characters, hyphens, and underscores, and must be no longer than 80 characters".into());
        }
        if !seen.insert(id) {
            return Err("batch entry Id must be unique".into());
        }
    }
    Ok(())
}

fn batch_error(id: &str, err: &str) -> BatchResultErrorEntry {
    BatchResultErrorEntry {
        id: id.to_string(),
        sender_fault: true,
        code: crate::errors::error_code(err),
        message: err.to_string(),
    }
}

/// Mirrors `newOpaqueID`: `<prefix>-<random>`. The legacy id uses crypto randomness
/// and is not behaviorally observable, so a unique time+counter id suffices.
fn new_opaque_id(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos:x}{n:x}")
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
