//! Mirrors the message lifecycle from
//! `internal/services/sqs/{message_handlers,message_core}.rs`:
//! send (+FIFO dedup), receive (visibility + FIFO group blocking + DLQ
//! redrive), delete, change-visibility, their batch variants, retention
//! cleanup, and the `receivedMessage` projection.
//!
//! `receive_messages_once` performs a single non-waiting attempt (validate +
//! scan); `receive_messages` layers a bounded `std::thread::sleep` retry loop
//! on top for synchronous callers (tests, and `dispatch_json`/`dispatch_query`
//! when called directly rather than through the socket server).
//! `wait_time_seconds == 0` returns immediately, matching the legacy fast
//! path. The async JSON and Query HTTP servers (`http.rs`) instead drive
//! `receive_messages_once` from a tokio loop so the server lock is released
//! between attempts rather than held across the whole wait; the legacy
//! wait-channel is purely an early-wake optimization and is not behaviorally
//! required either way. Both loops pin the queue's `QueueIdentity` from the
//! first attempt and pass it back in on every later attempt, so a
//! delete+recreate of the same queue name mid-poll surfaces as
//! `QueueDoesNotExist` instead of silently continuing against the new queue.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::hashing::{md5_hex, md5_of_message_attributes, MessageAttributeValue};
use crate::model::{DeduplicationState, MessageState};
use crate::server::{
    is_fifo_queue, queue_name_from_url, QueueIdentity, QueueState, Server, MAX_DELAY_SECONDS,
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
        // Snapshot only the fields this function mutates (messages, sequence,
        // dedup) rather than the whole QueueState — attributes/tags/name/url/
        // arn/timestamps are never touched here.
        let previous_messages = self.queues.get(&name).unwrap().messages.clone();
        let previous_sequence = self.queues.get(&name).unwrap().sequence;
        let previous_dedup = self.queues.get(&name).unwrap().dedup.clone();

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
            return Err(format!(
                "DelaySeconds must be no greater than {MAX_DELAY_SECONDS}"
            ));
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
            let queue = self.queues.get_mut(&name).unwrap();
            queue.messages = previous_messages;
            queue.sequence = previous_sequence;
            queue.dedup = previous_dedup;
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

    /// Single non-waiting attempt: validates the request, then performs one
    /// scan for available messages. Returns the scan result, the effective
    /// (clamped) `WaitTimeSeconds` (so a caller driving its own poll loop
    /// knows how many ~100ms slices to keep polling for), and the queue's
    /// current `QueueIdentity`.
    ///
    /// `pinned`: on the first attempt, pass `None` and keep the returned
    /// identity; on every later attempt of the same poll, pass it back in as
    /// `Some(&identity)`. If the queue was deleted and a same-named queue
    /// recreated in between, the identity no longer matches and this returns
    /// the same error a deleted queue would (`"queue does not exist"`),
    /// rather than silently resuming the scan against the new queue.
    ///
    /// Safe to call repeatedly — the validation only reads immutable
    /// `Config` fields and `input`, never mutable queue state.
    pub fn receive_messages_once(
        &mut self,
        input: &ReceiveMessageRequest,
        pinned: Option<&QueueIdentity>,
    ) -> Result<(Vec<ReceivedMessage>, i64, QueueIdentity), String> {
        if input.queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        let name = queue_name_from_url(&input.queue_url);
        if name.is_empty() {
            return Err("queue does not exist".into());
        }
        let identity = self
            .queue_identity(&name)
            .ok_or_else(|| "queue does not exist".to_string())?;
        if let Some(pinned) = pinned {
            if pinned != &identity {
                return Err("queue does not exist".into());
            }
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

        let messages = self.receive_available_messages(
            &name,
            max_messages,
            input.visibility_timeout,
            &input.message_attribute_names,
            &requested_system_attribute_names(input),
        )?;
        Ok((messages, wait_seconds, identity))
    }

    /// Long-poll receive. `wait_time_seconds == 0` returns immediately.
    /// Blocks the calling thread for the wait via `std::thread::sleep` — used
    /// directly by tests and by `dispatch_json`/`dispatch_query` when called
    /// directly rather than through the socket server. The socket server's
    /// JSON and Query paths instead drive `receive_messages_once` from an
    /// async loop in `http.rs` so the server lock isn't held across the
    /// sleeps.
    pub fn receive_messages(
        &mut self,
        input: &ReceiveMessageRequest,
    ) -> Result<Vec<ReceivedMessage>, String> {
        let (first, wait_seconds, identity) = self.receive_messages_once(input, None)?;
        let attempts = (wait_seconds * 10 + 1).max(1); // ~100ms slices, mirroring the legacy loop
        if !first.is_empty() || attempts == 1 {
            return Ok(first);
        }
        for attempt in 1..attempts {
            std::thread::sleep(Duration::from_millis(100));
            let (messages, _, _) = self.receive_messages_once(input, Some(&identity))?;
            if !messages.is_empty() || attempt == attempts - 1 {
                return Ok(messages);
            }
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
        // Snapshot only the queues this scan can mutate: the target queue
        // itself, plus its redrive-policy DLQ target (if any) since expired
        // messages may be redriven there via `move_to_dlq_if_needed` below.
        // The policy is read once here since it does not change mid-scan.
        let dlq_name = redrive_policy_from_queue(self.queues.get(name).unwrap()).and_then(|p| {
            self.queues
                .iter()
                .find(|(_, q)| q.arn == p.dead_letter_target_arn)
                .map(|(n, _)| n.clone())
        });
        let previous_queue = self.queues.get(name).unwrap().clone();
        let previous_dlq = dlq_name
            .as_ref()
            .filter(|n| n.as_str() != name)
            .map(|n| (n.clone(), self.queues.get(n).unwrap().clone()));
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
            return Err(format!(
                "VisibilityTimeout must be no greater than {MAX_VISIBILITY_TIMEOUT_SECONDS}"
            ));
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
                self.queues.insert(name.to_string(), previous_queue);
                if let Some((n, snapshot)) = previous_dlq {
                    self.queues.insert(n, snapshot);
                }
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
        self.apply_to_receipted_message(&name, receipt_handle, |m, _now| {
            m.deleted = true;
            m.receipt_handle = String::new();
        })
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
            return Err(format!(
                "VisibilityTimeout must be no greater than {MAX_VISIBILITY_TIMEOUT_SECONDS}"
            ));
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        self.apply_to_receipted_message(&name, receipt_handle, |m, now| {
            m.invisible_until = add_seconds(now, visibility_seconds);
        })
    }

    /// Shared shape for `delete_message` / `change_message_visibility`:
    /// stage the mutation in memory via `stage_receipted_mutation`, persist
    /// once, and roll back the single affected message on persist failure.
    fn apply_to_receipted_message(
        &mut self,
        name: &str,
        receipt_handle: &str,
        mutate: impl FnOnce(&mut MessageState, &str),
    ) -> Result<(), String> {
        match self.stage_receipted_mutation(name, receipt_handle, mutate) {
            StagedMutation::Rejected(e) => Err(e),
            StagedMutation::Applied {
                idx,
                previous,
                pending_err,
            } => {
                if let Err(e) = self.persist() {
                    self.queues.get_mut(name).unwrap().messages[idx] = previous;
                    return Err(e);
                }
                match pending_err {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            }
        }
    }

    /// Locates the non-deleted message with receipt handle `receipt_handle`
    /// in queue `name` and, unless its visibility has already expired,
    /// applies `mutate` in memory (does not persist). An expired handle is
    /// tombstoned (receipt handle cleared) same as legacy, but reported via
    /// `pending_err` rather than persisted immediately, so callers — single
    /// item or batch — can persist once for however many entries they stage.
    fn stage_receipted_mutation(
        &mut self,
        name: &str,
        receipt_handle: &str,
        mutate: impl FnOnce(&mut MessageState, &str),
    ) -> StagedMutation {
        if receipt_handle.is_empty() {
            return StagedMutation::Rejected("ReceiptHandle is required".into());
        }
        let now = now_rfc3339();
        let idx = self
            .queues
            .get(name)
            .unwrap()
            .messages
            .iter()
            .position(|m| !m.deleted && m.receipt_handle == receipt_handle);
        let idx = match idx {
            None => return StagedMutation::Rejected("receipt handle is invalid".into()),
            Some(i) => i,
        };
        let previous = self.queues.get(name).unwrap().messages[idx].clone();
        let expired = !before(&now, &previous.invisible_until);
        if expired {
            self.queues.get_mut(name).unwrap().messages[idx].receipt_handle = String::new();
            return StagedMutation::Applied {
                idx,
                previous,
                pending_err: Some("receipt handle is invalid".into()),
            };
        }
        mutate(&mut self.queues.get_mut(name).unwrap().messages[idx], &now);
        StagedMutation::Applied {
            idx,
            previous,
            pending_err: None,
        }
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
        let name = queue_name_from_url(queue_url);
        let mut result = BatchResult::default();
        let mut staged: Vec<StagedBatchEntry> = Vec::new();
        for entry in entries {
            if entry.receipt_handle.is_empty() {
                result
                    .failed
                    .push(batch_error(&entry.id, "ReceiptHandle is required"));
                continue;
            }
            if name.is_empty() || !self.queues.contains_key(&name) {
                result
                    .failed
                    .push(batch_error(&entry.id, "queue does not exist"));
                continue;
            }
            match self.stage_receipted_mutation(&name, &entry.receipt_handle, |m, _now| {
                m.deleted = true;
                m.receipt_handle = String::new();
            }) {
                StagedMutation::Rejected(e) => result.failed.push(batch_error(&entry.id, &e)),
                StagedMutation::Applied {
                    idx,
                    previous,
                    pending_err,
                } => staged.push((entry.id.clone(), idx, previous, pending_err)),
            }
        }
        apply_staged_batch(self, &name, staged, &mut result);
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
        let name = queue_name_from_url(queue_url);
        let mut result = BatchResult::default();
        let mut staged: Vec<StagedBatchEntry> = Vec::new();
        for entry in entries {
            if entry.receipt_handle.is_empty() {
                result
                    .failed
                    .push(batch_error(&entry.id, "ReceiptHandle is required"));
                continue;
            }
            if entry.visibility_timeout < 0 {
                result.failed.push(batch_error(
                    &entry.id,
                    "VisibilityTimeout must be non-negative",
                ));
                continue;
            }
            if entry.visibility_timeout > MAX_VISIBILITY_TIMEOUT_SECONDS {
                result.failed.push(batch_error(
                    &entry.id,
                    &format!(
                        "VisibilityTimeout must be no greater than {MAX_VISIBILITY_TIMEOUT_SECONDS}"
                    ),
                ));
                continue;
            }
            if name.is_empty() || !self.queues.contains_key(&name) {
                result
                    .failed
                    .push(batch_error(&entry.id, "queue does not exist"));
                continue;
            }
            let visibility_timeout = entry.visibility_timeout;
            match self.stage_receipted_mutation(&name, &entry.receipt_handle, move |m, now| {
                m.invisible_until = add_seconds(now, visibility_timeout);
            }) {
                StagedMutation::Rejected(e) => result.failed.push(batch_error(&entry.id, &e)),
                StagedMutation::Applied {
                    idx,
                    previous,
                    pending_err,
                } => staged.push((entry.id.clone(), idx, previous, pending_err)),
            }
        }
        apply_staged_batch(self, &name, staged, &mut result);
        Ok(result)
    }
}

/// Outcome of `stage_receipted_mutation`: either the entry never touched
/// state (`Rejected`, a pure validation failure), or a mutation was applied
/// in memory at `messages[idx]` and awaits the batch's single persist.
/// `pending_err` is set when the mutation itself represents an eventual
/// failure (the expired-handle case) that should still surface once
/// persisted.
enum StagedMutation {
    Applied {
        idx: usize,
        previous: MessageState,
        pending_err: Option<String>,
    },
    Rejected(String),
}

/// (batch entry id, message index, pre-mutation message, pending failure).
type StagedBatchEntry = (String, usize, MessageState, Option<String>);

/// Persists all staged mutations for a batch once; on failure, restores
/// every affected message (in reverse staging order, so repeated indices —
/// e.g. duplicate receipt handles in one batch — unwind correctly) and
/// reports the persist error for every staged entry. On success, resolves
/// each entry's `pending_err` into the final successful/failed split.
fn apply_staged_batch(
    server: &mut Server,
    name: &str,
    staged: Vec<StagedBatchEntry>,
    result: &mut BatchResult,
) {
    if staged.is_empty() {
        return;
    }
    if let Err(e) = server.persist() {
        for (_, idx, previous, _) in staged.iter().rev() {
            server.queues.get_mut(name).unwrap().messages[*idx] = previous.clone();
        }
        for (id, _, _, _) in staged {
            result.failed.push(batch_error(&id, &e));
        }
        return;
    }
    for (id, _, _, pending_err) in staged {
        match pending_err {
            Some(e) => result.failed.push(batch_error(&id, &e)),
            None => result.successful.push(IdResultEntry { id }),
        }
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
