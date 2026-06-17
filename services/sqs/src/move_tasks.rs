//! Mirrors `internal/services/sqs/deadletter_move_tasks.rs` +
//! `resolveOriginalSourceLocked` (from dashboard.rs): dead-letter source-queue
//! listing and the message-move-task lifecycle (start / list / cancel).

use crate::model::{MoveTaskState, ZERO_TIME};
use crate::server::{queue_name_from_url, Server};
use crate::time_fmt::{now_rfc3339, unix_nanos_from_rfc3339};

/// A single move-task result projection (mirrors legacy `messageMoveTaskResult`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageMoveTaskResult {
    pub task_handle: String,
    pub status: String,
    pub source_arn: String,
    pub destination_arn: String,
    pub approximate_number_of_messages_moved: i64,
}

impl From<&MoveTaskState> for MessageMoveTaskResult {
    fn from(t: &MoveTaskState) -> Self {
        MessageMoveTaskResult {
            task_handle: t.task_handle.clone(),
            status: t.status.clone(),
            source_arn: t.source_arn.clone(),
            destination_arn: t.destination_arn.clone(),
            approximate_number_of_messages_moved: t.approximate_number_of_messages_moved,
        }
    }
}

impl Server {
    /// Mirrors `listDeadLetterSourceQueueURLs`: URLs of queues whose RedrivePolicy
    /// names `queue_url`'s queue as the dead-letter target, sorted.
    pub fn list_dead_letter_source_queue_urls(
        &self,
        queue_url: &str,
    ) -> Result<Vec<String>, String> {
        let name = queue_name_from_url(queue_url);
        if name.is_empty() {
            return Err("queue does not exist".into());
        }
        let target = match self.queues.get(&name) {
            None => return Err("queue does not exist".into()),
            Some(q) => q,
        };
        let mut sources: Vec<String> = Vec::new();
        for queue in self.queues.values() {
            if let Some(policy) = redrive_policy_or_none(queue) {
                if policy.dead_letter_target_arn == target.arn {
                    sources.push(queue.url.clone());
                }
            }
        }
        sources.sort();
        Ok(sources)
    }

    /// Mirrors `startMessageMoveTask`: redrives non-deleted messages from the
    /// source queue back to the destination (or each message's original source
    /// when no destination is given), records a COMPLETED task, and persists.
    pub fn start_message_move_task(
        &mut self,
        source_arn: &str,
        destination_arn: &str,
    ) -> Result<MoveTaskState, String> {
        if source_arn.is_empty() {
            return Err("SourceArn is required".into());
        }
        let source_name = self.queue_name_by_arn(source_arn);
        let source_name = match source_name {
            None => return Err("queue does not exist".into()),
            Some(n) => n,
        };
        if !destination_arn.is_empty() && self.queue_name_by_arn(destination_arn).is_none() {
            return Err("destination queue does not exist".into());
        }

        // Mirrors legacy exactly: no retention cleanup here; moved messages are
        // tombstoned in place (Deleted=true), not removed from the slice.
        let now = now_rfc3339();

        // Plan the moves first (need a separate &mut borrow per destination).
        let messages = self
            .queues
            .get(&source_name)
            .ok_or_else(|| "queue does not exist".to_string())?
            .messages
            .clone();
        let mut moves: Vec<(usize, String, crate::model::MessageState)> = Vec::new();
        for (i, message) in messages.iter().enumerate() {
            if message.deleted {
                continue;
            }
            let target_name = if !destination_arn.is_empty() {
                self.queue_name_by_arn(destination_arn)
            } else {
                self.resolve_original_source(message)
            };
            if let Some(target) = target_name {
                let mut moved = message.clone();
                moved.invisible_until = ZERO_TIME.to_string();
                moved.receipt_handle = String::new();
                moved.receive_count = 0;
                moved.first_receive_at = ZERO_TIME.to_string();
                moved.available_at = now.clone();
                moved.dead_letter_source_arn = String::new();
                moves.push((i, target, moved));
            }
        }
        let moved_count = moves.len() as i64;
        for (i, target, moved) in moves {
            self.queues
                .get_mut(&target)
                .ok_or_else(|| "destination queue does not exist".to_string())?
                .messages
                .push(moved);
            let src_msg = self
                .queues
                .get_mut(&source_name)
                .ok_or_else(|| "queue does not exist".to_string())?
                .messages
                .get_mut(i)
                .ok_or_else(|| "queue does not exist".to_string())?;
            src_msg.deleted = true;
            src_msg.receipt_handle = String::new();
        }

        let task = MoveTaskState {
            task_handle: new_opaque_id("mvt"),
            source_arn: source_arn.to_string(),
            destination_arn: destination_arn.to_string(),
            status: "COMPLETED".to_string(),
            started_at: now,
            approximate_number_of_messages_moved: moved_count,
        };
        self.move_tasks
            .insert(task.task_handle.clone(), task.clone());
        self.persist()?;
        Ok(task)
    }

    /// Mirrors `listMessageMoveTasks`: tasks for `source_arn`, newest-first,
    /// capped at `max_results` (0 → all).
    pub fn list_message_move_tasks(
        &self,
        source_arn: &str,
        max_results: i64,
    ) -> Result<Vec<MoveTaskState>, String> {
        if source_arn.is_empty() {
            return Err("SourceArn is required".into());
        }
        if self.queue_name_by_arn(source_arn).is_none() {
            return Err("queue does not exist".into());
        }
        let mut tasks: Vec<MoveTaskState> = self
            .move_tasks
            .values()
            .filter(|t| t.source_arn == source_arn)
            .cloned()
            .collect();
        tasks.sort_by(|a, b| {
            unix_nanos_from_rfc3339(&b.started_at).cmp(&unix_nanos_from_rfc3339(&a.started_at))
        });
        // Mirrors legacy: maxResults defaults to and is capped at 10.
        let mut limit = max_results;
        if limit <= 0 || limit > 10 {
            limit = 10;
        }
        if tasks.len() as i64 > limit {
            tasks.truncate(limit as usize);
        }
        Ok(tasks)
    }

    /// Mirrors `cancelMessageMoveTask`: returns the moved count for an existing
    /// task. (The legacy server does not change task status on cancel.)
    pub fn cancel_message_move_task(&mut self, task_handle: &str) -> Result<i64, String> {
        if task_handle.is_empty() {
            return Err("TaskHandle is required".into());
        }
        match self.move_tasks.get(task_handle) {
            None => Err("message move task does not exist".into()),
            Some(t) => Ok(t.approximate_number_of_messages_moved),
        }
    }

    fn queue_name_by_arn(&self, arn: &str) -> Option<String> {
        self.queues
            .iter()
            .find(|(_, q)| q.arn == arn)
            .map(|(n, _)| n.clone())
    }

    /// Mirrors `resolveOriginalSourceLocked`: the queue whose ARN equals the
    /// message's recorded dead-letter source.
    fn resolve_original_source(&self, message: &crate::model::MessageState) -> Option<String> {
        if message.dead_letter_source_arn.is_empty() {
            return None;
        }
        self.queue_name_by_arn(&message.dead_letter_source_arn)
    }
}

/// Mirrors `redrivePolicyFromQueue`: the policy iff valid (count ≥ 1, ARN set).
fn redrive_policy_or_none(
    queue: &crate::server::QueueState,
) -> Option<crate::policy::RedrivePolicy> {
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

/// Mirrors `newOpaqueID` — a unique, non-behaviorally-observable id.
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
