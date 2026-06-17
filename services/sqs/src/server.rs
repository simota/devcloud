//! Mirrors the in-memory `Server` + queue lifecycle / attributes / tags /
//! policy logic from `internal/services/sqs/{server,queue_handlers,
//! queue_attributes,tags_policy,persistence}.rs`.
//!
//! This part lands the queue-management core (no message send/receive yet, no
//! HTTP layer). State is held in-memory and persisted to `state.json` in the
//! byte-compatible schema from `persistence.rs`. Errors are `String`s carrying
//! the legacy wording verbatim (the responses layer maps message substrings to AWS
//! error codes).

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::model::{DeduplicationState, MessageState};
use crate::persistence::{PersistedQueue, PersistedState};
use crate::policy::{
    normalized_permission_actions, parse_redrive_allow_policy, parse_redrive_policy, QueuePolicy,
    QueuePolicyPrincipal, QueuePolicyStatement,
};
use crate::time_fmt::{now_rfc3339, unix_from_rfc3339};

pub const MAX_DELAY_SECONDS: i64 = 900;
pub const MAX_VISIBILITY_TIMEOUT_SECONDS: i64 = 43200;

/// Mirrors legacy `Config` (queue-management subset; message/scheduler fields land
/// with later parts).
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub region: String,
    pub account_id: String,
    pub queue_url_host: String,
    pub auth_mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub storage_path: String,
    pub max_queues: i64,
    pub max_message_bytes: i64,
    pub max_receive_batch_size: i64,
    pub default_visibility_timeout_seconds: i64,
    pub default_delay_seconds: i64,
    pub default_message_retention_seconds: i64,
    pub default_receive_wait_time_seconds: i64,
}

/// In-memory queue state. Mirrors legacy `queueState`; timestamps are RFC 3339
/// strings (matching the persisted form).
#[derive(Clone, Debug, Default)]
pub struct QueueState {
    pub name: String,
    pub url: String,
    pub arn: String,
    pub attributes: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub created_at: String,
    pub modified_at: String,
    pub messages: Vec<MessageState>,
    pub sequence: u64,
    pub dedup: BTreeMap<String, DeduplicationState>,
}

pub struct Server {
    config: Config,
    pub(crate) queues: BTreeMap<String, QueueState>,
    pub(crate) move_tasks: BTreeMap<String, crate::model::MoveTaskState>,
    load_err: Option<String>,
}

fn default_str<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.is_empty() {
        fallback
    } else {
        value
    }
}

impl Server {
    pub fn new(config: Config) -> Self {
        let mut server = Server {
            config,
            queues: BTreeMap::new(),
            move_tasks: BTreeMap::new(),
            load_err: None,
        };
        if !server.config.storage_path.trim().is_empty() {
            if let Err(e) = server.load() {
                server.load_err = Some(e);
            }
        }
        server
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn load_err(&self) -> Option<&str> {
        self.load_err.as_deref()
    }

    // --- config getters (mirror persistence.rs) ---

    fn max_queues(&self) -> i64 {
        if self.config.max_queues <= 0 {
            256
        } else {
            self.config.max_queues
        }
    }
    pub fn max_message_bytes(&self) -> i64 {
        if self.config.max_message_bytes <= 0 {
            1024 * 1024
        } else {
            self.config.max_message_bytes
        }
    }
    pub(crate) fn max_receive_batch_size(&self) -> i64 {
        let v = self.config.max_receive_batch_size;
        if v <= 0 || v > 10 {
            10
        } else {
            v
        }
    }
    pub(crate) fn default_visibility_timeout_seconds(&self) -> i64 {
        if self.config.default_visibility_timeout_seconds <= 0 {
            30
        } else {
            self.config.default_visibility_timeout_seconds
        }
    }
    fn default_delay_seconds(&self) -> i64 {
        if self.config.default_delay_seconds < 0 {
            0
        } else {
            self.config.default_delay_seconds
        }
    }
    fn default_message_retention_seconds(&self) -> i64 {
        if self.config.default_message_retention_seconds <= 0 {
            345600
        } else {
            self.config.default_message_retention_seconds
        }
    }
    pub(crate) fn default_receive_wait_time_seconds(&self) -> i64 {
        // Mirrors legacy clamp: < 0 → 0, > 20 → 20, else as-is.
        self.config.default_receive_wait_time_seconds.clamp(0, 20)
    }

    // --- URL / ARN (mirror queue_handlers.rs) ---

    pub fn queue_url(&self, name: &str) -> String {
        let mut host = self.config.queue_url_host.clone();
        if host.is_empty() {
            host = "127.0.0.1".to_string();
        }
        if !host.contains(':') {
            if let Some((_, port)) = self.config.addr.split_once(':') {
                if !port.is_empty() {
                    host = format!("{host}:{port}");
                }
            }
        }
        let account = default_str(&self.config.account_id, "000000000000");
        format!("http://{host}/{account}/{name}")
    }

    pub fn queue_arn(&self, name: &str) -> String {
        format!(
            "arn:aws:sqs:{}:{}:{}",
            default_str(&self.config.region, "us-east-1"),
            default_str(&self.config.account_id, "000000000000"),
            name
        )
    }

    pub fn default_queue_attributes(&self) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert(
            "DelaySeconds".into(),
            self.default_delay_seconds().to_string(),
        );
        m.insert(
            "MaximumMessageSize".into(),
            self.max_message_bytes().to_string(),
        );
        m.insert(
            "MessageRetentionPeriod".into(),
            self.default_message_retention_seconds().to_string(),
        );
        m.insert(
            "ReceiveMessageWaitTimeSeconds".into(),
            self.default_receive_wait_time_seconds().to_string(),
        );
        m.insert(
            "VisibilityTimeout".into(),
            self.default_visibility_timeout_seconds().to_string(),
        );
        m
    }

    // --- queue lifecycle (mirror queue_handlers.rs) ---

    pub fn create_queue(
        &mut self,
        name: &str,
        attrs: &BTreeMap<String, String>,
        tags: &BTreeMap<String, String>,
    ) -> Result<QueueState, String> {
        if name.is_empty() {
            return Err("QueueName is required".into());
        }
        if !crate::validation::valid_queue_name(name) {
            return Err("QueueName must contain only alphanumeric characters, hyphens, underscores, and optional .fifo suffix".into());
        }
        let mut normalized = self.default_queue_attributes();
        for (k, v) in attrs {
            normalized.insert(k.clone(), v.clone());
        }
        let is_fifo_name = name.ends_with(".fifo");
        if normalized.get("FifoQueue").map(String::as_str) == Some("true") && !is_fifo_name {
            return Err("FIFO queues must use a .fifo suffix".into());
        }
        if is_fifo_name && normalized.get("FifoQueue").map(String::as_str) != Some("true") {
            return Err("queues with .fifo suffix must set FifoQueue to true".into());
        }

        if let Some(existing) = self.queues.get(name) {
            if !same_attributes(&existing.attributes, &normalized) {
                return Err("queue name exists with different attributes".into());
            }
            return Ok(clone_queue(existing));
        }
        let max = self.max_queues();
        if max > 0 && self.queues.len() as i64 >= max {
            return Err("queue limit exceeded".into());
        }
        let now = now_rfc3339();
        let queue = QueueState {
            name: name.to_string(),
            url: self.queue_url(name),
            arn: self.queue_arn(name),
            attributes: normalized,
            tags: tags.clone(),
            created_at: now.clone(),
            modified_at: now,
            messages: Vec::new(),
            sequence: 0,
            dedup: BTreeMap::new(),
        };
        self.validate_queue_attributes(&queue, attrs)?;
        self.queues.insert(name.to_string(), queue.clone());
        if let Err(e) = self.persist() {
            self.queues.remove(name);
            return Err(e);
        }
        Ok(clone_queue(self.queues.get(name).unwrap()))
    }

    pub fn list_queue_urls(&self, prefix: &str) -> Vec<String> {
        // BTreeMap iterates sorted, matching legacy sort.Strings(names).
        self.queues
            .iter()
            .filter(|(name, _)| prefix.is_empty() || name.starts_with(prefix))
            .map(|(_, q)| q.url.clone())
            .collect()
    }

    pub fn queue_by_name(&self, name: &str) -> Option<QueueState> {
        self.queues.get(name).map(clone_queue)
    }

    pub fn queue_by_url(&self, queue_url: &str) -> Option<QueueState> {
        let name = queue_name_from_url(queue_url);
        if name.is_empty() {
            return None;
        }
        self.queue_by_name(&name)
    }

    pub fn delete_queue(&mut self, queue_url: &str) -> bool {
        let name = queue_name_from_url(queue_url);
        if name.is_empty() {
            return false;
        }
        match self.queues.remove(&name) {
            None => false,
            Some(queue) => {
                if let Err(_e) = self.persist() {
                    self.queues.insert(name, queue);
                    return false;
                }
                true
            }
        }
    }

    pub fn purge_queue(&mut self, queue_url: &str) -> Result<(), String> {
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let previous_messages = {
            let queue = self
                .queues
                .get_mut(&name)
                .ok_or_else(|| "queue does not exist".to_string())?;
            std::mem::take(&mut queue.messages)
        };
        if let Err(e) = self.persist() {
            if let Some(queue) = self.queues.get_mut(&name) {
                queue.messages = previous_messages;
            }
            return Err(e);
        }
        Ok(())
    }

    // --- attributes (mirror queue_attributes.rs) ---

    pub fn get_queue_attributes(
        &mut self,
        queue_url: &str,
        names: &[String],
    ) -> Result<BTreeMap<String, String>, String> {
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let Some(queue) = self.queues.get(&name) else {
            return Err("queue does not exist".into());
        };
        let attrs = queue_attributes_with_computed(queue);
        filter_queue_attributes(attrs, names)
    }

    pub fn update_queue_attributes(
        &mut self,
        queue_url: &str,
        attrs: &BTreeMap<String, String>,
    ) -> Result<QueueState, String> {
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let previous_queue = clone_queue(
            self.queues
                .get(&name)
                .ok_or_else(|| "queue does not exist".to_string())?,
        );
        self.validate_queue_attributes(&previous_queue, attrs)?;
        {
            let q = self
                .queues
                .get_mut(&name)
                .ok_or_else(|| "queue does not exist".to_string())?;
            for (k, v) in attrs {
                q.attributes.insert(k.clone(), v.clone());
            }
            q.modified_at = now_rfc3339();
        }
        if let Err(e) = self.persist() {
            self.queues.insert(name.clone(), previous_queue);
            return Err(e);
        }
        self.queues
            .get(&name)
            .map(clone_queue)
            .ok_or_else(|| "queue does not exist".to_string())
    }

    fn validate_queue_attributes(
        &self,
        queue: &QueueState,
        attrs: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        for name in attrs.keys() {
            if !is_settable_queue_attribute(name) {
                return Err(format!("unknown queue attribute name \"{name}\""));
            }
        }
        validate_queue_attribute_values(attrs)?;
        if let Some(allow) = attrs.get("RedriveAllowPolicy") {
            if !allow.is_empty() {
                parse_redrive_allow_policy(allow)?;
            }
        }
        let policy_raw = attrs.get("RedrivePolicy").map(String::as_str).unwrap_or("");
        if policy_raw.is_empty() {
            return Ok(());
        }
        let policy = parse_redrive_policy(policy_raw)?;
        if policy.max_receive_count < 1 {
            return Err("RedrivePolicy maxReceiveCount must be greater than zero".into());
        }
        let dlq = self
            .queues
            .values()
            .find(|q| q.arn == policy.dead_letter_target_arn);
        let dlq = match dlq {
            None => return Err("RedrivePolicy deadLetterTargetArn queue does not exist".into()),
            Some(q) => q,
        };
        if is_fifo_queue(queue) != is_fifo_queue(dlq) {
            return Err(
                "RedrivePolicy source queue and dead-letter queue must use the same queue type"
                    .into(),
            );
        }
        validate_redrive_allow_policy(dlq, &queue.arn)
    }

    // --- tags (mirror tags_policy.rs) ---

    pub fn tag_queue(
        &mut self,
        queue_url: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        if tags.is_empty() {
            return Err("Tags is required".into());
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        for k in tags.keys() {
            if k.is_empty() {
                return Err("tag key is required".into());
            }
        }
        let previous_tags = self
            .queues
            .get(&name)
            .ok_or_else(|| "queue does not exist".to_string())?
            .tags
            .clone();
        {
            let q = self
                .queues
                .get_mut(&name)
                .ok_or_else(|| "queue does not exist".to_string())?;
            for (k, v) in tags {
                q.tags.insert(k.clone(), v.clone());
            }
        }
        if let Err(e) = self.persist() {
            if let Some(q) = self.queues.get_mut(&name) {
                q.tags = previous_tags;
            }
            return Err(e);
        }
        Ok(())
    }

    pub fn untag_queue(&mut self, queue_url: &str, tag_keys: &[String]) -> Result<(), String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        if tag_keys.is_empty() {
            return Err("TagKeys is required".into());
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let previous_tags = self
            .queues
            .get(&name)
            .ok_or_else(|| "queue does not exist".to_string())?
            .tags
            .clone();
        {
            let q = self
                .queues
                .get_mut(&name)
                .ok_or_else(|| "queue does not exist".to_string())?;
            for k in tag_keys {
                q.tags.remove(k);
            }
        }
        if let Err(e) = self.persist() {
            if let Some(q) = self.queues.get_mut(&name) {
                q.tags = previous_tags;
            }
            return Err(e);
        }
        Ok(())
    }

    pub fn list_queue_tags(&self, queue_url: &str) -> Result<BTreeMap<String, String>, String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() {
            return Err("queue does not exist".into());
        }
        match self.queues.get(&name) {
            None => Err("queue does not exist".into()),
            Some(q) => Ok(q.tags.clone()),
        }
    }

    // --- policy / permissions (mirror tags_policy.rs) ---

    pub fn add_permission(
        &mut self,
        queue_url: &str,
        label: &str,
        account_ids: &[String],
        actions: &[String],
    ) -> Result<(), String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        if label.is_empty() {
            return Err("Label is required".into());
        }
        if account_ids.is_empty() {
            return Err("AWSAccountIds is required".into());
        }
        if actions.is_empty() {
            return Err("Actions is required".into());
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let queue = self.queues.get(&name).unwrap();
        let mut policy = queue_policy_from_attribute(queue)?;
        let statement = QueuePolicyStatement {
            sid: label.to_string(),
            effect: "Allow".to_string(),
            principal: QueuePolicyPrincipal {
                aws: account_ids.to_vec(),
            },
            action: normalized_permission_actions(actions),
            resource: queue.arn.clone(),
        };
        if let Some(existing) = policy.statement.iter_mut().find(|s| s.sid == label) {
            *existing = statement;
        } else {
            policy.statement.push(statement);
        }
        let encoded = serde_json::to_string(&policy).map_err(|e| e.to_string())?;
        self.queues
            .get_mut(&name)
            .unwrap()
            .attributes
            .insert("Policy".into(), encoded);
        self.persist()
    }

    pub fn remove_permission(&mut self, queue_url: &str, label: &str) -> Result<(), String> {
        if queue_url.is_empty() {
            return Err("QueueUrl is required".into());
        }
        if label.is_empty() {
            return Err("Label is required".into());
        }
        let name = queue_name_from_url(queue_url);
        if name.is_empty() || !self.queues.contains_key(&name) {
            return Err("queue does not exist".into());
        }
        let queue = self.queues.get(&name).unwrap();
        let mut policy = queue_policy_from_attribute(queue)?;
        policy.statement.retain(|s| s.sid != label);
        let q = self.queues.get_mut(&name).unwrap();
        if policy.statement.is_empty() {
            q.attributes.remove("Policy");
            return self.persist();
        }
        let encoded = serde_json::to_string(&policy).map_err(|e| e.to_string())?;
        q.attributes.insert("Policy".into(), encoded);
        self.persist()
    }

    // --- persistence (mirror persistence.rs) ---

    fn state_path(&self) -> PathBuf {
        PathBuf::from(&self.config.storage_path).join("state.json")
    }

    pub fn persist(&self) -> Result<(), String> {
        if self.config.storage_path.trim().is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.config.storage_path).map_err(|e| e.to_string())?;
        let mut persisted = PersistedState::default();
        for (name, q) in &self.queues {
            persisted.queues.insert(
                name.clone(),
                PersistedQueue {
                    name: q.name.clone(),
                    url: q.url.clone(),
                    arn: q.arn.clone(),
                    attributes: q.attributes.clone(),
                    tags: q.tags.clone(),
                    created_at: q.created_at.clone(),
                    modified_at: queue_last_modified_at(q),
                    messages: q.messages.clone(),
                    sequence: q.sequence,
                    dedup: q.dedup.clone(),
                },
            );
        }
        persisted.move_tasks = self.move_tasks.clone();
        let data = persisted.to_json_bytes();
        let path = self.state_path();
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &data).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
    }

    fn load(&mut self) -> Result<(), String> {
        let data = match std::fs::read(self.state_path()) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let persisted = PersistedState::from_json(&data)?;
        for (handle, mut task) in persisted.move_tasks {
            let key = if handle.is_empty() {
                task.task_handle.clone()
            } else {
                handle
            };
            if key.is_empty() {
                continue;
            }
            if task.task_handle.is_empty() {
                task.task_handle = key.clone();
            }
            self.move_tasks.insert(key, task);
        }
        let now = now_rfc3339();
        for (name, pq) in persisted.queues {
            let key = if name.is_empty() {
                pq.name.clone()
            } else {
                name
            };
            if key.is_empty() {
                continue;
            }
            let mut q = QueueState {
                name: key.clone(),
                url: pq.url,
                arn: pq.arn,
                attributes: pq.attributes,
                tags: pq.tags,
                created_at: pq.created_at,
                modified_at: pq.modified_at,
                messages: pq.messages,
                sequence: pq.sequence,
                dedup: pq.dedup,
            };
            if q.url.is_empty() {
                q.url = self.queue_url(&key);
            }
            if q.arn.is_empty() {
                q.arn = self.queue_arn(&key);
            }
            if q.attributes.is_empty() {
                q.attributes = self.default_queue_attributes();
            }
            if q.created_at.is_empty() || q.created_at == crate::model::ZERO_TIME {
                q.created_at = now.clone();
            }
            if q.modified_at.is_empty() || q.modified_at == crate::model::ZERO_TIME {
                q.modified_at = q.created_at.clone();
            }
            self.queues.insert(key, q);
        }
        Ok(())
    }
}

// --- free helpers ---

pub fn queue_name_from_url(queue_url: &str) -> String {
    // Strip scheme + authority to get the path, then take the last segment if
    // there are at least two path segments (matching legacy url.Parse + trim).
    let after_scheme = match queue_url.find("://") {
        Some(i) => &queue_url[i + 3..],
        None => queue_url,
    };
    // Drop the query/fragment.
    let after_scheme = after_scheme
        .split(['?', '#'])
        .next()
        .unwrap_or(after_scheme);
    let path = if queue_url.contains("://") {
        // Everything after the first '/' is the path.
        match after_scheme.find('/') {
            Some(i) => &after_scheme[i..],
            None => "",
        }
    } else {
        after_scheme
    };
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts.len() < 2 {
        return String::new();
    }
    parts[parts.len() - 1].to_string()
}

fn clone_queue(queue: &QueueState) -> QueueState {
    queue.clone()
}

fn queue_last_modified_at(queue: &QueueState) -> String {
    if queue.modified_at.is_empty() || queue.modified_at == crate::model::ZERO_TIME {
        queue.created_at.clone()
    } else {
        queue.modified_at.clone()
    }
}

fn same_attributes(left: &BTreeMap<String, String>, right: &BTreeMap<String, String>) -> bool {
    left == right
}

pub fn is_fifo_queue(queue: &QueueState) -> bool {
    queue
        .attributes
        .get("FifoQueue")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
        || queue.name.ends_with(".fifo")
}

fn queue_attributes_with_computed(queue: &QueueState) -> BTreeMap<String, String> {
    let mut attrs = queue.attributes.clone();
    attrs.insert("QueueArn".into(), queue.arn.clone());
    attrs.insert(
        "CreatedTimestamp".into(),
        unix_from_rfc3339(&queue.created_at).to_string(),
    );
    attrs.insert(
        "LastModifiedTimestamp".into(),
        unix_from_rfc3339(&queue_last_modified_at(queue)).to_string(),
    );
    // Counts are 0 in this part (message lifecycle lands later); the legacy server
    // computes these from message visibility — wired in the message-handler part.
    attrs.insert("ApproximateNumberOfMessages".into(), "0".into());
    attrs.insert("ApproximateNumberOfMessagesNotVisible".into(), "0".into());
    attrs.insert("ApproximateNumberOfMessagesDelayed".into(), "0".into());
    attrs
}

fn filter_queue_attributes(
    attrs: BTreeMap<String, String>,
    names: &[String],
) -> Result<BTreeMap<String, String>, String> {
    if names.is_empty() || names.iter().any(|n| n == "All") {
        return Ok(attrs);
    }
    let mut filtered = BTreeMap::new();
    for name in names {
        if !is_readable_queue_attribute(name) {
            return Err(format!("unknown queue attribute name \"{name}\""));
        }
        if let Some(value) = attrs.get(name) {
            filtered.insert(name.clone(), value.clone());
        }
    }
    Ok(filtered)
}

fn is_readable_queue_attribute(name: &str) -> bool {
    name == "All" || QUEUE_ATTRIBUTE_NAMES.contains(&name)
}

fn is_settable_queue_attribute(name: &str) -> bool {
    if name.starts_with("ApproximateNumberOfMessages")
        || name == "QueueArn"
        || name == "CreatedTimestamp"
        || name == "LastModifiedTimestamp"
    {
        return false;
    }
    QUEUE_ATTRIBUTE_NAMES.contains(&name)
}

fn validate_queue_attribute_values(attrs: &BTreeMap<String, String>) -> Result<(), String> {
    let bounds: &[(&str, i64, i64)] = &[
        ("DelaySeconds", 0, MAX_DELAY_SECONDS),
        ("MaximumMessageSize", 1024, 1048576),
        ("MessageRetentionPeriod", 60, 1209600),
        ("ReceiveMessageWaitTimeSeconds", 0, 20),
        ("VisibilityTimeout", 0, MAX_VISIBILITY_TIMEOUT_SECONDS),
        ("KmsDataKeyReusePeriodSeconds", 60, 86400),
    ];
    for (name, min, max) in bounds {
        if let Some(value) = attrs.get(*name) {
            let parsed: i64 = value
                .parse()
                .ok()
                .filter(|&v| v >= 0)
                .ok_or_else(|| format!("invalid attribute value for {name}"))?;
            if parsed < *min || parsed > *max {
                return Err(format!("invalid attribute value for {name}"));
            }
        }
    }
    for name in [
        "ContentBasedDeduplication",
        "FifoQueue",
        "SqsManagedSseEnabled",
    ] {
        if let Some(value) = attrs.get(name) {
            if !value.eq_ignore_ascii_case("true") && !value.eq_ignore_ascii_case("false") {
                return Err(format!("invalid attribute value for {name}"));
            }
        }
    }
    Ok(())
}

fn validate_redrive_allow_policy(dlq: &QueueState, source_arn: &str) -> Result<(), String> {
    let raw = dlq
        .attributes
        .get("RedriveAllowPolicy")
        .map(String::as_str)
        .unwrap_or("");
    if raw.is_empty() {
        return Ok(());
    }
    let policy = parse_redrive_allow_policy(raw)?;
    match policy.permission.as_str() {
        "allowAll" => Ok(()),
        "denyAll" => Err("RedriveAllowPolicy does not allow this dead-letter queue".into()),
        "byQueue" => {
            if policy.source_queue_arns.iter().any(|a| a == source_arn) {
                Ok(())
            } else {
                Err("RedriveAllowPolicy does not allow this source queue".into())
            }
        }
        _ => {
            Err("RedriveAllowPolicy redrivePermission must be allowAll, denyAll, or byQueue".into())
        }
    }
}

fn queue_policy_from_attribute(queue: &QueueState) -> Result<QueuePolicy, String> {
    let raw = queue
        .attributes
        .get("Policy")
        .map(|s| s.trim())
        .unwrap_or("");
    if raw.is_empty() {
        return Ok(QueuePolicy {
            version: "2012-10-17".into(),
            id: format!("{}/SQSDefaultPolicy", queue.arn),
            statement: Vec::new(),
        });
    }
    let mut policy: QueuePolicy =
        serde_json::from_str(raw).map_err(|_| "Policy must be valid JSON".to_string())?;
    if policy.version.is_empty() {
        policy.version = "2012-10-17".into();
    }
    if policy.id.is_empty() {
        policy.id = format!("{}/SQSDefaultPolicy", queue.arn);
    }
    Ok(policy)
}

const QUEUE_ATTRIBUTE_NAMES: &[&str] = &[
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesDelayed",
    "ApproximateNumberOfMessagesNotVisible",
    "ContentBasedDeduplication",
    "CreatedTimestamp",
    "DeduplicationScope",
    "DelaySeconds",
    "FifoQueue",
    "FifoThroughputLimit",
    "KmsDataKeyReusePeriodSeconds",
    "KmsMasterKeyId",
    "LastModifiedTimestamp",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    "Policy",
    "QueueArn",
    "ReceiveMessageWaitTimeSeconds",
    "RedriveAllowPolicy",
    "RedrivePolicy",
    "SqsManagedSseEnabled",
    "VisibilityTimeout",
];
