//! In-memory server state + the topic operations.
//!
//! Mirrors `internal/services/pubsub/{server,topic_handlers,persistence}.rs`.
//! This part lands topic lifecycle (Create/Get/Patch/Delete/List) plus the
//! topic→subscriptions / topic→snapshots listings. Subscriptions, snapshots,
//! schemas, messages, IAM, and the HTTP server arrive in later parts.
//!
//! Each operation returns `Result<RestResponse, ApiError>`. The response carries
//! the HTTP status and the already-encoded body so the HTTP layer is a thin
//! renderer.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::errors::ApiError;
use crate::model::{Schema, Snapshot, Subscription, Topic};
use crate::paths;
use crate::persistence::{MessageStateFile, ResourceFile};

/// Default project, defaulted like legacy `defaultString(cfg.Project, "devcloud")`.
const DEFAULT_PROJECT: &str = "devcloud";

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub project: String,
    pub auth_mode: String,
    pub bearer_token: String,
    pub storage_path: String,
    pub message_storage_path: String,
    pub default_ack_deadline_seconds: i64,
    pub message_retention_seconds: i64,
    pub max_ack_deadline_seconds: i64,
    pub max_pull_messages: i64,
    /// Mirrors legacy `Config.StreamingPullDisabled` (= `!EnableStreamingPull`). When
    /// true, `StreamingPull` returns `Unimplemented "streaming pull is disabled"`.
    pub streaming_pull_disabled: bool,
}

impl Config {
    fn project(&self) -> &str {
        if self.project.is_empty() {
            DEFAULT_PROJECT
        } else {
            &self.project
        }
    }
}

/// A rendered REST response: HTTP status + encoded body (empty for 204), plus
/// optional `Allow` / `WWW-Authenticate` headers set by the router.
#[derive(Debug)]
pub struct RestResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub allow: Option<String>,
    pub www_authenticate: bool,
}

impl RestResponse {
    pub(crate) fn ok_struct<T: serde::Serialize>(value: &T) -> Self {
        RestResponse {
            status: 200,
            body: crate::wire_json::to_vec(value),
            allow: None,
            www_authenticate: false,
        }
    }
    fn no_content() -> Self {
        RestResponse {
            status: 204,
            body: Vec::new(),
            allow: None,
            www_authenticate: false,
        }
    }
}

pub struct Server {
    config: Config,
    pub(crate) topics: BTreeMap<String, Topic>,
    pub(crate) subscriptions: BTreeMap<String, Subscription>,
    pub(crate) snapshots: BTreeMap<String, Snapshot>,
    pub(crate) schemas: BTreeMap<String, Schema>,
    pub(crate) messages: BTreeMap<String, crate::model::PubsubMessage>,
    pub(crate) deliveries: BTreeMap<String, Vec<crate::model::DeliveryRecord>>,
    next_message_id: u64,
    next_ack_id: u64,
    load_err: Option<String>,
    /// Test hook for `createdAt`/`updatedAt` (RFC3339Nano string).
    fixed_now: Option<String>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        let mut server = Server {
            config,
            topics: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            schemas: BTreeMap::new(),
            messages: BTreeMap::new(),
            deliveries: BTreeMap::new(),
            next_message_id: 0,
            next_ack_id: 0,
            load_err: None,
            fixed_now: None,
        };
        if !server.config.storage_path.is_empty() || !server.config.message_storage_path.is_empty()
        {
            if let Err(err) = server.load() {
                server.load_err = Some(err);
            }
        }
        server
    }

    /// Pins the clock for deterministic tests.
    pub fn set_fixed_now(&mut self, rfc3339nano: &str) {
        self.fixed_now = Some(rfc3339nano.to_string());
    }

    pub fn load_err(&self) -> Option<&str> {
        self.load_err.as_deref()
    }

    fn now(&self) -> String {
        self.fixed_now
            .clone()
            .unwrap_or_else(crate::time_fmt::now_rfc3339nano)
    }

    /// Current time as `(unix_secs, nanos)` for delivery arithmetic.
    pub(crate) fn now_parts(&self) -> (i64, u32) {
        crate::time_fmt::parse_rfc3339(&self.now()).unwrap_or((0, 0))
    }

    /// Default-resolved project for the dashboard snapshot, mirroring legacy
    /// `defaultString(s.config.Project, "devcloud")`.
    pub(crate) fn snapshot_project(&self) -> String {
        self.config.project().to_string()
    }

    fn resource_file_path(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.config.storage_path).join("resources.json")
    }

    fn message_state_file_path(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.config.message_storage_path).join("pubsub.json")
    }

    fn load(&mut self) -> Result<(), String> {
        if !self.config.storage_path.is_empty() {
            match std::fs::read(self.resource_file_path()) {
                Ok(data) => {
                    let file = ResourceFile::from_slice(&data).map_err(|e| e.to_string())?;
                    for t in file.topics {
                        if !t.name.is_empty() {
                            self.topics.insert(t.name.clone(), t);
                        }
                    }
                    for sub in file.subscriptions {
                        if !sub.name.is_empty() {
                            self.subscriptions.insert(sub.name.clone(), sub);
                        }
                    }
                    for snap in file.snapshots {
                        if !snap.name.is_empty() {
                            self.snapshots.insert(snap.name.clone(), snap);
                        }
                    }
                    for sch in file.schemas {
                        if !sch.name.is_empty() {
                            self.schemas.insert(sch.name.clone(), sch);
                        }
                    }
                    // When message state lives in resources.json (no separate
                    // message store), load it here.
                    if self.config.message_storage_path.is_empty() {
                        self.load_message_state(
                            file.messages,
                            file.deliveries,
                            file.next_message_id,
                            file.next_ack_id,
                        );
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        if self.config.message_storage_path.is_empty() {
            return Ok(());
        }
        match std::fs::read(self.message_state_file_path()) {
            Ok(data) => {
                let file = MessageStateFile::from_slice(&data).map_err(|e| e.to_string())?;
                self.messages.clear();
                self.deliveries.clear();
                self.next_message_id = 0;
                self.load_message_state(
                    file.messages,
                    file.deliveries,
                    file.next_message_id,
                    file.next_ack_id,
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
        Ok(())
    }

    fn load_message_state(
        &mut self,
        messages: Vec<crate::model::PubsubMessage>,
        deliveries: BTreeMap<String, Vec<crate::model::DeliveryRecord>>,
        next_message_id: u64,
        next_ack_id: u64,
    ) {
        for m in messages {
            if !m.message_id.is_empty() {
                if let Ok(id) = m.message_id.parse::<u64>() {
                    if id > self.next_message_id {
                        self.next_message_id = id;
                    }
                }
                self.messages.insert(m.message_id.clone(), m);
            }
        }
        for (sub, records) in deliveries {
            if !sub.is_empty() {
                self.deliveries.insert(sub, records);
            }
        }
        if next_message_id > self.next_message_id {
            self.next_message_id = next_message_id;
        }
        self.next_ack_id = next_ack_id;
    }

    /// Persists state byte-compatibly with `saveResourcesLocked`: resources.json
    /// (topics/subscriptions/snapshots/schemas, plus message state when no
    /// separate message store) and pubsub.json (message state) when a message
    /// store is configured. Cleanup of unreferenced messages runs first.
    pub(crate) fn persist(&mut self) -> Result<(), ApiError> {
        if self.config.storage_path.is_empty() && self.config.message_storage_path.is_empty() {
            self.cleanup_unreferenced_messages();
            return Ok(());
        }
        self.cleanup_unreferenced_messages();

        let messages: Vec<crate::model::PubsubMessage> = self.messages.values().cloned().collect();
        let deliveries: BTreeMap<String, Vec<crate::model::DeliveryRecord>> = self
            .deliveries
            .iter()
            .filter(|(_, r)| !r.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let include_message_state = self.config.message_storage_path.is_empty();

        if !self.config.storage_path.is_empty() {
            std::fs::create_dir_all(&self.config.storage_path)
                .map_err(|_| ApiError::internal("pubsub resource store unavailable"))?;
            let mut file = ResourceFile {
                topics: self.topics.values().cloned().collect(),
                subscriptions: self.subscriptions.values().cloned().collect(),
                snapshots: self.snapshots.values().cloned().collect(),
                schemas: self.schemas.values().cloned().collect(),
                ..Default::default()
            };
            if include_message_state {
                file.messages = messages.clone();
                file.deliveries = deliveries.clone();
                file.next_message_id = self.next_message_id;
                file.next_ack_id = self.next_ack_id;
            }
            write_atomic(&self.resource_file_path(), &file.to_bytes())?;
        }
        if self.config.message_storage_path.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.config.message_storage_path)
            .map_err(|_| ApiError::internal("pubsub resource store unavailable"))?;
        let msg_file = MessageStateFile {
            messages,
            deliveries,
            next_message_id: self.next_message_id,
            next_ack_id: self.next_ack_id,
        };
        write_atomic(&self.message_state_file_path(), &msg_file.to_bytes())
    }

    /// Drops messages no subscription/snapshot references, mirroring
    /// `cleanupUnreferencedMessagesLocked`.
    fn cleanup_unreferenced_messages(&mut self) {
        if self.messages.is_empty() {
            return;
        }
        let mut referenced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for (sub_name, records) in &self.deliveries {
            let retain_acked = self
                .subscriptions
                .get(sub_name)
                .map(|s| s.retain_acked_messages)
                .unwrap_or(false);
            for r in records {
                if r.acked && !retain_acked {
                    continue;
                }
                referenced.insert(r.message_id.clone());
            }
        }
        for snap in self.snapshots.values() {
            for r in &snap.deliveries {
                referenced.insert(r.message_id.clone());
            }
        }
        self.messages.retain(|id, _| referenced.contains(id));
    }

    /// Mirrors `cleanupRetainedMessagesLocked`: folds (a) expired-snapshot
    /// cleanup and (b) message-retention expiry. `now_secs` is whole UNIX seconds
    /// from the shared clock (`now_parts()`); retention math is exact to the
    /// nanosecond against each message's publishTime.
    pub(crate) fn cleanup_retained_messages(&mut self, now_secs: i64) {
        self.cleanup_expired_snapshots(now_secs);
        if self.config.message_retention_seconds <= 0 || self.messages.is_empty() {
            return;
        }
        let now_nanos = (now_secs as i128) * 1_000_000_000;
        // Per-subscription retention: drop deliveries whose message published
        // before `now - retention` (or whose message is gone).
        let sub_names: Vec<String> = self.deliveries.keys().cloned().collect();
        for sub_name in sub_names {
            let retention_nanos = self.subscription_message_retention_nanos(&sub_name);
            let cutoff = now_nanos - retention_nanos;
            let deliveries = self.deliveries.get(&sub_name).cloned().unwrap_or_default();
            let kept: Vec<crate::model::DeliveryRecord> = deliveries
                .into_iter()
                .filter(|d| match self.messages.get(&d.message_id) {
                    Some(message) => match crate::time_fmt::parse_rfc3339(&message.publish_time) {
                        Some((secs, nanos)) => {
                            let published = (secs as i128) * 1_000_000_000 + nanos as i128;
                            // Keep when NOT before cutoff (matches legacy
                            // `!publishedAt.Before(cutoff)`).
                            published >= cutoff
                        }
                        None => false,
                    },
                    None => false,
                })
                .collect();
            if kept.is_empty() {
                self.deliveries.remove(&sub_name);
            } else {
                self.deliveries.insert(sub_name, kept);
            }
        }
        // Snapshots use the global retention window.
        let global_cutoff =
            now_nanos - (self.config.message_retention_seconds as i128) * 1_000_000_000;
        let snap_names: Vec<String> = self.snapshots.keys().cloned().collect();
        for name in snap_names {
            let Some(snapshot) = self.snapshots.get(&name).cloned() else {
                continue;
            };
            let kept: Vec<crate::model::DeliveryRecord> = snapshot
                .deliveries
                .into_iter()
                .filter(|d| match self.messages.get(&d.message_id) {
                    Some(message) => match crate::time_fmt::parse_rfc3339(&message.publish_time) {
                        Some((secs, nanos)) => {
                            let published = (secs as i128) * 1_000_000_000 + nanos as i128;
                            published >= global_cutoff
                        }
                        None => false,
                    },
                    None => false,
                })
                .collect();
            if let Some(snap) = self.snapshots.get_mut(&name) {
                snap.deliveries = kept;
            }
        }
        self.cleanup_unreferenced_messages();
    }

    /// Mirrors `cleanupExpiredSnapshotsLocked`.
    fn cleanup_expired_snapshots(&mut self, _now_secs: i64) {
        // `snapshot_expired` reads the shared clock (`self.now()`), the same
        // instant `now_secs` derives from, so the verdict matches legacy
        // `snapshotExpired(snapshot, now)`.
        let expired: Vec<String> = self
            .snapshots
            .iter()
            .filter(|(_, snap)| self.snapshot_expired(snap))
            .map(|(name, _)| name.clone())
            .collect();
        for name in expired {
            self.snapshots.remove(&name);
        }
    }

    /// Mirrors `subscriptionMessageRetentionLocked` — returns the effective
    /// retention as nanoseconds: subscription override, else topic override, else
    /// the configured global fallback.
    fn subscription_message_retention_nanos(&self, subscription_name: &str) -> i128 {
        let fallback = (self.config.message_retention_seconds as i128) * 1_000_000_000;
        let Some(subscription) = self.subscriptions.get(subscription_name) else {
            return fallback;
        };
        if subscription.message_retention_duration.trim().is_empty() {
            let Some(topic) = self.topics.get(&subscription.topic) else {
                return fallback;
            };
            if topic.message_retention_duration.trim().is_empty() {
                return fallback;
            }
            return match crate::duration::parse_go_duration(&topic.message_retention_duration) {
                Some(nanos) if nanos > 0 => nanos,
                _ => fallback,
            };
        }
        match crate::duration::parse_go_duration(&subscription.message_retention_duration) {
            Some(nanos) if nanos > 0 => nanos,
            _ => fallback,
        }
    }

    // --- topic operations -------------------------------------------------

    /// `PUT /v1/projects/<p>/topics/<id>` — create a topic.
    pub fn create_topic(
        &mut self,
        project: &str,
        topic_id: &str,
        request: &Topic,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::topic_name(project, topic_id);
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !request.name.is_empty() && request.name != name {
            return Err(ApiError::invalid_argument(
                "topic name does not match request path",
            ));
        }
        validate_topic_metadata(request)?;
        let now = self.now();
        let topic = Topic {
            name: name.clone(),
            labels: request.labels.clone(),
            created_at: now.clone(),
            updated_at: now,
            message_retention_duration: request.message_retention_duration.clone(),
            schema_settings: request.schema_settings.clone(),
            kms_key_name: request.kms_key_name.clone(),
        };
        if self.topics.contains_key(&name) {
            return Err(ApiError::already_exists("topic already exists"));
        }
        self.topics.insert(name.clone(), topic.clone());
        if let Err(err) = self.persist() {
            self.topics.remove(&name);
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&topic))
    }

    /// `GET /v1/projects/<p>/topics/<id>`.
    pub fn get_topic(&self, project: &str, topic_id: &str) -> Result<RestResponse, ApiError> {
        let name = paths::topic_name(project, topic_id);
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        match self.topics.get(&name) {
            Some(topic) => Ok(RestResponse::ok_struct(topic)),
            None => Err(ApiError::not_found("topic not found")),
        }
    }

    /// `PATCH /v1/projects/<p>/topics/<id>` — apply an update mask.
    pub fn patch_topic(
        &mut self,
        project: &str,
        topic_id: &str,
        patch: &Topic,
        fields: &[String],
    ) -> Result<RestResponse, ApiError> {
        let name = paths::topic_name(project, topic_id);
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !patch.name.is_empty() && patch.name != name {
            return Err(ApiError::invalid_argument(
                "topic name does not match request path",
            ));
        }
        validate_topic_metadata(patch)?;
        let Some(mut topic) = self.topics.get(&name).cloned() else {
            return Err(ApiError::not_found("topic not found"));
        };
        for field in fields {
            match field.as_str() {
                "labels" => topic.labels = patch.labels.clone(),
                "messageRetentionDuration" => {
                    topic.message_retention_duration = patch.message_retention_duration.clone()
                }
                "schemaSettings" => topic.schema_settings = patch.schema_settings.clone(),
                "kmsKeyName" => topic.kms_key_name = patch.kms_key_name.clone(),
                _ => {}
            }
        }
        topic.updated_at = self.now();
        let previous = self.topics.insert(name.clone(), topic.clone());
        if let Err(err) = self.persist() {
            match previous {
                Some(p) => {
                    self.topics.insert(name, p);
                }
                None => {
                    self.topics.remove(&name);
                }
            }
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&topic))
    }

    /// `DELETE /v1/projects/<p>/topics/<id>`.
    pub fn delete_topic(
        &mut self,
        project: &str,
        topic_id: &str,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::topic_name(project, topic_id);
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !self.topics.contains_key(&name) {
            return Err(ApiError::not_found("topic not found"));
        }
        for sub in self.subscriptions.values() {
            if sub.topic == name && !sub.detached {
                return Err(ApiError::failed_precondition(
                    "topic has attached subscriptions",
                ));
            }
        }
        let removed = self.topics.remove(&name);
        if let Err(err) = self.persist() {
            if let Some(t) = removed {
                self.topics.insert(name, t);
            }
            return Err(err);
        }
        Ok(RestResponse::no_content())
    }

    /// `GET /v1/projects/<p>/topics` — list topics in a project.
    pub fn list_topics(
        &self,
        project: &str,
        page_size: i64,
        page_token: i64,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        // BTreeMap iteration is name-sorted, matching legacy explicit sort.
        let topics: Vec<&Topic> = self
            .topics
            .values()
            .filter(|t| paths::resource_project(&t.name) == project)
            .collect();
        let (start, end, next) = page_bounds(topics.len(), page_token, page_size);
        let page: Vec<Topic> = topics[start..end].iter().map(|t| (*t).clone()).collect();
        Ok(RestResponse::ok_struct(
            &crate::responses::ListTopicsResponse {
                next_page_token: next,
                topics: page,
            },
        ))
    }

    /// `GET /v1/projects/<p>/topics/<id>/subscriptions`.
    pub fn list_topic_subscriptions(
        &self,
        project: &str,
        topic_id: &str,
        page_size: i64,
        page_token: i64,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::topic_name(project, topic_id);
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !self.topics.contains_key(&name) {
            return Err(ApiError::not_found("topic not found"));
        }
        let mut subs: Vec<String> = self
            .subscriptions
            .values()
            .filter(|s| s.topic == name && !s.detached)
            .map(|s| s.name.clone())
            .collect();
        subs.sort();
        let (start, end, next) = page_bounds(subs.len(), page_token, page_size);
        Ok(RestResponse::ok_struct(
            &crate::responses::ListSubscriptionNamesResponse {
                next_page_token: next,
                subscriptions: subs[start..end].to_vec(),
            },
        ))
    }

    /// `GET /v1/projects/<p>/topics/<id>/snapshots`.
    pub fn list_topic_snapshots(
        &self,
        project: &str,
        topic_id: &str,
        page_size: i64,
        page_token: i64,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::topic_name(project, topic_id);
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !self.topics.contains_key(&name) {
            return Err(ApiError::not_found("topic not found"));
        }
        // Snapshot expiry is handled in the snapshots part; here all snapshots
        // for the topic are listed by name.
        let mut snaps: Vec<String> = self
            .snapshots
            .values()
            .filter(|s| s.topic == name)
            .map(|s| s.name.clone())
            .collect();
        snaps.sort();
        let (start, end, next) = page_bounds(snaps.len(), page_token, page_size);
        Ok(RestResponse::ok_struct(
            &crate::responses::ListSnapshotNamesResponse {
                next_page_token: next,
                snapshots: snaps[start..end].to_vec(),
            },
        ))
    }

    // --- subscription operations ------------------------------------------

    fn default_ack_deadline(&self) -> i64 {
        if self.config.default_ack_deadline_seconds > 0 {
            self.config.default_ack_deadline_seconds
        } else {
            10
        }
    }

    fn max_ack_deadline(&self) -> i64 {
        if self.config.max_ack_deadline_seconds > 0 {
            self.config.max_ack_deadline_seconds
        } else {
            600
        }
    }

    /// Configured (defaulted) max ack deadline, exposed for the StreamingPull
    /// modack bounds check.
    pub fn max_ack_deadline_value(&self) -> i64 {
        self.max_ack_deadline()
    }

    /// `PUT /v1/projects/<p>/subscriptions/<id>`.
    pub fn create_subscription(
        &mut self,
        project: &str,
        subscription_id: &str,
        request: &Subscription,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::subscription_name(project, subscription_id);
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if request.topic.is_empty() {
            return Err(ApiError::invalid_argument("subscription topic is required"));
        }
        if !paths::valid_full_topic_name(&request.topic) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if request.ack_deadline_seconds < 0 {
            return Err(ApiError::invalid_argument(
                "ackDeadlineSeconds must be non-negative",
            ));
        }
        let mut ack = request.ack_deadline_seconds;
        if ack == 0 {
            ack = self.default_ack_deadline();
        }
        if ack > self.max_ack_deadline() {
            return Err(ApiError::invalid_argument(
                "ackDeadlineSeconds exceeds maxAckDeadlineSeconds",
            ));
        }
        crate::validation::validate_subscription_filter(&request.filter)?;
        crate::validation::validate_subscription_metadata(
            &request.message_retention_duration,
            request.expiration_policy.as_ref(),
        )?;
        crate::validation::validate_dead_letter_policy(request.dead_letter_policy.as_ref())?;
        crate::validation::validate_retry_policy(request.retry_policy.as_ref())?;
        crate::validation::validate_push_config(request.push_config.as_ref())?;

        if self.subscriptions.contains_key(&name) {
            return Err(ApiError::already_exists("subscription already exists"));
        }
        if !self.topics.contains_key(&request.topic) {
            return Err(ApiError::not_found("topic not found"));
        }
        if !self.dead_letter_topic_exists(request.dead_letter_policy.as_ref()) {
            return Err(ApiError::not_found("dead-letter topic not found"));
        }
        let now = self.now();
        let subscription = Subscription {
            name: name.clone(),
            ack_deadline_seconds: ack,
            created_at: now.clone(),
            updated_at: now,
            labels: request.labels.clone(),
            ..request.clone()
        };
        self.subscriptions
            .insert(name.clone(), subscription.clone());
        if let Err(err) = self.persist() {
            self.subscriptions.remove(&name);
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&subscription))
    }

    /// `GET /v1/projects/<p>/subscriptions/<id>`.
    pub fn get_subscription(
        &self,
        project: &str,
        subscription_id: &str,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::subscription_name(project, subscription_id);
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        match self.subscriptions.get(&name) {
            Some(s) => Ok(RestResponse::ok_struct(s)),
            None => Err(ApiError::not_found("subscription not found")),
        }
    }

    /// `PATCH /v1/projects/<p>/subscriptions/<id>`.
    pub fn patch_subscription(
        &mut self,
        project: &str,
        subscription_id: &str,
        patch: &Subscription,
        fields: &[String],
    ) -> Result<RestResponse, ApiError> {
        let name = paths::subscription_name(project, subscription_id);
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !patch.name.is_empty() && patch.name != name {
            return Err(ApiError::invalid_argument(
                "subscription name does not match request path",
            ));
        }
        if patch.ack_deadline_seconds < 0 {
            return Err(ApiError::invalid_argument(
                "ackDeadlineSeconds must be non-negative",
            ));
        }
        if patch.ack_deadline_seconds > self.max_ack_deadline() {
            return Err(ApiError::invalid_argument(
                "ackDeadlineSeconds exceeds maxAckDeadlineSeconds",
            ));
        }
        let has = |f: &str| fields.iter().any(|x| x == f);
        if has("filter") {
            crate::validation::validate_subscription_filter(&patch.filter)?;
        }
        if has("messageRetentionDuration") || has("expirationPolicy") {
            crate::validation::validate_subscription_metadata(
                &patch.message_retention_duration,
                patch.expiration_policy.as_ref(),
            )?;
        }
        if has("deadLetterPolicy") {
            crate::validation::validate_dead_letter_policy(patch.dead_letter_policy.as_ref())?;
        }
        if has("retryPolicy") {
            crate::validation::validate_retry_policy(patch.retry_policy.as_ref())?;
        }
        if has("pushConfig") {
            crate::validation::validate_push_config(patch.push_config.as_ref())?;
        }

        let Some(mut sub) = self.subscriptions.get(&name).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        if has("deadLetterPolicy")
            && !self.dead_letter_topic_exists(patch.dead_letter_policy.as_ref())
        {
            return Err(ApiError::not_found("dead-letter topic not found"));
        }
        if has("topic") && !patch.topic.is_empty() && patch.topic != sub.topic {
            return Err(ApiError::failed_precondition(
                "subscription topic cannot be changed",
            ));
        }
        if has("labels") {
            sub.labels = patch.labels.clone();
        }
        if has("ackDeadlineSeconds") {
            sub.ack_deadline_seconds = if patch.ack_deadline_seconds == 0 {
                self.default_ack_deadline()
            } else {
                patch.ack_deadline_seconds
            };
        }
        if has("enableMessageOrdering") {
            sub.enable_message_ordering = patch.enable_message_ordering;
        }
        if has("enableExactlyOnceDelivery") {
            sub.enable_exactly_once_delivery = patch.enable_exactly_once_delivery;
        }
        if has("retainAckedMessages") {
            sub.retain_acked_messages = patch.retain_acked_messages;
        }
        if has("messageRetentionDuration") {
            sub.message_retention_duration = patch.message_retention_duration.clone();
        }
        if has("expirationPolicy") {
            sub.expiration_policy = patch.expiration_policy.clone();
        }
        if has("filter") {
            sub.filter = patch.filter.clone();
        }
        if has("deadLetterPolicy") {
            sub.dead_letter_policy = patch.dead_letter_policy.clone();
        }
        if has("retryPolicy") {
            sub.retry_policy = patch.retry_policy.clone();
        }
        if has("pushConfig") {
            sub.push_config = patch.push_config.clone();
        }
        sub.updated_at = self.now();
        let previous = self.subscriptions.insert(name.clone(), sub.clone());
        if let Err(err) = self.persist() {
            match previous {
                Some(p) => {
                    self.subscriptions.insert(name, p);
                }
                None => {
                    self.subscriptions.remove(&name);
                }
            }
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&sub))
    }

    /// `DELETE /v1/projects/<p>/subscriptions/<id>`.
    pub fn delete_subscription(
        &mut self,
        project: &str,
        subscription_id: &str,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::subscription_name(project, subscription_id);
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !self.subscriptions.contains_key(&name) {
            return Err(ApiError::not_found("subscription not found"));
        }
        let removed = self.subscriptions.remove(&name);
        // Snapshots referencing this subscription are removed too.
        let drop: Vec<String> = self
            .snapshots
            .iter()
            .filter(|(_, snap)| snap.subscription == name)
            .map(|(k, _)| k.clone())
            .collect();
        let removed_snaps: Vec<_> = drop
            .iter()
            .filter_map(|k| self.snapshots.remove(k).map(|v| (k.clone(), v)))
            .collect();
        if let Err(err) = self.persist() {
            if let Some(s) = removed {
                self.subscriptions.insert(name, s);
            }
            for (k, v) in removed_snaps {
                self.snapshots.insert(k, v);
            }
            return Err(err);
        }
        Ok(RestResponse::no_content())
    }

    /// `GET /v1/projects/<p>/subscriptions` — list full subscriptions.
    pub fn list_subscriptions(
        &self,
        project: &str,
        page_size: i64,
        page_token: i64,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let subs: Vec<Subscription> = self
            .subscriptions
            .values()
            .filter(|s| paths::resource_project(&s.name) == project)
            .cloned()
            .collect();
        let (start, end, next) = page_bounds(subs.len(), page_token, page_size);
        Ok(RestResponse::ok_struct(
            &crate::responses::ListSubscriptionsResponse {
                next_page_token: next,
                subscriptions: subs[start..end].to_vec(),
            },
        ))
    }

    /// `POST /v1/projects/<p>/subscriptions/<id>:modifyPushConfig`.
    pub fn modify_push_config(
        &mut self,
        project: &str,
        subscription_id: &str,
        push_config: Option<&Value>,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::subscription_name(project, subscription_id);
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        crate::validation::validate_push_config(push_config)?;
        let Some(mut sub) = self.subscriptions.get(&name).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        sub.push_config = normalize_any_map(push_config);
        sub.updated_at = self.now();
        let previous = self.subscriptions.insert(name.clone(), sub);
        if let Err(err) = self.persist() {
            if let Some(p) = previous {
                self.subscriptions.insert(name, p);
            }
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&serde_json::Map::new()))
    }

    /// `POST /v1/projects/<p>/subscriptions/<id>:detach`.
    pub fn detach_subscription(
        &mut self,
        project: &str,
        subscription_id: &str,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::subscription_name(project, subscription_id);
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let Some(mut sub) = self.subscriptions.get(&name).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        sub.detached = true;
        sub.updated_at = self.now();
        let removed_snaps: Vec<String> = self
            .snapshots
            .iter()
            .filter(|(_, snap)| snap.subscription == name)
            .map(|(k, _)| k.clone())
            .collect();
        let backup: Vec<_> = removed_snaps
            .iter()
            .filter_map(|k| self.snapshots.remove(k).map(|v| (k.clone(), v)))
            .collect();
        let previous = self.subscriptions.insert(name.clone(), sub);
        if let Err(err) = self.persist() {
            match previous {
                Some(p) => {
                    self.subscriptions.insert(name, p);
                }
                None => {
                    self.subscriptions.remove(&name);
                }
            }
            for (k, v) in backup {
                self.snapshots.insert(k, v);
            }
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&serde_json::Map::new()))
    }

    fn dead_letter_topic_exists(&self, policy: Option<&Value>) -> bool {
        let topic = crate::validation::dead_letter_topic(policy);
        if topic.is_empty() {
            return true;
        }
        self.topics.contains_key(&topic)
    }

    // --- snapshot operations ----------------------------------------------

    /// `PUT /v1/projects/<p>/snapshots/<id>` — create a snapshot of a
    /// subscription. `subscription` is the full subscription name.
    pub fn create_snapshot(
        &mut self,
        project: &str,
        snapshot_id: &str,
        subscription: &str,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::snapshot_name(project, snapshot_id);
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(snapshot_id) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if self.snapshots.contains_key(&name) {
            return Err(ApiError::already_exists("snapshot already exists"));
        }
        let Some(sub) = self.subscriptions.get(subscription).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        let snapshot = Snapshot {
            name: name.clone(),
            topic: sub.topic.clone(),
            subscription: sub.name.clone(),
            expire_time: self.snapshot_expire_time(),
            // Deliveries are captured from the subscription's pending records;
            // empty here (no messages yet) — the messages part fills them in.
            deliveries: Vec::new(),
            ..Default::default()
        };
        self.snapshots.insert(name.clone(), snapshot.clone());
        if let Err(err) = self.persist() {
            self.snapshots.remove(&name);
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&snapshot_public(&snapshot)))
    }

    /// `GET /v1/projects/<p>/snapshots/<id>`.
    pub fn get_snapshot(&self, project: &str, snapshot_id: &str) -> Result<RestResponse, ApiError> {
        let name = paths::snapshot_name(project, snapshot_id);
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(snapshot_id) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        match self.snapshots.get(&name) {
            Some(snap) if !self.snapshot_expired(snap) => {
                Ok(RestResponse::ok_struct(&snapshot_public(snap)))
            }
            _ => Err(ApiError::not_found("snapshot not found")),
        }
    }

    /// `DELETE /v1/projects/<p>/snapshots/<id>`.
    pub fn delete_snapshot(
        &mut self,
        project: &str,
        snapshot_id: &str,
    ) -> Result<RestResponse, ApiError> {
        let name = paths::snapshot_name(project, snapshot_id);
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(snapshot_id) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        if !self.snapshots.contains_key(&name) {
            return Err(ApiError::not_found("snapshot not found"));
        }
        let removed = self.snapshots.remove(&name);
        if let Err(err) = self.persist() {
            if let Some(s) = removed {
                self.snapshots.insert(name, s);
            }
            return Err(err);
        }
        Ok(RestResponse::no_content())
    }

    /// `GET /v1/projects/<p>/snapshots` — list non-expired snapshots.
    pub fn list_snapshots(
        &self,
        project: &str,
        page_size: i64,
        page_token: i64,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let snaps: Vec<Snapshot> = self
            .snapshots
            .values()
            .filter(|s| paths::resource_project(&s.name) == project && !self.snapshot_expired(s))
            .map(snapshot_public)
            .collect();
        let (start, end, next) = page_bounds(snaps.len(), page_token, page_size);
        Ok(RestResponse::ok_struct(
            &crate::responses::ListSnapshotsResponse {
                next_page_token: next,
                snapshots: snaps[start..end].to_vec(),
            },
        ))
    }

    fn snapshot_expire_time(&self) -> String {
        match crate::time_fmt::parse_rfc3339(&self.now()) {
            Some((secs, nanos)) => {
                crate::time_fmt::rfc3339nano_from_unix(secs + 7 * 24 * 3600, nanos)
            }
            None => String::new(),
        }
    }

    fn snapshot_expired(&self, snapshot: &Snapshot) -> bool {
        let expire = snapshot.expire_time.trim();
        if expire.is_empty() {
            return false;
        }
        let Some((exp_secs, _)) = crate::time_fmt::parse_rfc3339(expire) else {
            return false;
        };
        let Some((now_secs, _)) = crate::time_fmt::parse_rfc3339(&self.now()) else {
            return false;
        };
        // Expired when expireTime is not after now.
        exp_secs <= now_secs
    }

    // --- schema operations ------------------------------------------------

    /// `POST /v1/projects/<p>/schemas?schemaId=<id>` and
    /// `PUT /v1/projects/<p>/schemas/<id>` — create a schema. `schema_id` is the
    /// path/query id; `request` is the body.
    pub fn create_schema(
        &mut self,
        project: &str,
        schema_id: &str,
        request: &Schema,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if schema_id.trim().is_empty() {
            return Err(ApiError::invalid_argument("schemaId is required"));
        }
        if !paths::valid_resource_id(schema_id) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        let name = paths::schema_name(project, schema_id);
        if !request.name.is_empty() && request.name != name {
            return Err(ApiError::invalid_argument(
                "schema name does not match request path",
            ));
        }
        if !request.type_.is_empty() && !crate::validation::valid_schema_type(&request.type_) {
            return Err(ApiError::invalid_argument("invalid schema type"));
        }
        crate::validation::validate_schema_definition(&request.type_, &request.definition)?;
        if self.schemas.contains_key(&name) {
            return Err(ApiError::already_exists("schema already exists"));
        }
        let schema = Schema {
            name: name.clone(),
            revision_id: if request.revision_id.is_empty() {
                "1".to_string()
            } else {
                request.revision_id.clone()
            },
            ..request.clone()
        };
        self.schemas.insert(name.clone(), schema.clone());
        if let Err(err) = self.persist() {
            self.schemas.remove(&name);
            return Err(err);
        }
        Ok(RestResponse::ok_struct(&schema))
    }

    /// `GET /v1/projects/<p>/schemas/<id>` — `view` is `""`/`FULL`/`BASIC`.
    pub fn get_schema(
        &self,
        project: &str,
        schema_id: &str,
        view: &str,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(schema_id) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        let name = paths::schema_name(project, schema_id);
        match self.schemas.get(&name) {
            Some(schema) => Ok(RestResponse::ok_struct(&schema_public(schema, view))),
            None => Err(ApiError::not_found("schema not found")),
        }
    }

    /// `DELETE /v1/projects/<p>/schemas/<id>`.
    pub fn delete_schema(
        &mut self,
        project: &str,
        schema_id: &str,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(schema_id) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        let name = paths::schema_name(project, schema_id);
        if !self.schemas.contains_key(&name) {
            return Err(ApiError::not_found("schema not found"));
        }
        let removed = self.schemas.remove(&name);
        if let Err(err) = self.persist() {
            if let Some(s) = removed {
                self.schemas.insert(name, s);
            }
            return Err(err);
        }
        Ok(RestResponse::no_content())
    }

    /// `GET /v1/projects/<p>/schemas` — list schemas (`view` applied).
    pub fn list_schemas(
        &self,
        project: &str,
        view: &str,
        page_size: i64,
        page_token: i64,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let schemas: Vec<Schema> = self
            .schemas
            .values()
            .filter(|s| paths::resource_project(&s.name) == project)
            .map(|s| schema_public(s, view))
            .collect();
        let (start, end, next) = page_bounds(schemas.len(), page_token, page_size);
        Ok(RestResponse::ok_struct(
            &crate::responses::ListSchemasResponse {
                next_page_token: next,
                schemas: schemas[start..end].to_vec(),
            },
        ))
    }

    /// `POST /v1/projects/<p>/schemas:validateMessage`.
    pub fn validate_message(
        &self,
        project: &str,
        request: &Value,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let obj = request.as_object().cloned().unwrap_or_default();
        let name = obj.get("name").and_then(Value::as_str).unwrap_or("");
        let schema_val = obj.get("schema");
        let inline: Schema = schema_val
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let has_inline = !schema_is_empty(&inline);
        let message = obj.get("message").and_then(Value::as_str).unwrap_or("");
        let encoding = obj.get("encoding").and_then(Value::as_str).unwrap_or("");

        if name.is_empty() && !has_inline {
            return Err(ApiError::invalid_argument(
                "schema name or inline schema is required",
            ));
        }
        if !name.is_empty() && has_inline {
            return Err(ApiError::invalid_argument(
                "only one of schema name or inline schema may be set",
            ));
        }
        if !encoding.is_empty() && !crate::validation::valid_schema_encoding(encoding) {
            return Err(ApiError::invalid_argument("invalid schema encoding"));
        }
        if !message.is_empty() {
            let decoded = crate::validation::decode_base64_bytes(message)
                .ok_or_else(|| ApiError::invalid_argument("message must be base64 encoded"))?;
            if !crate::validation::valid_schema_message_data(&decoded, encoding) {
                return Err(ApiError::invalid_argument(
                    "message is invalid for schema encoding",
                ));
            }
        }
        if !name.is_empty() {
            if !paths::valid_full_schema_name(name) {
                return Err(ApiError::invalid_argument("invalid schema name"));
            }
            if paths::resource_project(name) != project {
                return Err(ApiError::failed_precondition(
                    "schema belongs to a different project",
                ));
            }
            if !self.schemas.contains_key(name) {
                return Err(ApiError::not_found("schema not found"));
            }
            return Ok(RestResponse::ok_struct(&serde_json::Map::new()));
        }
        if !inline.name.is_empty() {
            if !paths::valid_full_schema_name(&inline.name) {
                return Err(ApiError::invalid_argument("invalid schema name"));
            }
            if paths::resource_project(&inline.name) != project {
                return Err(ApiError::failed_precondition(
                    "schema belongs to a different project",
                ));
            }
        }
        if !crate::validation::valid_schema_type(&inline.type_) {
            return Err(ApiError::invalid_argument("invalid schema type"));
        }
        crate::validation::validate_schema_definition(&inline.type_, &inline.definition)?;
        Ok(RestResponse::ok_struct(&serde_json::Map::new()))
    }

    // --- message operations -----------------------------------------------

    fn default_ack_deadline_cfg(&self) -> i64 {
        if self.config.default_ack_deadline_seconds > 0 {
            self.config.default_ack_deadline_seconds
        } else {
            10
        }
    }

    fn max_pull_messages(&self) -> i64 {
        if self.config.max_pull_messages > 0 {
            self.config.max_pull_messages
        } else {
            1000
        }
    }

    /// `POST /v1/projects/<p>/topics/<id>:publish`.
    pub fn publish(
        &mut self,
        project: &str,
        topic_id: &str,
        messages: &[Value],
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let name = paths::topic_name(project, topic_id);
        if messages.is_empty() {
            return Err(ApiError::invalid_argument("messages are required"));
        }
        for m in messages {
            let data = m.get("data").and_then(Value::as_str).unwrap_or("");
            let attrs = m.get("attributes").and_then(Value::as_object);
            validate_publish_message(data, attrs)?;
        }
        let Some(topic) = self.topics.get(&name).cloned() else {
            return Err(ApiError::not_found("topic not found"));
        };
        for m in messages {
            let data = m.get("data").and_then(Value::as_str).unwrap_or("");
            validate_message_against_topic_schema(data, topic.schema_settings.as_ref())?;
        }
        let now = self.now();
        let mut message_ids = Vec::with_capacity(messages.len());
        for incoming in messages {
            self.next_message_id += 1;
            let message_id = self.next_message_id.to_string();
            let attributes: BTreeMap<String, String> = incoming
                .get("attributes")
                .and_then(Value::as_object)
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let message = crate::model::PubsubMessage {
                data: incoming
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                attributes,
                message_id: message_id.clone(),
                publish_time: now.clone(),
                ordering_key: incoming
                    .get("orderingKey")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            };
            self.messages.insert(message_id.clone(), message.clone());
            let matched: Vec<String> = self
                .subscriptions
                .values()
                .filter(|sub| {
                    sub.topic == name
                        && !sub.detached
                        && subscription_matches_message(sub, &message)
                })
                .map(|sub| sub.name.clone())
                .collect();
            for sub_name in matched {
                self.deliveries
                    .entry(sub_name)
                    .or_default()
                    .push(crate::model::DeliveryRecord {
                        message_id: message_id.clone(),
                        ..Default::default()
                    });
            }
            message_ids.push(message_id);
        }
        self.persist()?;
        Ok(RestResponse::ok_struct(&serde_json::json!({
            "messageIds": message_ids,
        })))
    }

    /// `POST /v1/projects/<p>/subscriptions/<id>:pull`.
    pub fn pull(
        &mut self,
        project: &str,
        subscription_id: &str,
        max_messages: i64,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let name = paths::subscription_name(project, subscription_id);
        let mut max = if max_messages <= 0 { 1 } else { max_messages };
        if max > self.max_pull_messages() {
            max = self.max_pull_messages();
        }
        let Some(sub) = self.subscriptions.get(&name).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        if sub.detached {
            return Err(ApiError::failed_precondition("subscription is detached"));
        }
        if subscription_push_endpoint(&sub).is_some() {
            return Err(ApiError::failed_precondition(
                "subscription is configured for push delivery",
            ));
        }
        let (now_secs, _) = self.now_parts();
        self.cleanup_retained_messages(now_secs);
        self.expire_leases(now_secs);
        let ack_deadline = if sub.ack_deadline_seconds > 0 {
            sub.ack_deadline_seconds
        } else {
            self.default_ack_deadline_cfg()
        };

        let mut blocked: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if sub.enable_message_ordering {
            for d in self.deliveries.get(&name).into_iter().flatten() {
                if d.acked || !crate::delivery::after(&d.lease_deadline, now_secs) {
                    continue;
                }
                if let Some(m) = self.messages.get(&d.message_id) {
                    if !m.ordering_key.is_empty() {
                        blocked.insert(m.ordering_key.clone());
                    }
                }
            }
        }

        let mut received: Vec<Value> = Vec::new();
        let mut deliveries = self.deliveries.get(&name).cloned().unwrap_or_default();
        let mut dead_lettered: Vec<(String, crate::model::PubsubMessage)> = Vec::new();
        // Index-based loop: each iteration mutates `deliveries[i]` while reading
        // `self.messages`, so a by-value iterator does not fit.
        #[allow(clippy::needless_range_loop)]
        for i in 0..deliveries.len() {
            if received.len() as i64 >= max {
                break;
            }
            if deliveries[i].acked
                || crate::delivery::after(&deliveries[i].lease_deadline, now_secs)
            {
                continue;
            }
            if crate::delivery::after(&deliveries[i].next_delivery_time, now_secs) {
                if sub.enable_message_ordering {
                    if let Some(m) = self.messages.get(&deliveries[i].message_id) {
                        if !m.ordering_key.is_empty() {
                            blocked.insert(m.ordering_key.clone());
                        }
                    }
                }
                continue;
            }
            let Some(message) = self.messages.get(&deliveries[i].message_id).cloned() else {
                continue;
            };
            if let Some((dl_topic, max_attempts)) = self.dead_letter_target(&sub) {
                if deliveries[i].delivery_attempt >= max_attempts {
                    self.next_message_id += 1;
                    let dl_id = self.next_message_id.to_string();
                    let dl_msg = crate::model::PubsubMessage {
                        message_id: dl_id.clone(),
                        publish_time: self.now(),
                        ..message.clone()
                    };
                    dead_lettered.push((dl_topic, dl_msg));
                    deliveries[i].acked = true;
                    deliveries[i].ack_id = String::new();
                    deliveries[i].lease_deadline = crate::model::ZERO_TIME.to_string();
                    deliveries[i].next_delivery_time = crate::model::ZERO_TIME.to_string();
                    continue;
                }
            }
            if sub.enable_message_ordering && !message.ordering_key.is_empty() {
                if blocked.contains(&message.ordering_key) {
                    continue;
                }
                blocked.insert(message.ordering_key.clone());
            }
            self.next_ack_id += 1;
            deliveries[i].ack_id = format!("{}-{}", deliveries[i].message_id, self.next_ack_id);
            deliveries[i].lease_deadline = crate::delivery::plus_seconds(now_secs, ack_deadline);
            deliveries[i].next_delivery_time = crate::model::ZERO_TIME.to_string();
            deliveries[i].delivery_attempt += 1;
            received.push(serde_json::json!({
                "ackId": deliveries[i].ack_id,
                "message": pull_message_value(&message),
                "deliveryAttempt": deliveries[i].delivery_attempt,
            }));
        }

        for (dl_topic, dl_msg) in dead_lettered {
            self.messages
                .insert(dl_msg.message_id.clone(), dl_msg.clone());
            let dl_subs: Vec<String> = self
                .subscriptions
                .values()
                .filter(|c| c.topic == dl_topic && !c.detached)
                .map(|c| c.name.clone())
                .collect();
            for cn in dl_subs {
                self.deliveries
                    .entry(cn)
                    .or_default()
                    .push(crate::model::DeliveryRecord {
                        message_id: dl_msg.message_id.clone(),
                        ..Default::default()
                    });
            }
        }

        let compacted = compact_acked(deliveries, sub.retain_acked_messages);
        self.deliveries.insert(name.clone(), compacted);
        self.persist()?;
        if received.is_empty() {
            return Ok(RestResponse::ok_struct(&serde_json::Map::new()));
        }
        Ok(RestResponse::ok_struct(&serde_json::json!({
            "receivedMessages": received,
        })))
    }

    /// `POST /v1/projects/<p>/subscriptions/<id>:acknowledge`.
    pub fn acknowledge(
        &mut self,
        project: &str,
        subscription_id: &str,
        ack_ids: &[String],
    ) -> Result<RestResponse, ApiError> {
        self.update_ack_deadlines(project, subscription_id, ack_ids, None, true)
    }

    /// `POST /v1/projects/<p>/subscriptions/<id>:modifyAckDeadline`.
    pub fn modify_ack_deadline(
        &mut self,
        project: &str,
        subscription_id: &str,
        ack_ids: &[String],
        ack_deadline_seconds: i64,
    ) -> Result<RestResponse, ApiError> {
        self.update_ack_deadlines(
            project,
            subscription_id,
            ack_ids,
            Some(ack_deadline_seconds),
            false,
        )
    }

    fn update_ack_deadlines(
        &mut self,
        project: &str,
        subscription_id: &str,
        ack_ids: &[String],
        ack_deadline_seconds: Option<i64>,
        acknowledge: bool,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if ack_ids.is_empty() {
            return Ok(RestResponse::ok_struct(&serde_json::Map::new()));
        }
        for ack_id in ack_ids {
            if ack_id.trim().is_empty() {
                return Err(ApiError::invalid_argument(
                    "ackIds must not contain empty values",
                ));
            }
        }
        if let Some(deadline) = ack_deadline_seconds {
            if deadline < 0 {
                return Err(ApiError::invalid_argument(
                    "ackDeadlineSeconds must be non-negative",
                ));
            }
            let max = if self.config.max_ack_deadline_seconds > 0 {
                self.config.max_ack_deadline_seconds
            } else {
                600
            };
            if deadline > max {
                return Err(ApiError::invalid_argument(
                    "ackDeadlineSeconds exceeds maxAckDeadlineSeconds",
                ));
            }
        }
        let name = paths::subscription_name(project, subscription_id);
        let Some(sub) = self.subscriptions.get(&name).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        let (now_secs, _) = self.now_parts();
        self.expire_leases(now_secs);
        let id_set: std::collections::BTreeSet<&String> = ack_ids.iter().collect();
        let mut deliveries = self.deliveries.get(&name).cloned().unwrap_or_default();
        for d in &mut deliveries {
            if !id_set.contains(&d.ack_id) || d.acked {
                continue;
            }
            if acknowledge {
                d.acked = true;
                d.ack_id = String::new();
                d.lease_deadline = crate::model::ZERO_TIME.to_string();
                d.next_delivery_time = crate::model::ZERO_TIME.to_string();
                continue;
            }
            let deadline = ack_deadline_seconds.unwrap_or(0);
            if deadline == 0 {
                d.ack_id = String::new();
                d.lease_deadline = crate::model::ZERO_TIME.to_string();
                d.next_delivery_time = crate::model::ZERO_TIME.to_string();
            } else {
                d.lease_deadline = crate::delivery::plus_seconds(now_secs, deadline);
                d.next_delivery_time = crate::model::ZERO_TIME.to_string();
            }
        }
        let compacted = compact_acked(deliveries, sub.retain_acked_messages);
        self.deliveries.insert(name, compacted);
        self.persist()?;
        Ok(RestResponse::ok_struct(&serde_json::Map::new()))
    }

    /// Expires leases past their deadline, applying retry backoff. Mirrors
    /// `expireLeasesLocked`.
    pub(crate) fn expire_leases(&mut self, now_secs: i64) {
        let names: Vec<String> = self.deliveries.keys().cloned().collect();
        for sub_name in names {
            let mut deliveries = self.deliveries.get(&sub_name).cloned().unwrap_or_default();
            let mut changed = false;
            for d in &mut deliveries {
                if d.acked
                    || crate::delivery::is_zero(&d.lease_deadline)
                    || crate::delivery::after(&d.lease_deadline, now_secs)
                {
                    continue;
                }
                d.ack_id = String::new();
                let lease_secs = crate::delivery::unix_secs(&d.lease_deadline);
                d.lease_deadline = crate::model::ZERO_TIME.to_string();
                let backoff = self.retry_backoff(&sub_name, d.delivery_attempt);
                if backoff > 0 {
                    d.next_delivery_time = crate::delivery::plus_seconds(lease_secs, backoff);
                }
                changed = true;
            }
            if changed {
                self.deliveries.insert(sub_name, deliveries);
            }
        }
    }

    /// Retry backoff in seconds for the next attempt, mirroring
    /// `subscriptionRetryBackoffLocked`.
    fn retry_backoff(&self, sub_name: &str, delivery_attempt: i64) -> i64 {
        let Some(sub) = self.subscriptions.get(sub_name) else {
            return 0;
        };
        let policy = sub.retry_policy.as_ref();
        let Some(min) = policy_backoff_secs(policy, "minimumBackoff") else {
            return 0;
        };
        let max = policy_backoff_secs(policy, "maximumBackoff");
        let mut backoff = min;
        let mut attempt = 1;
        while attempt < delivery_attempt {
            if let Some(m) = max {
                if backoff >= m {
                    return m;
                }
            }
            backoff = backoff.saturating_mul(2);
            attempt += 1;
        }
        if let Some(m) = max {
            if backoff > m {
                return m;
            }
        }
        backoff
    }

    /// The dead-letter `(topic, maxDeliveryAttempts)` for a subscription.
    fn dead_letter_target(&self, sub: &Subscription) -> Option<(String, i64)> {
        let policy = sub.dead_letter_policy.as_ref()?.as_object()?;
        let max = policy.get("maxDeliveryAttempts").and_then(Value::as_i64)?;
        let topic = policy.get("deadLetterTopic").and_then(Value::as_str)?;
        if topic.is_empty() || !self.topics.contains_key(topic) {
            return None;
        }
        Some((topic.to_string(), max))
    }

    // --- IAM (stub) -------------------------------------------------------

    /// IAM action for a topic, mirroring `handleTopicIAM` + `handleIAMAction`.
    pub fn topic_iam(
        &self,
        project: &str,
        topic_id: &str,
        action: &str,
        body: &Value,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(topic_id) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        let name = paths::topic_name(project, topic_id);
        if !self.topics.contains_key(&name) {
            return Err(ApiError::not_found("topic not found"));
        }
        iam_action(action, body)
    }

    /// IAM action for a subscription, mirroring `handleSubscriptionIAM`.
    pub fn subscription_iam(
        &self,
        project: &str,
        subscription_id: &str,
        action: &str,
        body: &Value,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        let name = paths::subscription_name(project, subscription_id);
        if !self.subscriptions.contains_key(&name) {
            return Err(ApiError::not_found("subscription not found"));
        }
        iam_action(action, body)
    }

    // --- Seek -------------------------------------------------------------

    /// `POST /v1/projects/<p>/subscriptions/<id>:seek`. Exactly one of
    /// `snapshot`/`time` must be set.
    pub fn seek(
        &mut self,
        project: &str,
        subscription_id: &str,
        snapshot: &str,
        time: &str,
    ) -> Result<RestResponse, ApiError> {
        if !paths::valid_project_id(project) {
            return Err(ApiError::invalid_argument("invalid project name"));
        }
        if !paths::valid_resource_id(subscription_id) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if snapshot.is_empty() && time.is_empty() {
            return Err(ApiError::invalid_argument("snapshot or time is required"));
        }
        if !snapshot.is_empty() && !time.is_empty() {
            return Err(ApiError::invalid_argument(
                "only one of snapshot or time may be set",
            ));
        }
        let mut seek_secs = 0i64;
        if !time.is_empty() {
            match crate::time_fmt::parse_rfc3339(time) {
                Some((secs, _)) => seek_secs = secs,
                None => return Err(ApiError::invalid_argument("invalid seek time")),
            }
        }
        if !snapshot.is_empty() && !paths::valid_full_snapshot_name(snapshot) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        let name = paths::subscription_name(project, subscription_id);
        if !self.subscriptions.contains_key(&name) {
            return Err(ApiError::not_found("subscription not found"));
        }
        if !time.is_empty() {
            let replayed = self.seek_deliveries_by_time(&name, seek_secs);
            self.deliveries.insert(name, replayed);
            self.persist()?;
            return Ok(RestResponse::ok_struct(&serde_json::Map::new()));
        }
        let snap = match self.snapshots.get(snapshot) {
            Some(s) if !self.snapshot_expired(s) => s.clone(),
            _ => return Err(ApiError::not_found("snapshot not found")),
        };
        if snap.subscription != name {
            return Err(ApiError::failed_precondition(
                "snapshot belongs to a different subscription",
            ));
        }
        let replayed = snapshot_deliveries(&snap.deliveries);
        self.deliveries.insert(name, replayed);
        self.persist()?;
        Ok(RestResponse::ok_struct(&serde_json::Map::new()))
    }

    /// Replays deliveries published at or after `seek_secs`, mirroring
    /// `seekDeliveriesByTimeLocked`.
    fn seek_deliveries_by_time(
        &self,
        sub_name: &str,
        seek_secs: i64,
    ) -> Vec<crate::model::DeliveryRecord> {
        let mut replayed = Vec::new();
        for delivery in self.deliveries.get(sub_name).into_iter().flatten() {
            let Some(message) = self.messages.get(&delivery.message_id) else {
                continue;
            };
            let Some((pub_secs, _)) = crate::time_fmt::parse_rfc3339(&message.publish_time) else {
                continue;
            };
            if pub_secs < seek_secs {
                continue;
            }
            replayed.push(crate::model::DeliveryRecord {
                message_id: delivery.message_id.clone(),
                ..Default::default()
            });
        }
        replayed
    }

    // --- health -----------------------------------------------------------

    /// The `{service,status,protocol}` body for healthz/readyz.
    pub fn health_body(&self) -> Value {
        serde_json::json!({"service": "pubsub", "status": "running", "protocol": "rest"})
    }

    /// True when the resource store loaded cleanly (for readyz).
    pub fn ready(&self) -> bool {
        self.load_err.is_none()
    }

    /// Whether a request is authorized under the configured auth mode, mirroring
    /// `authorize`. `bearer_token` is the token extracted from `Authorization:
    /// Bearer <token>` (empty if absent).
    pub fn authorized(&self, bearer_token: &str) -> bool {
        match self.config.auth_mode.trim().to_lowercase().as_str() {
            "" | "off" | "relaxed" => true,
            "oauth-relaxed" => !bearer_token.is_empty(),
            "bearer-dev" | "strict" => {
                let expected = self.config.bearer_token.trim();
                if bearer_token.is_empty() || expected.is_empty() {
                    return false;
                }
                constant_time_eq(bearer_token.as_bytes(), expected.as_bytes())
            }
            _ => false,
        }
    }

    /// The configured (defaulted) project, for tests/handlers.
    pub fn default_project(&self) -> &str {
        self.config.project()
    }

    // ====================================================================
    // gRPC business logic.
    //
    // These methods mirror the legacy gRPC adapter in
    // `internal/services/pubsub/{topic,subscription,pull,snapshot,schema}_grpc.rs`
    // — NOT the REST handlers above. The two protocols share state but differ in
    // several behaviors (DeleteTopic orphans vs rejects, name auto-generation,
    // snapshot delivery capture, detached filtering, …), so these are authored
    // against the gRPC source for differential parity. Each returns model structs
    // / `ApiError` whose code maps to a gRPC status in `grpc.rs`. Inputs are
    // already proto→model converted by the adapter.
    // ====================================================================

    // --- Publisher --------------------------------------------------------

    /// `Publisher.CreateTopic`, mirroring `topic_grpc.rs:CreateTopic`.
    pub fn grpc_create_topic(&mut self, request: &Topic) -> Result<Topic, ApiError> {
        if !paths::valid_full_topic_name(&request.name) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        validate_topic_metadata(request)?;
        let now = self.now();
        let topic = Topic {
            name: request.name.clone(),
            labels: request.labels.clone(),
            created_at: now.clone(),
            updated_at: now,
            message_retention_duration: request.message_retention_duration.clone(),
            schema_settings: request.schema_settings.clone(),
            kms_key_name: request.kms_key_name.clone(),
        };
        if self.topics.contains_key(&topic.name) {
            return Err(ApiError::already_exists("topic already exists"));
        }
        self.topics.insert(topic.name.clone(), topic.clone());
        if let Err(_err) = self.persist() {
            self.topics.remove(&topic.name);
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(topic)
    }

    /// `Publisher.GetTopic`, mirroring `topic_grpc.rs:GetTopic`.
    pub fn grpc_get_topic(&self, topic: &str) -> Result<Topic, ApiError> {
        if !paths::valid_full_topic_name(topic) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        self.topics
            .get(topic)
            .cloned()
            .ok_or_else(|| ApiError::not_found("topic not found"))
    }

    /// `Publisher.UpdateTopic`, mirroring `topic_grpc.rs:UpdateTopic`. `paths`
    /// are canonical (snake_case) update-mask paths.
    pub fn grpc_update_topic(
        &mut self,
        update: &Topic,
        mask_paths: &[String],
    ) -> Result<Topic, ApiError> {
        if !paths::valid_full_topic_name(&update.name) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        let canonical = grpc_normalize_mask(mask_paths)?;
        validate_topic_metadata(update)?;
        let Some(mut topic) = self.topics.get(&update.name).cloned() else {
            return Err(ApiError::not_found("topic not found"));
        };
        for path in &canonical {
            match grpc_canonical_mask_path(path).as_str() {
                "labels" => topic.labels = update.labels.clone(),
                "message_retention_duration" => {
                    topic.message_retention_duration = update.message_retention_duration.clone()
                }
                "schema_settings" => topic.schema_settings = update.schema_settings.clone(),
                "kms_key_name" => topic.kms_key_name = update.kms_key_name.clone(),
                _ => {
                    return Err(ApiError::invalid_argument(format!(
                        "unsupported topic update mask path {:?}",
                        path
                    )))
                }
            }
        }
        topic.updated_at = self.now();
        self.topics.insert(topic.name.clone(), topic.clone());
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(topic)
    }

    /// `Publisher.ListTopics`, mirroring `topic_grpc.rs:ListTopics`. `project` is
    /// the bare project id (already parsed from `projects/<p>`).
    pub fn grpc_list_topics(
        &self,
        project: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<Topic>, String), ApiError> {
        let topics: Vec<Topic> = self
            .topics
            .values()
            .filter(|t| paths::resource_project(&t.name) == project)
            .cloned()
            .collect();
        let (start, end, next) = grpc_page_bounds(topics.len(), page_size, page_token)?;
        Ok((topics[start..end].to_vec(), next))
    }

    /// `Publisher.ListTopicSubscriptions`, mirroring
    /// `topic_grpc.rs:ListTopicSubscriptions` (does NOT filter detached).
    pub fn grpc_list_topic_subscriptions(
        &self,
        topic: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<String>, String), ApiError> {
        if !paths::valid_full_topic_name(topic) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !self.topics.contains_key(topic) {
            return Err(ApiError::not_found("topic not found"));
        }
        let mut subs: Vec<String> = self
            .subscriptions
            .values()
            .filter(|s| s.topic == topic)
            .map(|s| s.name.clone())
            .collect();
        subs.sort();
        let (start, end, next) = grpc_page_bounds(subs.len(), page_size, page_token)?;
        Ok((subs[start..end].to_vec(), next))
    }

    /// `Publisher.ListTopicSnapshots`, mirroring
    /// `topic_grpc.rs:ListTopicSnapshots` (drops expired).
    pub fn grpc_list_topic_snapshots(
        &self,
        topic: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<String>, String), ApiError> {
        if !paths::valid_full_topic_name(topic) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !self.topics.contains_key(topic) {
            return Err(ApiError::not_found("topic not found"));
        }
        let mut snaps: Vec<String> = self
            .snapshots
            .values()
            .filter(|s| s.topic == topic && !self.snapshot_expired(s))
            .map(|s| s.name.clone())
            .collect();
        snaps.sort();
        let (start, end, next) = grpc_page_bounds(snaps.len(), page_size, page_token)?;
        Ok((snaps[start..end].to_vec(), next))
    }

    /// `Publisher.Publish`, mirroring `topic_grpc.rs:Publish`. Each incoming
    /// message is `(base64_data, attributes, ordering_key)`.
    pub fn grpc_publish(
        &mut self,
        topic_name: &str,
        messages: &[(String, BTreeMap<String, String>, String)],
    ) -> Result<Vec<String>, ApiError> {
        if !paths::valid_full_topic_name(topic_name) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if messages.is_empty() {
            return Err(ApiError::invalid_argument("messages are required"));
        }
        for (data, attrs, _) in messages {
            let attr_map: serde_json::Map<String, Value> = attrs
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();
            validate_publish_message(data, Some(&attr_map))?;
        }
        let Some(topic) = self.topics.get(topic_name).cloned() else {
            return Err(ApiError::not_found("topic not found"));
        };
        for (data, _, _) in messages {
            validate_message_against_topic_schema(data, topic.schema_settings.as_ref())?;
        }
        let now = self.now();
        let mut message_ids = Vec::with_capacity(messages.len());
        for (data, attrs, ordering_key) in messages {
            self.next_message_id += 1;
            let message_id = self.next_message_id.to_string();
            let message = crate::model::PubsubMessage {
                data: data.clone(),
                attributes: attrs.clone(),
                message_id: message_id.clone(),
                publish_time: now.clone(),
                ordering_key: ordering_key.clone(),
            };
            self.messages.insert(message_id.clone(), message.clone());
            let matched: Vec<String> = self
                .subscriptions
                .values()
                .filter(|sub| {
                    sub.topic == topic_name
                        && !sub.detached
                        && subscription_matches_message(sub, &message)
                })
                .map(|sub| sub.name.clone())
                .collect();
            for sub_name in matched {
                self.deliveries
                    .entry(sub_name)
                    .or_default()
                    .push(crate::model::DeliveryRecord {
                        message_id: message_id.clone(),
                        ..Default::default()
                    });
            }
            message_ids.push(message_id);
        }
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(message_ids)
    }

    /// `Publisher.DeleteTopic`, mirroring `topic_grpc.rs:DeleteTopic`. Unlike
    /// REST, attached subscriptions are orphaned to `_deleted-topic_` rather than
    /// rejected.
    pub fn grpc_delete_topic(&mut self, topic: &str) -> Result<(), ApiError> {
        if !paths::valid_full_topic_name(topic) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !self.topics.contains_key(topic) {
            return Err(ApiError::not_found("topic not found"));
        }
        self.topics.remove(topic);
        let now = self.now();
        let affected: Vec<String> = self
            .subscriptions
            .iter()
            .filter(|(_, sub)| sub.topic == topic)
            .map(|(name, _)| name.clone())
            .collect();
        for name in affected {
            if let Some(sub) = self.subscriptions.get_mut(&name) {
                sub.topic = "_deleted-topic_".to_string();
                sub.updated_at = now.clone();
            }
        }
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    /// `Publisher.DetachSubscription`, mirroring
    /// `subscription_grpc.rs:DetachSubscription`.
    pub fn grpc_detach_subscription(&mut self, subscription: &str) -> Result<(), ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        let Some(mut sub) = self.subscriptions.get(subscription).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        sub.detached = true;
        sub.updated_at = self.now();
        self.subscriptions.insert(sub.name.clone(), sub);
        self.deliveries.remove(subscription);
        let drop: Vec<String> = self
            .snapshots
            .iter()
            .filter(|(_, snap)| snap.subscription == subscription)
            .map(|(k, _)| k.clone())
            .collect();
        for k in drop {
            self.snapshots.remove(&k);
        }
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    // --- Subscriber -------------------------------------------------------

    /// `Subscriber.CreateSubscription`, mirroring
    /// `subscription_grpc.rs:CreateSubscription` (auto-generates an empty name).
    pub fn grpc_create_subscription(
        &mut self,
        mut request: Subscription,
    ) -> Result<Subscription, ApiError> {
        if !paths::valid_full_topic_name(&request.topic) {
            return Err(ApiError::invalid_argument("invalid topic name"));
        }
        if !request.name.is_empty() && !paths::valid_full_subscription_name(&request.name) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if request.ack_deadline_seconds == 0 {
            request.ack_deadline_seconds = self.default_ack_deadline();
        }
        crate::validation::validate_subscription_metadata(
            &request.message_retention_duration,
            request.expiration_policy.as_ref(),
        )?;
        crate::validation::validate_subscription_filter(&request.filter)?;
        crate::validation::validate_dead_letter_policy(request.dead_letter_policy.as_ref())?;
        crate::validation::validate_retry_policy(request.retry_policy.as_ref())?;
        crate::validation::validate_push_config(request.push_config.as_ref())?;

        let now = self.now();
        request.created_at = now.clone();
        request.updated_at = now;
        if request.name.is_empty() {
            request.name =
                self.generated_subscription_name(&paths::resource_project(&request.topic));
        }
        if self.subscriptions.contains_key(&request.name) {
            return Err(ApiError::already_exists("subscription already exists"));
        }
        if !self.topics.contains_key(&request.topic) {
            return Err(ApiError::not_found("topic not found"));
        }
        if !self.dead_letter_topic_exists(request.dead_letter_policy.as_ref()) {
            return Err(ApiError::not_found("dead-letter topic not found"));
        }
        self.subscriptions
            .insert(request.name.clone(), request.clone());
        if let Err(_err) = self.persist() {
            self.subscriptions.remove(&request.name);
            self.deliveries.remove(&request.name);
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(request)
    }

    /// `Subscriber.GetSubscription`, mirroring
    /// `subscription_grpc.rs:GetSubscription`.
    pub fn grpc_get_subscription(&self, subscription: &str) -> Result<Subscription, ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        self.subscriptions
            .get(subscription)
            .cloned()
            .ok_or_else(|| ApiError::not_found("subscription not found"))
    }

    /// `Subscriber.UpdateSubscription`, mirroring
    /// `subscription_grpc.rs:UpdateSubscription`.
    pub fn grpc_update_subscription(
        &mut self,
        update: &Subscription,
        mask_paths: &[String],
    ) -> Result<Subscription, ApiError> {
        if !paths::valid_full_subscription_name(&update.name) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        let canonical = grpc_normalize_mask(mask_paths)?;
        if update.ack_deadline_seconds > self.max_ack_deadline() {
            return Err(ApiError::invalid_argument(
                "ackDeadlineSeconds exceeds maxAckDeadlineSeconds",
            ));
        }
        crate::validation::validate_subscription_metadata(
            &update.message_retention_duration,
            update.expiration_policy.as_ref(),
        )?;
        crate::validation::validate_subscription_filter(&update.filter)?;
        crate::validation::validate_dead_letter_policy(update.dead_letter_policy.as_ref())?;
        crate::validation::validate_retry_policy(update.retry_policy.as_ref())?;
        crate::validation::validate_push_config(update.push_config.as_ref())?;

        let Some(mut sub) = self.subscriptions.get(&update.name).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        let has = |target: &str| {
            canonical
                .iter()
                .any(|p| grpc_canonical_mask_path(p) == target)
        };
        if has("dead_letter_policy")
            && !self.dead_letter_topic_exists(update.dead_letter_policy.as_ref())
        {
            return Err(ApiError::not_found("dead-letter topic not found"));
        }
        for path in &canonical {
            match grpc_canonical_mask_path(path).as_str() {
                "topic" => {
                    if !update.topic.is_empty() && update.topic != sub.topic {
                        return Err(ApiError::failed_precondition(
                            "subscription topic cannot be changed",
                        ));
                    }
                }
                "labels" => sub.labels = update.labels.clone(),
                "ack_deadline_seconds" => {
                    sub.ack_deadline_seconds = if update.ack_deadline_seconds == 0 {
                        self.default_ack_deadline()
                    } else {
                        update.ack_deadline_seconds
                    };
                }
                "enable_message_ordering" => {
                    sub.enable_message_ordering = update.enable_message_ordering
                }
                "enable_exactly_once_delivery" => {
                    sub.enable_exactly_once_delivery = update.enable_exactly_once_delivery
                }
                "retain_acked_messages" => sub.retain_acked_messages = update.retain_acked_messages,
                "message_retention_duration" => {
                    sub.message_retention_duration = update.message_retention_duration.clone()
                }
                "filter" => sub.filter = update.filter.clone(),
                "dead_letter_policy" => sub.dead_letter_policy = update.dead_letter_policy.clone(),
                "retry_policy" => sub.retry_policy = update.retry_policy.clone(),
                "push_config" => sub.push_config = update.push_config.clone(),
                _ => {
                    return Err(ApiError::invalid_argument(format!(
                        "unsupported subscription update mask path {:?}",
                        path
                    )))
                }
            }
        }
        sub.updated_at = self.now();
        self.subscriptions.insert(sub.name.clone(), sub.clone());
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(sub)
    }

    /// `Subscriber.ListSubscriptions`, mirroring
    /// `subscription_grpc.rs:ListSubscriptions`.
    pub fn grpc_list_subscriptions(
        &self,
        project: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<Subscription>, String), ApiError> {
        let subs: Vec<Subscription> = self
            .subscriptions
            .values()
            .filter(|s| paths::resource_project(&s.name) == project)
            .cloned()
            .collect();
        let (start, end, next) = grpc_page_bounds(subs.len(), page_size, page_token)?;
        Ok((subs[start..end].to_vec(), next))
    }

    /// `Subscriber.DeleteSubscription`, mirroring
    /// `subscription_grpc.rs:DeleteSubscription`.
    pub fn grpc_delete_subscription(&mut self, subscription: &str) -> Result<(), ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if !self.subscriptions.contains_key(subscription) {
            return Err(ApiError::not_found("subscription not found"));
        }
        self.subscriptions.remove(subscription);
        self.deliveries.remove(subscription);
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    /// `Subscriber.ModifyPushConfig`, mirroring
    /// `subscription_grpc.rs:ModifyPushConfig`.
    pub fn grpc_modify_push_config(
        &mut self,
        subscription: &str,
        push_config: Option<&Value>,
    ) -> Result<(), ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        crate::validation::validate_push_config(push_config)?;
        let Some(mut sub) = self.subscriptions.get(subscription).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        sub.push_config = normalize_any_map(push_config);
        sub.updated_at = self.now();
        self.subscriptions.insert(sub.name.clone(), sub);
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    /// `Subscriber.Pull`, mirroring `pull_grpc.rs:Pull`. Returns received
    /// messages as `(ack_id, message, delivery_attempt)`. The blocking wait for
    /// `return_immediately=false` is handled by the adapter.
    pub fn grpc_pull(
        &mut self,
        subscription: &str,
        max_messages: i32,
    ) -> Result<Vec<(String, crate::model::PubsubMessage, i64)>, ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        let mut max = if max_messages <= 0 {
            1
        } else {
            max_messages as i64
        };
        if max > self.max_pull_messages() {
            max = self.max_pull_messages();
        }
        let Some(sub) = self.subscriptions.get(subscription).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        if sub.detached {
            return Err(ApiError::failed_precondition("subscription is detached"));
        }
        if subscription_push_endpoint(&sub).is_some() {
            return Err(ApiError::failed_precondition(
                "subscription is configured for push delivery",
            ));
        }
        let (now_secs, _) = self.now_parts();
        self.cleanup_retained_messages(now_secs);
        self.expire_leases(now_secs);
        let ack_deadline = if sub.ack_deadline_seconds > 0 {
            sub.ack_deadline_seconds
        } else {
            self.default_ack_deadline_cfg()
        };
        let (received, deliveries) =
            self.pull_with_ack_deadline_core(&sub, max, now_secs, ack_deadline);
        let compacted = compact_acked(deliveries, sub.retain_acked_messages);
        self.deliveries.insert(sub.name.clone(), compacted);
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(received)
    }

    /// Pull core mirroring `pull_grpc.rs:pullWithAckDeadlineLocked` + dead-letter
    /// handling. Returns `(received, working_deliveries)` WITHOUT compaction so
    /// callers (unary Pull, streaming response) can post-process — streaming
    /// applies byte flow-control to `working_deliveries` before compaction.
    fn pull_with_ack_deadline_core(
        &mut self,
        sub: &Subscription,
        max: i64,
        now_secs: i64,
        ack_deadline: i64,
    ) -> (
        Vec<(String, crate::model::PubsubMessage, i64)>,
        Vec<crate::model::DeliveryRecord>,
    ) {
        let name = &sub.name;
        let mut blocked: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if sub.enable_message_ordering {
            for d in self.deliveries.get(name).into_iter().flatten() {
                if d.acked || !crate::delivery::after(&d.lease_deadline, now_secs) {
                    continue;
                }
                if let Some(m) = self.messages.get(&d.message_id) {
                    if !m.ordering_key.is_empty() {
                        blocked.insert(m.ordering_key.clone());
                    }
                }
            }
        }
        let mut received: Vec<(String, crate::model::PubsubMessage, i64)> = Vec::new();
        let mut deliveries = self.deliveries.get(name).cloned().unwrap_or_default();
        let mut dead_lettered: Vec<(String, crate::model::PubsubMessage)> = Vec::new();
        #[allow(clippy::needless_range_loop)]
        for i in 0..deliveries.len() {
            if received.len() as i64 >= max {
                break;
            }
            if deliveries[i].acked
                || crate::delivery::after(&deliveries[i].lease_deadline, now_secs)
            {
                continue;
            }
            if crate::delivery::after(&deliveries[i].next_delivery_time, now_secs) {
                if sub.enable_message_ordering {
                    if let Some(m) = self.messages.get(&deliveries[i].message_id) {
                        if !m.ordering_key.is_empty() {
                            blocked.insert(m.ordering_key.clone());
                        }
                    }
                }
                continue;
            }
            let Some(message) = self.messages.get(&deliveries[i].message_id).cloned() else {
                continue;
            };
            if let Some((dl_topic, max_attempts)) = self.dead_letter_target(sub) {
                if deliveries[i].delivery_attempt >= max_attempts {
                    self.next_message_id += 1;
                    let dl_id = self.next_message_id.to_string();
                    let dl_msg = crate::model::PubsubMessage {
                        message_id: dl_id.clone(),
                        publish_time: self.now(),
                        ..message.clone()
                    };
                    dead_lettered.push((dl_topic, dl_msg));
                    deliveries[i].acked = true;
                    deliveries[i].ack_id = String::new();
                    deliveries[i].lease_deadline = crate::model::ZERO_TIME.to_string();
                    deliveries[i].next_delivery_time = crate::model::ZERO_TIME.to_string();
                    continue;
                }
            }
            if sub.enable_message_ordering && !message.ordering_key.is_empty() {
                if blocked.contains(&message.ordering_key) {
                    continue;
                }
                blocked.insert(message.ordering_key.clone());
            }
            self.next_ack_id += 1;
            deliveries[i].ack_id = format!("{}-{}", deliveries[i].message_id, self.next_ack_id);
            deliveries[i].lease_deadline = crate::delivery::plus_seconds(now_secs, ack_deadline);
            deliveries[i].next_delivery_time = crate::model::ZERO_TIME.to_string();
            deliveries[i].delivery_attempt += 1;
            received.push((
                deliveries[i].ack_id.clone(),
                message,
                deliveries[i].delivery_attempt,
            ));
        }
        for (dl_topic, dl_msg) in dead_lettered {
            self.messages
                .insert(dl_msg.message_id.clone(), dl_msg.clone());
            let dl_subs: Vec<String> = self
                .subscriptions
                .values()
                .filter(|c| c.topic == dl_topic && !c.detached)
                .map(|c| c.name.clone())
                .collect();
            for cn in dl_subs {
                self.deliveries
                    .entry(cn)
                    .or_default()
                    .push(crate::model::DeliveryRecord {
                        message_id: dl_msg.message_id.clone(),
                        ..Default::default()
                    });
            }
        }
        let _ = name;
        (received, deliveries)
    }

    /// Whether a pull may return immediately, mirroring `pullMayReturn`. Used by
    /// the gRPC adapter's blocking-wait loop.
    pub fn grpc_pull_may_return(&mut self, subscription: &str) -> bool {
        let Some(sub) = self.subscriptions.get(subscription).cloned() else {
            return true;
        };
        if sub.detached || subscription_push_endpoint(&sub).is_some() {
            return true;
        }
        let (now_secs, _) = self.now_parts();
        self.expire_leases(now_secs);
        let mut blocked: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if sub.enable_message_ordering {
            for d in self.deliveries.get(subscription).into_iter().flatten() {
                if d.acked || !crate::delivery::after(&d.lease_deadline, now_secs) {
                    continue;
                }
                if let Some(m) = self.messages.get(&d.message_id) {
                    if !m.ordering_key.is_empty() {
                        blocked.insert(m.ordering_key.clone());
                    }
                }
            }
        }
        for d in self.deliveries.get(subscription).into_iter().flatten() {
            if d.acked || crate::delivery::after(&d.lease_deadline, now_secs) {
                continue;
            }
            if crate::delivery::after(&d.next_delivery_time, now_secs) {
                continue;
            }
            let Some(message) = self.messages.get(&d.message_id) else {
                continue;
            };
            if sub.enable_message_ordering
                && !message.ordering_key.is_empty()
                && blocked.contains(&message.ordering_key)
            {
                continue;
            }
            return true;
        }
        false
    }

    // --- StreamingPull (mirror streaming_pull_grpc.rs) --------------------

    /// Whether StreamingPull is disabled (legacy `config.StreamingPullDisabled`).
    pub fn streaming_pull_disabled(&self) -> bool {
        self.config.streaming_pull_disabled
    }

    /// `validateStreamingPullSubscription` — NotFound / FailedPrecondition checks.
    pub fn grpc_validate_streaming_subscription(&self, subscription: &str) -> Result<(), ApiError> {
        let Some(sub) = self.subscriptions.get(subscription) else {
            return Err(ApiError::not_found("subscription not found"));
        };
        if sub.detached {
            return Err(ApiError::failed_precondition("subscription is detached"));
        }
        if subscription_push_endpoint(sub).is_some() {
            return Err(ApiError::failed_precondition(
                "subscription is configured for push delivery",
            ));
        }
        Ok(())
    }

    /// `streamingAckDeadline` — clamp/validate the per-stream ack deadline.
    pub fn grpc_stream_ack_deadline(&self, seconds: i32) -> Result<i64, ApiError> {
        if seconds < 0 {
            return Err(ApiError::invalid_argument(
                "streamAckDeadlineSeconds must be non-negative",
            ));
        }
        if seconds as i64 > self.max_ack_deadline() {
            return Err(ApiError::invalid_argument(
                "streamAckDeadlineSeconds exceeds maxAckDeadlineSeconds",
            ));
        }
        if seconds == 0 {
            return Ok(self.default_ack_deadline_cfg());
        }
        Ok(seconds as i64)
    }

    /// `streamingPullResponse` — one delivery batch. Returns `(ack_id, message,
    /// attempt)` after byte flow-control trimming + compaction + persist.
    /// `max_messages` is the remaining message capacity; `outstanding_bytes` is
    /// the bytes already outstanding on the stream. Empty result → no batch.
    pub fn grpc_streaming_response(
        &mut self,
        subscription: &str,
        max_messages: i64,
        ack_deadline: i64,
        max_outstanding_bytes: i64,
        outstanding_bytes: i64,
    ) -> Result<Vec<(String, crate::model::PubsubMessage, i64)>, ApiError> {
        let Some(sub) = self.subscriptions.get(subscription).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        if sub.detached {
            return Err(ApiError::failed_precondition("subscription is detached"));
        }
        if subscription_push_endpoint(&sub).is_some() {
            return Err(ApiError::failed_precondition(
                "subscription is configured for push delivery",
            ));
        }
        let (now_secs, _) = self.now_parts();
        self.cleanup_retained_messages(now_secs);
        self.expire_leases(now_secs);
        let (mut received, mut deliveries) =
            self.pull_with_ack_deadline_core(&sub, max_messages, now_secs, ack_deadline);
        if received.is_empty() {
            return Ok(Vec::new());
        }
        if max_outstanding_bytes > 0 {
            // Mirror legacy: keep the prefix that fits (always keep at least the
            // first message), then release the unsent tail's ack ids.
            let mut remaining = max_outstanding_bytes - outstanding_bytes;
            let mut keep = 0usize;
            for item in received.iter() {
                let size = streaming_received_message_size(&item.0, &item.1) as i64;
                if size > remaining && keep > 0 {
                    break;
                }
                keep += 1;
                remaining -= size;
            }
            for item in received[keep..].iter() {
                release_unsent_streaming_ack_id(&mut deliveries, &item.0);
            }
            received.truncate(keep);
        }
        let compacted = compact_acked(deliveries, sub.retain_acked_messages);
        self.deliveries.insert(sub.name.clone(), compacted);
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(received)
    }

    /// `pruneStreamingOutstandingAckIDs` — drop outstanding ack ids whose lease is
    /// no longer active. Returns the set of still-active outstanding ack ids.
    pub fn grpc_prune_outstanding(
        &mut self,
        subscription: &str,
        outstanding: &mut BTreeMap<String, i64>,
    ) {
        if outstanding.is_empty() {
            return;
        }
        let (now_secs, _) = self.now_parts();
        self.expire_leases(now_secs);
        let mut active: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for d in self.deliveries.get(subscription).into_iter().flatten() {
            if d.ack_id.is_empty()
                || d.acked
                || !crate::delivery::after(&d.lease_deadline, now_secs)
            {
                continue;
            }
            active.insert(d.ack_id.clone());
        }
        outstanding.retain(|ack_id, _| active.contains(ack_id));
    }

    /// `releaseStreamingOutstandingAckIDs` — on stream teardown, clear the lease
    /// for any still-outstanding unacked delivery so it is promptly redeliverable.
    pub fn grpc_release_outstanding(
        &mut self,
        subscription: &str,
        outstanding: &BTreeMap<String, i64>,
    ) {
        if outstanding.is_empty() {
            return;
        }
        let Some(sub) = self.subscriptions.get(subscription).cloned() else {
            return;
        };
        let mut deliveries = self
            .deliveries
            .get(subscription)
            .cloned()
            .unwrap_or_default();
        let mut changed = false;
        for ack_id in outstanding.keys() {
            for d in &mut deliveries {
                if &d.ack_id != ack_id || d.acked {
                    continue;
                }
                d.ack_id = String::new();
                d.lease_deadline = crate::model::ZERO_TIME.to_string();
                d.next_delivery_time = crate::model::ZERO_TIME.to_string();
                changed = true;
                break;
            }
        }
        if !changed {
            return;
        }
        let compacted = compact_acked(deliveries, sub.retain_acked_messages);
        self.deliveries.insert(subscription.to_string(), compacted);
        let _ = self.persist();
    }

    /// `Subscriber.Acknowledge`, mirroring `pull_grpc.rs:Acknowledge`.
    pub fn grpc_acknowledge(
        &mut self,
        subscription: &str,
        ack_ids: &[String],
    ) -> Result<(), ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        self.grpc_update_ack_deadline(subscription, ack_ids, 0, true)
    }

    /// `Subscriber.ModifyAckDeadline`, mirroring `pull_grpc.rs:ModifyAckDeadline`.
    pub fn grpc_modify_ack_deadline(
        &mut self,
        subscription: &str,
        ack_ids: &[String],
        ack_deadline_seconds: i32,
    ) -> Result<(), ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if ack_deadline_seconds < 0 {
            return Err(ApiError::invalid_argument(
                "ackDeadlineSeconds must be non-negative",
            ));
        }
        if ack_deadline_seconds as i64 > self.max_ack_deadline() {
            return Err(ApiError::invalid_argument(
                "ackDeadlineSeconds exceeds maxAckDeadlineSeconds",
            ));
        }
        self.grpc_update_ack_deadline(subscription, ack_ids, ack_deadline_seconds as i64, false)
    }

    /// StreamingPull ack/modack core, exposing `updateAckDeadlineLocked` to the
    /// gRPC stream driver (no subscription-name validation — mirrors legacy, which
    /// validated the name once on the initial request).
    pub fn grpc_stream_update_ack_deadline(
        &mut self,
        subscription: &str,
        ack_ids: &[String],
        ack_deadline_seconds: i64,
        acknowledge: bool,
    ) -> Result<(), ApiError> {
        self.grpc_update_ack_deadline(subscription, ack_ids, ack_deadline_seconds, acknowledge)
    }

    /// Shared ack/modify core mirroring `pull_grpc.rs:updateAckDeadlineLocked`.
    fn grpc_update_ack_deadline(
        &mut self,
        subscription: &str,
        ack_ids: &[String],
        ack_deadline_seconds: i64,
        acknowledge: bool,
    ) -> Result<(), ApiError> {
        if ack_ids.is_empty() {
            return Ok(());
        }
        for ack_id in ack_ids {
            if ack_id.trim().is_empty() {
                return Err(ApiError::invalid_argument(
                    "ackIds must not contain empty values",
                ));
            }
        }
        let Some(sub) = self.subscriptions.get(subscription).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        let (now_secs, _) = self.now_parts();
        self.expire_leases(now_secs);
        let id_set: std::collections::BTreeSet<&String> = ack_ids.iter().collect();
        let mut deliveries = self
            .deliveries
            .get(subscription)
            .cloned()
            .unwrap_or_default();
        for d in &mut deliveries {
            if !id_set.contains(&d.ack_id) || d.acked {
                continue;
            }
            if acknowledge {
                d.acked = true;
                d.ack_id = String::new();
                d.lease_deadline = crate::model::ZERO_TIME.to_string();
                d.next_delivery_time = crate::model::ZERO_TIME.to_string();
                continue;
            }
            if ack_deadline_seconds == 0 {
                d.ack_id = String::new();
                d.lease_deadline = crate::model::ZERO_TIME.to_string();
                d.next_delivery_time = crate::model::ZERO_TIME.to_string();
            } else {
                d.lease_deadline = crate::delivery::plus_seconds(now_secs, ack_deadline_seconds);
                d.next_delivery_time = crate::model::ZERO_TIME.to_string();
            }
        }
        let compacted = compact_acked(deliveries, sub.retain_acked_messages);
        self.deliveries.insert(subscription.to_string(), compacted);
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    // --- Snapshots --------------------------------------------------------

    /// `Subscriber.GetSnapshot`, mirroring `snapshot_grpc.rs:GetSnapshot`.
    pub fn grpc_get_snapshot(&self, snapshot: &str) -> Result<Snapshot, ApiError> {
        if !paths::valid_full_snapshot_name(snapshot) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        match self.snapshots.get(snapshot) {
            Some(snap) if !self.snapshot_expired(snap) => Ok(snapshot_public(snap)),
            _ => Err(ApiError::not_found("snapshot not found")),
        }
    }

    /// `Subscriber.ListSnapshots`, mirroring `snapshot_grpc.rs:ListSnapshots`.
    pub fn grpc_list_snapshots(
        &self,
        project: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<Snapshot>, String), ApiError> {
        let snaps: Vec<Snapshot> = self
            .snapshots
            .values()
            .filter(|s| paths::resource_project(&s.name) == project && !self.snapshot_expired(s))
            .map(snapshot_public)
            .collect();
        let (start, end, next) = grpc_page_bounds(snaps.len(), page_size, page_token)?;
        Ok((snaps[start..end].to_vec(), next))
    }

    /// `Subscriber.CreateSnapshot`, mirroring `snapshot_grpc.rs:CreateSnapshot`
    /// (captures the subscription's deliveries, auto-generates an empty name).
    pub fn grpc_create_snapshot(
        &mut self,
        name: &str,
        subscription: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<Snapshot, ApiError> {
        if !name.is_empty() && !paths::valid_full_snapshot_name(name) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if self.snapshots.contains_key(name) {
            return Err(ApiError::already_exists("snapshot already exists"));
        }
        let Some(sub) = self.subscriptions.get(subscription).cloned() else {
            return Err(ApiError::not_found("subscription not found"));
        };
        let snapshot_name = if name.is_empty() {
            self.generated_snapshot_name(&paths::resource_project(&sub.name))
        } else {
            name.to_string()
        };
        let snapshot = Snapshot {
            name: snapshot_name.clone(),
            topic: sub.topic.clone(),
            subscription: sub.name.clone(),
            expire_time: self.snapshot_expire_time(),
            labels: labels.clone(),
            deliveries: snapshot_deliveries(self.deliveries.get(subscription).map_or(&[], |v| v)),
        };
        self.snapshots
            .insert(snapshot_name.clone(), snapshot.clone());
        if let Err(_err) = self.persist() {
            self.snapshots.remove(&snapshot_name);
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(snapshot_public(&snapshot))
    }

    /// `Subscriber.DeleteSnapshot`, mirroring `snapshot_grpc.rs:DeleteSnapshot`.
    pub fn grpc_delete_snapshot(&mut self, snapshot: &str) -> Result<(), ApiError> {
        if !paths::valid_full_snapshot_name(snapshot) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        if !self.snapshots.contains_key(snapshot) {
            return Err(ApiError::not_found("snapshot not found"));
        }
        self.snapshots.remove(snapshot);
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    /// `Subscriber.UpdateSnapshot`, mirroring `snapshot_grpc.rs:UpdateSnapshot`
    /// (only `labels` is updatable).
    pub fn grpc_update_snapshot(
        &mut self,
        update: &Snapshot,
        mask_paths: &[String],
    ) -> Result<Snapshot, ApiError> {
        if !paths::valid_full_snapshot_name(&update.name) {
            return Err(ApiError::invalid_argument("invalid snapshot name"));
        }
        let canonical = grpc_normalize_mask(mask_paths)?;
        let expired = self
            .snapshots
            .get(&update.name)
            .map(|s| self.snapshot_expired(s))
            .unwrap_or(true);
        let Some(mut snapshot) = self.snapshots.get(&update.name).cloned() else {
            return Err(ApiError::not_found("snapshot not found"));
        };
        if expired {
            return Err(ApiError::not_found("snapshot not found"));
        }
        for path in &canonical {
            match grpc_canonical_mask_path(path).as_str() {
                "labels" => snapshot.labels = update.labels.clone(),
                _ => {
                    return Err(ApiError::invalid_argument(format!(
                        "unsupported snapshot update mask path {:?}",
                        path
                    )))
                }
            }
        }
        self.snapshots
            .insert(snapshot.name.clone(), snapshot.clone());
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(snapshot_public(&snapshot))
    }

    /// `Subscriber.Seek`, mirroring `snapshot_grpc.rs:Seek`. Exactly one of
    /// `snapshot`/`time_secs` is set (`time_secs` is `Some` for a time target).
    pub fn grpc_seek(
        &mut self,
        subscription: &str,
        snapshot: Option<&str>,
        time_secs: Option<i64>,
    ) -> Result<(), ApiError> {
        if !paths::valid_full_subscription_name(subscription) {
            return Err(ApiError::invalid_argument("invalid subscription name"));
        }
        if snapshot.is_none() && time_secs.is_none() {
            return Err(ApiError::invalid_argument("snapshot or time is required"));
        }
        if !self.subscriptions.contains_key(subscription) {
            return Err(ApiError::not_found("subscription not found"));
        }
        if let Some(secs) = time_secs {
            let replayed = self.seek_deliveries_by_time_grpc(subscription, secs);
            self.deliveries.insert(subscription.to_string(), replayed);
        } else if let Some(snap_name) = snapshot {
            if !paths::valid_full_snapshot_name(snap_name) {
                return Err(ApiError::invalid_argument("invalid snapshot name"));
            }
            let snap = match self.snapshots.get(snap_name) {
                Some(s) if !self.snapshot_expired(s) => s.clone(),
                _ => return Err(ApiError::not_found("snapshot not found")),
            };
            if snap.subscription != subscription {
                return Err(ApiError::failed_precondition(
                    "snapshot belongs to a different subscription",
                ));
            }
            let replayed = snapshot_deliveries(&snap.deliveries);
            self.deliveries.insert(subscription.to_string(), replayed);
        }
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    /// Mirror of `seekDeliveriesByTimeLocked` for the gRPC path (the REST variant
    /// is private; replicated to avoid widening visibility).
    fn seek_deliveries_by_time_grpc(
        &self,
        sub_name: &str,
        seek_secs: i64,
    ) -> Vec<crate::model::DeliveryRecord> {
        let mut replayed = Vec::new();
        for delivery in self.deliveries.get(sub_name).into_iter().flatten() {
            let Some(message) = self.messages.get(&delivery.message_id) else {
                continue;
            };
            let Some((pub_secs, _)) = crate::time_fmt::parse_rfc3339(&message.publish_time) else {
                continue;
            };
            if pub_secs < seek_secs {
                continue;
            }
            replayed.push(crate::model::DeliveryRecord {
                message_id: delivery.message_id.clone(),
                ..Default::default()
            });
        }
        replayed
    }

    fn generated_subscription_name(&self, project: &str) -> String {
        let project = if paths::valid_project_id(project) {
            project.to_string()
        } else {
            self.config.project().to_string()
        };
        let mut i = 1;
        loop {
            let name = paths::subscription_name(&project, &format!("devcloud-auto-sub-{i}"));
            if !self.subscriptions.contains_key(&name) {
                return name;
            }
            i += 1;
        }
    }

    fn generated_snapshot_name(&self, project: &str) -> String {
        let project = if paths::valid_project_id(project) {
            project.to_string()
        } else {
            self.config.project().to_string()
        };
        let mut i = 1;
        loop {
            let name = paths::snapshot_name(&project, &format!("devcloud-auto-snapshot-{i}"));
            if !self.snapshots.contains_key(&name) {
                return name;
            }
            i += 1;
        }
    }

    // --- SchemaService ----------------------------------------------------

    /// `SchemaService.CreateSchema`, mirroring `schema_grpc.rs:CreateSchema`.
    /// `project` is the bare id from `parent`. Returns the FULL-view schema.
    pub fn grpc_create_schema(
        &mut self,
        project: &str,
        schema_id: &str,
        request: &Schema,
    ) -> Result<Schema, ApiError> {
        if !paths::valid_resource_id(schema_id) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        let mut schema = grpc_schema_from_request(request)?;
        schema.name = paths::schema_name(project, schema_id);
        if schema.revision_id.is_empty() {
            schema.revision_id = "1".to_string();
        }
        schema.revision_create_time = self.now();
        schema.revisions = vec![grpc_current_revision(&schema)];
        if self.schemas.contains_key(&schema.name) {
            return Err(ApiError::already_exists("schema already exists"));
        }
        self.schemas.insert(schema.name.clone(), schema.clone());
        if let Err(_err) = self.persist() {
            self.schemas.remove(&schema.name);
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(grpc_schema_view(&schema, "FULL"))
    }

    /// `SchemaService.GetSchema`, mirroring `schema_grpc.rs:GetSchema`.
    pub fn grpc_get_schema(&self, name: &str, view: &str) -> Result<Schema, ApiError> {
        if !paths::valid_full_schema_name(name) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        self.schemas
            .get(name)
            .map(|s| grpc_schema_view(s, view))
            .ok_or_else(|| ApiError::not_found("schema not found"))
    }

    /// `SchemaService.ListSchemas`, mirroring `schema_grpc.rs:ListSchemas`.
    pub fn grpc_list_schemas(
        &self,
        project: &str,
        view: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<Schema>, String), ApiError> {
        let resolved = grpc_list_schema_view(view);
        let schemas: Vec<Schema> = self
            .schemas
            .values()
            .filter(|s| paths::resource_project(&s.name) == project)
            .map(|s| grpc_schema_view(s, &resolved))
            .collect();
        let (start, end, next) = grpc_page_bounds(schemas.len(), page_size, page_token)?;
        Ok((schemas[start..end].to_vec(), next))
    }

    /// `SchemaService.ListSchemaRevisions`, mirroring
    /// `schema_grpc.rs:ListSchemaRevisions`.
    pub fn grpc_list_schema_revisions(
        &self,
        name: &str,
        view: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<Schema>, String), ApiError> {
        if !paths::valid_full_schema_name(name) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        let Some(schema) = self.schemas.get(name) else {
            return Err(ApiError::not_found("schema not found"));
        };
        let resolved = grpc_list_schema_view(view);
        let revisions = grpc_schema_revisions(schema);
        let (start, end, next) = grpc_page_bounds(revisions.len(), page_size, page_token)?;
        let page: Vec<Schema> = revisions[start..end]
            .iter()
            .map(|r| {
                let s = grpc_revision_to_schema(r, &schema.name);
                grpc_schema_view(&s, &resolved)
            })
            .collect();
        Ok((page, next))
    }

    /// `SchemaService.CommitSchema`, mirroring `schema_grpc.rs:CommitSchema`.
    pub fn grpc_commit_schema(&mut self, name: &str, request: &Schema) -> Result<Schema, ApiError> {
        if !paths::valid_full_schema_name(name) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        let revision = grpc_schema_from_request(request)?;
        if !revision.name.is_empty() && revision.name != name {
            return Err(ApiError::invalid_argument("schema name mismatch"));
        }
        let Some(mut schema) = self.schemas.get(name).cloned() else {
            return Err(ApiError::not_found("schema not found"));
        };
        if revision.type_ != schema.type_ {
            return Err(ApiError::failed_precondition(
                "schema type cannot be changed",
            ));
        }
        let mut rev = revision;
        rev.name = schema.name.clone();
        rev.revision_id = grpc_next_revision_id(&grpc_schema_revisions(&schema));
        rev.revision_create_time = self.now();
        schema.type_ = rev.type_.clone();
        schema.definition = rev.definition.clone();
        schema.revision_id = rev.revision_id.clone();
        schema.revision_create_time = rev.revision_create_time.clone();
        let mut revs = grpc_schema_revisions(&schema);
        revs.push(grpc_current_revision(&rev));
        schema.revisions = revs;
        self.schemas.insert(schema.name.clone(), schema.clone());
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(grpc_schema_view(&schema, "FULL"))
    }

    /// `SchemaService.RollbackSchema`, mirroring
    /// `schema_grpc.rs:RollbackSchema`.
    pub fn grpc_rollback_schema(
        &mut self,
        name: &str,
        revision_id: &str,
    ) -> Result<Schema, ApiError> {
        if !paths::valid_full_schema_name(name) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        if revision_id.trim().is_empty() {
            return Err(ApiError::invalid_argument("revision id is required"));
        }
        let Some(mut schema) = self.schemas.get(name).cloned() else {
            return Err(ApiError::not_found("schema not found"));
        };
        let revisions = grpc_schema_revisions(&schema);
        let Some(mut target) = revisions
            .iter()
            .find(|r| r.revision_id == revision_id)
            .cloned()
        else {
            return Err(ApiError::not_found("schema revision not found"));
        };
        target.revision_id = grpc_next_revision_id(&revisions);
        target.revision_create_time = self.now();
        schema.type_ = target.type_.clone();
        schema.definition = target.definition.clone();
        schema.revision_id = target.revision_id.clone();
        schema.revision_create_time = target.revision_create_time.clone();
        let mut revs = revisions;
        revs.push(target);
        schema.revisions = revs;
        self.schemas.insert(schema.name.clone(), schema.clone());
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(grpc_schema_view(&schema, "FULL"))
    }

    /// `SchemaService.DeleteSchemaRevision`, mirroring
    /// `schema_grpc.rs:DeleteSchemaRevision`. `name`/`revision_id` are already
    /// disambiguated (the `name@revision` form split by the adapter).
    pub fn grpc_delete_schema_revision(
        &mut self,
        name: &str,
        revision_id: &str,
    ) -> Result<Schema, ApiError> {
        if !paths::valid_full_schema_name(name) || revision_id.is_empty() {
            return Err(ApiError::invalid_argument("invalid schema revision name"));
        }
        let Some(mut schema) = self.schemas.get(name).cloned() else {
            return Err(ApiError::not_found("schema not found"));
        };
        let revisions = grpc_schema_revisions(&schema);
        if revisions.len() <= 1 {
            return Err(ApiError::failed_precondition(
                "cannot delete the only schema revision",
            ));
        }
        let mut kept = Vec::with_capacity(revisions.len() - 1);
        let mut deleted = false;
        for r in revisions {
            if r.revision_id == revision_id {
                deleted = true;
                continue;
            }
            kept.push(r);
        }
        if !deleted {
            return Err(ApiError::not_found("schema revision not found"));
        }
        let Some(current) = kept.last().cloned() else {
            return Err(ApiError::failed_precondition(
                "cannot delete the only schema revision",
            ));
        };
        schema.type_ = current.type_.clone();
        schema.definition = current.definition.clone();
        schema.revision_id = current.revision_id.clone();
        schema.revision_create_time = current.revision_create_time.clone();
        schema.revisions = kept;
        self.schemas.insert(schema.name.clone(), schema.clone());
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(grpc_schema_view(&schema, "FULL"))
    }

    /// `SchemaService.DeleteSchema`, mirroring `schema_grpc.rs:DeleteSchema`.
    pub fn grpc_delete_schema(&mut self, name: &str) -> Result<(), ApiError> {
        if !paths::valid_full_schema_name(name) {
            return Err(ApiError::invalid_argument("invalid schema name"));
        }
        if !self.schemas.contains_key(name) {
            return Err(ApiError::not_found("schema not found"));
        }
        self.schemas.remove(name);
        if self.persist().is_err() {
            return Err(ApiError::internal("pubsub resource store unavailable"));
        }
        Ok(())
    }

    /// `SchemaService.ValidateSchema`, mirroring `schema_grpc.rs:ValidateSchema`.
    pub fn grpc_validate_schema(&self, request: &Schema) -> Result<(), ApiError> {
        grpc_schema_from_request(request).map(|_| ())
    }

    /// `SchemaService.ValidateMessage`, mirroring
    /// `schema_grpc.rs:ValidateMessage`. `name`/`inline` are the oneof targets;
    /// `message` is the raw (already-decoded) bytes; `encoding` is the string form.
    pub fn grpc_validate_message(
        &self,
        project: &str,
        name: &str,
        inline: Option<&Schema>,
        message: &[u8],
        encoding: &str,
    ) -> Result<(), ApiError> {
        if !crate::validation::valid_schema_encoding(encoding) {
            return Err(ApiError::invalid_argument("invalid schema encoding"));
        }
        if name.is_empty() && inline.is_none() {
            return Err(ApiError::invalid_argument(
                "schema name or inline schema is required",
            ));
        }
        if !name.is_empty() && inline.is_some() {
            return Err(ApiError::invalid_argument(
                "only one of schema name or inline schema may be set",
            ));
        }
        if !name.is_empty() {
            if !paths::valid_full_schema_name(name) {
                return Err(ApiError::invalid_argument("invalid schema name"));
            }
            if paths::resource_project(name) != project {
                return Err(ApiError::failed_precondition(
                    "schema belongs to a different project",
                ));
            }
            if !self.schemas.contains_key(name) {
                return Err(ApiError::not_found("schema not found"));
            }
            if !crate::validation::valid_schema_message_data(message, encoding) {
                return Err(ApiError::invalid_argument(
                    "message is invalid for schema encoding",
                ));
            }
            return Ok(());
        }
        let Some(inline) = inline else {
            return Err(ApiError::invalid_argument(
                "schema name or inline schema is required",
            ));
        };
        let schema = grpc_schema_from_request(inline)?;
        if !schema.name.is_empty() {
            if !paths::valid_full_schema_name(&schema.name) {
                return Err(ApiError::invalid_argument("invalid schema name"));
            }
            if paths::resource_project(&schema.name) != project {
                return Err(ApiError::failed_precondition(
                    "schema belongs to a different project",
                ));
            }
        }
        if !crate::validation::valid_schema_message_data(message, encoding) {
            return Err(ApiError::invalid_argument(
                "message is invalid for schema encoding",
            ));
        }
        Ok(())
    }
}

// --- gRPC free helpers (mirror grpc.rs / schema_grpc.rs) -------------------

/// `grpcPageBounds` — page over a slice using a numeric string token.
fn grpc_page_bounds(
    total: usize,
    page_size: i32,
    page_token: &str,
) -> Result<(usize, usize, String), ApiError> {
    let mut start = 0usize;
    if !page_token.is_empty() {
        match page_token.parse::<i64>() {
            Ok(parsed) if parsed >= 0 && parsed as usize <= total => start = parsed as usize,
            _ => return Err(ApiError::invalid_argument("invalid page token")),
        }
    }
    let mut limit = total;
    if page_size > 0 && (page_size as usize) < limit {
        limit = page_size as usize;
    }
    let mut end = start + limit;
    if end > total {
        end = total;
    }
    let next = if end < total {
        end.to_string()
    } else {
        String::new()
    };
    Ok((start, end, next))
}

/// `grpcUpdateMaskPaths` — require ≥1 non-empty path.
fn grpc_normalize_mask(paths: &[String]) -> Result<Vec<String>, ApiError> {
    if paths.is_empty() {
        return Err(ApiError::invalid_argument("update mask paths are required"));
    }
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        let trimmed = p.trim();
        if trimmed.is_empty() {
            return Err(ApiError::invalid_argument(
                "update mask paths must not contain empty values",
            ));
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

/// `grpcCanonicalMaskPath` — camelCase/dotted → snake_case underscores.
fn grpc_canonical_mask_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for (i, c) in path.chars().enumerate() {
        if c == '.' {
            out.push('.');
        } else if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out.replace('.', "_")
}

/// `schemaResourceFromProto` — validates a schema request body.
fn grpc_schema_from_request(schema: &Schema) -> Result<Schema, ApiError> {
    if !crate::validation::valid_schema_type(&schema.type_) {
        return Err(ApiError::invalid_argument("invalid schema type"));
    }
    if !schema.name.is_empty() && !paths::valid_full_schema_name(&schema.name) {
        return Err(ApiError::invalid_argument("invalid schema name"));
    }
    crate::validation::validate_schema_definition(&schema.type_, &schema.definition)?;
    Ok(Schema {
        name: schema.name.clone(),
        type_: schema.type_.clone(),
        definition: schema.definition.clone(),
        revision_id: schema.revision_id.clone(),
        revision_create_time: String::new(),
        revisions: Vec::new(),
    })
}

/// `schemaResource.currentRevision`.
fn grpc_current_revision(schema: &Schema) -> crate::model::SchemaRevision {
    crate::model::SchemaRevision {
        type_: schema.type_.clone(),
        definition: schema.definition.clone(),
        revision_id: schema.revision_id.clone(),
        revision_create_time: schema.revision_create_time.clone(),
    }
}

/// `schemaResource.revisions` — non-empty revision list (or the current one).
fn grpc_schema_revisions(schema: &Schema) -> Vec<crate::model::SchemaRevision> {
    if !schema.revisions.is_empty() {
        let revs: Vec<crate::model::SchemaRevision> = schema
            .revisions
            .iter()
            .filter(|r| !r.revision_id.is_empty())
            .cloned()
            .collect();
        if !revs.is_empty() {
            return revs;
        }
    }
    vec![grpc_current_revision(schema)]
}

/// `schemaRevisionResource.toSchema`.
fn grpc_revision_to_schema(r: &crate::model::SchemaRevision, name: &str) -> Schema {
    Schema {
        name: name.to_string(),
        type_: r.type_.clone(),
        definition: r.definition.clone(),
        revision_id: r.revision_id.clone(),
        revision_create_time: r.revision_create_time.clone(),
        revisions: Vec::new(),
    }
}

/// `nextSchemaRevisionID`.
fn grpc_next_revision_id(revisions: &[crate::model::SchemaRevision]) -> String {
    let mut max = 0i64;
    for r in revisions {
        if let Ok(id) = r.revision_id.parse::<i64>() {
            if id > max {
                max = id;
            }
        }
    }
    (max + 1).to_string()
}

/// `schemaResource.toProto(view)` — BASIC drops the definition.
fn grpc_schema_view(schema: &Schema, view: &str) -> Schema {
    let definition = if view == "BASIC" {
        String::new()
    } else {
        schema.definition.clone()
    };
    Schema {
        name: schema.name.clone(),
        type_: schema.type_.clone(),
        definition,
        revision_id: schema.revision_id.clone(),
        revision_create_time: schema.revision_create_time.clone(),
        revisions: Vec::new(),
    }
}

/// `listSchemaView` — unspecified view → BASIC.
fn grpc_list_schema_view(view: &str) -> String {
    if view.is_empty() || view == "SCHEMA_VIEW_UNSPECIFIED" {
        "BASIC".to_string()
    } else {
        view.to_string()
    }
}

/// Renders an IAM action, mirroring `handleIAMAction`.
fn iam_action(action: &str, body: &Value) -> Result<RestResponse, ApiError> {
    match action {
        "getIamPolicy" => Ok(RestResponse::ok_struct(&serde_json::json!({
            "version": 1, "bindings": [],
        }))),
        "setIamPolicy" => {
            let policy = body
                .get("policy")
                .filter(|p| p.is_object())
                .cloned()
                .unwrap_or_else(|| serde_json::json!({"version": 1, "bindings": []}));
            Ok(RestResponse::ok_struct(&policy))
        }
        "testIamPermissions" => {
            let perms = body.get("permissions").cloned().unwrap_or(Value::Null);
            let perms = if perms.is_array() {
                perms
            } else {
                Value::Array(Vec::new())
            };
            Ok(RestResponse::ok_struct(&serde_json::json!({
                "permissions": perms,
            })))
        }
        _ => Err(ApiError::not_found("not found")),
    }
}

/// Constant-time byte comparison.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Snapshot deliveries: unacked records with lease fields cleared, mirroring
/// `snapshotDeliveries`.
fn snapshot_deliveries(
    deliveries: &[crate::model::DeliveryRecord],
) -> Vec<crate::model::DeliveryRecord> {
    deliveries
        .iter()
        .filter(|d| !d.acked)
        .map(|d| crate::model::DeliveryRecord {
            message_id: d.message_id.clone(),
            ack_id: String::new(),
            lease_deadline: crate::model::ZERO_TIME.to_string(),
            next_delivery_time: crate::model::ZERO_TIME.to_string(),
            delivery_attempt: d.delivery_attempt,
            acked: false,
        })
        .collect()
}

/// `streamingReceivedMessageSize` — the flow-control byte size of a received
/// message: decoded data length + ack id length + ordering key length + sum of
/// attribute key/value lengths (matches the legacy computation on the proto message).
fn streaming_received_message_size(ack_id: &str, message: &crate::model::PubsubMessage) -> usize {
    let data_len = crate::validation::decode_base64_bytes(&message.data)
        .map(|b| b.len())
        .unwrap_or(0);
    let mut size = data_len + ack_id.len() + message.ordering_key.len();
    for (k, v) in &message.attributes {
        size += k.len() + v.len();
    }
    size
}

/// `releaseUnsentStreamingAckIDLocked` — clear the lease for an unsent delivery
/// and decrement its attempt counter (it was never actually delivered).
fn release_unsent_streaming_ack_id(deliveries: &mut [crate::model::DeliveryRecord], ack_id: &str) {
    for d in deliveries.iter_mut() {
        if d.ack_id != ack_id || d.acked {
            continue;
        }
        d.ack_id = String::new();
        d.lease_deadline = crate::model::ZERO_TIME.to_string();
        d.next_delivery_time = crate::model::ZERO_TIME.to_string();
        if d.delivery_attempt > 0 {
            d.delivery_attempt -= 1;
        }
        return;
    }
}

/// Validates publish-message content, mirroring `validatePublishMessage`.
fn validate_publish_message(
    data: &str,
    attributes: Option<&serde_json::Map<String, Value>>,
) -> Result<(), ApiError> {
    let attr_count = attributes.map(|a| a.len()).unwrap_or(0);
    if data.is_empty() && attr_count == 0 {
        return Err(ApiError::invalid_argument(
            "message data or attributes are required",
        ));
    }
    if !data.is_empty() && crate::validation::decode_base64_bytes(data).is_none() {
        return Err(ApiError::invalid_argument(
            "message data must be base64 encoded",
        ));
    }
    if let Some(attrs) = attributes {
        for key in attrs.keys() {
            if key.trim().is_empty() {
                return Err(ApiError::invalid_argument(
                    "message attributes must not contain empty keys",
                ));
            }
        }
    }
    Ok(())
}

/// Validates a message against a topic's schema settings, mirroring
/// `validateMessageAgainstTopicSchemaSettings`.
fn validate_message_against_topic_schema(
    data: &str,
    schema_settings: Option<&Value>,
) -> Result<(), ApiError> {
    let Some(settings) = schema_settings.and_then(Value::as_object) else {
        return Ok(());
    };
    if settings.is_empty() {
        return Ok(());
    }
    let encoding = settings
        .get("encoding")
        .and_then(Value::as_str)
        .unwrap_or("");
    if encoding.is_empty() {
        return Ok(());
    }
    let decoded = crate::validation::decode_base64_bytes(data)
        .ok_or_else(|| ApiError::invalid_argument("message data must be base64 encoded"))?;
    if !crate::validation::valid_schema_message_data(&decoded, encoding) {
        return Err(ApiError::invalid_argument(
            "message is invalid for topic schema encoding",
        ));
    }
    Ok(())
}

/// Whether a subscription's filter matches a message, mirroring
/// `subscriptionMatchesMessage`.
fn subscription_matches_message(sub: &Subscription, message: &crate::model::PubsubMessage) -> bool {
    let filter = sub.filter.trim();
    if filter.is_empty() {
        return true;
    }
    if let Some((key, op, value)) = crate::validation::parse_comparison_filter(filter) {
        let actual = message
            .attributes
            .get(&key)
            .map(String::as_str)
            .unwrap_or("");
        return if op == "!=" {
            actual != value
        } else {
            actual == value
        };
    }
    if let Some((key, prefix)) = crate::validation::parse_prefix_filter(filter) {
        let actual = message
            .attributes
            .get(&key)
            .map(String::as_str)
            .unwrap_or("");
        return actual.starts_with(&prefix);
    }
    false
}

/// The push endpoint of a subscription, if configured.
fn subscription_push_endpoint(sub: &Subscription) -> Option<String> {
    sub.push_config
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|o| o.get("pushEndpoint"))
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
}

/// Builds the pull-response `message` object, mirroring the legacy map: `attributes`
/// is `null` when absent, `orderingKey` is always present.
fn pull_message_value(message: &crate::model::PubsubMessage) -> Value {
    let attributes = if message.attributes.is_empty() {
        Value::Null
    } else {
        serde_json::to_value(&message.attributes).unwrap()
    };
    serde_json::json!({
        "data": message.data,
        "attributes": attributes,
        "messageId": message.message_id,
        "publishTime": message.publish_time,
        "orderingKey": message.ordering_key,
    })
}

/// Drops acked deliveries unless `retain_acked`, mirroring
/// `compactAckedDeliveries`.
fn compact_acked(
    deliveries: Vec<crate::model::DeliveryRecord>,
    retain_acked: bool,
) -> Vec<crate::model::DeliveryRecord> {
    if retain_acked {
        return deliveries;
    }
    deliveries.into_iter().filter(|d| !d.acked).collect()
}

/// Retry-policy backoff in seconds (`None` when absent/invalid).
fn policy_backoff_secs(policy: Option<&Value>, field: &str) -> Option<i64> {
    let value = policy
        .and_then(Value::as_object)
        .and_then(|o| o.get(field))
        .and_then(Value::as_str)?;
    match crate::duration::parse_go_duration(value) {
        Some(nanos) if nanos >= 0 => Some((nanos / 1_000_000_000) as i64),
        _ => None,
    }
}

/// A snapshot with `deliveries` hidden, mirroring `snapshotResource.public()`.
fn snapshot_public(snapshot: &Snapshot) -> Snapshot {
    Snapshot {
        deliveries: Vec::new(),
        ..snapshot.clone()
    }
}

/// A schema with `definition` hidden for `BASIC` view, mirroring
/// `schemaResource.public(view)`.
fn schema_public(schema: &Schema, view: &str) -> Schema {
    if view == "BASIC" {
        Schema {
            definition: String::new(),
            ..schema.clone()
        }
    } else {
        schema.clone()
    }
}

/// True when a schema resource carries no content, mirroring `emptySchemaResource`.
fn schema_is_empty(schema: &Schema) -> bool {
    schema.name.is_empty()
        && schema.type_.is_empty()
        && schema.definition.is_empty()
        && schema.revision_id.is_empty()
        && schema.revision_create_time.is_empty()
        && schema.revisions.is_empty()
}

/// Normalizes an optional any-map to `Some(empty-removed)` matching legacy
/// `copyAnyMap` (an empty/absent map becomes `None`).
fn normalize_any_map(value: Option<&Value>) -> Option<Value> {
    match value.and_then(Value::as_object) {
        Some(o) if !o.is_empty() => Some(Value::Object(o.clone())),
        _ => None,
    }
}

/// Writes `data` to `path` atomically via a `.tmp` rename, mirroring
/// `writeJSONFileAtomically`.
fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), ApiError> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, data)
        .map_err(|_| ApiError::internal("pubsub resource store unavailable"))?;
    std::fs::rename(&tmp, path).map_err(|_| ApiError::internal("pubsub resource store unavailable"))
}

/// Computes `(start, end, next_page_token)`, mirroring `pageBounds` (token is the
/// numeric end offset as a string, or `""` when exhausted). `start` is clamped to
/// `[0, total]`.
fn page_bounds(total: usize, page_token: i64, page_size: i64) -> (usize, usize, String) {
    let mut start = page_token.max(0) as usize;
    if start > total {
        start = total;
    }
    if page_size > 0 && start + (page_size as usize) < total {
        let end = start + page_size as usize;
        return (start, end, end.to_string());
    }
    (start, total, String::new())
}

/// Validates topic metadata, mirroring `validateTopicMetadata`.
fn validate_topic_metadata(topic: &Topic) -> Result<(), ApiError> {
    if !topic.message_retention_duration.trim().is_empty()
        && !crate::duration::valid_google_duration(&topic.message_retention_duration)
    {
        return Err(ApiError::invalid_argument(
            "messageRetentionDuration must be a non-negative duration",
        ));
    }
    if let Some(settings) = topic.schema_settings.as_ref().and_then(Value::as_object) {
        if settings.is_empty() {
            return Ok(());
        }
        let schema = settings
            .get("schema")
            .ok_or_else(|| ApiError::invalid_argument("schemaSettings.schema is required"))?;
        match schema.as_str() {
            Some(s) if paths::valid_full_schema_name(s) => {}
            _ => return Err(ApiError::invalid_argument("invalid schemaSettings.schema")),
        }
        if let Some(enc) = settings.get("encoding") {
            let ok = matches!(
                enc.as_str(),
                Some("") | Some("ENCODING_UNSPECIFIED") | Some("JSON") | Some("BINARY")
            );
            if !ok {
                return Err(ApiError::invalid_argument(
                    "invalid schemaSettings.encoding",
                ));
            }
        }
    }
    Ok(())
}
