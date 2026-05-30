//! In-memory server state + the topic operations.
//!
//! Mirrors `internal/services/pubsub/{server,topic_handlers,persistence}.go`.
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
use crate::persistence::ResourceFile;

/// Default project, defaulted like Go's `defaultString(cfg.Project, "devcloud")`.
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

/// A rendered REST response: HTTP status + encoded body (empty for 204).
#[derive(Debug)]
pub struct RestResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl RestResponse {
    fn ok_struct<T: serde::Serialize>(value: &T) -> Self {
        RestResponse {
            status: 200,
            body: crate::go_json::to_vec(value),
        }
    }
    fn no_content() -> Self {
        RestResponse {
            status: 204,
            body: Vec::new(),
        }
    }
}

pub struct Server {
    config: Config,
    pub(crate) topics: BTreeMap<String, Topic>,
    pub(crate) subscriptions: BTreeMap<String, Subscription>,
    pub(crate) snapshots: BTreeMap<String, Snapshot>,
    pub(crate) schemas: BTreeMap<String, Schema>,
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

    fn resource_file_path(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.config.storage_path).join("resources.json")
    }

    fn load(&mut self) -> Result<(), String> {
        if self.config.storage_path.is_empty() {
            return Ok(());
        }
        let data = match std::fs::read(self.resource_file_path()) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
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
        Ok(())
    }

    /// Persists `resources.json` byte-compatibly with `saveResourcesLocked`
    /// (topics/subscriptions/snapshots/schemas, sorted by name). Message state is
    /// handled by later parts; here we only carry the resource file.
    pub(crate) fn persist(&self) -> Result<(), ApiError> {
        if self.config.storage_path.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.config.storage_path)
            .map_err(|_| ApiError::internal("pubsub resource store unavailable"))?;
        let file = ResourceFile {
            topics: self.topics.values().cloned().collect(),
            subscriptions: self.subscriptions.values().cloned().collect(),
            snapshots: self.snapshots.values().cloned().collect(),
            schemas: self.schemas.values().cloned().collect(),
            ..Default::default()
        };
        let bytes = file.to_bytes();
        let tmp = self.resource_file_path().with_extension("json.tmp");
        std::fs::write(&tmp, &bytes)
            .map_err(|_| ApiError::internal("pubsub resource store unavailable"))?;
        std::fs::rename(&tmp, self.resource_file_path())
            .map_err(|_| ApiError::internal("pubsub resource store unavailable"))?;
        Ok(())
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
        // BTreeMap iteration is name-sorted, matching Go's explicit sort.
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

    /// The configured (defaulted) project, for tests/handlers.
    pub fn default_project(&self) -> &str {
        self.config.project()
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

/// Normalizes an optional any-map to `Some(empty-removed)` matching Go's
/// `copyAnyMap` (an empty/absent map becomes `None`).
fn normalize_any_map(value: Option<&Value>) -> Option<Value> {
    match value.and_then(Value::as_object) {
        Some(o) if !o.is_empty() => Some(Value::Object(o.clone())),
        _ => None,
    }
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
