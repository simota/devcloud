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

    /// The configured (defaulted) project, for tests/handlers.
    pub fn default_project(&self) -> &str {
        self.config.project()
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
