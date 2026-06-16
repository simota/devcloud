//! Server configuration and shared accessors — port of
//! `internal/services/bigquery/server.rs` plus the config-derived helpers from
//! `storage.rs` and the selfLink builders from `dashboard.rs`.
//!
//! Handlers are plain methods on [`Server`]; the HTTP routing layer lives in
//! `routes` and the binary entrypoint in `main.rs`.

use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_s3::store::FileBucketStore;

use crate::validation::path_escape;

/// Mirrors the legacy `Config` (minus `ObjectStore`, which is attached to the
/// [`Server`] via [`Server::with_object_store`] — the legacy field is the shared
/// S3 `BucketStore` used by jobs.insert load/extract).
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub project: String,
    pub location: String,
    pub auth_mode: String,
    pub bearer_token: String,
    pub storage_path: String,
    pub max_rows_per_table: i64,
    pub max_request_bytes: i64,
    pub max_result_rows: i64,
    pub default_legacy_sql: bool,
}

pub struct Server {
    pub(crate) config: Config,
    /// legacy `Config.ObjectStore` — `nil` ≙ `None` ("local GCS object store is
    /// not configured").
    pub(crate) object_store: Option<FileBucketStore>,
    /// Event-publisher seam: when enabled (binary only), successful mutations
    /// print `DEVCLOUD_EVENT {json}` lines on stdout for the legacy daemon's
    /// events bridge — same payloads as the legacy `events.Emit` calls. The legacy
    /// tests run with a nil publisher, so this defaults to off.
    pub(crate) events_enabled: bool,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Server {
            config,
            object_store: None,
            events_enabled: false,
        }
    }

    /// Attaches the shared S3 bucket store (legacy `Config.ObjectStore`).
    pub fn with_object_store(mut self, store: FileBucketStore) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Enables `DEVCLOUD_EVENT` stdout emission (daemon seam only).
    pub fn enable_events(mut self) -> Self {
        self.events_enabled = true;
        self
    }

    /// Mirrors the legacy `events.Emit(s.eventPublisher, …)` calls: never logs
    /// credentials or payload bodies, only resource identifiers.
    /// In single-binary mode the orchestrator installs an EVENT_SINK so events
    /// reach the dashboard relay without stdout. When `events_enabled` is set
    /// the stdout line is also emitted (daemon / standalone binary seam).
    pub(crate) fn emit_event(&self, event_type: &str, payload: serde_json::Value) {
        let json = serde_json::json!({
            "type": event_type,
            "service": "bigquery",
            "payload": payload,
        })
        .to_string();
        if let Some(tx) = crate::event_sink() {
            let _ = tx.send(json.clone());
        }
        if self.events_enabled {
            println!("DEVCLOUD_EVENT {json}");
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// legacy `defaultLocation`.
    pub(crate) fn default_location(&self) -> &str {
        if self.config.location.trim().is_empty() {
            "US"
        } else {
            &self.config.location
        }
    }

    /// legacy `maxRequestBytes` (default 10 MiB).
    pub(crate) fn max_request_bytes(&self) -> i64 {
        if self.config.max_request_bytes <= 0 {
            10 * 1024 * 1024
        } else {
            self.config.max_request_bytes
        }
    }

    /// legacy `maxRowsPerTable` (default 1,000,000).
    pub(crate) fn max_rows_per_table(&self) -> i64 {
        if self.config.max_rows_per_table <= 0 {
            1_000_000
        } else {
            self.config.max_rows_per_table
        }
    }

    /// legacy `maxResultRows` (default 10,000).
    pub(crate) fn max_result_rows(&self) -> i64 {
        if self.config.max_result_rows <= 0 {
            10_000
        } else {
            self.config.max_result_rows
        }
    }

    /// legacy `effectiveUseLegacySQL`.
    pub(crate) fn effective_use_legacy_sql(&self, value: Option<bool>) -> bool {
        value.unwrap_or(self.config.default_legacy_sql)
    }

    /// legacy `projectID` (default "devcloud").
    pub fn project_id(&self) -> &str {
        let project = self.config.project.trim();
        if project.is_empty() {
            "devcloud"
        } else {
            project
        }
    }

    /// legacy `datasetSelfLink`.
    pub(crate) fn dataset_self_link(&self, project_id: &str, dataset_id: &str) -> String {
        format!(
            "/bigquery/v2/projects/{}/datasets/{}",
            path_escape(project_id),
            path_escape(dataset_id)
        )
    }

    /// legacy `tableSelfLink`.
    pub(crate) fn table_self_link(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
    ) -> String {
        format!(
            "{}/tables/{}",
            self.dataset_self_link(project_id, dataset_id),
            path_escape(table_id)
        )
    }

    /// legacy `routineSelfLink`.
    pub(crate) fn routine_self_link(
        &self,
        project_id: &str,
        dataset_id: &str,
        routine_id: &str,
    ) -> String {
        format!(
            "{}/routines/{}",
            self.dataset_self_link(project_id, dataset_id),
            path_escape(routine_id)
        )
    }
}

/// Current wall clock as Unix nanoseconds (legacy `time.Now().UTC()`; the etag is
/// derived from `UnixNano()`, the millisecond strings from `UnixMilli()`).
pub(crate) fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_nanos() as i64
}
