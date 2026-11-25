//! Rust reimplementation of the devcloud GCS API surface (JSON API v1 + XML API).
//!
//! This crate intentionally reuses `devcloud_s3::store::FileBucketStore` so GCS
//! and S3 continue to share the same object-core layout under `.devcloud/s3`.

use std::sync::OnceLock;
use tokio::sync::mpsc::UnboundedSender;

static EVENT_SINK: OnceLock<UnboundedSender<String>> = OnceLock::new();

/// Installs a process-wide in-process sink for dashboard event JSON objects.
/// Called once by the single-binary orchestrator at startup. Each emitted event
/// is sent as the JSON object string `{"type":..,"service":..,"payload":..}`.
pub fn set_event_sink(tx: UnboundedSender<String>) {
    let _ = EVENT_SINK.set(tx);
}

/// Returns the installed event sink, if any.
pub(crate) fn event_sink() -> Option<&'static UnboundedSender<String>> {
    EVENT_SINK.get()
}

pub mod http;
pub mod iam_acl;
pub mod iam_enforce;
pub mod lifecycle;
pub mod notifications;
pub mod retention;
pub mod rsa_keys;
pub mod signed_urls;
pub mod state;
pub mod xml_api;
pub mod xml_multipart;
