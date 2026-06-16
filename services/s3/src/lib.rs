//! Rust reimplementation of the devcloud **S3** service (strangler-fig increment
//! #7 — the hub). S3 owns the `BucketStore` boundary that GCS, BigQuery, and
//! Redshift share, so it is migrated last among the storage services.
//!
//! The crate now covers the repo's S3 full acceptance gate: legacy-compatible JSON
//! persistence (`json.MarshalIndent` byte-for-byte), bucket/object/multipart
//! models, on-disk store behavior, XML responses, selected S3 subresources,
//! SigV4, HTTP routing, daemon integration, and dashboard event bridging.

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

pub mod base64;
pub mod csv;
pub mod hashes;
pub mod http;
pub mod introspect;
pub mod model;
pub mod objops;
pub mod percent;
pub mod responses;
pub mod store;
pub mod store_config;
pub mod store_inventory;
pub mod store_multipart;
pub mod store_objectlock;
pub mod store_objects;
pub mod time_fmt;
pub mod validation;
pub mod wire_json;
pub mod xml;
