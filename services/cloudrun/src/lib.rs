//! Local Cloud Run emulator for devcloud.
//!
//! Serves the Cloud Run Admin API v2 (services, revisions, operations, IAM
//! policies) and a data plane that reverse-proxies requests to each service's
//! latest revision, run as a local process (`containers[0].command`) or, when
//! enabled, under `docker run`. State persists to `state.json` under the
//! configured storage path.

use std::sync::OnceLock;
use tokio::sync::mpsc::UnboundedSender;

static EVENT_SINK: OnceLock<UnboundedSender<String>> = OnceLock::new();

/// Installs the process-wide dashboard event sink (called once by the
/// orchestrator). Events are `{"type":..,"service":"cloudrun","payload":..}`.
pub fn set_event_sink(tx: UnboundedSender<String>) {
    let _ = EVENT_SINK.set(tx);
}

pub(crate) fn event_sink() -> Option<&'static UnboundedSender<String>> {
    EVENT_SINK.get()
}

pub mod body;
pub mod http;
pub mod instances;
pub mod server;
pub mod time_fmt;

pub use server::{Config, Server};
