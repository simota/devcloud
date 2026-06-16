//! Rust reimplementation of the devcloud `mail` (SMTP) service.
//!
//! This is increment #1 of the legacy-to-Rust strangler-fig transmute. It preserves
//! the externally observable behavior of `internal/services/mail` —
//! SMTP reply codes, sequence rules, DATA framing, AUTH PLAIN/LOGIN, and MIME
//! parsing — verified by a 1:1 port of the legacy test suite (see `tests/`).
//!
//! Beyond SMTP it now serves the dashboard-facing HTTP introspect/control
//! surface (`http`, a port of `internal/services/mail/http.rs`) and emits
//! dashboard events to a process-wide in-process sink (mirroring the other
//! emitting crates), so the single-binary mail dashboard page works end-to-end.

use std::sync::OnceLock;
use tokio::sync::mpsc::UnboundedSender;

static EVENT_SINK: OnceLock<UnboundedSender<String>> = OnceLock::new();

/// Installs a process-wide in-process sink for dashboard event JSON objects.
/// Called once by the single-binary orchestrator at startup. Each emitted event
/// is sent as the JSON object string `{"type":..,"service":"mail","payload":..}`.
/// Payloads carry only identifiers/metadata (message id, from, to, subject) —
/// never message bodies or credentials, matching legacy `mail.Service`.
pub fn set_event_sink(tx: UnboundedSender<String>) {
    let _ = EVENT_SINK.set(tx);
}

/// Returns the installed event sink, if any.
pub(crate) fn event_sink() -> Option<&'static UnboundedSender<String>> {
    EVENT_SINK.get()
}

pub mod blob;
pub mod http;
pub mod model;
pub mod parser;
pub mod service;
pub mod smtp;
pub mod store;
pub mod time_fmt;

pub use blob::{BlobId, BlobStore, FileBlobStore};
pub use http::serve_http;
pub use model::{Attachment, Envelope, ListMessagesInput, ListMessagesResult, Message};
pub use parser::parse_message;
pub use service::Service;
pub use smtp::{SmtpConfig, SmtpServer, SMTP_AUTH_OFF, SMTP_AUTH_RELAXED, SMTP_AUTH_STRICT};
pub use store::{FileStore, RecordingStore, Store};
