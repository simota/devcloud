//! Rust reimplementation of the devcloud `mail` (SMTP) service.
//!
//! This is increment #1 of the Go→Rust strangler-fig transmute. It preserves
//! the externally observable behavior of `internal/services/mail` —
//! SMTP reply codes, sequence rules, DATA framing, AUTH PLAIN/LOGIN, and MIME
//! parsing — verified by a 1:1 port of the Go test suite (see `tests/`).

pub mod blob;
pub mod model;
pub mod parser;
pub mod service;
pub mod smtp;
pub mod store;
pub mod time_fmt;

pub use blob::{BlobId, BlobStore, FileBlobStore};
pub use model::{Attachment, Envelope, ListMessagesInput, ListMessagesResult, Message};
pub use parser::parse_message;
pub use service::Service;
pub use smtp::{SmtpConfig, SmtpServer, SMTP_AUTH_OFF, SMTP_AUTH_RELAXED, SMTP_AUTH_STRICT};
pub use store::{FileStore, RecordingStore, Store};
