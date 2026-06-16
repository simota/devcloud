//! Mirrors `internal/services/mail/service.rs`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::{Envelope, ListMessagesInput, ListMessagesResult, Message};
use crate::parser::parse_message;
use crate::store::Store;
use crate::time_fmt::now_rfc3339;

/// Receives parsed messages and appends them to the store. Mirrors legacy `Service`.
///
/// Mutating and ingest operations emit dashboard events to the process-wide
/// in-process sink (`crate::event_sink`), mirroring `events.Emit` in the legacy
/// `mail.Service`. Keeping emission in the service guarantees identical event
/// traffic regardless of which transport (SMTP or the HTTP control surface)
/// triggered the change. Payloads carry only identifiers/metadata — never
/// message bodies or credentials.
pub struct Service {
    store: Arc<dyn Store>,
}

impl Service {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }

    /// Mirrors `Service.Receive`: parse the raw body under the envelope, stamp
    /// id + received time, persist, then emit `mail.received`.
    pub fn receive(&self, envelope: Envelope, raw: &[u8]) -> Result<Message, String> {
        let mut msg = parse_message(raw, &envelope);
        msg.id = new_message_id();
        msg.received_at = Some(now_rfc3339());
        let stored = self.store.append(msg, raw)?;
        emit_event(
            "mail.received",
            serde_json::json!({
                "messageID": stored.id,
                "from": stored.from,
                "to": stored.to,
                "subject": stored.subject,
            }),
        );
        Ok(stored)
    }

    /// Mirrors `Service.List`: read-only passthrough to the store.
    pub fn list(&self, input: ListMessagesInput) -> Result<ListMessagesResult, String> {
        self.store.list(input)
    }

    /// Mirrors `Service.Get`: read-only passthrough to the store.
    pub fn get(&self, id: &str) -> Result<Option<Message>, String> {
        self.store.get(id)
    }

    /// Mirrors `Service.GetRaw`: read-only passthrough to the store.
    pub fn get_raw(&self, id: &str) -> Result<Option<Vec<u8>>, String> {
        self.store.get_raw(id)
    }

    /// Mirrors `Service.Delete`: remove one message, then emit `mail.deleted`.
    pub fn delete(&self, id: &str) -> Result<(), String> {
        self.store.delete(id)?;
        emit_event("mail.deleted", serde_json::json!({ "messageID": id }));
        Ok(())
    }

    /// Mirrors `Service.DeleteAll`: clear every message, then emit `mail.cleared`
    /// (no payload, matching legacy).
    pub fn delete_all(&self) -> Result<(), String> {
        self.store.delete_all()?;
        emit_event_no_payload("mail.cleared");
        Ok(())
    }
}

/// Emits a dashboard event with a payload to the in-process sink, if installed.
/// Mirrors legacy `events.Emit` with a non-empty `Payload`.
fn emit_event(event_type: &str, payload: serde_json::Value) {
    send_event(serde_json::json!({
        "type": event_type,
        "service": "mail",
        "payload": payload,
    }));
}

/// Emits a payload-less dashboard event (legacy omits `Payload` for `mail.cleared`,
/// so the field is absent rather than `null`/`{}`).
fn emit_event_no_payload(event_type: &str) {
    send_event(serde_json::json!({
        "type": event_type,
        "service": "mail",
    }));
}

fn send_event(event: serde_json::Value) {
    if let Some(tx) = crate::event_sink() {
        let _ = tx.send(event.to_string());
    }
}

/// Mirrors `newMessageID`: `msg_` + 24 hex chars. The legacy version uses crypto
/// randomness; the exact bytes are not behaviorally observable, so we derive a
/// unique id from time + an atomic counter.
fn new_message_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("msg_{:016x}{:08x}", nanos, n as u32)
}
