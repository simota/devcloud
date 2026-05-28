//! Mirrors `internal/services/mail/service.go`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::{Envelope, Message};
use crate::parser::parse_message;
use crate::store::Store;
use crate::time_fmt::now_rfc3339;

/// Receives parsed messages and appends them to the store. Mirrors Go `Service`.
///
/// Event publishing (`events.Emit`) is intentionally deferred: the Go publisher
/// tolerates a nil bus, and the in-process bus is a cross-service coupling
/// handled at the daemon seam, not in this increment.
pub struct Service {
    store: Arc<dyn Store>,
}

impl Service {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }

    /// Mirrors `Service.Receive`: parse the raw body under the envelope, stamp
    /// id + received time, and persist.
    pub fn receive(&self, envelope: Envelope, raw: &[u8]) -> Result<Message, String> {
        let mut msg = parse_message(raw, &envelope);
        msg.id = new_message_id();
        msg.received_at = Some(now_rfc3339());
        self.store.append(msg, raw)
    }
}

/// Mirrors `newMessageID`: `msg_` + 24 hex chars. The Go version uses crypto
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
