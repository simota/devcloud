//! Mirrors `internal/services/mail/service.rs`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use crate::model::{
    Envelope, ListMessagesInput, ListMessagesResult, MailboxSnapshot, Message, MessageEntry,
};
use crate::parser::parse_message;
use crate::store::{FileStore, Store};
use crate::time_fmt::now_rfc3339;
use crate::tls::constant_time_eq;
use crate::SMTP_AUTH_STRICT;

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
    mailbox_changes: watch::Sender<()>,
    accounts: Option<Accounts>,
    private: bool,
}

pub(crate) struct Accounts {
    store: Arc<FileStore>,
    users: BTreeMap<String, Arc<Account>>,
    recipients: BTreeMap<String, String>,
}

pub(crate) struct Account {
    password: String,
    senders: BTreeSet<String>,
    pub(crate) mailbox: Arc<Service>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AccountConfig {
    username: String,
    password: String,
    address: String,
    #[serde(default)]
    allowed_senders: Vec<String>,
}

impl Accounts {
    pub(crate) fn authenticate(
        &self,
        authzid: &str,
        user: &str,
        pass: &str,
    ) -> Option<Arc<Account>> {
        if !authzid.is_empty() && authzid != user {
            return None;
        }
        let account = self.users.get(user)?;
        constant_time_eq(pass.as_bytes(), account.password.as_bytes()).then(|| Arc::clone(account))
    }

    pub(crate) fn accepts_recipient(&self, address: &str) -> bool {
        mailbox_address(address).is_some_and(|address| self.recipients.contains_key(&address))
    }

    pub(crate) fn deliver(
        &self,
        envelope: &Envelope,
        helo: &str,
        raw: &[u8],
    ) -> Result<(), String> {
        let mut deliveries = BTreeMap::new();
        for recipient in &envelope.to {
            let owner = mailbox_address(recipient)
                .and_then(|key| self.recipients.get(&key))
                .ok_or_else(|| "unknown local recipient".to_string())?;
            deliveries.entry(owner).or_insert(recipient);
        }
        let mut message = parse_message(raw, envelope);
        message.envelope_from = Some(envelope.from.clone());
        message.helo = helo.to_string();
        message.received_at = Some(now_rfc3339());
        let records = deliveries
            .iter()
            .map(|(owner, recipient)| {
                let mut copy = message.clone();
                copy.id = new_message_id();
                copy.to = vec![(*recipient).clone()];
                ((*owner).clone(), copy)
            })
            .collect();
        self.store.deliver(records, raw)?;
        for owner in deliveries.keys() {
            self.users[*owner].mailbox.mailbox_changes.send_replace(());
        }
        Ok(())
    }
}

impl Account {
    pub(crate) fn allows_sender(&self, address: &str) -> bool {
        address.is_empty()
            || mailbox_address(address).is_some_and(|key| self.senders.contains(&key))
    }
}

impl Service {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            mailbox_changes: watch::Sender::new(()),
            accounts: None,
            private: false,
        }
    }

    /// Opt-in accounts for strict SMTP submission and IMAP/POP3 access.
    /// The root service remains the legacy shared inbox used by the HTTP API.
    pub fn with_accounts(
        store: Arc<FileStore>,
        auth_mode: &str,
        config: &str,
    ) -> Result<Self, String> {
        let mut service = Self::new(store.clone());
        if !auth_mode.trim().eq_ignore_ascii_case(SMTP_AUTH_STRICT) || config.trim().is_empty() {
            return Ok(service);
        }
        let entries: Vec<AccountConfig> = serde_json::from_str(config)
            .map_err(|_| "mail accounts must be a JSON array of account objects".to_string())?;
        if entries.is_empty() {
            return Ok(service);
        }
        let store = Arc::new(store.account_store());
        let mut users = BTreeMap::new();
        let mut recipients = BTreeMap::new();
        for (index, entry) in entries.into_iter().enumerate() {
            let invalid = || {
                format!(
                    "invalid or duplicate mail account at position {}",
                    index + 1
                )
            };
            if entry.username.is_empty()
                || !entry.username.bytes().all(|c| c.is_ascii_graphic())
                || entry.password.is_empty()
                || entry.password.trim() != entry.password
                || !entry
                    .password
                    .bytes()
                    .all(|c| c.is_ascii_graphic() || c == b' ')
                || users.contains_key(&entry.username)
            {
                return Err(invalid());
            }
            let address = mailbox_address(&entry.address).ok_or_else(invalid)?;
            if recipients
                .insert(address.clone(), entry.username.clone())
                .is_some()
            {
                return Err(invalid());
            }
            let mut senders = BTreeSet::from([address]);
            for sender in entry.allowed_senders {
                senders.insert(mailbox_address(&sender).ok_or_else(invalid)?);
            }
            let mut mailbox = Self::new(Arc::new(store.for_user(&entry.username)));
            mailbox.private = true;
            users.insert(
                entry.username,
                Arc::new(Account {
                    password: entry.password,
                    senders,
                    mailbox: Arc::new(mailbox),
                }),
            );
        }
        service.accounts = Some(Accounts {
            store,
            users,
            recipients,
        });
        Ok(service)
    }

    pub(crate) fn accounts(&self, auth_mode: &str) -> Option<&Accounts> {
        if auth_mode.trim().eq_ignore_ascii_case(SMTP_AUTH_STRICT) {
            self.accounts.as_ref()
        } else {
            None
        }
    }

    /// Mirrors `Service.Receive`: parse the raw body under the envelope, stamp
    /// id + received time, persist, then emit `mail.received`.
    pub fn receive(&self, envelope: Envelope, raw: &[u8]) -> Result<Message, String> {
        self.receive_inner(envelope, "", raw, false)
    }

    /// Capture the SMTP reverse path separately from the display From header.
    pub fn receive_from(
        &self,
        envelope: Envelope,
        helo: &str,
        raw: &[u8],
    ) -> Result<Message, String> {
        self.receive_inner(envelope, &sanitize_helo(helo), raw, true)
    }

    /// SMTP sessions retain a sanitized HELO across transactions.
    pub(crate) fn receive_from_session(
        &self,
        envelope: Envelope,
        helo: &str,
        raw: &[u8],
    ) -> Result<Message, String> {
        self.receive_inner(envelope, helo, raw, true)
    }

    fn receive_inner(
        &self,
        envelope: Envelope,
        helo: &str,
        raw: &[u8],
        capture: bool,
    ) -> Result<Message, String> {
        let mut msg = parse_message(raw, &envelope);
        if capture {
            msg.envelope_from = Some(envelope.from.clone());
            msg.helo = helo.to_string();
        }
        msg.id = new_message_id();
        msg.received_at = Some(now_rfc3339());
        let stored = self.store.append(msg, raw)?;
        self.message_received(&stored);
        Ok(stored)
    }

    pub(crate) fn append_to_folder(
        &self,
        name: &str,
        raw: &[u8],
        flags: BTreeSet<String>,
        internal_date: Option<String>,
    ) -> Result<(), String> {
        let mut message = parse_message(raw, &Envelope::default());
        message.id = new_message_id();
        message.received_at = internal_date;
        let stored = self.store.append_to_folder(name, message, raw, flags)?;
        self.message_received(&stored);
        Ok(())
    }

    fn message_received(&self, stored: &Message) {
        self.mailbox_changes.send_replace(());
        if self.private {
            return;
        }
        emit_event(
            "mail.received",
            serde_json::json!({
                "messageID": stored.id,
                "from": stored.from,
                "to": stored.to,
                "subject": stored.subject,
            }),
        );
    }

    /// Mirrors `Service.List`: read-only passthrough to the store.
    pub fn list(&self, input: ListMessagesInput) -> Result<ListMessagesResult, String> {
        self.store.list(input)
    }

    pub fn list_all(&self) -> Result<Vec<MessageEntry>, String> {
        self.store.list_all()
    }

    /// Chronological (oldest-first) snapshot with stable 1-based UIDs for IMAP/POP3.
    pub fn list_mailbox(&self) -> Result<MailboxSnapshot, String> {
        self.store.list_mailbox()
    }

    pub(crate) fn folders(&self) -> Result<BTreeSet<String>, String> {
        self.store.folders()
    }

    pub(crate) fn create_folder(&self, name: &str) -> Result<(), String> {
        self.store.create_folder(name)
    }

    pub(crate) fn list_folder(&self, name: &str) -> Result<Option<MailboxSnapshot>, String> {
        self.store.list_folder(name)
    }

    pub(crate) fn message_flags(&self, id: &str) -> Result<BTreeSet<String>, String> {
        self.store.message_flags(id)
    }

    pub(crate) fn update_flags(
        &self,
        ids: &[&str],
        update: impl Fn(&mut BTreeSet<String>) + Sync,
    ) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
        let flags = self.store.update_flags(ids, &update)?;
        self.mailbox_changes.send_replace(());
        Ok(flags)
    }

    pub(crate) fn expunge_flagged(&self, ids: &[&str]) -> Result<(), String> {
        let removed = self.store.expunge_flagged(ids)?;
        if !removed.is_empty() {
            self.mailbox_changes.send_replace(());
        }
        if !self.private {
            for id in removed {
                emit_event("mail.deleted", serde_json::json!({ "messageID": id }));
            }
        }
        Ok(())
    }

    pub(crate) fn mailbox_changes(&self) -> watch::Receiver<()> {
        self.mailbox_changes.subscribe()
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
        self.mailbox_changes.send_replace(());
        if !self.private {
            emit_event("mail.deleted", serde_json::json!({ "messageID": id }));
        }
        Ok(())
    }

    /// Mirrors `Service.DeleteAll`: clear every message, then emit `mail.cleared`
    /// (no payload, matching legacy).
    pub fn delete_all(&self) -> Result<(), String> {
        self.store.delete_all()?;
        self.mailbox_changes.send_replace(());
        if !self.private {
            emit_event_no_payload("mail.cleared");
        }
        Ok(())
    }
}

// Accept ordinary ASCII dot-atom addresses and compare only the domain
// case-insensitively. SMTP local-parts can be case-sensitive.
fn mailbox_address(address: &str) -> Option<String> {
    let (local, domain) = address.rsplit_once('@')?;
    let valid_local = local.split('.').all(|atom| {
        !atom.is_empty()
            && atom
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-/=?^_`{|}~".contains(&c))
    });
    let valid_domain = domain.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
    });
    (address.len() <= 254 && local.len() <= 64 && valid_local && valid_domain)
        .then(|| format!("{local}@{}", domain.to_ascii_lowercase()))
}

pub(crate) fn sanitize_helo(helo: &str) -> String {
    helo.chars().filter(|c| !c.is_control()).take(255).collect()
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
