//! Mirrors the `Store` interface from `internal/services/mail/service.rs` and
//! the JSONL-backed `FileStore` from `internal/storage/mailstore/store.rs`.
//!
//! `FileStore` persists message metadata as one compact JSON object per line in
//! `messages.jsonl` and the raw body in a content-addressed `BlobStore`. Deletes
//! are tombstones (a `deletedAt` timestamp) rewritten atomically. The on-disk
//! format is byte-compatible with the legacy store so the legacy dashboard can read what
//! the Rust store writes during the strangler-fig window.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::blob::BlobStore;
use crate::model::{ListMessagesInput, ListMessagesResult, Message};
use crate::time_fmt::{now_rfc3339, unix_from_rfc3339};

/// Storage backend for received messages. Mirrors legacy `mail.Store`.
///
/// Methods are synchronous (blocking) — matching the legacy store, whose operations
/// are plain calls — and are invoked from the async SMTP session directly.
pub trait Store: Send + Sync {
    fn append(&self, message: Message, raw: &[u8]) -> Result<Message, String>;
    fn list(&self, input: ListMessagesInput) -> Result<ListMessagesResult, String>;
    fn get(&self, id: &str) -> Result<Option<Message>, String>;
    fn get_raw(&self, id: &str) -> Result<Option<Vec<u8>>, String>;
    fn delete(&self, id: &str) -> Result<(), String>;
    fn delete_all(&self) -> Result<(), String>;
}

// ---------------------------------------------------------------------------
// RecordingStore — in-memory test/transient store (parity of legacy recordingStore)
// ---------------------------------------------------------------------------

/// In-memory store that records appended messages and raw bodies, for tests and
/// transient runs. Parity counterpart of the legacy test `recordingStore`.
#[derive(Default)]
pub struct RecordingStore {
    inner: Mutex<Recorded>,
}

#[derive(Default)]
struct Recorded {
    messages: Vec<Message>,
    raw: Vec<Vec<u8>>,
}

impl RecordingStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of stored (messages, raw bodies). Mirrors the legacy test helper.
    pub fn snapshot(&self) -> (Vec<Message>, Vec<Vec<u8>>) {
        let g = self.inner.lock().unwrap();
        (g.messages.clone(), g.raw.clone())
    }
}

impl Store for RecordingStore {
    fn append(&self, message: Message, raw: &[u8]) -> Result<Message, String> {
        let mut g = self.inner.lock().unwrap();
        g.messages.push(message.clone());
        g.raw.push(raw.to_vec());
        Ok(message)
    }

    fn list(&self, _input: ListMessagesInput) -> Result<ListMessagesResult, String> {
        Ok(ListMessagesResult::default())
    }

    fn get(&self, _id: &str) -> Result<Option<Message>, String> {
        Ok(None)
    }

    fn get_raw(&self, _id: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    fn delete(&self, _id: &str) -> Result<(), String> {
        Ok(())
    }

    fn delete_all(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FileStore — JSONL-backed persistent store (parity of legacy mailstore.FileStore)
// ---------------------------------------------------------------------------

const MAX_LIST_LIMIT: i32 = 100;

pub struct FileStore {
    root: PathBuf,
    blobs: Arc<dyn BlobStore>,
    /// Serializes all file access, mirroring the legacy store's `sync.Mutex`.
    lock: Mutex<()>,
}

impl FileStore {
    pub fn new(root: impl Into<PathBuf>, blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            root: root.into(),
            blobs,
            lock: Mutex::new(()),
        }
    }

    pub fn messages_path(&self) -> PathBuf {
        self.root.join("messages.jsonl")
    }

    /// Loads every record (including tombstones), in file order.
    fn load_all_locked(&self) -> Result<Vec<Message>, String> {
        let data = match fs::read(self.messages_path()) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("open messages log: {e}")),
        };
        let mut messages = Vec::new();
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let m: Message = serde_json::from_slice(line)
                .map_err(|e| format!("decode message metadata: {e}"))?;
            messages.push(m);
        }
        Ok(messages)
    }

    /// Loads only active (non-tombstoned) records.
    fn load_active(&self) -> Result<Vec<Message>, String> {
        let _g = self.lock.lock().unwrap();
        Ok(self
            .load_all_locked()?
            .into_iter()
            .filter(|m| m.deleted_at.is_none())
            .collect())
    }

    /// Rewrites the log, applying `mutate` to every record (active + tombstoned),
    /// via a temp file + atomic rename. Mirrors legacy `FileStore.rewrite`.
    fn rewrite(&self, mutate: impl Fn(Message) -> Message) -> Result<(), String> {
        let _g = self.lock.lock().unwrap();
        let messages = self.load_all_locked()?;
        fs::create_dir_all(&self.root).map_err(|e| format!("create mail store: {e}"))?;

        let tmp = self.root.join(format!(
            "messages-{}-{}.jsonl",
            std::process::id(),
            now_nanos()
        ));
        let mut buf = Vec::new();
        for message in messages {
            let line = serde_json::to_vec(&mutate(message))
                .map_err(|e| format!("write messages temp log: {e}"))?;
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
        write_atomic(&tmp, &self.messages_path(), &buf)
            .map_err(|e| format!("replace messages log: {e}"))
    }
}

impl Store for FileStore {
    fn append(&self, mut message: Message, raw: &[u8]) -> Result<Message, String> {
        fs::create_dir_all(&self.root).map_err(|e| format!("create mail store: {e}"))?;
        let raw_id = self.blobs.put(raw).map_err(|e| format!("put blob: {e}"))?;
        message.raw = raw_id;
        if message.received_at.is_none() {
            message.received_at = Some(now_rfc3339());
        }

        let _g = self.lock.lock().unwrap();
        let line =
            serde_json::to_vec(&message).map_err(|e| format!("append message metadata: {e}"))?;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.messages_path())
            .map_err(|e| format!("open messages log: {e}"))?;
        f.write_all(&line)
            .and_then(|_| f.write_all(b"\n"))
            .map_err(|e| format!("append message metadata: {e}"))?;
        Ok(message)
    }

    fn list(&self, input: ListMessagesInput) -> Result<ListMessagesResult, String> {
        let mut messages = self.load_active()?;
        // Newest first, by received time. Parse to a UNIX key so trimmed-zero
        // fractional seconds order chronologically (lexicographic would not).
        messages.sort_by(|a, b| {
            let ka = a.received_at.as_deref().map(unix_from_rfc3339);
            let kb = b.received_at.as_deref().map(unix_from_rfc3339);
            kb.cmp(&ka)
        });
        let mut limit = input.limit;
        if limit <= 0 || limit > MAX_LIST_LIMIT {
            limit = MAX_LIST_LIMIT;
        }
        messages.truncate(limit as usize);
        Ok(ListMessagesResult {
            messages,
            next_cursor: String::new(),
        })
    }

    fn get(&self, id: &str) -> Result<Option<Message>, String> {
        Ok(self.load_active()?.into_iter().find(|m| m.id == id))
    }

    fn get_raw(&self, id: &str) -> Result<Option<Vec<u8>>, String> {
        match self.get(id)? {
            None => Ok(None),
            Some(m) => self.blobs.get(&m.raw).map_err(|e| format!("get blob: {e}")),
        }
    }

    fn delete(&self, id: &str) -> Result<(), String> {
        let id = id.to_string();
        self.rewrite(move |mut m| {
            if m.id == id && m.deleted_at.is_none() {
                m.deleted_at = Some(now_rfc3339());
            }
            m
        })
    }

    fn delete_all(&self) -> Result<(), String> {
        self.rewrite(|mut m| {
            if m.deleted_at.is_none() {
                m.deleted_at = Some(now_rfc3339());
            }
            m
        })
    }
}

fn now_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Writes `data` to `path` via a temp file + atomic rename.
fn write_atomic(tmp: &Path, path: &Path, data: &[u8]) -> io::Result<()> {
    {
        let mut f = fs::File::create(tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    match fs::rename(tmp, path) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(tmp);
            Err(e)
        }
    }
}
