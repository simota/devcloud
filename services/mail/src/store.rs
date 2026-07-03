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

/// In-memory mirror of `messages.jsonl` (every record, including tombstones, in
/// file order). `None` until the on-disk log has been parsed successfully at
/// least once; kept `None` on parse failure so the failing read is retried (and
/// the same error re-surfaced) on the next call, matching the pre-index
/// behavior of re-reading the file on every access.
#[derive(Default)]
struct Index {
    messages: Option<Vec<Message>>,
}

pub struct FileStore {
    root: PathBuf,
    blobs: Arc<dyn BlobStore>,
    /// Serializes all file access AND guards the in-memory index, mirroring the
    /// legacy store's `sync.Mutex`.
    index: Mutex<Index>,
}

impl FileStore {
    pub fn new(root: impl Into<PathBuf>, blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            root: root.into(),
            blobs,
            index: Mutex::new(Index::default()),
        }
    }

    pub fn messages_path(&self) -> PathBuf {
        self.root.join("messages.jsonl")
    }

    /// Parses every record (including tombstones) straight from disk, in file
    /// order. Caller must hold `index`'s lock.
    fn read_log(&self) -> Result<Vec<Message>, String> {
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

    /// Ensures `guard.messages` holds the parsed log, loading it from disk on
    /// first use. Caller must hold `index`'s lock.
    fn ensure_loaded<'a>(&self, guard: &'a mut Index) -> Result<&'a mut Vec<Message>, String> {
        if guard.messages.is_none() {
            guard.messages = Some(self.read_log()?);
        }
        Ok(guard.messages.as_mut().unwrap())
    }

    /// Rewrites the log, applying `mutate` to every in-memory record (active +
    /// tombstoned), via a temp file + atomic rename, and updates the index to
    /// match. Mirrors legacy `FileStore.rewrite`.
    fn rewrite(&self, mutate: impl Fn(Message) -> Message) -> Result<(), String> {
        let mut guard = self.index.lock().unwrap();
        let messages = self.ensure_loaded(&mut guard)?.clone();
        fs::create_dir_all(&self.root).map_err(|e| format!("create mail store: {e}"))?;

        let tmp = self.root.join(format!(
            "messages-{}-{}.jsonl",
            std::process::id(),
            now_nanos()
        ));
        let mut buf = Vec::new();
        let mut mutated = Vec::with_capacity(messages.len());
        for message in messages {
            let m = mutate(message);
            let line =
                serde_json::to_vec(&m).map_err(|e| format!("write messages temp log: {e}"))?;
            buf.extend_from_slice(&line);
            buf.push(b'\n');
            mutated.push(m);
        }
        write_atomic(&tmp, &self.messages_path(), &buf)
            .map_err(|e| format!("replace messages log: {e}"))?;
        guard.messages = Some(mutated);
        Ok(())
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

        let mut guard = self.index.lock().unwrap();
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
        // Only keep the index in sync if it is already loaded — do not force a
        // load here, since a load failure (e.g. a pre-existing corrupt line)
        // must not fail an append that would otherwise have succeeded.
        if let Some(messages) = guard.messages.as_mut() {
            messages.push(message.clone());
        }
        Ok(message)
    }

    fn list(&self, input: ListMessagesInput) -> Result<ListMessagesResult, String> {
        let mut guard = self.index.lock().unwrap();
        let mut messages: Vec<Message> = self
            .ensure_loaded(&mut guard)?
            .iter()
            .filter(|m| m.deleted_at.is_none())
            .cloned()
            .collect();
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
        let mut guard = self.index.lock().unwrap();
        Ok(self
            .ensure_loaded(&mut guard)?
            .iter()
            .find(|m| m.deleted_at.is_none() && m.id == id)
            .cloned())
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
