//! Mirrors the `Store` interface from `internal/services/mail/service.rs` and
//! the JSONL-backed `FileStore` from `internal/storage/mailstore/store.rs`.
//!
//! `FileStore` persists message metadata as one compact JSON object per line in
//! `messages.jsonl` and the raw body in a content-addressed `BlobStore`. Deletes
//! are tombstones (a `deletedAt` timestamp) rewritten atomically. IMAP extends
//! the legacy records with optional folder and flag metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::blob::BlobStore;
use crate::model::{
    ListMessagesInput, ListMessagesResult, MailboxMessage, MailboxSnapshot, Message, MessageEntry,
};
use crate::time_fmt::{now_rfc3339, unix_from_rfc3339};

pub(crate) const INBOX: &str = "INBOX";

/// Storage backend for received messages. Mirrors legacy `mail.Store`.
///
/// Methods are synchronous (blocking) — matching the legacy store, whose operations
/// are plain calls — and are invoked from the async SMTP session directly.
pub trait Store: Send + Sync {
    fn append(&self, message: Message, raw: &[u8]) -> Result<Message, String>;
    fn list(&self, input: ListMessagesInput) -> Result<ListMessagesResult, String>;
    fn list_all(&self) -> Result<Vec<MessageEntry>, String> {
        Err("list_all unsupported".to_string())
    }
    /// Chronological (oldest-first) snapshot of active messages with stable
    /// 1-based UIDs and `uid_next` for IMAP/POP3 access.
    fn list_mailbox(&self) -> Result<MailboxSnapshot, String> {
        let mut messages = self.list(ListMessagesInput::default())?.messages;
        messages.reverse();
        let uid_next = (messages.len() as u32).saturating_add(1).max(1);
        let messages = messages
            .into_iter()
            .enumerate()
            .map(|(i, message)| MailboxMessage {
                uid: (i + 1) as u32,
                message,
                flags: BTreeSet::new(),
            })
            .collect();
        Ok(MailboxSnapshot { messages, uid_next })
    }
    fn folders(&self) -> Result<BTreeSet<String>, String> {
        Ok(BTreeSet::from([INBOX.to_string()]))
    }
    fn create_folder(&self, _name: &str) -> Result<(), String> {
        Err("CREATE unsupported".to_string())
    }
    fn list_folder(&self, name: &str) -> Result<Option<MailboxSnapshot>, String> {
        if name == INBOX {
            self.list_mailbox().map(Some)
        } else {
            Ok(None)
        }
    }
    fn append_to_folder(
        &self,
        _name: &str,
        _message: Message,
        _raw: &[u8],
        _flags: BTreeSet<String>,
    ) -> Result<Message, String> {
        Err("APPEND unsupported".to_string())
    }
    fn message_flags(&self, _id: &str) -> Result<BTreeSet<String>, String> {
        Ok(BTreeSet::new())
    }
    fn update_flags(
        &self,
        _ids: &[&str],
        _update: &(dyn Fn(&mut BTreeSet<String>) + Sync),
    ) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
        Err("STORE unsupported".to_string())
    }
    fn expunge_flagged(&self, _ids: &[&str]) -> Result<BTreeSet<String>, String> {
        Ok(BTreeSet::new())
    }
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

#[derive(Clone, Deserialize, Serialize)]
struct StoredMessage {
    #[serde(flatten)]
    message: Message,
    #[serde(
        rename = "imapFolder",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    folder: Option<String>,
    #[serde(
        rename = "imapFlags",
        default,
        skip_serializing_if = "BTreeSet::is_empty"
    )]
    flags: BTreeSet<String>,
}

impl StoredMessage {
    fn folder(&self) -> &str {
        self.folder.as_deref().unwrap_or(INBOX)
    }
}

/// In-memory mirror of `messages.jsonl` (every record, including tombstones, in
/// file order). `None` until the on-disk log has been parsed successfully at
/// least once; kept `None` on parse failure so the failing read is retried (and
/// the same error re-surfaced) on the next call, matching the pre-index
/// behavior of re-reading the file on every access.
#[derive(Default)]
struct Index {
    messages: Option<Vec<StoredMessage>>,
    folders: Option<BTreeSet<String>>,
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
    fn read_log(&self) -> Result<Vec<StoredMessage>, String> {
        let data = match fs::read(self.messages_path()) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("open messages log: {e}")),
        };
        let mut messages = Vec::new();
        let lines: Vec<&[u8]> = data.split(|&b| b == b'\n').collect();
        let last = lines.len() - 1;
        for (i, line) in lines.into_iter().enumerate() {
            if line.is_empty() {
                continue;
            }
            // A last line without its newline that does not parse is a torn
            // append (a crash or a full disk mid-write): never acknowledged,
            // so it is ignored rather than making the mailbox unreadable.
            if i == last && serde_json::from_slice::<StoredMessage>(line).is_err() {
                continue;
            }
            let m = serde_json::from_slice(line)
                .map_err(|e| format!("decode message metadata: {e}"))?;
            messages.push(m);
        }
        Ok(messages)
    }

    /// Ensures `guard.messages` holds the parsed log, loading it from disk on
    /// first use. Caller must hold `index`'s lock.
    fn ensure_loaded<'a>(
        &self,
        guard: &'a mut Index,
    ) -> Result<&'a mut Vec<StoredMessage>, String> {
        if guard.messages.is_none() {
            guard.messages = Some(self.read_log()?);
        }
        Ok(guard.messages.as_mut().unwrap())
    }

    fn load_folders<'a>(&self, guard: &'a mut Index) -> Result<&'a mut BTreeSet<String>, String> {
        if guard.folders.is_none() {
            let mut folders: BTreeSet<String> =
                match fs::read(self.root.join("imap-folders.json")) {
                    Ok(data) => serde_json::from_slice(&data)
                        .map_err(|_| "decode IMAP folders".to_string())?,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => BTreeSet::new(),
                    Err(e) => return Err(format!("read IMAP folders: {e}")),
                };
            folders.insert(INBOX.to_string());
            guard.folders = Some(folders);
        }
        Ok(guard.folders.as_mut().unwrap())
    }

    /// Apply changes to a copy so the index advances only after the atomic rename.
    fn rewrite(&self, mut mutate: impl FnMut(&mut StoredMessage) -> bool) -> Result<(), String> {
        let mut guard = self.index.lock().unwrap();
        let mut messages = self.ensure_loaded(&mut guard)?.clone();
        let mut changed = false;
        for record in &mut messages {
            changed |= mutate(record);
        }
        if !changed {
            return Ok(());
        }
        fs::create_dir_all(&self.root).map_err(|e| format!("create mail store: {e}"))?;

        let tmp = self.root.join(format!(
            "messages-{}-{}.jsonl",
            std::process::id(),
            now_nanos()
        ));
        let mut buf = Vec::new();
        for record in &mut messages {
            let m = &mut record.message;
            if m.deleted_at.is_some() {
                // The tombstone stays (legacy parity); its contents need not.
                m.text_body.clear();
                m.html_body.clear();
                m.headers.clear();
                m.attachments.clear();
                record.flags.clear();
            }
            let line =
                serde_json::to_vec(record).map_err(|e| format!("write messages temp log: {e}"))?;
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
        write_atomic(&tmp, &self.messages_path(), &buf)
            .map_err(|e| format!("replace messages log: {e}"))?;
        guard.messages = Some(messages);
        Ok(())
    }

    fn append_record(&self, mut record: StoredMessage, raw: &[u8]) -> Result<Message, String> {
        fs::create_dir_all(&self.root).map_err(|e| format!("create mail store: {e}"))?;
        let raw_id = self.blobs.put(raw).map_err(|e| format!("put blob: {e}"))?;
        record.message.raw = raw_id;
        if record.message.received_at.is_none() {
            record.message.received_at = Some(now_rfc3339());
        }

        let mut guard = self.index.lock().unwrap();
        let mut line =
            serde_json::to_vec(&record).map_err(|e| format!("append message metadata: {e}"))?;
        line.push(b'\n');
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.messages_path())
            .map_err(|e| format!("open messages log: {e}"))?;
        let original_len = drop_torn_tail(&self.messages_path(), &f)
            .map_err(|e| format!("open messages log: {e}"))?;
        if let Err(e) = f.write_all(&line) {
            // Leave no partial record behind for the next append to extend.
            let _ = f.set_len(original_len);
            return Err(format!("append message metadata: {e}"));
        }
        // Only keep the index in sync if it is already loaded — do not force a
        // load here, since a load failure (e.g. a pre-existing corrupt line)
        // must not fail an append that would otherwise have succeeded.
        let message = record.message.clone();
        if let Some(messages) = guard.messages.as_mut() {
            messages.push(record);
        }
        Ok(message)
    }
}

impl Store for FileStore {
    fn append(&self, message: Message, raw: &[u8]) -> Result<Message, String> {
        self.append_record(
            StoredMessage {
                message,
                folder: None,
                flags: BTreeSet::new(),
            },
            raw,
        )
    }

    fn list(&self, input: ListMessagesInput) -> Result<ListMessagesResult, String> {
        let mut guard = self.index.lock().unwrap();
        let mut messages: Vec<Message> = self
            .ensure_loaded(&mut guard)?
            .iter()
            .map(|record| &record.message)
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
            .map(|record| &record.message)
            .find(|m| m.deleted_at.is_none() && m.id == id)
            .cloned())
    }

    fn list_all(&self) -> Result<Vec<MessageEntry>, String> {
        let mut guard = self.index.lock().unwrap();
        let mut messages: Vec<_> = self
            .ensure_loaded(&mut guard)?
            .iter()
            .map(|record| &record.message)
            .filter(|m| m.deleted_at.is_none())
            .map(|m| MessageEntry {
                id: m.id.clone(),
                received_at: m.received_at.clone(),
            })
            .collect();
        drop(guard);
        messages.reverse();
        messages
            .sort_by_key(|m| std::cmp::Reverse(m.received_at.as_deref().map(unix_from_rfc3339)));
        Ok(messages)
    }

    fn list_mailbox(&self) -> Result<MailboxSnapshot, String> {
        Ok(self.list_folder(INBOX)?.unwrap_or_default())
    }

    fn folders(&self) -> Result<BTreeSet<String>, String> {
        let mut guard = self.index.lock().unwrap();
        Ok(self.load_folders(&mut guard)?.clone())
    }

    fn create_folder(&self, name: &str) -> Result<(), String> {
        let mut guard = self.index.lock().unwrap();
        let current = self.load_folders(&mut guard)?;
        let mut folders = current.clone();
        if !folders.insert(name.to_string()) {
            return Err("mailbox already exists".to_string());
        }
        fs::create_dir_all(&self.root).map_err(|e| format!("create mail store: {e}"))?;
        let data = serde_json::to_vec(&folders).map_err(|_| "encode IMAP folders".to_string())?;
        write_atomic(
            &self.root.join("imap-folders.tmp"),
            &self.root.join("imap-folders.json"),
            &data,
        )
        .map_err(|e| format!("save IMAP folders: {e}"))?;
        *current = folders;
        Ok(())
    }

    fn list_folder(&self, name: &str) -> Result<Option<MailboxSnapshot>, String> {
        let mut guard = self.index.lock().unwrap();
        if name != INBOX && !self.load_folders(&mut guard)?.contains(name) {
            return Ok(None);
        }
        let records = self.ensure_loaded(&mut guard)?;
        let uid_next = (records.len() as u32).saturating_add(1).max(1);
        let messages = records
            .iter()
            .enumerate()
            .filter(|(_, record)| record.folder() == name && record.message.deleted_at.is_none())
            .map(|(idx, record)| MailboxMessage {
                uid: (idx + 1) as u32,
                message: record.message.clone(),
                flags: record.flags.clone(),
            })
            .collect();
        Ok(Some(MailboxSnapshot { messages, uid_next }))
    }

    fn append_to_folder(
        &self,
        name: &str,
        message: Message,
        raw: &[u8],
        flags: BTreeSet<String>,
    ) -> Result<Message, String> {
        if name != INBOX && !self.folders()?.contains(name) {
            return Err("mailbox does not exist".to_string());
        }
        self.append_record(
            StoredMessage {
                message,
                folder: (name != INBOX).then(|| name.to_string()),
                flags,
            },
            raw,
        )
    }

    fn message_flags(&self, id: &str) -> Result<BTreeSet<String>, String> {
        let mut guard = self.index.lock().unwrap();
        Ok(self
            .ensure_loaded(&mut guard)?
            .iter()
            .find(|record| record.message.id == id && record.message.deleted_at.is_none())
            .map(|record| record.flags.clone())
            .unwrap_or_default())
    }

    fn update_flags(
        &self,
        ids: &[&str],
        update: &(dyn Fn(&mut BTreeSet<String>) + Sync),
    ) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
        let ids: BTreeSet<_> = ids.iter().copied().collect();
        let mut result = BTreeMap::new();
        self.rewrite(|record| {
            if record.message.deleted_at.is_some() || !ids.contains(record.message.id.as_str()) {
                return false;
            }
            let before = record.flags.clone();
            update(&mut record.flags);
            result.insert(record.message.id.clone(), record.flags.clone());
            before != record.flags
        })?;
        Ok(result)
    }

    fn expunge_flagged(&self, ids: &[&str]) -> Result<BTreeSet<String>, String> {
        let ids: BTreeSet<_> = ids.iter().copied().collect();
        let mut removed = BTreeSet::new();
        self.rewrite(|record| {
            if !ids.contains(record.message.id.as_str()) || !record.flags.contains("\\Deleted") {
                return false;
            }
            if tombstone(&mut record.message) {
                removed.insert(record.message.id.clone());
                return true;
            }
            false
        })?;
        Ok(removed)
    }

    fn get_raw(&self, id: &str) -> Result<Option<Vec<u8>>, String> {
        match self.get(id)? {
            None => Ok(None),
            Some(m) => self.blobs.get(&m.raw).map_err(|e| format!("get blob: {e}")),
        }
    }

    fn delete(&self, id: &str) -> Result<(), String> {
        self.rewrite(|record| record.message.id == id && tombstone(&mut record.message))
    }

    fn delete_all(&self) -> Result<(), String> {
        self.rewrite(|record| tombstone(&mut record.message))
    }
}

fn tombstone(message: &mut Message) -> bool {
    if message.deleted_at.is_some() {
        return false;
    }
    message.deleted_at = Some(now_rfc3339());
    true
}

fn now_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Makes sure the next record starts on its own line: an unterminated last
/// record that parses (a hand edit saved without a newline) gets its newline;
/// one that does not is the torn remains of an interrupted append and is cut
/// off. Returns the resulting length.
fn drop_torn_tail(path: &Path, file: &fs::File) -> io::Result<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(0);
    }
    let mut reader = fs::File::open(path)?;
    reader.seek(SeekFrom::Start(len - 1))?;
    let mut last = [0u8; 1];
    reader.read_exact(&mut last)?;
    if last[0] == b'\n' {
        return Ok(len);
    }
    reader.seek(SeekFrom::Start(0))?;
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    let keep = data.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    if serde_json::from_slice::<StoredMessage>(&data[keep..]).is_ok() {
        let mut appender = file;
        appender.write_all(b"\n")?;
        return Ok(len + 1);
    }
    file.set_len(keep as u64)?;
    Ok(keep as u64)
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
