//! Mirrors `internal/storage/blob/store.go`.
//!
//! Content-addressed blob store: the id is the lowercase hex SHA-256 of the
//! content, stored at `<root>/<id[0:2]>/<id[2:4]>/<id>.blob`. Writes go through
//! a temp file + atomic rename, matching the Go implementation byte-for-byte
//! (same id, same shard path), so a Rust writer and a Go reader interoperate.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Blob identifier — the lowercase hex SHA-256 of the content. Mirrors Go
/// `blob.ID`.
pub type BlobId = String;

/// Content-addressed blob store backed by the filesystem. Mirrors Go
/// `blob.Store` / `blob.FileStore`.
pub trait BlobStore: Send + Sync {
    fn put(&self, raw: &[u8]) -> io::Result<BlobId>;
    fn get(&self, id: &str) -> io::Result<Option<Vec<u8>>>;
    fn delete(&self, id: &str) -> io::Result<()>;
}

pub struct FileBlobStore {
    root: PathBuf,
}

impl FileBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Mirrors `FileStore.pathFor`: short ids (<4 chars) live flat at the root;
    /// longer ids are sharded by their first two byte-pairs.
    fn path_for(&self, id: &str) -> PathBuf {
        if id.len() < 4 {
            self.root.join(format!("{}.blob", id))
        } else {
            self.root
                .join(&id[..2])
                .join(&id[2..4])
                .join(format!("{}.blob", id))
        }
    }
}

impl BlobStore for FileBlobStore {
    fn put(&self, raw: &[u8]) -> io::Result<BlobId> {
        fs::create_dir_all(&self.root)?;

        let mut hasher = Sha256::new();
        hasher.update(raw);
        let id = hex_lower(&hasher.finalize());

        let path = self.path_for(&id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        write_atomic(&self.root, &path, raw)?;
        Ok(id)
    }

    fn get(&self, id: &str) -> io::Result<Option<Vec<u8>>> {
        match fs::File::open(self.path_for(id)) {
            Ok(mut f) => {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                Ok(Some(buf))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete(&self, id: &str) -> io::Result<()> {
        match fs::remove_file(self.path_for(id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Writes `data` to `path` via a temp file in `dir` + atomic rename, mirroring
/// Go's `os.CreateTemp` + `os.Rename` durability pattern.
fn write_atomic(dir: &Path, path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = unique_temp(dir, "blob-");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    match fs::rename(&tmp, path) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

/// Generates a collision-resistant temp path inside `dir`. Uniqueness comes from
/// the process id and a monotonically increasing counter, so it is safe under
/// concurrent writers without relying on randomness.
fn unique_temp(dir: &Path, prefix: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    dir.join(format!("{}{}-{}-{}", prefix, std::process::id(), nanos, n))
}
