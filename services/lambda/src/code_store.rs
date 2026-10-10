//! Content-addressed deployment packages with invocation leases.
//!
//! Each package version lives at `functions/<name>/code-<sha>.zip` plus its
//! extracted tree `functions/<name>/code-<sha>/`, so writing a new version
//! never touches the one the committed configuration points at. Invocations
//! hold a [`Lease`] on the tree they started with; retiring a leased version
//! removes its zip immediately but defers the tree until the last lease drops.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

#[derive(Default)]
struct Leases {
    counts: HashMap<PathBuf, usize>,
    /// Trees retired while leased; removed when their count reaches zero.
    doomed: HashSet<PathBuf>,
}

pub struct CodeStore {
    functions_root: PathBuf,
    leases: Arc<Mutex<Leases>>,
}

/// Keeps one extracted package tree alive for the duration of an invocation.
pub struct Lease {
    leases: Arc<Mutex<Leases>>,
    dir: PathBuf,
}

impl Lease {
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut l = self.leases.lock().unwrap();
        let remaining = match l.counts.get_mut(&self.dir) {
            Some(n) => {
                *n -= 1;
                *n
            }
            None => 0,
        };
        if remaining == 0 {
            l.counts.remove(&self.dir);
            if l.doomed.remove(&self.dir) {
                remove_tree(&self.dir);
            }
        }
    }
}

impl CodeStore {
    pub fn new(functions_root: PathBuf) -> Self {
        CodeStore {
            functions_root,
            leases: Arc::new(Mutex::new(Leases::default())),
        }
    }

    pub fn function_dir(&self, name: &str) -> PathBuf {
        self.functions_root.join(name)
    }

    pub fn zip_path(&self, name: &str, sha: &str) -> PathBuf {
        self.function_dir(name)
            .join(format!("code-{}.zip", suffix(sha)))
    }

    pub fn tree_path(&self, name: &str, sha: &str) -> PathBuf {
        self.function_dir(name)
            .join(format!("code-{}", suffix(sha)))
    }

    /// Writes the zip and its extracted tree for `sha` if they are not there
    /// yet. Never modifies any other version. Extraction errors are the
    /// caller's input error; IO errors are server errors.
    pub fn stage(&self, name: &str, zip: &[u8], sha: &str) -> Result<(), StageError> {
        let dir = self.function_dir(name);
        std::fs::create_dir_all(&dir).map_err(|e| StageError::Io(e.to_string()))?;
        let tree = self.tree_path(name, sha);
        // A re-deployed version that was waiting for deletion is live again.
        self.leases.lock().unwrap().doomed.remove(&tree);
        if !tree.is_dir() {
            extract_into(zip, &tree).map_err(StageError::Package)?;
        }
        let zip_path = self.zip_path(name, sha);
        if !zip_path.is_file() {
            let tmp = zip_path.with_extension("zip.tmp");
            write_synced(&tmp, zip).map_err(|e| StageError::Io(e.to_string()))?;
            std::fs::rename(&tmp, &zip_path).map_err(|e| StageError::Io(e.to_string()))?;
        }
        Ok(())
    }

    /// Re-creates a missing tree from its stored zip (startup recovery).
    pub fn ensure_tree(&self, name: &str, sha: &str) -> Result<(), String> {
        let tree = self.tree_path(name, sha);
        if tree.is_dir() {
            return Ok(());
        }
        let zip = std::fs::read(self.zip_path(name, sha))
            .map_err(|e| format!("read package for {name}: {e}"))?;
        extract_into(&zip, &tree)
    }

    pub fn lease(&self, name: &str, sha: &str) -> Lease {
        let dir = self.tree_path(name, sha);
        *self
            .leases
            .lock()
            .unwrap()
            .counts
            .entry(dir.clone())
            .or_insert(0) += 1;
        Lease {
            leases: Arc::clone(&self.leases),
            dir,
        }
    }

    /// Removes version `sha` of `name`: the zip now, the tree now or — when an
    /// invocation still uses it — once its last lease drops. Also removes the
    /// function directory once nothing is left in it.
    pub fn retire(&self, name: &str, sha: &str) {
        let _ = std::fs::remove_file(self.zip_path(name, sha));
        let tree = self.tree_path(name, sha);
        {
            let mut l = self.leases.lock().unwrap();
            if l.counts.get(&tree).copied().unwrap_or(0) > 0 {
                l.doomed.insert(tree);
                return;
            }
        }
        remove_tree(&tree);
    }
}

#[derive(Debug)]
pub enum StageError {
    /// The package itself is invalid (bad zip, unsafe paths).
    Package(String),
    Io(String),
}

/// Filesystem-safe name derived from the (base64) code hash.
fn suffix(sha: &str) -> String {
    hex::encode(Sha256::digest(sha.as_bytes()))[..16].to_string()
}

/// Writes `data` and flushes it to disk, so a rename that follows never
/// publishes a truncated file after a crash or power loss.
pub(crate) fn write_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(data)?;
    file.sync_all()
}

fn extract_into(zip: &[u8], tree: &Path) -> Result<(), String> {
    let tmp = tree.with_extension("extracting");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).map_err(|e| e.to_string())?;
    if let Err(e) = crate::zip::extract(zip, &tmp) {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(e);
    }
    std::fs::rename(&tmp, tree).map_err(|e| e.to_string())
}

/// Removes a package tree and, if that leaves it empty, its function dir.
fn remove_tree(tree: &Path) {
    let _ = std::fs::remove_dir_all(tree);
    if let Some(parent) = tree.parent() {
        let _ = std::fs::remove_dir(parent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(tag: &str) -> (CodeStore, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "devcloud-lambda-codestore-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        (CodeStore::new(root.clone()), root)
    }

    #[test]
    fn retire_defers_leased_tree_until_release() {
        let (s, root) = store("lease");
        let zip = crate::zip::build_stored(&[("data.txt", b"v1")]);
        s.stage("f", &zip, "sha1").unwrap();
        let lease = s.lease("f", "sha1");
        s.retire("f", "sha1");
        assert!(
            lease.dir().join("data.txt").is_file(),
            "leased tree must survive"
        );
        assert!(!s.zip_path("f", "sha1").exists());
        drop(lease);
        assert!(!s.tree_path("f", "sha1").exists());
        assert!(
            !s.function_dir("f").exists(),
            "empty function dir is removed"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn restaging_a_doomed_version_revives_it() {
        let (s, root) = store("revive");
        let zip = crate::zip::build_stored(&[("a.py", b"")]);
        s.stage("f", &zip, "sha1").unwrap();
        let lease = s.lease("f", "sha1");
        s.retire("f", "sha1");
        s.stage("f", &zip, "sha1").unwrap();
        drop(lease);
        assert!(s.tree_path("f", "sha1").is_dir());
        assert!(s.zip_path("f", "sha1").is_file());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn staging_leaves_other_versions_untouched() {
        let (s, root) = store("versions");
        s.stage("f", &crate::zip::build_stored(&[("v", b"1")]), "sha1")
            .unwrap();
        s.stage("f", &crate::zip::build_stored(&[("v", b"2")]), "sha2")
            .unwrap();
        assert_eq!(
            std::fs::read(s.tree_path("f", "sha1").join("v")).unwrap(),
            b"1"
        );
        assert_eq!(
            std::fs::read(s.tree_path("f", "sha2").join("v")).unwrap(),
            b"2"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
