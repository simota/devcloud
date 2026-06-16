//! Small shared helpers mirroring legacy daemon path/string utilities
//! (`internal/app/daemon.rs`).

use std::path::Path;

/// legacy `defaultString(a, b)`: returns `a` if non-empty, else `b`.
pub fn default_string(a: &str, b: &str) -> String {
    if a.is_empty() {
        b.to_string()
    } else {
        a.to_string()
    }
}

/// Mirrors legacy `redshiftDataDir`/`redisDataDir` scoping: empty ->
/// `<storage>/<default_sub>`; a `.devcloud`-rooted clean path is kept as-is;
/// otherwise the cleaned path is joined under `<storage>`.
pub fn scoped_data_dir(storage_path: &str, data_dir: &str, default_sub: &str) -> String {
    if data_dir.is_empty() {
        return Path::new(storage_path)
            .join(default_sub)
            .to_string_lossy()
            .into_owned();
    }
    let clean = clean_path(data_dir);
    if clean == ".devcloud" || clean.starts_with(".devcloud/") {
        return clean;
    }
    Path::new(storage_path)
        .join(&clean)
        .to_string_lossy()
        .into_owned()
}

/// Minimal `filepath.Clean` approximation over forward-slash paths — sufficient
/// for config-supplied data dirs (no symlinks). Collapses `.`/`..` and
/// duplicate separators.
fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for comp in p.split('/') {
        match comp {
            "" | "." => continue,
            ".." => {
                if let Some(last) = out.last() {
                    if *last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !rooted {
                    out.push("..");
                }
            }
            c => out.push(c),
        }
    }
    let mut s = String::new();
    if rooted {
        s.push('/');
    }
    s.push_str(&out.join("/"));
    if s.is_empty() {
        ".".to_string()
    } else {
        s
    }
}
