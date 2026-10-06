//! Minimal deployment-package zip reader.
//!
//! Lambda deployment packages are plain zip archives. This parses the central
//! directory (no zip64, no encryption) and supports the two compression methods
//! real packaging tools emit: stored (0) and DEFLATE (8). Entry names are
//! validated so an archive can never write outside the extraction root.

use std::path::{Component, Path, PathBuf};

const EOCD_SIG: u32 = 0x0605_4b50;
const CENTRAL_SIG: u32 = 0x0201_4b50;
const LOCAL_SIG: u32 = 0x0403_4b50;
const EOCD_MIN_LEN: usize = 22;
const MAX_UNCOMPRESSED_BYTES: usize = 250 * 1024 * 1024;

/// One file entry from the archive, already decompressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub data: Vec<u8>,
    /// Unix permission bits from the external attributes, when present.
    pub mode: Option<u32>,
    pub is_dir: bool,
}

fn u16_at(b: &[u8], off: usize) -> Result<u16, String> {
    b.get(off..off + 2)
        .map(|s| u16::from_le_bytes([s[0], s[1]]))
        .ok_or_else(|| "truncated zip archive".to_string())
}

fn u32_at(b: &[u8], off: usize) -> Result<u32, String> {
    b.get(off..off + 4)
        .map(|s| u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
        .ok_or_else(|| "truncated zip archive".to_string())
}

const TOO_LARGE: &str = "Unzipped size must be smaller than 262144000 bytes";

/// Parses every entry of `archive`.
pub fn read_entries(archive: &[u8]) -> Result<Vec<Entry>, String> {
    read_entries_with_limit(archive, MAX_UNCOMPRESSED_BYTES)
}

/// Like [`read_entries`], with `limit` bounding the bytes actually produced
/// across all entries. Header sizes are attacker-controlled, so the budget is
/// charged with real output and handed to the inflater as its hard cap.
fn read_entries_with_limit(archive: &[u8], limit: usize) -> Result<Vec<Entry>, String> {
    if archive.len() < EOCD_MIN_LEN {
        return Err(
            "Could not unzip uploaded file. Please check your file, then try to upload again."
                .to_string(),
        );
    }
    // The EOCD record sits at the end, optionally followed by a comment (<= 64 KiB).
    let search_start = archive.len().saturating_sub(EOCD_MIN_LEN + 0xFFFF);
    let eocd = (search_start..=archive.len() - EOCD_MIN_LEN)
        .rev()
        .find(|&i| u32_at(archive, i).ok() == Some(EOCD_SIG))
        .ok_or_else(|| {
            "Could not unzip uploaded file. Please check your file, then try to upload again."
                .to_string()
        })?;

    let total = u16_at(archive, eocd + 10)? as usize;
    let cd_offset = u32_at(archive, eocd + 16)? as usize;
    let mut entries = Vec::with_capacity(total);
    let mut pos = cd_offset;
    let mut remaining = limit;
    for _ in 0..total {
        if u32_at(archive, pos)? != CENTRAL_SIG {
            return Err("corrupt zip central directory".to_string());
        }
        let version_made_by = u16_at(archive, pos + 4)?;
        let flags = u16_at(archive, pos + 8)?;
        let method = u16_at(archive, pos + 10)?;
        let expected_crc = u32_at(archive, pos + 16)?;
        let compressed_size = u32_at(archive, pos + 20)? as usize;
        let uncompressed_size = u32_at(archive, pos + 24)? as usize;
        let name_len = u16_at(archive, pos + 28)? as usize;
        let extra_len = u16_at(archive, pos + 30)? as usize;
        let comment_len = u16_at(archive, pos + 32)? as usize;
        let external_attrs = u32_at(archive, pos + 38)?;
        let local_offset = u32_at(archive, pos + 42)? as usize;
        let name_bytes = archive
            .get(pos + 46..pos + 46 + name_len)
            .ok_or("truncated zip archive")?;
        let name = String::from_utf8_lossy(name_bytes).into_owned();
        pos += 46 + name_len + extra_len + comment_len;

        if flags & 0x1 != 0 {
            return Err(format!("encrypted zip entry {name:?} is not supported"));
        }
        // Cheap early reject on the declared size; the real check is below.
        if uncompressed_size > remaining {
            return Err(TOO_LARGE.to_string());
        }

        if u32_at(archive, local_offset)? != LOCAL_SIG {
            return Err(format!("corrupt local header for zip entry {name:?}"));
        }
        let local_name_len = u16_at(archive, local_offset + 26)? as usize;
        let local_extra_len = u16_at(archive, local_offset + 28)? as usize;
        let data_start = local_offset + 30 + local_name_len + local_extra_len;
        let raw = archive
            .get(data_start..data_start + compressed_size)
            .ok_or("truncated zip archive")?;
        let data = match method {
            0 if raw.len() > remaining => return Err(TOO_LARGE.to_string()),
            0 => raw.to_vec(),
            8 => {
                miniz_oxide::inflate::decompress_to_vec_with_limit(raw, remaining).map_err(|e| {
                    if e.status == miniz_oxide::inflate::TINFLStatus::HasMoreOutput {
                        TOO_LARGE.to_string()
                    } else {
                        // Only the status: the error's Debug output carries
                        // every byte inflated so far (megabytes for a few KB
                        // of input).
                        format!("inflate zip entry {name:?}: {:?}", e.status)
                    }
                })?
            }
            other => {
                return Err(format!(
                    "zip entry {name:?} uses unsupported compression method {other}"
                ))
            }
        };

        // A damaged archive must not deploy: the produced bytes have to match
        // the central directory's size and CRC-32 exactly.
        if data.len() != uncompressed_size || crc32(&data) != expected_crc {
            return Err(format!(
                "Could not unzip uploaded file: zip entry {name:?} is corrupt (size or CRC-32 mismatch)"
            ));
        }
        remaining -= data.len();

        // Upper byte 3 == Unix: the high 16 bits of the external attributes
        // carry st_mode.
        let mode = if version_made_by >> 8 == 3 {
            Some((external_attrs >> 16) & 0o7777)
        } else {
            None
        };
        entries.push(Entry {
            is_dir: name.ends_with('/'),
            name,
            data,
            mode,
        });
    }
    Ok(entries)
}

/// Resolves an entry name to a path under `root`, rejecting absolute paths and
/// any `..` component (zip-slip).
fn safe_join(root: &Path, name: &str) -> Result<PathBuf, String> {
    let rel = Path::new(name);
    let mut out = root.to_path_buf();
    for component in rel.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            _ => return Err(format!("zip entry {name:?} escapes the package root")),
        }
    }
    Ok(out)
}

/// Extracts `archive` into `root` (which must already exist and be empty).
pub fn extract(archive: &[u8], root: &Path) -> Result<(), String> {
    let entries = read_entries(archive)?;
    for entry in entries {
        let path = safe_join(root, &entry.name)?;
        if entry.is_dir {
            std::fs::create_dir_all(&path).map_err(|e| e.to_string())?;
            continue;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&path, &entry.data).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        if let Some(mode) = entry.mode.filter(|m| *m != 0) {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Builds a stored (uncompressed) zip archive. Used by tests and by tooling that
/// needs a deterministic package without an external `zip` binary.
pub fn build_stored(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for (name, data) in files {
        let offset = out.len() as u32;
        let crc = crc32(data);
        let size = data.len() as u32;
        out.extend_from_slice(&LOCAL_SIG.to_le_bytes());
        out.extend_from_slice(&20u16.to_le_bytes()); // version needed
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&0u16.to_le_bytes()); // method: stored
        out.extend_from_slice(&0u32.to_le_bytes()); // mod time + date
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(data);

        central.extend_from_slice(&CENTRAL_SIG.to_le_bytes());
        central.extend_from_slice(&((3u16 << 8) | 20).to_le_bytes()); // made by: unix
        central.extend_from_slice(&20u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&0u32.to_le_bytes());
        central.extend_from_slice(&crc.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&(name.len() as u16).to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes()); // extra
        central.extend_from_slice(&0u16.to_le_bytes()); // comment
        central.extend_from_slice(&0u16.to_le_bytes()); // disk
        central.extend_from_slice(&0u16.to_le_bytes()); // internal attrs
        central.extend_from_slice(&(0o100644u32 << 16).to_le_bytes());
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(name.as_bytes());
    }
    let cd_offset = out.len() as u32;
    let cd_len = central.len() as u32;
    out.extend_from_slice(&central);
    out.extend_from_slice(&EOCD_SIG.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&cd_len.to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_round_trip() {
        let archive = build_stored(&[("a.py", b"print(1)"), ("pkg/b.txt", b"hello")]);
        let entries = read_entries(&archive).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "a.py");
        assert_eq!(entries[0].data, b"print(1)");
        assert_eq!(entries[1].name, "pkg/b.txt");
        assert_eq!(entries[1].mode, Some(0o644));
    }

    /// Rewrites every central-directory `uncompressed_size` to `declared`.
    fn forge_declared_sizes(mut archive: Vec<u8>, declared: u32) -> Vec<u8> {
        let mut i = 0;
        while i + 4 <= archive.len() {
            if archive[i..i + 4] == CENTRAL_SIG.to_le_bytes() {
                archive[i + 24..i + 28].copy_from_slice(&declared.to_le_bytes());
            }
            i += 1;
        }
        archive
    }

    #[test]
    fn limit_counts_real_output_not_declared_sizes() {
        let big = vec![b'a'; 800];
        let honest = build_stored(&[("a", &big), ("b", &big)]);
        let err = read_entries_with_limit(&honest, 1000).unwrap_err();
        assert!(err.contains("Unzipped size"), "{err}");
        assert!(read_entries_with_limit(&honest, 1600).is_ok());
        // Understated sizes cannot sneak output past the budget either: the
        // real output is checked against the declared size.
        let forged = forge_declared_sizes(honest, 1);
        assert!(read_entries_with_limit(&forged, 1600).is_err());
    }

    #[test]
    fn corrupted_entry_data_is_rejected() {
        let mut archive = build_stored(&[("app.py", b"print('hello')")]);
        // Flip one byte of the stored file data (right after the local header).
        let data_start = 30 + "app.py".len();
        archive[data_start] ^= 0xFF;
        let err = read_entries(&archive).unwrap_err();
        assert!(err.contains("corrupt"), "{err}");
    }

    #[test]
    fn inflate_is_capped_by_the_remaining_budget() {
        let payload = vec![0u8; 64 * 1024];
        let compressed = miniz_oxide::deflate::compress_to_vec(&payload, 6);
        assert!(miniz_oxide::inflate::decompress_to_vec_with_limit(&compressed, 1024).is_err());
        // The parser surfaces the same cap as a size error.
        let mut archive = build_stored(&[("z", &payload)]);
        archive = deflate_single_entry(archive, "z", &payload, &compressed);
        let archive = forge_declared_sizes(archive, 1);
        let err = read_entries_with_limit(&archive, 1024).unwrap_err();
        assert!(err.contains("Unzipped size"), "{err}");
    }

    /// Rewrites a single stored entry named `name` as DEFLATE `compressed`.
    fn deflate_single_entry(
        archive: Vec<u8>,
        name: &str,
        payload: &[u8],
        compressed: &[u8],
    ) -> Vec<u8> {
        let data_start = 30 + name.len();
        let mut rebuilt = Vec::new();
        rebuilt.extend_from_slice(&archive[..8]);
        rebuilt.extend_from_slice(&8u16.to_le_bytes());
        rebuilt.extend_from_slice(&archive[10..18]);
        rebuilt.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        rebuilt.extend_from_slice(&archive[22..data_start]);
        rebuilt.extend_from_slice(compressed);
        let cd_start = data_start + payload.len();
        let new_cd_offset = rebuilt.len() as u32;
        let mut central = archive[cd_start..].to_vec();
        central[10..12].copy_from_slice(&8u16.to_le_bytes());
        central[20..24].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        let eocd = central.len() - 22;
        central[eocd + 16..eocd + 20].copy_from_slice(&new_cd_offset.to_le_bytes());
        rebuilt.extend_from_slice(&central);
        rebuilt
    }

    #[test]
    fn truncated_deflate_error_does_not_embed_inflated_data() {
        let payload = vec![b'a'; 4 * 1024 * 1024];
        let compressed = miniz_oxide::deflate::compress_to_vec(&payload, 6);
        let archive = build_stored(&[("z", &payload)]);
        let archive = deflate_single_entry(
            archive,
            "z",
            &payload,
            &compressed[..compressed.len() / 2]
                .iter()
                .chain(std::iter::repeat_n(
                    &0u8,
                    compressed.len() - compressed.len() / 2,
                ))
                .copied()
                .collect::<Vec<_>>(),
        );
        let err = read_entries(&archive).unwrap_err();
        assert!(
            err.starts_with("inflate zip entry"),
            "{}",
            &err[..err.len().min(200)]
        );
        assert!(err.len() < 200, "error is {} bytes", err.len());
    }

    #[test]
    fn crc32_matches_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn rejects_non_zip_payload() {
        assert!(read_entries(b"definitely not a zip archive").is_err());
    }

    #[test]
    fn rejects_path_traversal() {
        let archive = build_stored(&[("../evil.py", b"x")]);
        let dir = std::env::temp_dir().join(format!("devcloud-lambda-zip-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let err = extract(&archive, &dir).unwrap_err();
        assert!(err.contains("escapes"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn inflates_deflate_entries() {
        // `printf 'hello hello hello' | python3 -c 'import zlib,sys;...'` raw deflate.
        let payload = b"hello hello hello hello";
        let compressed = miniz_oxide::deflate::compress_to_vec(payload, 6);
        let mut archive = build_stored(&[("x.txt", payload)]);
        // Rewrite the single entry as DEFLATE: swap method + sizes + data.
        let name_len = 5usize;
        let data_start = 30 + name_len;
        let mut rebuilt = Vec::new();
        rebuilt.extend_from_slice(&archive[..8]);
        rebuilt.extend_from_slice(&8u16.to_le_bytes());
        rebuilt.extend_from_slice(&archive[10..18]);
        rebuilt.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        rebuilt.extend_from_slice(&archive[22..data_start]);
        rebuilt.extend_from_slice(&compressed);
        let cd_start = data_start + payload.len();
        let new_cd_offset = rebuilt.len() as u32;
        let mut central = archive[cd_start..].to_vec();
        central[10..12].copy_from_slice(&8u16.to_le_bytes());
        central[20..24].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        let eocd = central.len() - 22;
        central[eocd + 16..eocd + 20].copy_from_slice(&new_cd_offset.to_le_bytes());
        rebuilt.extend_from_slice(&central);
        archive = rebuilt;

        let entries = read_entries(&archive).unwrap();
        assert_eq!(entries[0].data, payload);
    }
}
