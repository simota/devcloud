//! Deterministic checksum/ETag helpers matching the legacy S3 store byte-for-byte:
//! MD5 ETags, the multipart composite ETag, the CRC32C (Castagnoli) base64
//! checksum, and `Content-MD5` validation.

use crate::base64;
use md5::{Digest, Md5};

/// CRC32C (Castagnoli) of `data`, big-endian, base64-StdEncoding — matching legacy
/// `base64.StdEncoding.EncodeToString(be32(crc32.Checksum(data, Castagnoli)))`.
pub fn crc32c_base64(data: &[u8]) -> String {
    let checksum = crc32c(data);
    base64::std_encode(&checksum.to_be_bytes())
}

/// CRC32C (Castagnoli) checksum: reflected poly 0x82F63B78, init/xorout all-ones.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    crc ^ 0xFFFF_FFFF
}

/// MD5 of `data` as a quoted lowercase-hex ETag (`"<hex>"`).
pub fn md5_etag(data: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(data);
    format!("\"{:x}\"", hasher.finalize())
}

/// Composite multipart ETag: MD5 of the concatenated raw part-MD5s, suffixed with
/// `-<count>`. If any part ETag is not 16 bytes of hex, falls back to `"<count>"`
/// (matching the legacy `multipartETag`).
pub fn multipart_etag(part_etags: &[String]) -> String {
    let mut hashes = Vec::with_capacity(part_etags.len() * 16);
    for etag in part_etags {
        let trimmed = etag.trim_matches('"');
        match hex::decode(trimmed) {
            Ok(raw) if raw.len() == 16 => hashes.extend_from_slice(&raw),
            _ => return format!("\"{}\"", part_etags.len()),
        }
    }
    let mut hasher = Md5::new();
    hasher.update(&hashes);
    format!("\"{:x}-{}\"", hasher.finalize(), part_etags.len())
}

/// `Content-MD5` validation failure, mirroring the legacy sentinel errors.
#[derive(Debug, PartialEq, Eq)]
pub enum ContentMd5Error {
    Invalid,
    Mismatch,
}

/// Validates a base64 `Content-MD5` header against `body`. An empty header is a
/// no-op (matching the legacy `validateContentMD5`).
pub fn validate_content_md5(header: &str, body: &[u8]) -> Result<(), ContentMd5Error> {
    if header.is_empty() {
        return Ok(());
    }
    let expected = match base64::std_decode(header) {
        Some(bytes) if bytes.len() == 16 => bytes,
        _ => return Err(ContentMd5Error::Invalid),
    };
    let mut hasher = Md5::new();
    hasher.update(body);
    if hasher.finalize().as_slice() != expected.as_slice() {
        return Err(ContentMd5Error::Mismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_known_vectors() {
        assert_eq!(crc32c_base64(b"content"), "Ya91Mw==");
        assert_eq!(crc32c_base64(b""), "AAAAAA==");
        assert_eq!(crc32c_base64(b"hello world"), "yZRlqg==");
    }

    #[test]
    fn md5_etag_quoted_hex() {
        assert_eq!(md5_etag(b"content"), "\"9a0364b9e99bb480dd25e1f0284c8555\"");
        assert_eq!(md5_etag(b""), "\"d41d8cd98f00b204e9800998ecf8427e\"");
    }

    #[test]
    fn multipart_etag_vectors() {
        assert_eq!(
            multipart_etag(&[
                "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string(),
                "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"".to_string(),
            ]),
            "\"5812295871323bf83b6f30d96ac98cf0-2\""
        );
        assert_eq!(
            multipart_etag(&["\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string()]),
            "\"73ad9750e8d5fcf7936433620b4baa21-1\""
        );
        // Non-hex part ETag falls back to the count.
        assert_eq!(multipart_etag(&["not-hex".to_string()]), "\"1\"");
    }

    #[test]
    fn content_md5_validation() {
        // base64(md5("content")) == mgNkuembtIDdJeHwKEyFVQ==
        assert_eq!(
            validate_content_md5("mgNkuembtIDdJeHwKEyFVQ==", b"content"),
            Ok(())
        );
        assert_eq!(validate_content_md5("", b"anything"), Ok(()));
        assert_eq!(
            validate_content_md5("not-base64-16", b"content"),
            Err(ContentMd5Error::Invalid)
        );
        assert_eq!(
            validate_content_md5("mgNkuembtIDdJeHwKEyFVQ==", b"different"),
            Err(ContentMd5Error::Mismatch)
        );
    }
}
