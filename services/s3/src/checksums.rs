//! Flexible checksums (`x-amz-checksum-*`) carried as `aws-chunked` trailers.
//!
//! The decoded payload is hashed with the algorithm named by each trailer
//! (`crc32`, `crc32c`, `crc64nvme`, `sha1`, `sha256`) and compared with the
//! base64 value the client sent. Validation is a payload-integrity check, not an
//! authorization check, so it runs in every auth mode.

use sha2::{Digest, Sha256};

use crate::base64;

const PREFIX: &str = "x-amz-checksum-";

/// Why a trailer checksum was refused. Each maps to an S3 error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrailerChecksumError {
    /// `x-amz-checksum-<name>` with an algorithm S3 does not define.
    UnsupportedAlgorithm,
    /// The value is not base64 of the algorithm's digest length.
    Malformed,
    /// `x-amz-trailer` declared a checksum the body does not carry.
    Missing,
    /// The value does not match the payload.
    Mismatch,
}

impl TrailerChecksumError {
    pub fn code(&self) -> &'static str {
        match self {
            TrailerChecksumError::Mismatch => "BadDigest",
            _ => "InvalidRequest",
        }
    }
}

/// Digest of `data` for checksum algorithm `name` (lowercase, without prefix).
pub fn digest(name: &str, data: &[u8]) -> Option<Vec<u8>> {
    Some(match name {
        "crc32" => crate::http::crc32_ieee(data).to_be_bytes().to_vec(),
        "crc32c" => crate::hashes::crc32c(data).to_be_bytes().to_vec(),
        "crc64nvme" => crc64nvme(data).to_be_bytes().to_vec(),
        "sha1" => crate::auth::sha1(data).to_vec(),
        "sha256" => Sha256::digest(data).to_vec(),
        _ => return None,
    })
}

/// Validates every `x-amz-checksum-*` trailer against `payload`, and that each
/// checksum named in the `x-amz-trailer` header (`declared`) was sent.
pub fn validate_trailers(
    trailers: &[(&str, &str)],
    declared: &str,
    payload: &[u8],
) -> Result<(), TrailerChecksumError> {
    for (name, value) in trailers {
        let lower = name.to_ascii_lowercase();
        let Some(algorithm) = lower.strip_prefix(PREFIX) else {
            continue;
        };
        let expected =
            digest(algorithm, payload).ok_or(TrailerChecksumError::UnsupportedAlgorithm)?;
        let sent = base64::std_decode(value.trim()).ok_or(TrailerChecksumError::Malformed)?;
        if sent.len() != expected.len() {
            return Err(TrailerChecksumError::Malformed);
        }
        if sent != expected {
            return Err(TrailerChecksumError::Mismatch);
        }
    }
    for name in declared.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let lower = name.to_ascii_lowercase();
        if lower.starts_with(PREFIX)
            && !trailers
                .iter()
                .any(|(sent, _)| sent.eq_ignore_ascii_case(&lower))
        {
            return Err(TrailerChecksumError::Missing);
        }
    }
    Ok(())
}

/// CRC-64/NVME: reflected poly 0x9A6C9329AC4BC9B5, init/xorout all-ones.
fn crc64nvme(data: &[u8]) -> u64 {
    let mut crc = u64::MAX;
    for &byte in data {
        crc ^= byte as u64;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x9A6C_9329_AC4B_C9B5
            } else {
                crc >> 1
            };
        }
    }
    crc ^ u64::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_match_catalogue_check_values() {
        let check = b"123456789";
        assert_eq!(
            digest("crc32", check).unwrap(),
            0xCBF4_3926u32.to_be_bytes()
        );
        assert_eq!(
            digest("crc32c", check).unwrap(),
            0xE306_9283u32.to_be_bytes()
        );
        assert_eq!(
            digest("crc64nvme", check).unwrap(),
            0xAE8B_1486_0A79_9888u64.to_be_bytes()
        );
        assert_eq!(
            hex::encode(digest("sha1", b"abc").unwrap()),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert!(digest("md5", check).is_none());
    }

    #[test]
    fn validates_trailers_against_payload() {
        let payload = b"hello world";
        let ok = [("x-amz-checksum-crc32", "DUoRhQ==")];
        assert_eq!(
            validate_trailers(&ok, "x-amz-checksum-crc32", payload),
            Ok(())
        );
        assert_eq!(
            validate_trailers(&[("x-amz-checksum-crc32", "AAAAAA==")], "", payload),
            Err(TrailerChecksumError::Mismatch)
        );
        assert_eq!(
            validate_trailers(&[("x-amz-checksum-crc32", "not base64!")], "", payload),
            Err(TrailerChecksumError::Malformed)
        );
        assert_eq!(
            validate_trailers(&[("x-amz-checksum-md5", "AAAA")], "", payload),
            Err(TrailerChecksumError::UnsupportedAlgorithm)
        );
        assert_eq!(
            validate_trailers(&[], "x-amz-checksum-sha256", payload),
            Err(TrailerChecksumError::Missing)
        );
        // Non-checksum trailers are ignored.
        assert_eq!(validate_trailers(&[("x-custom", "v")], "", payload), Ok(()));
    }
}
