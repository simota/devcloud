//! Server-Side Encryption with Customer-Provided Keys (SSE-C).
//!
//! Raw customer keys are validated in memory, used for authenticated encryption
//! of on-disk object bytes, and never persisted or logged. Only the algorithm
//! (`AES256`) and base64 MD5 digest of the key are stored in metadata.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use md5::{Digest as Md5Digest, Md5};
use sha2::Sha256;

use crate::base64;
use crate::http::{constant_time_eq, Request};

const FRAME_MAGIC: &[u8; 4] = b"SSEC";
const NONCE_LEN: usize = 16;
const MAC_LEN: usize = 32;
const FRAME_OVERHEAD: usize = 4 + 4 + NONCE_LEN + MAC_LEN;

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct SseCustomerKey {
    pub algorithm: String,
    pub key: [u8; 32],
    pub key_md5: String,
}

impl std::fmt::Debug for SseCustomerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseCustomerKey")
            .field("algorithm", &self.algorithm)
            .field("key_md5", &self.key_md5)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseCustomerHeaderError {
    Invalid,
}

pub fn parse_customer_key(req: &Request) -> Result<Option<SseCustomerKey>, SseCustomerHeaderError> {
    parse_customer_key_with_prefix(req, "x-amz-server-side-encryption-customer")
}

pub fn parse_copy_source_customer_key(
    req: &Request,
) -> Result<Option<SseCustomerKey>, SseCustomerHeaderError> {
    parse_customer_key_with_prefix(req, "x-amz-copy-source-server-side-encryption-customer")
}

fn parse_customer_key_with_prefix(
    req: &Request,
    prefix: &str,
) -> Result<Option<SseCustomerKey>, SseCustomerHeaderError> {
    let algorithm = req.header(&format!("{prefix}-algorithm")).trim();
    let key_b64 = req.header(&format!("{prefix}-key")).trim();
    let key_md5 = req.header(&format!("{prefix}-key-md5")).trim();

    if algorithm.is_empty() && key_b64.is_empty() && key_md5.is_empty() {
        return Ok(None);
    }
    if algorithm != "AES256" || key_b64.is_empty() || key_md5.is_empty() {
        return Err(SseCustomerHeaderError::Invalid);
    }
    let raw_key = base64::std_decode(key_b64).ok_or(SseCustomerHeaderError::Invalid)?;
    if raw_key.len() != 32 {
        return Err(SseCustomerHeaderError::Invalid);
    }
    let expected_md5_bytes = base64::std_decode(key_md5).ok_or(SseCustomerHeaderError::Invalid)?;
    if expected_md5_bytes.len() != 16 {
        return Err(SseCustomerHeaderError::Invalid);
    }
    let actual_md5 = Md5::digest(&raw_key);
    if !constant_time_eq(actual_md5.as_slice(), expected_md5_bytes.as_slice()) {
        return Err(SseCustomerHeaderError::Invalid);
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&raw_key);
    Ok(Some(SseCustomerKey {
        algorithm: "AES256".to_string(),
        key,
        key_md5: base64::std_encode(actual_md5.as_slice()),
    }))
}

pub fn generate_nonce(plaintext_len: usize) -> [u8; NONCE_LEN] {
    let seq = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hasher = Sha256::new();
    hasher.update(seq.to_be_bytes());
    hasher.update(nanos.to_be_bytes());
    hasher.update((plaintext_len as u64).to_be_bytes());
    let digest = hasher.finalize();
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&digest[..NONCE_LEN]);
    nonce
}

fn derive_subkeys(key: &[u8; 32]) -> ([u8; 32], [u8; 32]) {
    let enc = hmac_sha256(key, b"devcloud-s3-sse-c-enc");
    let mac = hmac_sha256(key, b"devcloud-s3-sse-c-mac");
    (enc, mac)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn xor_keystream(enc_key: &[u8; 32], nonce: &[u8; NONCE_LEN], data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    for (block_idx, chunk) in data.chunks(32).enumerate() {
        let mut msg = [0u8; NONCE_LEN + 8];
        msg[..NONCE_LEN].copy_from_slice(nonce);
        msg[NONCE_LEN..].copy_from_slice(&(block_idx as u64).to_be_bytes());
        let stream = hmac_sha256(enc_key, &msg);
        for (b, s) in chunk.iter().zip(stream.iter()) {
            out.push(b ^ s);
        }
    }
    out
}

/// Encrypts `plaintext` into a single authenticated SSE-C frame.
pub fn encrypt_frame(key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let nonce = generate_nonce(plaintext.len());
    let (enc_key, mac_key) = derive_subkeys(key);
    let ciphertext = xor_keystream(&enc_key, &nonce, plaintext);

    let mut frame = Vec::with_capacity(FRAME_OVERHEAD + ciphertext.len());
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
    frame.extend_from_slice(&nonce);
    frame.extend_from_slice(&ciphertext);

    let tag = hmac_sha256(&mac_key, &frame);
    frame.extend_from_slice(&tag);
    frame
}

/// Decrypts one or more concatenated SSE-C frames, verifying every frame's MAC.
pub fn decrypt_frames(key: &[u8; 32], framed: &[u8]) -> Option<Vec<u8>> {
    if framed.is_empty() {
        return None;
    }
    let (enc_key, mac_key) = derive_subkeys(key);
    let mut out = Vec::new();
    let mut rest = framed;
    while !rest.is_empty() {
        if rest.len() < FRAME_OVERHEAD || &rest[..4] != FRAME_MAGIC {
            return None;
        }
        let ct_len = u32::from_be_bytes(rest[4..8].try_into().ok()?) as usize;
        let frame_len = FRAME_OVERHEAD.checked_add(ct_len)?;
        if rest.len() < frame_len {
            return None;
        }
        let body_end = 8 + NONCE_LEN + ct_len;
        let expected_tag = hmac_sha256(&mac_key, &rest[..body_end]);
        let actual_tag = &rest[body_end..frame_len];
        if !constant_time_eq(&expected_tag, actual_tag) {
            return None;
        }
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&rest[8..8 + NONCE_LEN]);
        let ciphertext = &rest[8 + NONCE_LEN..body_end];
        out.extend_from_slice(&xor_keystream(&enc_key, &nonce, ciphertext));
        rest = &rest[frame_len..];
    }
    Some(out)
}

/// Verifies every frame's MAC without allocating the decrypted plaintext.
pub fn verify_frames(key: &[u8; 32], framed: &[u8]) -> bool {
    if framed.is_empty() {
        return false;
    }
    let (_, mac_key) = derive_subkeys(key);
    let mut rest = framed;
    while !rest.is_empty() {
        if rest.len() < FRAME_OVERHEAD || &rest[..4] != FRAME_MAGIC {
            return false;
        }
        let ct_len = u32::from_be_bytes(rest[4..8].try_into().unwrap()) as usize;
        let Some(frame_len) = FRAME_OVERHEAD.checked_add(ct_len) else {
            return false;
        };
        if rest.len() < frame_len {
            return false;
        }
        let body_end = 8 + NONCE_LEN + ct_len;
        let expected_tag = hmac_sha256(&mac_key, &rest[..body_end]);
        if !constant_time_eq(&expected_tag, &rest[body_end..frame_len]) {
            return false;
        }
        rest = &rest[frame_len..];
    }
    true
}

/// Returns the total plaintext byte length encoded across all frames, if valid.
pub fn framed_plaintext_len(framed: &[u8]) -> Option<usize> {
    if framed.is_empty() {
        return None;
    }
    let mut total = 0usize;
    let mut rest = framed;
    while !rest.is_empty() {
        if rest.len() < FRAME_OVERHEAD || &rest[..4] != FRAME_MAGIC {
            return None;
        }
        let ct_len = u32::from_be_bytes(rest[4..8].try_into().ok()?) as usize;
        let frame_len = FRAME_OVERHEAD.checked_add(ct_len)?;
        if rest.len() < frame_len {
            return None;
        }
        total = total.checked_add(ct_len)?;
        rest = &rest[frame_len..];
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_single_and_concatenated_frames() {
        let key = [0x42u8; 32];
        let wrong_key = [0x43u8; 32];
        let f1 = encrypt_frame(&key, b"hello ");
        let f2 = encrypt_frame(&key, b"world");
        assert!(!f1.windows(5).any(|w| w == b"hello"));

        let mut combined = f1;
        combined.extend_from_slice(&f2);
        assert_eq!(framed_plaintext_len(&combined), Some(11));
        assert!(verify_frames(&key, &combined));
        assert!(!verify_frames(&wrong_key, &combined));
        assert_eq!(decrypt_frames(&key, &combined).unwrap(), b"hello world");
        assert!(decrypt_frames(&wrong_key, &combined).is_none());

        let mut tampered = combined.clone();
        tampered[26] ^= 0x01;
        assert!(decrypt_frames(&key, &tampered).is_none());
    }
}
