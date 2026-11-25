//! RSA key handling for `GOOG4-RSA-SHA256` and V2 signed URL verification.
//!
//! Keys are registered per service-account email (the signed URL's access ID).
//! Accepted inputs are PEM `PUBLIC KEY` (SubjectPublicKeyInfo), `RSA PUBLIC
//! KEY` (PKCS#1), `PRIVATE KEY` (PKCS#8), `RSA PRIVATE KEY` (PKCS#1), and
//! Google service-account JSON key files. Only the public half is retained;
//! private key material is dropped as soon as the public key is derived.

use devcloud_s3::base64;
use ring::rand::SystemRandom;
use ring::signature::{
    RsaKeyPair, UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256, RSA_PKCS1_SHA256,
};
use serde::Deserialize;

/// DER-encoded PKCS#1 `RSAPublicKey`.
#[derive(Clone, PartialEq, Eq)]
pub struct RsaPublicKey(Vec<u8>);

impl std::fmt::Debug for RsaPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RsaPublicKey({} bytes)", self.0.len())
    }
}

impl RsaPublicKey {
    /// Parses a PEM public or private RSA key and returns its public half.
    pub fn from_pem(pem: &str) -> Result<Self, String> {
        let (label, der) = decode_pem(pem)?;
        match label.as_str() {
            "PUBLIC KEY" => spki_to_pkcs1(&der).map(Self),
            "RSA PUBLIC KEY" => Ok(Self(der)),
            "PRIVATE KEY" => RsaKeyPair::from_pkcs8(&der)
                .map(|kp| Self(kp.public().as_ref().to_vec()))
                .map_err(|e| format!("invalid PKCS#8 RSA private key: {e}")),
            "RSA PRIVATE KEY" => RsaKeyPair::from_der(&der)
                .map(|kp| Self(kp.public().as_ref().to_vec()))
                .map_err(|e| format!("invalid PKCS#1 RSA private key: {e}")),
            other => Err(format!("unsupported PEM block {other:?}")),
        }
    }

    /// Verifies an RSASSA-PKCS1-v1_5 SHA-256 signature over `message`.
    pub fn verify_sha256(&self, message: &[u8], signature: &[u8]) -> bool {
        UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, &self.0)
            .verify(message, signature)
            .is_ok()
    }
}

#[derive(Deserialize)]
struct ServiceAccountKeyFile {
    #[serde(default)]
    client_email: String,
    #[serde(default)]
    private_key: String,
}

/// Parses a Google service-account JSON key file into `(client_email, public key)`.
pub fn parse_service_account_json(json: &str) -> Result<(String, RsaPublicKey), String> {
    let parsed: ServiceAccountKeyFile =
        serde_json::from_str(json).map_err(|_| "invalid service account JSON".to_string())?;
    let email = parsed.client_email.trim().to_string();
    if email.is_empty() {
        return Err("service account JSON has no client_email".to_string());
    }
    let key = RsaPublicKey::from_pem(&parsed.private_key)?;
    Ok((email, key))
}

/// Signs `message` with RSASSA-PKCS1-v1_5 SHA-256 using a PEM PKCS#8 or
/// PKCS#1 private key. Intended for generating local test signed URLs.
pub fn sign_sha256_pem(private_key_pem: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    let (label, der) = decode_pem(private_key_pem)?;
    let key_pair = match label.as_str() {
        "PRIVATE KEY" => RsaKeyPair::from_pkcs8(&der),
        "RSA PRIVATE KEY" => RsaKeyPair::from_der(&der),
        other => return Err(format!("unsupported private key PEM block {other:?}")),
    }
    .map_err(|e| format!("invalid RSA private key: {e}"))?;
    let mut signature = vec![0u8; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            message,
            &mut signature,
        )
        .map_err(|_| "RSA signing failed".to_string())?;
    Ok(signature)
}

fn decode_pem(pem: &str) -> Result<(String, Vec<u8>), String> {
    let begin = pem
        .find("-----BEGIN ")
        .ok_or_else(|| "missing PEM BEGIN line".to_string())?;
    let after_begin = &pem[begin + "-----BEGIN ".len()..];
    let label_end = after_begin
        .find("-----")
        .ok_or_else(|| "malformed PEM BEGIN line".to_string())?;
    let label = after_begin[..label_end].to_string();
    let body_start = &after_begin[label_end + "-----".len()..];
    let end_marker = format!("-----END {label}-----");
    let body_end = body_start
        .find(&end_marker)
        .ok_or_else(|| "missing PEM END line".to_string())?;
    let b64: String = body_start[..body_end]
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    let der = base64::std_decode(&b64).ok_or_else(|| "invalid PEM base64".to_string())?;
    Ok((label, der))
}

/// Reads one DER TLV, returning `(tag, contents, rest)`.
fn der_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first_len, rest) = rest.split_first()?;
    let (len, rest) = if first_len & 0x80 == 0 {
        (first_len as usize, rest)
    } else {
        let n = (first_len & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, &b| (acc << 8) | b as usize);
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

const OID_RSA_ENCRYPTION: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01];

/// Extracts the PKCS#1 `RSAPublicKey` from a DER `SubjectPublicKeyInfo`.
fn spki_to_pkcs1(der: &[u8]) -> Result<Vec<u8>, String> {
    let err = || "invalid SubjectPublicKeyInfo".to_string();
    let (tag, spki, _) = der_tlv(der).ok_or_else(err)?;
    if tag != 0x30 {
        return Err(err());
    }
    let (tag, alg, rest) = der_tlv(spki).ok_or_else(err)?;
    if tag != 0x30 {
        return Err(err());
    }
    let (tag, oid, _) = der_tlv(alg).ok_or_else(err)?;
    if tag != 0x06 || oid != OID_RSA_ENCRYPTION {
        return Err("public key is not an RSA key".to_string());
    }
    let (tag, bits, _) = der_tlv(rest).ok_or_else(err)?;
    match bits.split_first() {
        Some((0, key)) if tag == 0x03 => Ok(key.to_vec()),
        _ => Err(err()),
    }
}
