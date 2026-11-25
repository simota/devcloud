//! SigV4a (`AWS4-ECDSA-P256-SHA256`) primitives shared by the devcloud
//! AWS-compatible services: the access-key-derived ECDSA P-256 key, signature
//! verification (and RFC 6979 deterministic signing for tests and local
//! clients), and the chunk-signature chain of
//! `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]` bodies.
//!
//! Each service still builds its own canonical request; this crate only covers
//! what is specific to the asymmetric variant.

pub mod p256;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use p256::{AffinePoint, U256};

pub const ALGORITHM: &str = "AWS4-ECDSA-P256-SHA256";
/// String-to-sign algorithm line of each `aws-chunked` data chunk.
pub const CHUNK_ALGORITHM: &str = "AWS4-ECDSA-P256-SHA256-PAYLOAD";
/// String-to-sign algorithm line of the signed trailer.
pub const TRAILER_ALGORITHM: &str = "AWS4-ECDSA-P256-SHA256-TRAILER";
pub const STREAMING_PAYLOAD: &str = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD";
pub const STREAMING_PAYLOAD_TRAILER: &str = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER";
/// The AWS CRT signer right-pads chunk signatures with `*` to this length so
/// the framed body length does not depend on the DER signature length.
pub const PADDED_SIGNATURE_LEN: usize = 144;

/// Whether `payload_hash` (`x-amz-content-sha256`) selects SigV4a chunk signing.
pub fn is_streaming(payload_hash: &str) -> bool {
    matches!(payload_hash, STREAMING_PAYLOAD | STREAMING_PAYLOAD_TRAILER)
}

/// `<date>/<service>/aws4_request`: SigV4a scopes omit the region, which moves
/// to `X-Amz-Region-Set`.
pub fn credential_scope(date_stamp: &str, service: &str) -> String {
    format!("{date_stamp}/{service}/aws4_request")
}

pub fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// Whether a `X-Amz-Region-Set` value (comma-separated, `*` wildcards allowed)
/// covers `region`.
pub fn region_set_matches(region_set: &str, region: &str) -> bool {
    region_set
        .split(',')
        .map(str::trim)
        .any(|r| !r.is_empty() && (r == "*" || r == region))
}

/// The fields of an `AWS4-ECDSA-P256-SHA256 Credential=..., SignedHeaders=...,
/// Signature=...` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization<'a> {
    pub access_key: &'a str,
    pub date_stamp: &'a str,
    pub service: &'a str,
    pub signed_headers: &'a str,
    pub signature: &'a str,
}

pub fn is_authorization(header: &str) -> bool {
    header
        .strip_prefix(ALGORITHM)
        .is_some_and(|rest| rest.starts_with(' '))
}

/// Parses a SigV4a `Authorization` header. `None` when it is not SigV4a, the
/// credential is not `<key>/<date>/<service>/aws4_request`, or a field is empty.
pub fn parse_authorization(header: &str) -> Option<Authorization<'_>> {
    let rest = header.trim().strip_prefix(ALGORITHM)?.strip_prefix(' ')?;
    let mut credential = "";
    let mut signed_headers = "";
    let mut signature = "";
    for part in rest.split(',') {
        match part.trim().split_once('=') {
            Some(("Credential", v)) => credential = v,
            Some(("SignedHeaders", v)) => signed_headers = v,
            Some(("Signature", v)) => signature = v,
            _ => {}
        }
    }
    let mut scope = credential.split('/');
    let (Some(access_key), Some(date_stamp), Some(service), Some("aws4_request"), None) = (
        scope.next(),
        scope.next(),
        scope.next(),
        scope.next(),
        scope.next(),
    ) else {
        return None;
    };
    if [access_key, date_stamp, service, signed_headers, signature]
        .iter()
        .any(|v| v.is_empty())
    {
        return None;
    }
    Some(Authorization {
        access_key,
        date_stamp,
        service,
        signed_headers,
        signature,
    })
}

/// Strips the `*` padding the CRT signer appends to chunk signatures.
pub fn trim_padding(signature: &str) -> &str {
    signature.trim_end_matches('*')
}

/// Whether `signature` is hex-encoded ASN.1 DER `SEQUENCE { r, s }` (padding
/// allowed). Used by shape-only checks that do not hold the secret.
pub fn is_signature_shape(signature: &str) -> bool {
    hex::decode(trim_padding(signature))
        .ok()
        .and_then(|der| parse_der_signature(&der))
        .is_some()
}

/// The ECDSA P-256 key pair SigV4a derives from an access key and secret.
#[derive(Debug, Clone)]
pub struct SigningKey {
    d: U256,
    public: AffinePoint,
}

impl SigningKey {
    /// Derives the key with the NIST SP 800-108 counter-mode HMAC-SHA256 KDF
    /// AWS specifies for SigV4a (`AWS4A` + secret, label `AWS4-ECDSA-P256-SHA256`,
    /// context = access key || counter).
    pub fn derive(access_key: &str, secret: &str) -> Option<Self> {
        let key = format!("AWS4A{secret}");
        for counter in 1u8..=254u8 {
            let mut input = Vec::with_capacity(4 + ALGORITHM.len() + 1 + access_key.len() + 5);
            input.extend_from_slice(&1u32.to_be_bytes());
            input.extend_from_slice(ALGORITHM.as_bytes());
            input.push(0x00);
            input.extend_from_slice(access_key.as_bytes());
            input.push(counter);
            input.extend_from_slice(&256u32.to_be_bytes());
            let c = U256::from_be_bytes(&hmac_sha256(key.as_bytes(), &[&input]));
            if c <= p256::N_MINUS_2 {
                return Self::from_scalar(c.add_one());
            }
        }
        None
    }

    /// Builds a key from a private scalar in `[1, n-1]`.
    pub fn from_scalar(d: U256) -> Option<Self> {
        if d.is_zero() || d >= p256::N {
            return None;
        }
        Some(SigningKey {
            d,
            public: p256::scalar_mul_g(&d),
        })
    }

    /// Affine public key coordinates `(x, y)`, big-endian.
    pub fn public_key(&self) -> ([u8; 32], [u8; 32]) {
        (self.public.x.to_be_bytes(), self.public.y.to_be_bytes())
    }

    /// Verifies a hex DER ECDSA-SHA256 signature over `message`.
    pub fn verify(&self, message: &[u8], signature_hex: &str) -> bool {
        let Ok(der) = hex::decode(trim_padding(signature_hex)) else {
            return false;
        };
        let Some((r, s)) = parse_der_signature(&der) else {
            return false;
        };
        let z = U256::from_be_bytes(&Sha256::digest(message).into());
        p256::ecdsa_verify(&self.public, &z, &r, &s)
    }

    /// Signs `message` with ECDSA-SHA256 using the RFC 6979 deterministic nonce
    /// and returns the hex DER signature.
    pub fn sign(&self, message: &[u8]) -> String {
        let digest: [u8; 32] = Sha256::digest(message).into();
        let k = rfc6979_nonce(&self.d, &digest);
        let z = U256::from_be_bytes(&digest);
        hex::encode(p256::ecdsa_sign_der_with_k(&self.d, &z, &k))
    }
}

/// RFC 6979 section 3.2 nonce for P-256 with SHA-256 (qlen = hlen = 256).
fn rfc6979_nonce(d: &U256, digest: &[u8; 32]) -> U256 {
    let x = d.to_be_bytes();
    let h = p256::reduce_mod(U256::from_be_bytes(digest), p256::N).to_be_bytes();
    let mut v = [0x01u8; 32];
    let mut k = hmac_sha256(&[0u8; 32], &[&v, &[0x00], &x, &h]);
    v = hmac_sha256(&k, &[&v]);
    k = hmac_sha256(&k, &[&v, &[0x01], &x, &h]);
    v = hmac_sha256(&k, &[&v]);
    loop {
        v = hmac_sha256(&k, &[&v]);
        let candidate = U256::from_be_bytes(&v);
        if !candidate.is_zero() && candidate < p256::N {
            return candidate;
        }
        k = hmac_sha256(&k, &[&v, &[0x00]]);
        v = hmac_sha256(&k, &[&v]);
    }
}

/// Parses an ASN.1 DER `SEQUENCE { INTEGER r, INTEGER s }` signature.
pub fn parse_der_signature(der: &[u8]) -> Option<(U256, U256)> {
    if der.len() < 8 || der[0] != 0x30 {
        return None;
    }
    let seq_len = der[1] as usize;
    if seq_len + 2 != der.len() {
        return None;
    }
    let (r, rest) = parse_der_uint(&der[2..])?;
    let (s, rest) = parse_der_uint(rest)?;
    if !rest.is_empty() {
        return None;
    }
    Some((r, s))
}

fn parse_der_uint(input: &[u8]) -> Option<(U256, &[u8])> {
    if input.len() < 3 || input[0] != 0x02 {
        return None;
    }
    let len = input[1] as usize;
    if len == 0 || input.len() < 2 + len {
        return None;
    }
    let raw = &input[2..2 + len];
    // Reject negative integers or non-minimal DER padding.
    if raw[0] & 0x80 != 0 {
        return None;
    }
    let trimmed = if raw.len() > 1 && raw[0] == 0x00 {
        if raw[1] & 0x80 == 0 {
            return None;
        }
        &raw[1..]
    } else {
        raw
    };
    if trimmed.len() > 32 {
        return None;
    }
    let mut buf = [0u8; 32];
    buf[32 - trimmed.len()..].copy_from_slice(trimmed);
    Some((U256::from_be_bytes(&buf), &input[2 + len..]))
}

/// Canonical form of signed trailers: `lowercase-name:value\n` per trailer.
pub fn canonical_trailers(trailers: &[(&str, &str)]) -> String {
    trailers
        .iter()
        .map(|(name, value)| format!("{}:{}\n", name.to_ascii_lowercase(), value.trim()))
        .collect()
}

/// The chunk-signature chain of a SigV4a `aws-chunked` body. Each chunk (and
/// the trailer) is signed over the previous signature, starting from the seed
/// signature of the request itself.
#[derive(Debug, Clone)]
pub struct ChunkChain {
    key: SigningKey,
    amz_date: String,
    scope: String,
    previous: String,
}

impl ChunkChain {
    pub fn new(key: SigningKey, amz_date: &str, scope: &str, seed_signature: &str) -> Self {
        ChunkChain {
            key,
            amz_date: amz_date.to_string(),
            scope: scope.to_string(),
            previous: trim_padding(seed_signature).to_ascii_lowercase(),
        }
    }

    fn chunk_string_to_sign(&self, data: &[u8]) -> String {
        format!(
            "{CHUNK_ALGORITHM}\n{}\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            self.previous,
            sha256_hex(b""),
            sha256_hex(data)
        )
    }

    fn trailer_string_to_sign(&self, canonical_trailers: &[u8]) -> String {
        format!(
            "{TRAILER_ALGORITHM}\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            self.previous,
            sha256_hex(canonical_trailers)
        )
    }

    fn advance(&mut self, ok: bool, signature: &str) -> bool {
        if ok {
            self.previous = trim_padding(signature).to_ascii_lowercase();
        }
        ok
    }

    pub fn verify_chunk(&mut self, signature: &str, data: &[u8]) -> bool {
        let ok = self
            .key
            .verify(self.chunk_string_to_sign(data).as_bytes(), signature);
        self.advance(ok, signature)
    }

    pub fn verify_trailer(&mut self, signature: &str, canonical_trailers: &[u8]) -> bool {
        let ok = self.key.verify(
            self.trailer_string_to_sign(canonical_trailers).as_bytes(),
            signature,
        );
        self.advance(ok, signature)
    }

    pub fn sign_chunk(&mut self, data: &[u8]) -> String {
        let signature = self.key.sign(self.chunk_string_to_sign(data).as_bytes());
        self.previous = signature.clone();
        signature
    }

    pub fn sign_trailer(&mut self, canonical_trailers: &[u8]) -> String {
        let signature = self
            .key
            .sign(self.trailer_string_to_sign(canonical_trailers).as_bytes());
        self.previous = signature.clone();
        signature
    }
}

/// The trailer signature (if sent) and the trailers it covers.
pub type SignedTrailer<'a> = (Option<&'a str>, &'a [(&'a str, &'a str)]);

/// Verifies every chunk `(signature, data)` in order (the terminal empty chunk
/// included) and, when `trailer` is given, the trailer signature over the
/// canonical trailers. A missing signature fails.
pub fn verify_chunk_chain<'a>(
    mut chain: ChunkChain,
    chunks: impl IntoIterator<Item = (Option<&'a str>, &'a [u8])>,
    trailer: Option<SignedTrailer<'_>>,
) -> bool {
    for (signature, data) in chunks {
        if !signature.is_some_and(|signature| chain.verify_chunk(signature, data)) {
            return false;
        }
    }
    match trailer {
        None => true,
        Some((signature, trailers)) => signature.is_some_and(|signature| {
            chain.verify_trailer(signature, canonical_trailers(trailers).as_bytes())
        }),
    }
}

/// Frames `chunks` as a signed `aws-chunked` body (the empty terminal chunk is
/// appended), optionally followed by signed `trailers`. With `pad`, signatures
/// are `*`-padded to [`PADDED_SIGNATURE_LEN`] like the AWS CRT signer does.
pub fn encode_signed_chunks(
    chain: &mut ChunkChain,
    chunks: &[&[u8]],
    trailers: Option<&[(&str, &str)]>,
    pad: bool,
) -> Vec<u8> {
    let padded = |signature: String| {
        if pad {
            format!("{signature:*<PADDED_SIGNATURE_LEN$}")
        } else {
            signature
        }
    };
    let mut out = Vec::new();
    for chunk in chunks.iter().copied().chain([&b""[..]]) {
        let signature = padded(chain.sign_chunk(chunk));
        out.extend(format!("{:x};chunk-signature={signature}\r\n", chunk.len()).into_bytes());
        out.extend_from_slice(chunk);
        if !chunk.is_empty() {
            out.extend_from_slice(b"\r\n");
        }
    }
    if let Some(trailers) = trailers {
        let canonical = canonical_trailers(trailers);
        for (name, value) in trailers {
            out.extend(format!("{name}:{value}\r\n").into_bytes());
        }
        let signature = padded(chain.sign_trailer(canonical.as_bytes()));
        out.extend(format!("x-amz-trailer-signature:{signature}\r\n").into_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out
}

pub fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests;
