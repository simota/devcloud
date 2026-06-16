//! Dependency-free base64 matching legacy `encoding/base64`.
//!
//! The S3 store uses two alphabets:
//!  - **StdEncoding** (padded) — for the CRC32C checksum string and the
//!    `Content-MD5` request header.
//!  - **RawURLEncoding** (URL alphabet, no padding) — for encoding object keys
//!    and config IDs into on-disk path segments.

const STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn encode(input: &[u8], alphabet: &[u8; 64], pad: bool) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(alphabet[((n >> 18) & 0x3f) as usize] as char);
        out.push(alphabet[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(alphabet[((n >> 6) & 0x3f) as usize] as char);
        } else if pad {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(alphabet[(n & 0x3f) as usize] as char);
        } else if pad {
            out.push('=');
        }
    }
    out
}

fn decode(input: &str, alphabet: &[u8; 64]) -> Option<Vec<u8>> {
    let mut lookup = [255u8; 256];
    for (i, &c) in alphabet.iter().enumerate() {
        lookup[c as usize] = i as u8;
    }
    let mut vals: Vec<u8> = Vec::with_capacity(input.len());
    for &b in input.as_bytes() {
        if b == b'=' {
            break;
        }
        let v = lookup[b as usize];
        if v == 255 {
            return None;
        }
        vals.push(v);
    }
    let mut out = Vec::with_capacity(vals.len() * 3 / 4);
    for chunk in vals.chunks(4) {
        if chunk.len() == 1 {
            return None; // a single leftover sextet is invalid
        }
        let n = (chunk[0] as u32) << 18
            | (chunk[1] as u32) << 12
            | (*chunk.get(2).unwrap_or(&0) as u32) << 6
            | (*chunk.get(3).unwrap_or(&0) as u32);
        out.push((n >> 16) as u8);
        if chunk.len() >= 3 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() >= 4 {
            out.push(n as u8);
        }
    }
    Some(out)
}

/// `base64.StdEncoding.EncodeToString` — padded, standard alphabet.
pub fn std_encode(input: &[u8]) -> String {
    encode(input, STD, true)
}

/// `base64.StdEncoding.DecodeString` — padded, standard alphabet.
pub fn std_decode(input: &str) -> Option<Vec<u8>> {
    decode(input, STD)
}

/// `base64.RawURLEncoding.EncodeToString` — URL alphabet, no padding.
pub fn raw_url_encode(input: &[u8]) -> String {
    encode(input, URL, false)
}

/// `base64.RawURLEncoding.DecodeString` — URL alphabet, no padding.
pub fn raw_url_decode(input: &str) -> Option<Vec<u8>> {
    decode(input, URL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn std_roundtrip_and_padding() {
        assert_eq!(std_encode(b""), "");
        assert_eq!(std_encode(b"f"), "Zg==");
        assert_eq!(std_encode(b"fo"), "Zm8=");
        assert_eq!(std_encode(b"foo"), "Zm9v");
        assert_eq!(std_encode(b"foob"), "Zm9vYg==");
        assert_eq!(std_decode("Zm9vYg==").unwrap(), b"foob");
    }

    #[test]
    fn raw_url_no_padding() {
        // RawURLEncoding drops padding and uses '-'/'_'.
        let key = b"path/to/object.txt";
        let enc = raw_url_encode(key);
        assert!(!enc.contains('='));
        assert_eq!(raw_url_decode(&enc).unwrap(), key);
    }

    #[test]
    fn url_alphabet_distinct_from_std() {
        // 0xfb 0xff encodes to "-_8" under the URL alphabet ("+/" under std).
        assert_eq!(raw_url_encode(&[0xfb, 0xff]), "-_8");
        assert_eq!(std_encode(&[0xfb, 0xff]), "+/8=");
    }
}
