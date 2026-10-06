//! Minimal AWS SigV4 signer for dashboard → AWS-protocol service forwarding,
//! used when a service runs with `strict` / `signed-relaxed` auth. Produces the
//! same canonical form the services verify (path bytes percent-encoded with
//! `/` kept, query pairs encoded then sorted, body hash signed).

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub struct Credentials<'a> {
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

/// Headers (`x-amz-date`, `x-amz-content-sha256`, `authorization`) signing
/// `method path_and_query` sent to `host` with `body`, dated `amz_date`
/// (`YYYYMMDDTHHMMSSZ`).
pub fn sign(
    method: &str,
    path_and_query: &str,
    host: &str,
    body: &[u8],
    creds: &Credentials,
    amz_date: &str,
) -> Vec<(String, String)> {
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    let payload_hash = hex::encode(Sha256::digest(body));
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical = format!(
        "{method}\n{}\n{}\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n\n{signed_headers}\n{payload_hash}",
        encode(path, "/"),
        canonical_query(query),
    );
    let date = &amz_date[..8];
    let scope = format!("{date}/{}/{}/aws4_request", creds.region, creds.service);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let key = [creds.region, creds.service, "aws4_request"].iter().fold(
        mac(format!("AWS4{}", creds.secret_access_key).as_bytes(), date),
        |k, part| mac(&k, part),
    );
    let signature = hex::encode(mac(&key, &to_sign));
    vec![
        ("x-amz-date".to_string(), amz_date.to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash),
        (
            "authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
                creds.access_key_id
            ),
        ),
    ]
}

/// Current UTC time as `YYYYMMDDTHHMMSSZ`.
pub fn amz_date_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Howard Hinnant's days → civil date.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn mac(key: &[u8], data: &str) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    m.update(data.as_bytes());
    m.finalize().into_bytes().to_vec()
}

fn encode(value: &str, keep: &str) -> String {
    value
        .bytes()
        .map(|b| {
            let c = b as char;
            if c.is_ascii_alphanumeric() || "-_.~".contains(c) || keep.contains(c) {
                c.to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
        match (
            bytes[i],
            bytes.get(i + 1).copied().and_then(hex),
            bytes.get(i + 2).copied().and_then(hex),
        ) {
            (b'%', Some(hi), Some(lo)) => {
                out.push(hi << 4 | lo);
                i += 3;
            }
            (b'+', _, _) => {
                out.push(b' ');
                i += 1;
            }
            (b, _, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn canonical_query(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (encode(&decode(k), ""), encode(&decode(v), ""))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amz_date_has_basic_format() {
        let d = amz_date_now();
        assert_eq!(d.len(), 16);
        assert!(d.starts_with("20") && d.ends_with('Z') && d.as_bytes()[8] == b'T');
    }

    #[test]
    fn canonical_forms() {
        assert_eq!(
            encode("/2015-03-31/functions/a%3Ab", "/"),
            "/2015-03-31/functions/a%253Ab"
        );
        assert_eq!(canonical_query("b=2&a=%C3%A9"), "a=%C3%A9&b=2");
    }
}
