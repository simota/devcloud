//! Minimal client for the AWS JSON protocol (`X-Amz-Target` POSTs), used by
//! event source polling to read the local SQS and DynamoDB Streams services
//! over loopback. Requests are SigV4-signed when credentials are configured,
//! so a `strict` SQS or DynamoDB accepts them; `relaxed` services ignore the
//! signature. Request and response bodies are never logged.

use std::time::Duration;

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

/// Bound on one request, connect to last byte.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A local AWS-protocol service the poller reads from.
#[derive(Clone, Default)]
pub struct AwsEndpoint {
    /// `http://host:port`.
    pub url: String,
    pub region: String,
    /// Empty: requests are sent unsigned.
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl std::fmt::Debug for AwsEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsEndpoint")
            .field("url", &self.url)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// `host:port` of an `http://` URL (port 80 when absent).
fn authority(url: &str) -> Result<(String, String, u16), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("unsupported endpoint {url}: only http:// is supported"))?;
    let host_port = rest.split('/').next().unwrap_or("");
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| format!("invalid port in endpoint {url}"))?,
        ),
        None => (host_port.to_string(), 80),
    };
    if host.is_empty() {
        return Err(format!("invalid endpoint {url}"));
    }
    Ok((host_port.to_string(), host, port))
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// `YYYYMMDDTHHMMSSZ` for the current time.
fn amz_date_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let rfc = crate::time_fmt::rfc3339_from_unix(secs, 0);
    // 2026-01-02T03:04:05Z -> 20260102T030405Z
    rfc.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}

/// The SigV4 `Authorization` header for a POST to `/` with the given
/// headers (`(lowercase name, value)`, all of which are signed).
fn authorization(
    ep: &AwsEndpoint,
    service: &str,
    amz_date: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> String {
    let mut sorted: Vec<(&str, &str)> = headers.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let canonical_headers: String = sorted
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect();
    let signed_headers = sorted.iter().map(|(k, _)| *k).collect::<Vec<_>>().join(";");
    let canonical_request = format!(
        "POST\n/\n\n{canonical_headers}\n{signed_headers}\n{}",
        sha256_hex(body)
    );
    let date = &amz_date[..8];
    let scope = format!("{date}/{}/{service}/aws4_request", ep.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let k_date = hmac(
        format!("AWS4{}", ep.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac(&k_date, ep.region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        ep.access_key_id
    )
}

/// POSTs `body` as operation `target` (e.g. `AmazonSQS.ReceiveMessage`) and
/// returns the JSON response. An error response becomes
/// `Err("<type>: <message>")`.
pub async fn call(
    ep: &AwsEndpoint,
    service: &str,
    target: &str,
    body: &Value,
) -> Result<Value, String> {
    let (host_header, host, port) = authority(&ep.url)?;
    let payload = serde_json::to_vec(body).unwrap_or_default();
    let amz_date = amz_date_now();
    let content_type = "application/x-amz-json-1.0";
    // Sent (and signed) so verifiers hash the actual payload.
    let payload_hash = sha256_hex(&payload);
    let mut head = format!(
        "POST / HTTP/1.1\r\nHost: {host_header}\r\nContent-Type: {content_type}\r\nX-Amz-Target: {target}\r\nX-Amz-Date: {amz_date}\r\nX-Amz-Content-Sha256: {payload_hash}\r\nContent-Length: {}\r\nConnection: close\r\n",
        payload.len()
    );
    if !ep.access_key_id.is_empty() {
        let auth = authorization(
            ep,
            service,
            &amz_date,
            &[
                ("content-type", content_type),
                ("host", &host_header),
                ("x-amz-content-sha256", &payload_hash),
                ("x-amz-date", &amz_date),
                ("x-amz-target", target),
            ],
            &payload,
        );
        head.push_str(&format!("Authorization: {auth}\r\n"));
    }
    head.push_str("\r\n");
    let exchange = async {
        let mut s = TcpStream::connect((host.as_str(), port)).await?;
        s.write_all(head.as_bytes()).await?;
        s.write_all(&payload).await?;
        crate::container::read_response(&mut s).await
    };
    let resp = tokio::time::timeout(REQUEST_TIMEOUT, exchange)
        .await
        .map_err(|_| format!("{target}: request to {} timed out", ep.url))?
        .map_err(|e| format!("{target}: request to {} failed: {e}", ep.url))?;
    let json: Value = if resp.body.is_empty() {
        Value::Object(Default::default())
    } else {
        serde_json::from_slice(&resp.body)
            .map_err(|_| format!("{target}: {} answered with a non-JSON body", ep.url))?
    };
    if resp.status != 200 {
        let kind = json
            .get("__type")
            .or_else(|| json.get("code"))
            .and_then(Value::as_str)
            .unwrap_or("Error");
        let kind = kind.rsplit('#').next().unwrap_or(kind);
        let message = json
            .get("message")
            .or_else(|| json.get("Message"))
            .and_then(Value::as_str)
            .unwrap_or("");
        return Err(format!("{target}: {kind}: {message}"));
    }
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_endpoint_authority() {
        assert_eq!(
            authority("http://127.0.0.1:19324").unwrap(),
            (
                "127.0.0.1:19324".to_string(),
                "127.0.0.1".to_string(),
                19324
            )
        );
        assert_eq!(authority("http://sqs/").unwrap().2, 80);
        assert!(authority("https://x:1").is_err());
        assert!(authority("http://:1").is_err());
    }

    #[test]
    fn amz_date_is_compact_utc() {
        let d = amz_date_now();
        assert_eq!(d.len(), 16, "{d}");
        assert_eq!(&d[8..9], "T");
        assert!(d.ends_with('Z'));
    }

    /// The AWS SigV4 test suite's `post-vanilla` case.
    #[test]
    fn matches_the_sigv4_test_suite_post_vanilla() {
        let ep = AwsEndpoint {
            url: "http://127.0.0.1:1".into(),
            region: "us-east-1".into(),
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
        };
        let headers = [
            ("host", "example.amazonaws.com"),
            ("x-amz-date", "20150830T123600Z"),
        ];
        assert_eq!(
            authorization(&ep, "service", "20150830T123600Z", &headers, b""),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"
        );
    }
}
