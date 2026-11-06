use devcloud_s3::http::{route, route_with_auth, AuthConfig, Request, Response};
use devcloud_s3::percent::aws_percent_encode;
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::time_fmt::rfc3339_seconds_from_unix;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

const SECRET: &str = "test-secret";
const STREAMING: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
const UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

fn store() -> FileBucketStore {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "devcloud-s3-sigv4-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = FileBucketStore::new(dir);
    assert_eq!(
        route(&store, &Request::new("PUT", "/data", Vec::new())).status,
        200
    );
    store
}

fn auth() -> AuthConfig {
    AuthConfig {
        auth_mode: "strict".to_string(),
        access_key_id: "test-key".to_string(),
        secret_access_key: SECRET.to_string(),
        region: "us-east-1".to_string(),
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn amz_date(unix: i64) -> String {
    rfc3339_seconds_from_unix(unix).replace(['-', ':'], "")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn signing_key(date_stamp: &str) -> Vec<u8> {
    let date = hmac_sha256(format!("AWS4{SECRET}").as_bytes(), date_stamp.as_bytes());
    let region = hmac_sha256(&date, b"us-east-1");
    let service = hmac_sha256(&region, b"s3");
    hmac_sha256(&service, b"aws4_request")
}

struct Signed {
    request: Request,
    signature: String,
    amz_date: String,
}

/// Signs independently of the server, the way an SDK does: `target` is the
/// already encoded wire path, used verbatim as the canonical URI.
fn sign_at(
    method: &str,
    target: &str,
    body: Vec<u8>,
    extra_headers: &[(&str, &str)],
    payload_hash: &str,
    unix: i64,
) -> Signed {
    let date = amz_date(unix);
    let date_stamp = &date[..8];
    let mut request = Request::new(method, target, body);
    request
        .headers
        .insert("host".to_string(), "s3.test:9000".to_string());
    request
        .headers
        .insert("x-amz-date".to_string(), date.clone());
    request
        .headers
        .insert("x-amz-content-sha256".to_string(), payload_hash.to_string());
    for (name, value) in extra_headers {
        request.headers.insert(name.to_string(), value.to_string());
    }
    let names: Vec<&String> = request.headers.keys().collect();
    let signed_headers = names
        .iter()
        .map(|n| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = request
        .headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let canonical_request = [
        method,
        target.split('?').next().unwrap(),
        "",
        &canonical_headers,
        &signed_headers,
        payload_hash,
    ]
    .join("\n");
    let scope = format!("{date_stamp}/us-east-1/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = hex::encode(hmac_sha256(
        &signing_key(date_stamp),
        string_to_sign.as_bytes(),
    ));
    request.headers.insert(
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential=test-key/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        ),
    );
    Signed {
        request,
        signature,
        amz_date: date,
    }
}

fn sign(method: &str, target: &str, body: &[u8], extra: &[(&str, &str)]) -> Request {
    sign_at(
        method,
        target,
        body.to_vec(),
        extra,
        &sha256_hex(body),
        now(),
    )
    .request
}

fn code(response: &Response) -> String {
    let body = String::from_utf8_lossy(&response.body);
    let start = body.find("<Code>").map(|i| i + 6).unwrap_or(0);
    let end = body.find("</Code>").unwrap_or(0);
    body[start..end].to_string()
}

/// Builds a signed `aws-chunked` body chaining from `seed`, returning the framing
/// and the terminal chunk's signature.
fn signed_chunks(chunks: &[&[u8]], seed: &str, date: &str, trailer: Option<&str>) -> Vec<u8> {
    let date_stamp = &date[..8];
    let scope = format!("{date_stamp}/us-east-1/s3/aws4_request");
    let key = signing_key(date_stamp);
    let empty = sha256_hex(b"");
    let mut previous = seed.to_string();
    let mut out = Vec::new();
    for chunk in chunks.iter().copied().chain([&b""[..]]) {
        let to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{date}\n{scope}\n{previous}\n{empty}\n{}",
            sha256_hex(chunk)
        );
        previous = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
        out.extend(format!("{:x};chunk-signature={previous}\r\n", chunk.len()).into_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    if let Some(trailer) = trailer {
        out.truncate(out.len() - 2);
        let to_sign = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{date}\n{scope}\n{previous}\n{}",
            sha256_hex(format!("{trailer}\n").as_bytes())
        );
        let signature = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
        out.extend(
            format!("{trailer}\r\nx-amz-trailer-signature:{signature}\r\n\r\n").into_bytes(),
        );
    }
    out
}

fn stored(store: &FileBucketStore, key: &str) -> Response {
    route(
        store,
        &Request::new("GET", &format!("/data/{key}"), Vec::new()),
    )
}

type Tamper = fn(&mut Vec<u8>);

const STREAM_HEADERS: [(&str, &str); 2] = [
    ("content-encoding", "aws-chunked"),
    ("x-amz-decoded-content-length", "11"),
];

fn streaming_put(target: &str, chunks: &[&[u8]], tamper: impl Fn(&mut Vec<u8>)) -> Request {
    let mut signed = sign_at("PUT", target, Vec::new(), &STREAM_HEADERS, STREAMING, now());
    let mut body = signed_chunks(chunks, &signed.signature, &signed.amz_date, None);
    tamper(&mut body);
    signed.request.body = body;
    signed.request
}

#[test]
fn encoded_keys_verify_without_double_encoding() {
    let store = store();
    let auth = auth();
    let target = "/data/dir/a%20b%2Bc%E3%81%82.txt";
    let put = sign("PUT", target, b"body", &[]);
    assert_eq!(route_with_auth(&store, &put, &auth).status, 200);
    let got = route_with_auth(&store, &sign("GET", target, b"", &[]), &auth);
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"body");
    assert_eq!(stored(&store, "dir/a%20b%2Bc%E3%81%82.txt").body, b"body");
}

#[test]
fn wrong_secret_and_tampering_are_rejected() {
    let store = store();
    let mut other = auth();
    other.secret_access_key = "another".to_string();
    let put = sign("PUT", "/data/k", b"body", &[]);
    let response = route_with_auth(&store, &put, &other);
    assert_eq!(response.status, 403);
    assert_eq!(code(&response), "SignatureDoesNotMatch");

    let mut retargeted = sign("PUT", "/data/k", b"body", &[]);
    retargeted.path = "/data/other".to_string();
    let response = route_with_auth(&store, &retargeted, &auth());
    assert_eq!(code(&response), "SignatureDoesNotMatch");
}

#[test]
fn stale_or_future_requests_are_rejected() {
    let store = store();
    let auth = auth();
    for offset in [-901, 901] {
        let signed = sign_at(
            "GET",
            "/",
            Vec::new(),
            &[],
            &sha256_hex(b""),
            now() + offset,
        );
        let response = route_with_auth(&store, &signed.request, &auth);
        assert_eq!(response.status, 403);
        assert_eq!(code(&response), "RequestTimeTooSkewed");
    }
    let tolerated = sign_at("GET", "/", Vec::new(), &[], &sha256_hex(b""), now() - 600);
    assert_eq!(
        route_with_auth(&store, &tolerated.request, &auth).status,
        200
    );
}

#[test]
fn malformed_dates_are_rejected_without_panicking() {
    let store = store();
    let mut req = sign("GET", "/", b"", &[]);
    req.headers
        .insert("x-amz-date".to_string(), "あああああZ".to_string());
    assert_eq!("あああああZ".len(), 16);
    assert_eq!(route_with_auth(&store, &req, &auth()).status, 403);

    let mut req = sign("GET", "/", b"", &[]);
    let authorization = req.headers["authorization"].clone();
    let credential_date = amz_date(now())[..8].to_string();
    req.headers.insert(
        "authorization".to_string(),
        authorization.replace(&credential_date, "20000101"),
    );
    assert_eq!(
        code(&route_with_auth(&store, &req, &auth())),
        "AuthorizationHeaderMalformed"
    );
}

#[test]
fn host_and_behavioural_amz_headers_must_be_signed() {
    let store = store();
    let auth = auth();
    let mut unsigned_header = sign("PUT", "/data/copy", b"", &[]);
    unsigned_header
        .headers
        .insert("x-amz-copy-source".to_string(), "/data/secret".to_string());
    let response = route_with_auth(&store, &unsigned_header, &auth);
    assert_eq!(response.status, 403);
    assert_eq!(code(&response), "AccessDenied");

    let mut user_agent = sign("GET", "/", b"", &[]);
    user_agent
        .headers
        .insert("x-amz-user-agent".to_string(), "aws-sdk-js/3".to_string());
    assert_eq!(route_with_auth(&store, &user_agent, &auth).status, 200);

    let mut no_host = sign("GET", "/", b"", &[]);
    let value = no_host.headers["authorization"].replace("SignedHeaders=host;", "SignedHeaders=");
    no_host.headers.insert("authorization".to_string(), value);
    assert_eq!(
        code(&route_with_auth(&store, &no_host, &auth)),
        "AccessDenied"
    );
}

#[test]
fn content_sha256_header_may_be_left_out_of_signed_headers() {
    let store = store();
    let auth = auth();
    let date = amz_date(now());
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let hash = sha256_hex(b"");
    let sign_with = |payload_hash: &str| {
        let canonical = format!(
            "GET\n/\n\nhost:s3.test:9000\nx-amz-date:{date}\n\nhost;x-amz-date\n{payload_hash}"
        );
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
            sha256_hex(canonical.as_bytes())
        );
        hex::encode(hmac_sha256(&signing_key(&date[..8]), to_sign.as_bytes()))
    };
    let request = |signature: &str, sent_hash: &str| {
        let mut req = Request::new("GET", "/", Vec::new());
        req.headers
            .insert("host".to_string(), "s3.test:9000".to_string());
        req.headers.insert("x-amz-date".to_string(), date.clone());
        req.headers
            .insert("x-amz-content-sha256".to_string(), sent_hash.to_string());
        req.headers.insert(
            "authorization".to_string(),
            format!("AWS4-HMAC-SHA256 Credential=test-key/{scope}, SignedHeaders=host;x-amz-date, Signature={signature}"),
        );
        req
    };
    let valid = request(&sign_with(&hash), &hash);
    assert_eq!(route_with_auth(&store, &valid, &auth).status, 200);
    // Still bound by the signature, through the payload hash.
    let swapped = request(&sign_with(&hash), "UNSIGNED-PAYLOAD");
    assert_eq!(
        code(&route_with_auth(&store, &swapped, &auth)),
        "SignatureDoesNotMatch"
    );
}

#[test]
fn signed_streaming_upload_is_verified_and_decoded() {
    let store = store();
    let auth = auth();
    let put = streaming_put("/data/stream.txt", &[b"hello ", b"world"], |_| {});
    assert_eq!(route_with_auth(&store, &put, &auth).status, 200);
    let got = stored(&store, "stream.txt");
    assert_eq!(got.body, b"hello world");
    assert_eq!(got.headers.get("Content-Encoding"), None);
}

#[test]
fn streaming_tampering_is_rejected() {
    let store = store();
    let auth = auth();
    let cases: [(&str, Tamper); 2] = [
        ("flipped byte", |body| {
            let at = body.windows(5).position(|w| w == b"hello").unwrap();
            body[at] = b'j';
        }),
        ("corrupted signature", |body| {
            let at = body
                .windows(16)
                .position(|w| w == b"chunk-signature=")
                .unwrap()
                + 16;
            body[at] = if body[at] == b'0' { b'1' } else { b'0' };
        }),
    ];
    for (name, tamper) in cases {
        let put = streaming_put("/data/stream.txt", &[b"hello ", b"world"], tamper);
        let response = route_with_auth(&store, &put, &auth);
        assert_eq!(response.status, 403, "{name}");
        assert_eq!(code(&response), "SignatureDoesNotMatch", "{name}");
    }

    // Reordered chunks break the signature chain even though each one is intact.
    let mut signed = sign_at(
        "PUT",
        "/data/stream.txt",
        Vec::new(),
        &STREAM_HEADERS,
        STREAMING,
        now(),
    );
    let framed = signed_chunks(
        &[b"hello ", b"world"],
        &signed.signature,
        &signed.amz_date,
        None,
    );
    let text = String::from_utf8(framed).unwrap();
    let parts: Vec<&str> = text.split_inclusive("\r\n").collect();
    signed.request.body = [parts[2], parts[3], parts[0], parts[1], parts[4], parts[5]]
        .concat()
        .into_bytes();
    assert_eq!(
        code(&route_with_auth(&store, &signed.request, &auth)),
        "SignatureDoesNotMatch"
    );

    // A signed payload may not silently drop its chunk signatures.
    let mut signed = sign_at(
        "PUT",
        "/data/stream.txt",
        Vec::new(),
        &STREAM_HEADERS,
        STREAMING,
        now(),
    );
    signed.request.body = b"b\r\nhello world\r\n0\r\n\r\n".to_vec();
    assert_eq!(
        code(&route_with_auth(&store, &signed.request, &auth)),
        "SignatureDoesNotMatch"
    );
    assert_eq!(stored(&store, "stream.txt").status, 404);
}

#[test]
fn signed_trailer_is_verified() {
    let store = store();
    let auth = auth();
    let upload = |trailer_on_wire: &str| {
        let mut signed = sign_at(
            "PUT",
            "/data/trailer.txt",
            Vec::new(),
            &[
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "11"),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
            ],
            STREAMING_TRAILER,
            now(),
        );
        let framed = signed_chunks(
            &[b"hello world"],
            &signed.signature,
            &signed.amz_date,
            Some("x-amz-checksum-crc32:DQpIDQ=="),
        );
        let text = String::from_utf8(framed).unwrap();
        signed.request.body = text
            .replace("x-amz-checksum-crc32:DQpIDQ==", trailer_on_wire)
            .into_bytes();
        route_with_auth(&store, &signed.request, &auth)
    };
    assert_eq!(
        code(&upload("x-amz-checksum-crc32:AAAAAA==")),
        "SignatureDoesNotMatch"
    );
    assert_eq!(upload("x-amz-checksum-crc32:DQpIDQ==").status, 200);
    assert_eq!(stored(&store, "trailer.txt").body, b"hello world");
}

#[test]
fn unsigned_trailer_upload_is_decoded_in_strict_mode() {
    let store = store();
    let mut signed = sign_at(
        "PUT",
        "/data/unsigned.txt",
        Vec::new(),
        &STREAM_HEADERS,
        UNSIGNED_TRAILER,
        now(),
    );
    signed.request.body =
        b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DQpIDQ==\r\n\r\n".to_vec();
    assert_eq!(
        route_with_auth(&store, &signed.request, &auth()).status,
        200
    );
    assert_eq!(stored(&store, "unsigned.txt").body, b"hello world");
}

#[test]
fn unsupported_streaming_algorithm_is_not_implemented_in_strict_mode() {
    let store = store();
    let signed = sign_at(
        "PUT",
        "/data/k",
        b"0\r\n\r\n".to_vec(),
        &[],
        "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD",
        now(),
    );
    assert_eq!(
        route_with_auth(&store, &signed.request, &auth()).status,
        501
    );
}

#[test]
fn relaxed_mode_decodes_framing_instead_of_storing_it() {
    let store = store();
    let mut put = Request::new(
        "PUT",
        "/data/relaxed.txt",
        b"5;chunk-signature=ignored\r\nhello\r\n0;chunk-signature=ignored\r\n\r\n".to_vec(),
    );
    put.headers
        .insert("x-amz-content-sha256".to_string(), STREAMING.to_string());
    put.headers.insert(
        "content-encoding".to_string(),
        "gzip,aws-chunked".to_string(),
    );
    assert_eq!(route(&store, &put).status, 200);
    let got = stored(&store, "relaxed.txt");
    assert_eq!(got.body, b"hello");
    assert_eq!(got.headers.get("Content-Encoding").unwrap(), "gzip");
}

#[test]
fn malformed_or_short_streaming_bodies_are_rejected() {
    let store = store();
    for (body, decoded_length) in [
        (&b"5\r\nhe"[..], "2"),
        (b"plain", "5"),
        (b"5\r\nhello\r\n0\r\n\r\n", "6"),
    ] {
        let mut put = Request::new("PUT", "/data/bad.txt", body.to_vec());
        put.headers.insert(
            "x-amz-content-sha256".to_string(),
            UNSIGNED_TRAILER.to_string(),
        );
        put.headers.insert(
            "x-amz-decoded-content-length".to_string(),
            decoded_length.to_string(),
        );
        let response = route(&store, &put);
        assert_eq!(response.status, 400);
        assert_eq!(code(&response), "IncompleteBody");
    }
    assert_eq!(stored(&store, "bad.txt").status, 404);
}

fn presign(path: &str, signed_at: i64, expires: &str) -> Request {
    let date = amz_date(signed_at);
    let date_stamp = &date[..8];
    let scope = format!("{date_stamp}/us-east-1/s3/aws4_request");
    let credential = format!("test-key/{scope}");
    let query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={date}&X-Amz-Expires={expires}&X-Amz-SignedHeaders=host",
        aws_percent_encode(&credential, "")
    );
    let canonical_request =
        format!("GET\n{path}\n{query}\nhost:s3.test:9000\n\nhost\nUNSIGNED-PAYLOAD");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = hex::encode(hmac_sha256(
        &signing_key(date_stamp),
        string_to_sign.as_bytes(),
    ));
    let mut request = Request::new(
        "GET",
        &format!("{path}?{query}&X-Amz-Signature={signature}"),
        Vec::new(),
    );
    request
        .headers
        .insert("host".to_string(), "s3.test:9000".to_string());
    request
}

#[test]
fn presigned_urls_enforce_signature_and_time_window() {
    let store = store();
    let auth = auth();
    let put = sign("PUT", "/data/a%20b.txt", b"body", &[]);
    assert_eq!(route_with_auth(&store, &put, &auth).status, 200);

    let valid = route_with_auth(&store, &presign("/data/a%20b.txt", now(), "300"), &auth);
    assert_eq!(valid.status, 200);
    assert_eq!(valid.body, b"body");

    let expired = presign("/data/a%20b.txt", now() - 400, "300");
    assert_eq!(route_with_auth(&store, &expired, &auth).status, 403);
    let future = presign("/data/a%20b.txt", now() + 1000, "300");
    assert_eq!(route_with_auth(&store, &future, &auth).status, 403);

    let mut tampered = presign("/data/a%20b.txt", now(), "300");
    tampered
        .query
        .insert("X-Amz-Expires".to_string(), "600".to_string());
    assert_eq!(
        code(&route_with_auth(&store, &tampered, &auth)),
        "SignatureDoesNotMatch"
    );

    let mut retargeted = presign("/data/a%20b.txt", now(), "300");
    retargeted.path = "/data/other.txt".to_string();
    assert_eq!(
        code(&route_with_auth(&store, &retargeted, &auth)),
        "SignatureDoesNotMatch"
    );

    let mut extra = presign("/data/a%20b.txt", now(), "300");
    extra
        .headers
        .insert("x-amz-copy-source".to_string(), "/data/secret".to_string());
    assert_eq!(
        code(&route_with_auth(&store, &extra, &auth)),
        "AccessDenied"
    );
}
