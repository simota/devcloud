//! Function URLs: the `/2021-10-31/functions/<name>/url` control plane and
//! the data plane (Host / `/_url/` routing, payload format 2.0, response
//! mapping, CORS, AWS_IAM). Runs through `http::process`, no sockets.
//! Invocation tests skip themselves when python3 is absent.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine;
use devcloud_lambda::http::{process, Request};
use devcloud_lambda::zip::build_stored;
use devcloud_lambda::{Config, Server};
use serde_json::{json, Value};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-url-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config_for(dir: &std::path::Path) -> Config {
    Config {
        addr: "127.0.0.1:19010".into(),
        endpoint: "http://127.0.0.1:19010".into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
        ..Config::default()
    }
}

fn has_python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn headers_named(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .collect()
    }
}

async fn call(
    server: &Arc<Server>,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Resp {
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let req = Request {
        method: method.into(),
        raw_path,
        query,
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        body: body.to_vec(),
    };
    let r = process(server, &req).await;
    Resp {
        status: r.status,
        headers: r.headers,
        body: r.body,
    }
}

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Echoes the event, or shapes the response from `event["queryStringParameters"]["mode"]`.
const HANDLER: &str = r#"
import base64, json
def handler(event, context):
    mode = (event.get("queryStringParameters") or {}).get("mode")
    if mode == "plain":
        return {"hello": "world"}
    if mode == "string":
        return "just text"
    if mode == "binary":
        return {"statusCode": 201, "headers": {"content-type": "image/png", "x-custom": "1"},
                "body": base64.b64encode(b"\x89PNG").decode(), "isBase64Encoded": True,
                "cookies": ["a=1; Path=/", "b=2"]}
    if mode == "chunked":
        return {"statusCode": 200, "headers": {"transfer-encoding": "chunked",
                "Content-Length": "999", "connection": "keep-alive"}, "body": "hello"}
    if mode == "fail":
        raise RuntimeError("boom")
    return {"statusCode": 200, "headers": {"content-type": "application/json",
            "access-control-allow-origin": "https://from-function.example"},
            "body": json.dumps(event)}
"#;

async fn create_function(server: &Arc<Server>, name: &str) {
    let zip = build_stored(&[("app.py", HANDLER.as_bytes())]);
    let body = json!({
        "FunctionName": name,
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::000000000000:role/lambda",
        "Handler": "app.handler",
        "Timeout": 10,
        "Code": { "ZipFile": b64(&zip) },
    });
    let r = call(
        server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
}

async fn create_url(server: &Arc<Server>, name: &str, body: Value) -> Resp {
    call(
        server,
        "POST",
        &format!("/2021-10-31/functions/{name}/url"),
        &[],
        body.to_string().as_bytes(),
    )
    .await
}

/// `(url id, Host header value)` from a `FunctionUrl`.
fn url_parts(function_url: &str) -> (String, String) {
    let host = function_url
        .strip_prefix("http://")
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let id = host.split('.').next().unwrap().to_string();
    (id, host)
}

#[tokio::test]
async fn url_config_crud_and_persistence() {
    let dir = temp_dir("crud");
    let server = Arc::new(Server::new(config_for(&dir)));
    create_function(&server, "web").await;

    let missing = call(&server, "GET", "/2021-10-31/functions/web/url", &[], b"").await;
    assert_eq!(missing.status, 404);
    assert_eq!(
        missing.header("X-Amzn-ErrorType"),
        Some("ResourceNotFoundException")
    );

    let bad = create_url(&server, "web", json!({ "AuthType": "OPEN" })).await;
    assert_eq!(bad.status, 400);
    let no_auth = create_url(&server, "web", json!({})).await;
    assert_eq!(no_auth.status, 400);
    let bad_cors = create_url(
        &server,
        "web",
        json!({ "AuthType": "NONE", "Cors": { "MaxAge": 90000 } }),
    )
    .await;
    assert_eq!(bad_cors.status, 400);
    let other_qualifier = call(
        &server,
        "POST",
        "/2021-10-31/functions/web/url?Qualifier=prod",
        &[],
        json!({ "AuthType": "NONE" }).to_string().as_bytes(),
    )
    .await;
    assert_eq!(other_qualifier.status, 404);

    let created = create_url(
        &server,
        "web",
        json!({ "AuthType": "NONE", "Cors": { "AllowOrigins": ["*"] } }),
    )
    .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let c = created.json();
    let url = c["FunctionUrl"].as_str().unwrap().to_string();
    let (id, _) = url_parts(&url);
    assert_eq!(id.len(), 32);
    assert!(id
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()));
    assert_eq!(
        url,
        format!("http://{id}.lambda-url.us-east-1.localhost:19010/")
    );
    assert_eq!(
        c["FunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:web"
    );
    assert_eq!(c["AuthType"], "NONE");
    assert_eq!(c["InvokeMode"], "BUFFERED");
    assert_eq!(c["Cors"], json!({ "AllowOrigins": ["*"] }));
    assert!(c["CreationTime"].is_string());

    let dup = create_url(&server, "web", json!({ "AuthType": "NONE" })).await;
    assert_eq!(dup.status, 409);
    assert_eq!(
        dup.header("X-Amzn-ErrorType"),
        Some("ResourceConflictException")
    );

    let updated = call(
        &server,
        "PUT",
        "/2021-10-31/functions/web/url",
        &[],
        json!({ "AuthType": "AWS_IAM", "InvokeMode": "RESPONSE_STREAM" })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(updated.status, 200);
    let u = updated.json();
    assert_eq!(u["AuthType"], "AWS_IAM");
    assert_eq!(u["InvokeMode"], "RESPONSE_STREAM");
    assert_eq!(u["Cors"], json!({ "AllowOrigins": ["*"] }), "unchanged");
    assert_eq!(u["FunctionUrl"], url.as_str(), "the URL stays stable");
    assert!(u["LastModifiedTime"].is_string());

    // Survives a restart.
    let reloaded = Arc::new(Server::new(config_for(&dir)));
    let got = call(&reloaded, "GET", "/2021-10-31/functions/web/url", &[], b"").await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["FunctionUrl"], url.as_str());
    assert_eq!(got.json()["AuthType"], "AWS_IAM");
    let list = call(&reloaded, "GET", "/2021-10-31/functions/web/urls", &[], b"").await;
    assert_eq!(
        list.json()["FunctionUrlConfigs"].as_array().unwrap().len(),
        1
    );

    let deleted = call(
        &reloaded,
        "DELETE",
        "/2021-10-31/functions/web/url",
        &[],
        b"",
    )
    .await;
    assert_eq!(deleted.status, 204);
    let again = call(
        &reloaded,
        "DELETE",
        "/2021-10-31/functions/web/url",
        &[],
        b"",
    )
    .await;
    assert_eq!(again.status, 404);
    let list = call(&reloaded, "GET", "/2021-10-31/functions/web/urls", &[], b"").await;
    assert_eq!(list.json()["FunctionUrlConfigs"], json!([]));

    // DeleteFunction takes the URL with it.
    create_url(&reloaded, "web", json!({ "AuthType": "NONE" })).await;
    let id = url_parts(
        call(&reloaded, "GET", "/2021-10-31/functions/web/url", &[], b"")
            .await
            .json()["FunctionUrl"]
            .as_str()
            .unwrap(),
    )
    .0;
    call(&reloaded, "DELETE", "/2015-03-31/functions/web", &[], b"").await;
    let gone = call(&reloaded, "GET", &format!("/_url/{id}/"), &[], b"").await;
    assert_eq!(gone.status, 403);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn url_requests_become_payload_v2_events() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("event");
    let server = Arc::new(Server::new(config_for(&dir)));
    create_function(&server, "web").await;
    let url = create_url(&server, "web", json!({ "AuthType": "NONE" }))
        .await
        .json()["FunctionUrl"]
        .as_str()
        .unwrap()
        .to_string();
    let (id, host) = url_parts(&url);

    // Host form, from a cross-site browser page: function URLs are public.
    let r = call(
        &server,
        "POST",
        "/items/42?a=1&a=2&b=x%20y",
        &[
            ("Host", host.as_str()),
            ("Content-Type", "application/json"),
            ("Cookie", "s=1; t=2"),
            ("User-Agent", "test-agent"),
            ("Origin", "https://evil.example"),
            ("Sec-Fetch-Site", "cross-site"),
        ],
        br#"{"k":"v"}"#,
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.header("content-type"), Some("application/json"));
    let ev = r.json();
    assert_eq!(ev["version"], "2.0");
    assert_eq!(ev["routeKey"], "$default");
    assert_eq!(ev["rawPath"], "/items/42");
    assert_eq!(ev["rawQueryString"], "a=1&a=2&b=x%20y");
    assert_eq!(
        ev["queryStringParameters"],
        json!({ "a": "1,2", "b": "x y" })
    );
    assert_eq!(ev["cookies"], json!(["s=1", "t=2"]));
    assert!(ev["headers"].get("cookie").is_none());
    assert_eq!(ev["headers"]["user-agent"], "test-agent");
    assert_eq!(ev["body"], r#"{"k":"v"}"#);
    assert_eq!(ev["isBase64Encoded"], false);
    let ctx = &ev["requestContext"];
    assert_eq!(ctx["apiId"], id.as_str());
    assert_eq!(ctx["domainPrefix"], id.as_str());
    assert_eq!(
        ctx["domainName"],
        format!("{id}.lambda-url.us-east-1.localhost")
    );
    assert_eq!(ctx["accountId"], "anonymous");
    assert_eq!(ctx["http"]["method"], "POST");
    assert_eq!(ctx["http"]["path"], "/items/42");
    assert_eq!(ctx["http"]["userAgent"], "test-agent");
    assert_eq!(ctx["stage"], "$default");
    assert!(ctx["timeEpoch"].as_u64().unwrap() > 0);
    assert!(ctx["time"].as_str().unwrap().ends_with(" +0000"));
    assert!(ctx.get("authorizer").is_none());

    // Path form, binary body.
    let r = call(
        &server,
        "PUT",
        &format!("/_url/{id}/upload"),
        &[("Content-Type", "application/octet-stream")],
        &[0xff, 0x00, 0x01],
    )
    .await;
    let ev = r.json();
    assert_eq!(ev["rawPath"], "/upload");
    assert_eq!(ev["isBase64Encoded"], true);
    assert_eq!(ev["body"], b64(&[0xff, 0x00, 0x01]));
    assert!(ev.get("queryStringParameters").is_none());

    // The invocation is recorded like any other.
    let records = call(&server, "GET", "/_introspect/invocations", &[], b"").await;
    assert_eq!(records.json()["invocations"].as_array().unwrap().len(), 2);

    // Unknown URL ids are refused like AWS, never routed to the API.
    let unknown = call(
        &server,
        "GET",
        "/_url/00000000000000000000000000000000/",
        &[],
        b"",
    )
    .await;
    assert_eq!(unknown.status, 403);
    assert_eq!(unknown.json(), json!({ "Message": "Forbidden" }));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn handler_results_map_to_http_responses() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("mapping");
    let server = Arc::new(Server::new(config_for(&dir)));
    create_function(&server, "web").await;
    let (id, _) = url_parts(
        create_url(&server, "web", json!({ "AuthType": "NONE" }))
            .await
            .json()["FunctionUrl"]
            .as_str()
            .unwrap(),
    );
    let get = |mode: &'static str| {
        let server = Arc::clone(&server);
        let target = format!("/_url/{id}/?mode={mode}");
        async move { call(&server, "GET", &target, &[], b"").await }
    };

    let plain = get("plain").await;
    assert_eq!(plain.status, 200);
    assert_eq!(plain.header("content-type"), Some("application/json"));
    assert_eq!(plain.json(), json!({ "hello": "world" }));

    let string = get("string").await;
    assert_eq!(string.status, 200);
    assert_eq!(string.body, b"\"just text\"");

    let binary = get("binary").await;
    assert_eq!(binary.status, 201);
    assert_eq!(binary.body, b"\x89PNG");
    assert_eq!(binary.header("content-type"), Some("image/png"));
    assert_eq!(binary.header("x-custom"), Some("1"));
    assert_eq!(
        binary.headers_named("set-cookie"),
        vec!["a=1; Path=/", "b=2"]
    );

    // devcloud frames the body itself: framing headers from the function
    // would contradict it.
    let chunked = get("chunked").await;
    assert_eq!(chunked.body, b"hello");
    for h in ["transfer-encoding", "content-length", "connection"] {
        assert!(chunked.headers_named(h).is_empty(), "{h} must be dropped");
    }

    let fail = get("fail").await;
    assert_eq!(fail.status, 502);
    assert_eq!(fail.json(), json!({ "Message": "Internal Server Error" }));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn cors_configuration_answers_preflight_and_decorates_responses() {
    let dir = temp_dir("cors");
    let server = Arc::new(Server::new(config_for(&dir)));
    create_function(&server, "web").await;
    let (id, _) = url_parts(
        create_url(
            &server,
            "web",
            json!({ "AuthType": "AWS_IAM", "Cors": {
                "AllowOrigins": ["https://app.example"],
                "AllowMethods": ["GET", "POST"],
                "AllowHeaders": ["content-type"],
                "ExposeHeaders": ["x-custom"],
                "AllowCredentials": true,
                "MaxAge": 300,
            }}),
        )
        .await
        .json()["FunctionUrl"]
            .as_str()
            .unwrap(),
    );

    // Preflight is answered by devcloud itself, before AWS_IAM auth.
    let pre = call(
        &server,
        "OPTIONS",
        &format!("/_url/{id}/"),
        &[
            ("Origin", "https://app.example"),
            ("Access-Control-Request-Method", "POST"),
        ],
        b"",
    )
    .await;
    assert_eq!(pre.status, 200);
    assert_eq!(
        pre.header("access-control-allow-origin"),
        Some("https://app.example")
    );
    assert_eq!(pre.header("access-control-allow-methods"), Some("GET,POST"));
    assert_eq!(
        pre.header("access-control-allow-headers"),
        Some("content-type")
    );
    assert_eq!(pre.header("access-control-allow-credentials"), Some("true"));
    assert_eq!(pre.header("access-control-max-age"), Some("300"));

    let other = call(
        &server,
        "OPTIONS",
        &format!("/_url/{id}/"),
        &[
            ("Origin", "https://other.example"),
            ("Access-Control-Request-Method", "POST"),
        ],
        b"",
    )
    .await;
    assert_eq!(other.header("access-control-allow-origin"), None);

    if !has_python() {
        eprintln!("skipping invoke part: python3 not available");
        let _ = std::fs::remove_dir_all(dir);
        return;
    }
    // Switch to NONE to see the decorated response without signing.
    call(
        &server,
        "PUT",
        "/2021-10-31/functions/web/url",
        &[],
        json!({ "AuthType": "NONE" }).to_string().as_bytes(),
    )
    .await;
    let r = call(
        &server,
        "GET",
        &format!("/_url/{id}/"),
        &[("Origin", "https://app.example")],
        b"",
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        r.headers_named("access-control-allow-origin"),
        vec!["https://app.example"],
        "the Cors configuration replaces the function's own header"
    );
    assert_eq!(r.header("access-control-expose-headers"), Some("x-custom"));
    let _ = std::fs::remove_dir_all(dir);
}

/// Minimal SigV4 signer (service `lambda`, credentials `dev` / `dev`).
fn sign(method: &str, path: &str, body: &[u8], host: &str) -> Vec<(String, String)> {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type H = Hmac<Sha256>;
    fn mac(key: &[u8], data: &str) -> Vec<u8> {
        let mut m = H::new_from_slice(key).unwrap();
        m.update(data.as_bytes());
        m.finalize().into_bytes().to_vec()
    }
    let amz_date = "20261006T000000Z";
    let date = &amz_date[..8];
    let canonical = format!(
        "{method}\n{path}\n\nhost:{host}\nx-amz-date:{amz_date}\n\nhost;x-amz-date\n{}",
        hex::encode(Sha256::digest(body))
    );
    let scope = format!("{date}/us-east-1/lambda/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let k = mac(b"AWS4dev", date);
    let k = mac(&k, "us-east-1");
    let k = mac(&k, "lambda");
    let k = mac(&k, "aws4_request");
    let sig = hex::encode(mac(&k, &to_sign));
    vec![
        ("host".into(), host.into()),
        ("x-amz-date".into(), amz_date.into()),
        (
            "authorization".into(),
            format!("AWS4-HMAC-SHA256 Credential=dev/{scope}, SignedHeaders=host;x-amz-date, Signature={sig}"),
        ),
    ]
}

#[tokio::test]
async fn aws_iam_urls_require_a_signature() {
    let dir = temp_dir("iam");
    // Set up through a relaxed server, then serve the same state strictly.
    let setup = Arc::new(Server::new(config_for(&dir)));
    create_function(&setup, "web").await;
    let url = create_url(&setup, "web", json!({ "AuthType": "AWS_IAM" }))
        .await
        .json()["FunctionUrl"]
        .as_str()
        .unwrap()
        .to_string();
    let (id, host) = url_parts(&url);

    // Unsigned requests are refused even in relaxed mode.
    let unsigned = call(&setup, "GET", "/", &[("Host", host.as_str())], b"").await;
    assert_eq!(unsigned.status, 403);
    assert_eq!(unsigned.json(), json!({ "Message": "Forbidden" }));

    let strict = Arc::new(Server::new(Config {
        auth_mode: "strict".into(),
        access_key_id: "dev".into(),
        secret_access_key: "dev".into(),
        ..config_for(&dir)
    }));
    let unsigned = call(&strict, "GET", &format!("/_url/{id}/"), &[], b"").await;
    assert_eq!(unsigned.status, 403);

    let path = format!("/_url/{id}/hello");
    let headers = sign("GET", &path, b"", "127.0.0.1:19010");
    let mut forged = headers.clone();
    forged[2].1 = forged[2].1.replace("Signature=", "Signature=0");
    fn refs(h: &[(String, String)]) -> Vec<(&str, &str)> {
        h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
    }
    let bad = call(&strict, "GET", &path, &refs(&forged), b"").await;
    assert_eq!(bad.status, 403);

    if !has_python() {
        eprintln!("skipping invoke part: python3 not available");
        let _ = std::fs::remove_dir_all(dir);
        return;
    }
    let ok = call(&strict, "GET", &path, &refs(&headers), b"").await;
    assert_eq!(ok.status, 200, "{}", String::from_utf8_lossy(&ok.body));
    let ctx = &ok.json()["requestContext"];
    assert_eq!(ctx["accountId"], "000000000000");
    assert_eq!(ctx["authorizer"]["iam"]["accessKey"], "dev");
    assert_eq!(
        ctx["authorizer"]["iam"]["userArn"],
        "arn:aws:iam::000000000000:user/devcloud"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn cors_values_cannot_inject_header_lines() {
    let dir = temp_dir("corsinject");
    let server = Arc::new(Server::new(config_for(&dir)));
    let zip = build_stored(&[("app.py", HANDLER.as_bytes())]);
    let body = json!({
        "FunctionName": "web", "Runtime": "python3.12", "Role": "r",
        "Handler": "app.handler", "Code": { "ZipFile": b64(&zip) },
    });
    call(
        &server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    for key in [
        "AllowHeaders",
        "AllowMethods",
        "AllowOrigins",
        "ExposeHeaders",
    ] {
        let r = create_url(
            &server,
            "web",
            json!({ "AuthType": "NONE", "Cors": { key: ["x-test\r\nX-Injected: yes"] } }),
        )
        .await;
        assert_eq!(r.status, 400, "{key}: {}", String::from_utf8_lossy(&r.body));
    }
    let r = create_url(&server, "web", json!({ "AuthType": "NONE" })).await;
    assert_eq!(r.status, 201);
    let r = call(
        &server,
        "PUT",
        "/2021-10-31/functions/web/url",
        &[],
        json!({ "Cors": { "AllowOrigins": ["https://a\nb"] } })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400, "updates are validated too");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn reflected_origins_cannot_inject_header_lines() {
    let dir = temp_dir("origininject");
    let server = Arc::new(Server::new(config_for(&dir)));
    create_function(&server, "web").await;
    let (id, _) = url_parts(
        create_url(
            &server,
            "web",
            json!({ "AuthType": "NONE", "Cors": {
                "AllowOrigins": ["*"], "AllowMethods": ["*"], "AllowCredentials": true,
            } }),
        )
        .await
        .json()["FunctionUrl"]
            .as_str()
            .unwrap(),
    );
    let target = format!("/_url/{id}/");
    let ok = call(
        &server,
        "OPTIONS",
        &target,
        &[
            ("Origin", "https://app.example"),
            ("Access-Control-Request-Method", "GET"),
        ],
        b"",
    )
    .await;
    assert_eq!(
        ok.header("access-control-allow-origin"),
        Some("https://app.example")
    );
    // The request reader splits head lines at CRLF only, so a lone LF can
    // reach the origin; it must never be echoed.
    let evil = "https://app.example\nX-Injected: yes";
    let preflight = call(
        &server,
        "OPTIONS",
        &target,
        &[("Origin", evil), ("Access-Control-Request-Method", "GET")],
        b"",
    )
    .await;
    assert_eq!(preflight.header("access-control-allow-origin"), None);
    if has_python() {
        let r = call(&server, "GET", &target, &[("Origin", evil)], b"").await;
        assert_eq!(r.status, 200);
        assert_eq!(r.header("access-control-allow-origin"), None);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_request_headers_reach_the_event_combined() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = temp_dir("dupheaders");
    let server = Arc::new(Server::new(config_for(&dir)));
    create_function(&server, "web").await;
    let (id, _) = url_parts(
        create_url(&server, "web", json!({ "AuthType": "NONE" }))
            .await
            .json()["FunctionUrl"]
            .as_str()
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(devcloud_lambda::http::serve(
        listener,
        Arc::clone(&server),
        async {
            let _ = rx.await;
        },
    ));
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let request = format!(
        "GET /_url/{id}/ HTTP/1.1\r\nHost: x\r\nX-Tag: a\r\nX-Tag: b\r\nCookie: s=1\r\nCookie: t=2\r\n\r\n"
    );
    s.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf).into_owned();
    let body = resp.split("\r\n\r\n").nth(1).unwrap_or_default();
    let event: Value = serde_json::from_str(body).unwrap_or_else(|_| panic!("{resp}"));
    assert_eq!(event["headers"]["x-tag"], "a,b");
    assert_eq!(event["cookies"], json!(["s=1", "t=2"]));
    let _ = tx.send(());
    serving.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn credentialed_cors_answers_wildcards_with_concrete_values() {
    let dir = temp_dir("corscreds");
    let server = Arc::new(Server::new(config_for(&dir)));
    create_function(&server, "web").await;
    create_function(&server, "open").await;
    let cors = |credentials: bool| {
        json!({ "AuthType": "NONE", "Cors": {
            "AllowOrigins": ["https://app.example"], "AllowMethods": ["*"],
            "AllowHeaders": ["*"], "ExposeHeaders": ["*"], "AllowCredentials": credentials,
        } })
    };
    let id_of = |r: Resp| url_parts(r.json()["FunctionUrl"].as_str().unwrap()).0;
    let creds_id = id_of(create_url(&server, "web", cors(true)).await);
    let open_id = id_of(create_url(&server, "open", cors(false)).await);
    let preflight = |id: String, headers: &'static str| {
        let server = Arc::clone(&server);
        async move {
            call(
                &server,
                "OPTIONS",
                &format!("/_url/{id}/"),
                &[
                    ("Origin", "https://app.example"),
                    ("Access-Control-Request-Method", "PUT"),
                    ("Access-Control-Request-Headers", headers),
                ],
                b"",
            )
            .await
        }
    };

    // Credentials: `*` is literal to browsers, so echo what was requested.
    let r = preflight(creds_id.clone(), "content-type, x-custom").await;
    assert_eq!(r.header("access-control-allow-methods"), Some("PUT"));
    assert_eq!(
        r.header("access-control-allow-headers"),
        Some("content-type, x-custom")
    );
    assert_eq!(r.header("access-control-allow-credentials"), Some("true"));
    assert!(r
        .header("vary")
        .unwrap()
        .contains("Access-Control-Request-Headers"));
    // Only HTTP tokens are echoed.
    let r = preflight(creds_id.clone(), "x-a\nX-Injected: yes").await;
    assert_eq!(r.header("access-control-allow-headers"), None);

    // Without credentials the wildcard works as configured.
    let r = preflight(open_id, "content-type").await;
    assert_eq!(r.header("access-control-allow-methods"), Some("*"));
    assert_eq!(r.header("access-control-allow-headers"), Some("*"));

    if has_python() {
        let r = call(
            &server,
            "GET",
            &format!("/_url/{creds_id}/?mode=binary"),
            &[("Origin", "https://app.example")],
            b"",
        )
        .await;
        let expose = r
            .header("access-control-expose-headers")
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(
            expose.contains("x-custom") && !expose.split(',').any(|h| h == "*"),
            "{expose}"
        );
    }
    let _ = std::fs::remove_dir_all(dir);
}
