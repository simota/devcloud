use devcloud_s3::console::{dispatch, host_allowed, ConsoleConfig};
use devcloud_s3::http::{route, AuthConfig, Request, Response};
use devcloud_s3::store::FileBucketStore;

fn store() -> FileBucketStore {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "devcloud-s3-console-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    FileBucketStore::new(dir)
}

fn relaxed() -> ConsoleConfig {
    ConsoleConfig {
        auth: AuthConfig::default(),
        allowed_hosts: Vec::new(),
    }
}

fn strict() -> ConsoleConfig {
    ConsoleConfig {
        auth: AuthConfig {
            auth_mode: "strict".to_string(),
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            region: "ap-northeast-1".to_string(),
        },
        allowed_hosts: Vec::new(),
    }
}

fn request(method: &str, target: &str, body: &[u8]) -> Request {
    let mut req = Request::new(method, target, body.to_vec());
    req.headers
        .insert("host".to_string(), "localhost:9001".to_string());
    req
}

fn json(response: &Response) -> serde_json::Value {
    serde_json::from_slice(&response.body).unwrap()
}

#[test]
fn serves_the_embedded_app_with_a_restrictive_policy() {
    let store = store();
    let index = dispatch(&store, &request("GET", "/", b""), &relaxed());
    assert_eq!(index.status, 200);
    assert!(index.headers["Content-Type"].starts_with("text/html"));
    assert!(String::from_utf8_lossy(&index.body).contains("devcloud S3 console"));
    assert!(index.headers["Content-Security-Policy"].contains("default-src 'self'"));
    assert_eq!(index.headers["X-Content-Type-Options"], "nosniff");

    assert_eq!(
        dispatch(&store, &request("GET", "/missing.js", b""), &relaxed()).status,
        404
    );
    assert_eq!(
        dispatch(
            &store,
            &request("GET", "/../../Cargo.toml", b""),
            &relaxed()
        )
        .status,
        404
    );
    assert_eq!(
        dispatch(&store, &request("POST", "/", b""), &relaxed()).status,
        405
    );
}

#[test]
fn bucket_and_object_workflow_reaches_the_s3_store() {
    let store = store();
    let config = relaxed();
    let send = |method: &str, target: &str, body: &[u8]| {
        dispatch(&store, &request(method, target, body), &config)
    };

    assert_eq!(send("PUT", "/api/buckets/photos", b"").status, 200);
    let duplicate = send("PUT", "/api/buckets/photos", b"");
    assert_eq!(duplicate.status, 409);
    assert_eq!(json(&duplicate)["code"], "BucketAlreadyOwnedByYou");
    assert_eq!(send("PUT", "/api/buckets/Bad_Name", b"").status, 400);

    let mut upload = request(
        "PUT",
        "/api/buckets/photos/object?key=2026%2Fa%20b%E3%81%82.txt",
        b"hello",
    );
    upload
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    assert_eq!(dispatch(&store, &upload, &config).status, 200);
    // Written through the provider path: visible to S3 clients under the decoded key.
    let via_s3 = route(
        &store,
        &Request::new("GET", "/photos/2026/a%20b%E3%81%82.txt", Vec::new()),
    );
    assert_eq!(via_s3.body, b"hello");
    assert_eq!(via_s3.headers["Content-Type"], "text/plain");

    let buckets = json(&send("GET", "/api/buckets", b""));
    assert_eq!(buckets["buckets"][0]["name"], "photos");
    assert_eq!(buckets["buckets"][0]["objectCount"], 1);

    let listed = json(&send(
        "GET",
        "/api/buckets/photos/objects?prefix=2026%2F",
        b"",
    ));
    assert_eq!(listed["objects"][0]["key"], "2026/a bあ.txt");
    assert_eq!(listed["objects"][0]["size"], 5);
    let other = json(&send(
        "GET",
        "/api/buckets/photos/objects?prefix=2025%2F",
        b"",
    ));
    assert_eq!(other["objects"].as_array().unwrap().len(), 0);

    let download = send(
        "GET",
        "/api/buckets/photos/object?key=2026%2Fa%20b%E3%81%82.txt",
        b"",
    );
    assert_eq!(download.status, 200);
    assert_eq!(download.body, b"hello");
    assert_eq!(
        download.headers["Content-Disposition"],
        "attachment; filename*=UTF-8''a%20b%E3%81%82.txt"
    );

    let not_empty = send("DELETE", "/api/buckets/photos", b"");
    assert_eq!(not_empty.status, 409);
    assert_eq!(json(&not_empty)["code"], "BucketNotEmpty");

    assert_eq!(
        send(
            "DELETE",
            "/api/buckets/photos/object?key=2026%2Fa%20b%E3%81%82.txt",
            b""
        )
        .status,
        204
    );
    let gone = send(
        "GET",
        "/api/buckets/photos/object?key=2026%2Fa%20b%E3%81%82.txt",
        b"",
    );
    assert_eq!(gone.status, 404);
    assert_eq!(json(&gone)["code"], "NoSuchKey");
    assert_eq!(send("DELETE", "/api/buckets/photos", b"").status, 204);
}

#[test]
fn dot_segment_keys_address_their_own_object() {
    let store = store();
    let config = relaxed();
    let send = |method: &str, target: &str, body: &[u8]| {
        dispatch(&store, &request(method, target, body), &config)
    };
    assert_eq!(send("PUT", "/api/buckets/dots", b"").status, 200);
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/dots/x/../y", b"dotted".to_vec())
        )
        .status,
        200
    );
    assert_eq!(
        route(&store, &Request::new("PUT", "/dots/y", b"plain".to_vec())).status,
        200
    );

    assert_eq!(
        send("GET", "/api/buckets/dots/object?key=x%2F..%2Fy", b"").body,
        b"dotted"
    );
    assert_eq!(
        send("DELETE", "/api/buckets/dots/object?key=x%2F..%2Fy", b"").status,
        204
    );
    assert_eq!(
        route(&store, &Request::new("GET", "/dots/x/../y", Vec::new())).status,
        404
    );
    assert_eq!(
        route(&store, &Request::new("GET", "/dots/y", Vec::new())).body,
        b"plain"
    );

    assert_eq!(send("GET", "/api/buckets/dots/object", b"").status, 400);
    assert_eq!(send("GET", "/api/buckets/dots/objects/y", b"").status, 404);
}

#[test]
fn stored_html_downloads_instead_of_rendering_on_the_console_origin() {
    let store = store();
    let config = relaxed();
    assert_eq!(
        dispatch(&store, &request("PUT", "/api/buckets/web", b""), &config).status,
        200
    );
    let mut put = Request::new("PUT", "/web/x.html", b"<script>alert(1)</script>".to_vec());
    put.headers
        .insert("content-type".to_string(), "text/html".to_string());
    put.headers
        .insert("content-disposition".to_string(), "inline".to_string());
    assert_eq!(route(&store, &put).status, 200);

    let download = dispatch(
        &store,
        &request("GET", "/api/buckets/web/object?key=x.html", b""),
        &config,
    );
    assert_eq!(download.status, 200);
    assert!(download.headers["Content-Disposition"].starts_with("attachment"));
    assert!(download.headers["Content-Security-Policy"].contains("default-src 'none'"));
    assert_eq!(download.headers["X-Content-Type-Options"], "nosniff");
}

#[test]
fn cross_site_requests_cannot_mutate() {
    let store = store();
    let config = relaxed();
    assert_eq!(
        dispatch(&store, &request("PUT", "/api/buckets/keep", b""), &config).status,
        200
    );
    // HTML forms only send GET and POST, and no mutation uses them; everything
    // else needs a preflight, which gets no CORS grant.
    for target in [
        "/api/buckets/keep",
        "/api/buckets/keep/object?key=k",
        "/api/buckets",
    ] {
        for method in ["POST", "OPTIONS"] {
            let mut cross_site = request(method, target, b"x");
            cross_site
                .headers
                .insert("origin".to_string(), "https://evil.test".to_string());
            let response = dispatch(&store, &cross_site, &config);
            assert_eq!(response.status, 405, "{method} {target}");
            assert!(!response.headers.contains_key("Access-Control-Allow-Origin"));
        }
    }
    let buckets = json(&dispatch(
        &store,
        &request("GET", "/api/buckets", b""),
        &config,
    ));
    assert_eq!(buckets["buckets"][0]["objectCount"], 0);
}

#[test]
fn rebinding_hosts_are_denied_unless_listed() {
    let store = store();
    let mut rebind = request("GET", "/api/buckets", b"");
    rebind
        .headers
        .insert("host".to_string(), "evil.example.com".to_string());
    assert_eq!(dispatch(&store, &rebind, &relaxed()).status, 403);

    let mut config = relaxed();
    config.allowed_hosts = vec!["evil.example.com".to_string()];
    assert_eq!(dispatch(&store, &rebind, &config).status, 200);

    for allowed in [
        "localhost:9001",
        "127.0.0.1:9001",
        "[::1]:9001",
        "host.docker.internal:9001",
        "s3-console",
    ] {
        assert!(host_allowed(allowed, &[]), "{allowed}");
    }
    for denied in [
        "",
        "a.example.com",
        "a b",
        "x@localhost",
        "[::1",
        "localhost:notaport",
    ] {
        assert!(!host_allowed(denied, &[]), "{denied}");
    }
}

#[test]
fn strict_mode_requires_the_configured_credentials() {
    let store = store();
    let config = strict();
    for target in ["/", "/api/buckets", "/api/info"] {
        let response = dispatch(&store, &request("GET", target, b""), &config);
        assert_eq!(response.status, 401, "{target}");
        assert!(response.headers["WWW-Authenticate"].starts_with("Basic"));
    }
    let with_basic = |credentials: &str, method: &str, target: &str| {
        let mut req = request(method, target, b"");
        req.headers.insert(
            "authorization".to_string(),
            format!(
                "Basic {}",
                devcloud_s3::base64::std_encode(credentials.as_bytes())
            ),
        );
        dispatch(&store, &req, &config)
    };
    for wrong in ["key:wrong", "wrong:secret", "key", ":"] {
        assert_eq!(
            with_basic(wrong, "GET", "/api/buckets").status,
            401,
            "{wrong}"
        );
    }
    let info = with_basic("key:secret", "GET", "/api/info");
    assert_eq!(json(&info)["authMode"], "strict");
    assert_eq!(json(&info)["region"], "ap-northeast-1");
    // No SigV4 signature exists here: Basic alone authorizes the forwarded call.
    assert_eq!(
        with_basic("key:secret", "PUT", "/api/buckets/strict-ok").status,
        200
    );
    let mut unsigned_garbage = request("GET", "/api/buckets", b"");
    unsigned_garbage
        .headers
        .insert("authorization".to_string(), "Basic !!!".to_string());
    assert_eq!(dispatch(&store, &unsigned_garbage, &config).status, 401);
}
