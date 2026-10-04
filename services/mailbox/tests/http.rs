mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{json, request, Fixture};
use devcloud_mailbox::{config::Config, http, routes, static_files};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, watch};

#[test]
fn whole_port_auth_host_path_and_security_headers() {
    let f = Fixture::new();
    f.receive(b"Subject: s\r\n\r\nbody\r\n");
    let mut app = routes::App::new(
        f.app.service.clone(),
        devcloud_mail::http::HttpAuth {
            auth_mode: "strict".into(),
            username: "user".into(),
            password: "pass".into(),
        },
        "mailhog.example".into(),
        Vec::new(),
    );
    for target in [
        "/",
        "/assets/missing.js",
        "/api/v1/messages",
        "/api/v2/messages",
        "/api/mailbox/messages",
        "/api/v1/events",
        "/api/not-found",
    ] {
        let mut req = http::Request::new("GET", target);
        req.headers.insert("host".into(), "localhost".into());
        let response = routes::dispatch(&app, &req);
        assert_eq!(response.status, 401);
        assert!(response
            .headers
            .iter()
            .any(|(k, _)| k == "WWW-Authenticate"));
        req.headers
            .insert("authorization".into(), "Basic dXNlcjpwYXNz".into());
        let response = routes::dispatch(&app, &req);
        assert_ne!(response.status, 401);
        assert!(response
            .headers
            .iter()
            .any(|(k, v)| k == "X-Content-Type-Options" && v == "nosniff"));
        assert!(response
            .headers
            .iter()
            .any(|(k, v)| k == "Content-Security-Policy"
                && v == if target.starts_with("/api/") {
                    routes::API_CSP
                } else {
                    routes::SPA_CSP
                }));
        req.headers.insert("host".into(), "evil.example".into());
        assert_eq!(routes::dispatch(&app, &req).status, 403);
    }
    for mode in ["relaxed", "off"] {
        app.auth.auth_mode = mode.into();
        assert_eq!(request(&app, "GET", "/api/v2/messages").status, 200);
    }
    for host in [
        "localhost",
        "localhost:8025",
        "app.localhost",
        "127.0.0.1",
        "[::1]:8025",
        "10.1.2.3:8025",
        "mailbox",
        "host.docker.internal",
    ] {
        assert!(routes::host_allowed(host, &[]), "{host}");
    }
    for host in [
        "evil.example",
        "localhost.evil.example",
        "localhost@evil.example",
        "",
        "localhost:bad",
        "[::1]evil",
        "localhost\r\n",
    ] {
        assert!(!routes::host_allowed(host, &[]), "{host}");
    }
    assert!(routes::host_allowed(
        "example.test:8025",
        &["example.test".into()]
    ));
    assert!(routes::host_allowed("evil.example", &["*".into()]));
    for path in [
        "/%2e%2e/%2e%2e/etc/passwd",
        "/assets/../../Cargo.toml",
        "/assets//x",
        "/assets/%00",
        "/assets/%5cx",
        "/assets/missing.js",
        "/api/missing",
        "/missing.js",
    ] {
        assert_eq!(request(&app, "GET", path).status, 404, "{path}");
    }
    assert_eq!(request(&app, "GET", "/some/client/route").status, 200);
    assert_eq!(request(&app, "OPTIONS", "/api/v1/messages").status, 405);
    assert_eq!(request(&app, "POST", "/api/v1/messages").status, 405);
    assert_eq!(json(&request(&app, "GET", "/api/v2/messages"))["total"], 1);
    assert!(!request(&app, "GET", "/")
        .headers
        .iter()
        .any(|(k, _)| k.starts_with("Access-Control-")));
}

#[test]
fn host_denial_body_explains_allowlist_and_preserves_security_headers() {
    let f = Fixture::new();
    for path in ["/", "/api/v2/messages", "/api/mailbox/missing/html"] {
        let mut req = http::Request::new("GET", path);
        req.headers
            .insert("host".into(), "mailhog.local:8025".into());
        let response = routes::dispatch(&f.app, &req);
        assert_eq!(response.status, 403);
        let body = String::from_utf8(response.body).unwrap();
        assert!(body.starts_with("Host denied: mailhog.local:8025\n"));
        assert!(body.contains("DEVCLOUD_MAILBOX_ALLOWED_HOSTS=<comma list>"));
        assert!(body.contains("DEVCLOUD_MAILBOX_ALLOWED_HOSTS=*"));
        assert!(body.contains("disable the Host allowlist check"));
        for (name, value) in [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("X-Content-Type-Options", "nosniff"),
            (
                "Content-Security-Policy",
                if path.starts_with("/api/") {
                    routes::API_CSP
                } else {
                    routes::SPA_CSP
                },
            ),
        ] {
            assert!(response
                .headers
                .iter()
                .any(|(k, v)| k == name && v == value));
        }
    }
}

#[test]
fn host_denial_body_strips_controls_before_capping_at_255_characters() {
    let f = Fixture::new();
    for (host, expected) in [
        (
            "evil\r\n\t\0\u{7f}\u{85}.example".into(),
            "evil.example".into(),
        ),
        (format!("{}tail.example", "a".repeat(255)), "a".repeat(255)),
        (
            format!("\r\n{}tail", "界\u{85}".repeat(255)),
            "界".repeat(255),
        ),
        (String::new(), String::new()),
    ] {
        let mut req = http::Request::new("GET", "/api/v2/messages");
        req.headers.insert("host".into(), host);
        let response = routes::dispatch(&f.app, &req);
        assert_eq!(response.status, 403);
        let body = String::from_utf8(response.body).unwrap();
        assert_eq!(
            body.lines().next().unwrap(),
            format!("Host denied: {expected}")
        );
        assert!(body.chars().all(|c| !c.is_control() || c == '\n'));
        assert!(body.contains("DEVCLOUD_MAILBOX_ALLOWED_HOSTS=*"));
    }
}

#[test]
fn output_headers_and_disposition_are_safe_and_types_are_allowlisted() {
    assert!(http::safe_header("Bad\r\nHeader", "value").is_none());
    assert_eq!(
        http::safe_header("X-Test", "safe\r\nX-Injected: value\u{7}\t")
            .unwrap()
            .1,
        "safeX-Injected: value\t"
    );
    let response = routes::secure(
        http::Response::text(200, "body")
            .with_header("Bad\nHeader", "value")
            .with_header("X-Test", "safe\r\nX-Injected: value"),
        "/api/test",
    );
    let wire = http::response_head(&response, false);
    let wire = String::from_utf8(wire).unwrap();
    assert!(!wire.contains("\r\nX-Injected:"));
    assert!(!wire.contains("Bad\nHeader"));
    let disposition = http::content_disposition("日本語\"\\\r\n.txt");
    assert!(disposition.contains("filename*=UTF-8''"));
    assert!(!disposition.contains(['\r', '\n']));
    for value in [
        "text/html",
        "image/svg+xml",
        "text/plain\r\nX-Evil: x",
        "unknown/a",
    ] {
        assert_eq!(http::safe_content_type(value), "application/octet-stream");
    }
    for value in [
        "image/png",
        "application/pdf",
        "text/csv",
        "application/json",
        "application/zip",
    ] {
        assert_eq!(http::safe_content_type(value), value);
    }
}

#[test]
fn env_defaults_validation_storage_probe_and_corruption_error() {
    let defaults = Config::from_lookup(|_| None).unwrap();
    assert_eq!(defaults.smtp_addr.to_string(), "127.0.0.1:1025");
    assert_eq!(defaults.http_addr.to_string(), "127.0.0.1:8025");
    for (name, value) in [
        ("AUTH_MODE", "strict"),
        ("AUTH_MODE", "unknown"),
        ("MAX_BYTES", "-1"),
        ("SMTP_ADDR", "invalid"),
        ("HOSTNAME", "bad\r\nvalue"),
    ] {
        assert!(Config::from_lookup(
            |key| (key == format!("DEVCLOUD_MAILBOX_{name}")).then(|| value.to_string())
        )
        .is_err());
    }
    let f = Fixture::new();
    let config = Config::from_lookup(|key| {
        (key == "DEVCLOUD_MAILBOX_STORAGE").then(|| f.root.to_string_lossy().into_owned())
    })
    .unwrap();
    config.prepare_storage().unwrap();
    std::fs::write(
        f.root.join("mail/messages.jsonl"),
        b"{\"id\":\"old\"}\nprivate-body-marker invalid\n",
    )
    .unwrap();
    let error = config.prepare_storage().unwrap_err();
    assert!(error.contains("line 2"));
    assert!(!error.contains("private-body-marker"));
    let file = f.root.join("blocked");
    std::fs::write(&file, b"file").unwrap();
    let config = Config::from_lookup(|key| {
        (key == "DEVCLOUD_MAILBOX_STORAGE").then(|| file.to_string_lossy().into_owned())
    })
    .unwrap();
    let error = config.prepare_storage().unwrap_err();
    assert!(error.contains("uid"));
    assert!(error.contains("DEVCLOUD_MAILBOX_STORAGE"));
}

#[test]
fn every_referenced_embedded_asset_exists() {
    let index = std::str::from_utf8(static_files::asset("/index.html").unwrap()).unwrap();
    let mut count = 0;
    for quote in ['\'', '"'] {
        for value in index.split(quote) {
            if value.starts_with("/assets/") || value.starts_with("assets/") {
                let path = format!(
                    "/{}",
                    value
                        .trim_start_matches('/')
                        .split(['?', '#'])
                        .next()
                        .unwrap()
                );
                assert!(static_files::asset(&path).is_some(), "{path}");
                count += 1;
            }
        }
    }
    // A scaffold may have no bundled references yet; every present reference
    // must still resolve. The browser acceptance gate requires the built UI.
    assert!(count > 0 || !index.contains("/assets/"));
}

#[tokio::test]
async fn transport_rejects_oversize_duplicate_and_transfer_encoded_requests() {
    for bytes in [
        b"GET /api/v2/messages HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
            .to_vec(),
        b"POST /api/v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1048577\r\n\r\n"
            .to_vec(),
        b"GET / HTTP/1.1\r\nHost: localhost\r\nHost: evil.example\r\n\r\n".to_vec(),
        format!(
            "GET / HTTP/1.1\r\nHost: localhost\r\nX-Huge: {}\r\n\r\n",
            "x".repeat(100 * 1024)
        )
        .into_bytes(),
    ] {
        let (mut client, mut server) = tokio::io::duplex(256 * 1024);
        client.write_all(&bytes).await.unwrap();
        assert!(http::read_request(&mut server).await.is_err());
    }
    let (mut client, mut server) = tokio::io::duplex(1024);
    client
        .write_all(b"GET /api/v2/messages?limit=+3 HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let req = http::read_request(&mut server).await.unwrap().unwrap();
    assert_eq!(req.param("limit"), " 3");
    assert_eq!(req.path, "/api/v2/messages");
}

#[tokio::test]
async fn http_header_idle_timeout_is_bounded() {
    let (_client, mut server) = tokio::io::duplex(1024);
    let error = tokio::time::timeout(Duration::from_secs(12), http::read_request(&mut server))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
}

async fn head(stream: &mut tokio::io::DuplexStream) -> String {
    let mut bytes = Vec::new();
    loop {
        let b = stream.read_u8().await.unwrap();
        bytes.push(b);
        if bytes.ends_with(b"\r\n\r\n") {
            return String::from_utf8(bytes).unwrap();
        }
    }
}

#[tokio::test]
async fn sse_fanout_capacity_shutdown_and_lagged_stream() {
    let f = Fixture::new();
    let (cancel, shutdown) = watch::channel(false);
    let mut clients = Vec::new();
    let mut tasks = Vec::new();
    for n in 0..65 {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        tasks.push(tokio::spawn(http::handle_connection(
            server,
            f.app.clone(),
            shutdown.clone(),
        )));
        client
            .write_all(b"GET /api/v1/events HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let headers = head(&mut client).await;
        assert!(headers.starts_with(if n == 64 {
            "HTTP/1.1 503"
        } else {
            "HTTP/1.1 200"
        }));
        if n < 64 {
            assert!(headers.contains("Transfer-Encoding: chunked"));
            assert!(headers.contains("Cache-Control: no-cache"));
        }
        clients.push(client);
    }
    let message = f.receive(b"Subject: new\r\n\r\nbody\r\n");
    f.app.publish(&message.id);
    for client in clients.iter_mut().take(2) {
        let mut data = vec![0; 4096];
        let n = tokio::time::timeout(Duration::from_secs(1), client.read(&mut data))
            .await
            .unwrap()
            .unwrap();
        let text = String::from_utf8_lossy(&data[..n]);
        assert!(text.contains("data: {"));
        assert!(text.contains(&message.id));
    }
    cancel.send(true).unwrap();
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(f.app.sse_slots.available_permits(), 64);

    let (tx, rx) = broadcast::channel(2);
    for _ in 0..3 {
        tx.send(Arc::<str>::from("{}")).unwrap();
    }
    let (_cancel, shutdown) = watch::channel(false);
    let (_client, mut server) = tokio::io::duplex(1024);
    tokio::time::timeout(
        Duration::from_secs(1),
        http::stream_events(&mut server, rx, shutdown),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn sse_slow_writes_time_out_and_heartbeats_are_comments() {
    let (tx, rx) = broadcast::channel(2);
    tx.send(Arc::<str>::from("x".repeat(4096))).unwrap();
    let (_cancel, shutdown) = watch::channel(false);
    let (_client, mut server) = tokio::io::duplex(16);
    let error = tokio::time::timeout(
        Duration::from_secs(7),
        http::stream_events(&mut server, rx, shutdown),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    let (_tx, rx) = broadcast::channel(2);
    let (cancel, shutdown) = watch::channel(false);
    let (mut client, mut server) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move { http::stream_events(&mut server, rx, shutdown).await });
    let mut buf = [0; 8];
    let n = tokio::time::timeout(Duration::from_secs(17), client.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"3\r\n:\n\n\r\n");
    cancel.send(true).unwrap();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn disconnected_sse_releases_capacity_without_waiting_for_heartbeat() {
    let f = Fixture::new();
    let (_cancel, shutdown) = watch::channel(false);
    let (mut client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(http::handle_connection(server, f.app.clone(), shutdown));
    client
        .write_all(b"GET /api/v1/events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    assert!(head(&mut client).await.starts_with("HTTP/1.1 200"));
    assert_eq!(f.app.sse_slots.available_permits(), 63);
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.app.sse_slots.available_permits(), 64);
}
