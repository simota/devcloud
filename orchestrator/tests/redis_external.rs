#![cfg(unix)]

use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_rustls::TlsAcceptor;

struct Workspace {
    root: PathBuf,
    child: Option<Child>,
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

async fn command(reader: &mut (impl AsyncBufRead + Unpin)) -> Option<Vec<String>> {
    let mut line = String::new();
    reader.read_line(&mut line).await.ok()?;
    let count: usize = line.strip_prefix('*')?.trim().parse().ok()?;
    let mut args = Vec::new();
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await.ok()?;
        let len: usize = line.strip_prefix('$')?.trim().parse().ok()?;
        let mut bytes = vec![0; len + 2];
        reader.read_exact(&mut bytes).await.ok()?;
        if &bytes[len..] != b"\r\n" {
            return None;
        }
        args.push(String::from_utf8(bytes[..len].to_vec()).ok()?);
    }
    Some(args)
}

fn bulk(value: &str) -> String {
    format!("${}\r\n{value}\r\n", value.len())
}

async fn fake_redis(
    stream: impl AsyncRead + AsyncWrite + Unpin,
    wrong_auth: Arc<AtomicBool>,
    config_allowed: bool,
    acl: bool,
) {
    let mut stream = BufReader::new(stream);
    let mut authenticated = false;
    let mut db = 0;
    while let Some(args) = command(&mut stream).await {
        let reply = match args.first().map(String::as_str) {
            Some("AUTH") => {
                let expected: &[_] = if acl {
                    &["AUTH", "dashboard-reader", "upstream-secret"]
                } else {
                    &["AUTH", "upstream-secret"]
                };
                authenticated = args == expected;
                if authenticated {
                    "+OK\r\n".into()
                } else {
                    wrong_auth.store(true, Ordering::SeqCst);
                    "-WRONGPASS invalid credentials\r\n".into()
                }
            }
            _ if !authenticated => "-NOAUTH authentication required\r\n".into(),
            Some("PING") => "+PONG\r\n".into(),
            Some("SELECT") => {
                db = args[1].parse().unwrap();
                "+OK\r\n".into()
            }
            Some("CONFIG") if config_allowed => {
                format!("*2\r\n{}{}", bulk("databases"), bulk("16"))
            }
            Some("CONFIG") => "-NOPERM this user cannot run CONFIG\r\n".into(),
            Some("INFO") => bulk(&format!(
                "redis_version:7.0.0\r\nconnected_clients:2\r\nused_memory_human:1M\r\ndb{db}:keys=1,expires=0\r\n"
            )),
            Some("GET") => bulk(&format!("database-{db}")),
            _ => "-ERR unsupported test command\r\n".into(),
        };
        if stream.get_mut().write_all(reply.as_bytes()).await.is_err() {
            break;
        }
    }
}

async fn http(port: u16, method: &str, path: &str, body: &str) -> Result<(u16, Value), String> {
    let exchange = async {
        let mut stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|e| e.to_string())?;
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|e| e.to_string())?;
        let end = response
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or("missing response head")?;
        let status = String::from_utf8_lossy(&response[..end])
            .split_whitespace()
            .nth(1)
            .ok_or("missing response status")?
            .parse()
            .map_err(|_| "invalid response status")?;
        let body = serde_json::from_slice(&response[end + 4..])
            .map_err(|e| format!("invalid response JSON: {e}"))?;
        Ok((status, body))
    };
    tokio::time::timeout(Duration::from_secs(2), exchange)
        .await
        .map_err(|_| "HTTP request timed out".to_string())?
}

#[tokio::test]
async fn external_redis_dashboard_uses_its_url_password_port_and_database() {
    check_external_redis(3, true, 16, false).await;
}

#[tokio::test]
async fn external_redis_without_config_permission_accepts_a_database_above_the_default_range() {
    check_external_redis(20, false, 21, false).await;
}

#[tokio::test]
async fn external_rediss_dashboard_uses_tls_and_url_acl_credentials_in_relaxed_mode() {
    check_external_redis(3, false, 16, true).await;
}

async fn check_external_redis(
    database: i64,
    config_allowed: bool,
    expected_database_count: i64,
    tls: bool,
) {
    let root = std::env::temp_dir().join(format!(
        "devcloud-redis-external-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut workspace = Workspace { root, child: None };
    fs::create_dir_all(workspace.root.join(".devcloud")).unwrap();
    let (scheme, username, auth_mode, ca_setting, acceptor) = if tls {
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let ca_file = workspace.root.join("redis-ca.pem");
        fs::write(&ca_file, cert.cert.pem()).unwrap();
        let server =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der())
                        .into(),
                )
                .unwrap();
        (
            "rediss",
            "dashboard%2Dreader",
            "relaxed",
            format!("    externalTlsCaFile: \"{}\"\n", ca_file.display()),
            Some(TlsAcceptor::from(Arc::new(server))),
        )
    } else {
        ("redis", "", "strict", String::new(), None)
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_port = listener.local_addr().unwrap().port();
    let wrong_auth = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&wrong_auth);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let backend = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        tokio::pin!(stopped);
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                connection = listener.accept() => {
                    let (stream, _) = connection.unwrap();
                    let observed = Arc::clone(&observed);
                    let acceptor = acceptor.clone();
                    connections.spawn(async move {
                        if let Some(acceptor) = acceptor {
                            let stream = acceptor.accept(stream).await.unwrap();
                            fake_redis(stream, observed, config_allowed, true).await;
                        } else {
                            fake_redis(stream, observed, config_allowed, false).await;
                        }
                    });
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });

    let reserved: Vec<_> = (0..4)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = reserved
        .iter()
        .map(|s| s.local_addr().unwrap().port())
        .collect();
    let (dashboard, relay, control, managed) = (ports[0], ports[1], ports[2], ports[3]);
    fs::write(
        workspace.root.join(".devcloud/config.yaml"),
        format!(
            "server:\n  dashboardPort: {dashboard}\n  eventRelayPort: {relay}\n  redisHttpPort: {control}\n  redisPort: {managed}\nauth:\n  redis:\n    mode: {auth_mode}\n    password: control-only-secret\nservices:\n  redis:\n    mode: external\n    externalUrl: {scheme}://{username}:upstream%2Dsecret@127.0.0.1:{upstream_port}/{database}\n{ca_setting}    binaryPath: /redis-must-not-be-started\n"
        ),
    )
    .unwrap();
    let log = fs::File::create(workspace.root.join("stderr.log")).unwrap();
    drop(reserved);
    workspace.child = Some(
        Command::new(env!("CARGO_BIN_EXE_devcloud"))
            .args(["up", "redis"])
            .current_dir(&workspace.root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Ok((200, body)) = http(dashboard, "GET", "/api/redis/status", "").await {
            break body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "external Redis never became ready: {}",
            fs::read_to_string(workspace.root.join("stderr.log")).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(status["mode"], "external");
    assert_eq!(status["address"], format!("127.0.0.1:{upstream_port}"));
    assert_eq!(status["currentDB"], database);
    assert_eq!(status["databaseCount"], expected_database_count);
    assert_eq!(status["currentDBKeys"], 1);
    // The selected DB must also remain available in the normal UI switcher,
    // whose range comes from databaseCount rather than the external URL.
    for db in [0, database] {
        let (status, result) = http(
            dashboard,
            "POST",
            "/api/redis/select-db",
            &json!({"db": db}).to_string(),
        )
        .await
        .unwrap();
        assert_eq!(status, 200);
        assert_eq!(result["currentDB"], db);
    }
    let (status, result) = http(
        dashboard,
        "POST",
        "/api/redis/command",
        &json!({"command": "GET", "args": ["probe"]}).to_string(),
    )
    .await
    .unwrap();
    assert_eq!(status, 200);
    assert_eq!(result["rows"], json!([format!("database-{database}")]));
    assert!(!wrong_auth.load(Ordering::SeqCst));
    let (_, registry) = http(dashboard, "GET", "/api/services", "").await.unwrap();
    let registry = registry.to_string();
    assert!(registry.contains(&format!("{scheme}://127.0.0.1:{upstream_port}/{database}")));
    for credential in [
        "upstream-secret",
        "upstream%2Dsecret",
        "control-only-secret",
        "dashboard%2Dreader",
        "dashboard-reader",
    ] {
        assert!(
            !registry.contains(credential),
            "registry exposed a credential"
        );
    }
    // Dropping the child guard also keeps assertion failures from leaving a daemon.
    drop(workspace);
    let _ = stop.send(());
    backend.await.unwrap();
}
