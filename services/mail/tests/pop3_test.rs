//! POP3 + STLS + POP3S integration tests backed by the shared `FileStore`.

use std::path::PathBuf;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use devcloud_mail::{
    Envelope, FileBlobStore, FileStore, MailTls, Pop3Config, Pop3Server, Service, Store,
};
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "devcloud-mail-pop3-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn new_service(tmp: &TempDir) -> Arc<Service> {
    let blobs = Arc::new(FileBlobStore::new(tmp.0.join("blobs")));
    let store: Arc<dyn Store> = Arc::new(FileStore::new(tmp.0.join("mail"), blobs));
    Arc::new(Service::new(store))
}

fn connector_trusting(certs: &[CertificateDer<'static>]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    for c in certs {
        roots.add(c.clone()).unwrap();
    }
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

struct Pop3Client<S> {
    stream: S,
    buf: Vec<u8>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Pop3Client<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn into_inner(self) -> S {
        assert!(self.buf.is_empty());
        self.stream
    }

    async fn send(&mut self, line: &str) {
        self.stream
            .write_all(format!("{line}\r\n").as_bytes())
            .await
            .unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn read_line(&mut self) -> String {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return String::from_utf8_lossy(&line).into_owned();
            }
            let mut tmp = [0u8; 1024];
            let n = self.stream.read(&mut tmp).await.unwrap();
            if n == 0 {
                let rem = std::mem::take(&mut self.buf);
                return String::from_utf8_lossy(&rem).into_owned();
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn read_multiline(&mut self) -> (String, Vec<String>) {
        let status = self.read_line().await;
        let mut lines = Vec::new();
        loop {
            let line = self.read_line().await;
            if line == "." {
                break;
            }
            lines.push(line);
        }
        (status, lines)
    }
}

#[tokio::test]
async fn pop3_lists_retrieves_dot_stuffs_top_and_handles_dele_rset_quit() {
    let tmp = TempDir::new("core");
    let service = new_service(&tmp);

    let m1 = service
        .receive(
            Envelope {
                from: "alice@example.com".into(),
                to: vec!["bob@example.com".into()],
            },
            b"From: alice@example.com\r\nSubject: First\r\n\r\nline1\r\n.dotline\r\nline3\r\n",
        )
        .unwrap();
    let m2 = service
        .receive(
            Envelope {
                from: "carol@example.com".into(),
                to: vec!["bob@example.com".into()],
            },
            b"From: carol@example.com\r\nSubject: Second\r\n\r\nhello second\r\n",
        )
        .unwrap();

    let server = Pop3Server::new(
        Pop3Config {
            auth_mode: "relaxed".into(),
            ..Default::default()
        },
        Arc::clone(&service),
    );

    // Session 1: inspect, dot-stuffing, TOP, DELE + RSET, DELE + abrupt disconnect (no commit).
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let handle = tokio::spawn(server.clone().handle_conn(server_io));
        let mut c = Pop3Client::new(client_io);

        assert!(c.read_line().await.starts_with("+OK"));

        c.send("CAPA").await;
        let (st, caps) = c.read_multiline().await;
        assert!(st.starts_with("+OK"));
        assert!(caps.contains(&"USER".to_string()));
        assert!(caps.contains(&"UIDL".to_string()));
        assert!(caps.contains(&"TOP".to_string()));
        assert!(!caps.contains(&"STLS".to_string()));

        // Commands before auth must fail.
        c.send("STAT").await;
        assert!(c.read_line().await.starts_with("-ERR"));

        // Relaxed auth accepts arbitrary credentials.
        c.send("USER anyone").await;
        assert!(c.read_line().await.starts_with("+OK"));
        c.send("PASS anything").await;
        assert!(c.read_line().await.starts_with("+OK"));

        c.send("STAT").await;
        let stat = c.read_line().await;
        assert!(stat.starts_with("+OK 2 "), "stat = {stat}");

        c.send("LIST").await;
        let (st, list_lines) = c.read_multiline().await;
        assert!(st.starts_with("+OK 2 "));
        assert_eq!(list_lines.len(), 2);

        c.send("UIDL").await;
        let (st, uidl_lines) = c.read_multiline().await;
        assert!(st.starts_with("+OK"));
        assert_eq!(
            uidl_lines,
            vec![format!("1 {}", m1.id), format!("2 {}", m2.id)]
        );
        c.send("UIDL 2").await;
        assert_eq!(c.read_line().await, format!("+OK 2 {}", m2.id));

        // RETR must dot-stuff ".dotline" -> "..dotline".
        c.send("RETR 1").await;
        let (st, retr_lines) = c.read_multiline().await;
        assert!(st.starts_with("+OK"));
        assert!(
            retr_lines.contains(&"..dotline".to_string()),
            "{retr_lines:?}"
        );
        assert!(retr_lines.contains(&"Subject: First".to_string()));

        // TOP 1 1 returns headers + blank line + 1 body line.
        c.send("TOP 1 1").await;
        let (st, top_lines) = c.read_multiline().await;
        assert!(st.starts_with("+OK"));
        assert_eq!(
            top_lines,
            vec!["From: alice@example.com", "Subject: First", "", "line1"]
        );

        // DELE + RSET restores message 1.
        c.send("DELE 1").await;
        assert!(c.read_line().await.starts_with("+OK"));
        c.send("RETR 1").await;
        assert!(c.read_line().await.starts_with("-ERR"));
        c.send("RSET").await;
        assert!(c.read_line().await.starts_with("+OK"));
        c.send("STAT").await;
        assert!(c.read_line().await.starts_with("+OK 2 "));

        // DELE 1 then disconnect WITHOUT QUIT: must NOT delete from store.
        c.send("DELE 1").await;
        assert!(c.read_line().await.starts_with("+OK"));
        drop(c);
        handle.await.unwrap();
    }
    assert!(service.get(&m1.id).unwrap().is_some());

    // Session 2: AUTH PLAIN + DELE 1 + QUIT commits deletion to shared store.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let handle = tokio::spawn(server.clone().handle_conn(server_io));
        let mut c = Pop3Client::new(client_io);

        assert!(c.read_line().await.starts_with("+OK"));
        let plain = BASE64.encode(b"\x00bob\x00pw");
        c.send(&format!("AUTH PLAIN {plain}")).await;
        assert!(c.read_line().await.starts_with("+OK"));
        c.send("DELE 1").await;
        assert!(c.read_line().await.starts_with("+OK"));
        c.send("QUIT").await;
        assert!(c.read_line().await.starts_with("+OK"));
        drop(c);
        handle.await.unwrap();
    }
    assert!(service.get(&m1.id).unwrap().is_none());
    assert!(service.get(&m2.id).unwrap().is_some());
}

#[tokio::test]
async fn pop3_strict_auth_stls_require_and_pop3s() {
    let tmp = TempDir::new("tls");
    let service = new_service(&tmp);
    service
        .receive(
            Envelope {
                from: "s@example.com".into(),
                to: vec!["r@example.com".into()],
            },
            b"Subject: Secure POP3\r\n\r\nbody\r\n",
        )
        .unwrap();

    let tls = MailTls::self_signed(true).unwrap();
    let connector = connector_trusting(tls.certs_der());

    // 1. Plaintext listener with tls.require=true + strict auth.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let server = Pop3Server::new(
            Pop3Config {
                auth_mode: "strict".into(),
                username: "dev".into(),
                password: "secret".into(),
                ..Default::default()
            },
            Arc::clone(&service),
        )
        .with_tls(tls.clone());
        let handle = tokio::spawn(server.handle_conn(server_io));

        let mut plain = Pop3Client::new(client_io);
        assert!(plain.read_line().await.starts_with("+OK"));
        plain.send("CAPA").await;
        let (_, caps) = plain.read_multiline().await;
        assert!(caps.contains(&"STLS".to_string()));
        assert!(!caps.contains(&"USER".to_string()));

        // USER before STLS is refused when require=true.
        plain.send("USER dev").await;
        assert!(plain.read_line().await.starts_with("-ERR"));

        // Upgrade via STLS.
        plain.send("STLS").await;
        assert!(plain.read_line().await.starts_with("+OK"));
        let name = ServerName::try_from("localhost".to_string()).unwrap();
        let tls_stream = connector.connect(name, plain.into_inner()).await.unwrap();
        let mut tc = Pop3Client::new(tls_stream);

        tc.send("CAPA").await;
        let (_, tls_caps) = tc.read_multiline().await;
        assert!(!tls_caps.contains(&"STLS".to_string()));
        assert!(tls_caps.contains(&"USER".to_string()));

        // Wrong password rejected in strict mode.
        tc.send("USER dev").await;
        assert!(tc.read_line().await.starts_with("+OK"));
        tc.send("PASS wrong").await;
        assert!(tc.read_line().await.starts_with("-ERR"));

        // Right password succeeds.
        tc.send("USER dev").await;
        assert!(tc.read_line().await.starts_with("+OK"));
        tc.send("PASS secret").await;
        assert!(tc.read_line().await.starts_with("+OK"));
        tc.send("STAT").await;
        assert!(tc.read_line().await.starts_with("+OK 1 "));
        tc.send("QUIT").await;
        assert!(tc.read_line().await.starts_with("+OK"));
        drop(tc);
        handle.await.unwrap();
    }

    // 2. Implicit TLS (POP3S).
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let pop3s = Pop3Server::new(
            Pop3Config {
                auth_mode: "relaxed".into(),
                ..Default::default()
            },
            Arc::clone(&service),
        )
        .with_tls(tls);
        let handle = tokio::spawn(pop3s.handle_tls_conn(server_io));

        let name = ServerName::try_from("localhost".to_string()).unwrap();
        let tls_stream = connector.connect(name, client_io).await.unwrap();
        let mut sc = Pop3Client::new(tls_stream);
        assert!(sc.read_line().await.starts_with("+OK"));
        sc.send("USER u").await;
        assert!(sc.read_line().await.starts_with("+OK"));
        sc.send("PASS p").await;
        assert!(sc.read_line().await.starts_with("+OK"));
        sc.send("STAT").await;
        assert!(sc.read_line().await.starts_with("+OK 1 "));
        sc.send("QUIT").await;
        assert!(sc.read_line().await.starts_with("+OK"));
        drop(sc);
        handle.await.unwrap();
    }
}
