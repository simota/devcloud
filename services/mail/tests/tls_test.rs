//! TLS / STARTTLS / SMTPS tests for `devcloud-mail`.

use std::path::PathBuf;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use devcloud_mail::{
    mail_tls_from_config, MailTls, RecordingStore, Service, SmtpConfig, SmtpServer,
};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType};
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

struct TestPki {
    dir: PathBuf,
    ca: CertificateDer<'static>,
}

impl Drop for TestPki {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl TestPki {
    fn cert(&self) -> String {
        self.dir.join("server.pem").to_string_lossy().into_owned()
    }
    fn key(&self) -> String {
        self.dir.join("server.key").to_string_lossy().into_owned()
    }
}

fn issue_ca_cert(tag: &str) -> TestPki {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-mail-tls-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "devcloud mail test CA");
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let server = params.signed_by(&server_key, &ca, &ca_key).unwrap();

    std::fs::write(dir.join("server.pem"), server.pem()).unwrap();
    std::fs::write(dir.join("server.key"), server_key.serialize_pem()).unwrap();
    TestPki {
        dir,
        ca: ca.der().clone(),
    }
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

struct LineClient<S> {
    stream: S,
    buf: Vec<u8>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> LineClient<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn into_inner(self) -> S {
        assert!(
            self.buf.is_empty(),
            "unexpected unread bytes before TLS upgrade"
        );
        self.stream
    }

    async fn send_line(&mut self, line: &str) {
        self.stream
            .write_all(format!("{line}\r\n").as_bytes())
            .await
            .expect("write");
        self.stream.flush().await.expect("flush");
    }

    async fn read_raw_line(&mut self) -> String {
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
            let n = self.stream.read(&mut tmp).await.expect("read");
            if n == 0 {
                let line = std::mem::take(&mut self.buf);
                return String::from_utf8_lossy(&line).into_owned();
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn read_reply(&mut self) -> String {
        let mut line = self.read_raw_line().await;
        let mut lines = vec![line.clone()];
        while line.len() >= 4 && line.as_bytes()[3] == b'-' {
            line = self.read_raw_line().await;
            lines.push(line.clone());
        }
        lines.join("\n")
    }
}

#[tokio::test]
async fn smtp_starttls_with_self_signed_cert_upgrades_and_resets_state() {
    let store = Arc::new(RecordingStore::new());
    let service = Arc::new(Service::new(store.clone()));
    let tls = MailTls::self_signed(false).unwrap();
    let connector = connector_trusting(tls.certs_der());

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let server = SmtpServer::new(
        SmtpConfig {
            max_message_bytes: 1024 * 1024,
            auth_mode: "relaxed".to_string(),
            ..Default::default()
        },
        service,
    )
    .with_tls(tls);
    let handle = tokio::spawn(server.handle_conn(server_io));

    let mut plain = LineClient::new(client_io);
    assert!(plain.read_reply().await.starts_with("220 "));
    plain.send_line("EHLO client.local").await;
    let ehlo = plain.read_reply().await;
    assert!(ehlo.contains("STARTTLS"), "ehlo = {ehlo:?}");

    plain.send_line("STARTTLS extra").await;
    assert!(plain.read_reply().await.starts_with("501 "));

    plain.send_line("STARTTLS").await;
    assert!(plain.read_reply().await.starts_with("220 "));

    let server_name = ServerName::try_from("localhost".to_string()).unwrap();
    let tls_stream = connector
        .connect(server_name, plain.into_inner())
        .await
        .expect("TLS handshake");
    let mut tls_client = LineClient::new(tls_stream);

    // RFC 3207 §4.2: prior EHLO state is discarded, so MAIL before EHLO fails.
    tls_client.send_line("MAIL FROM:<a@example.com>").await;
    assert!(tls_client.read_reply().await.starts_with("503 "));

    // Fresh EHLO over TLS must NOT advertise STARTTLS again.
    tls_client.send_line("EHLO client.local").await;
    let tls_ehlo = tls_client.read_reply().await;
    assert!(tls_ehlo.starts_with("250-"), "ehlo = {tls_ehlo:?}");
    assert!(!tls_ehlo.contains("STARTTLS"), "ehlo = {tls_ehlo:?}");
    assert!(tls_ehlo.contains("AUTH PLAIN LOGIN"), "ehlo = {tls_ehlo:?}");

    // Second STARTTLS inside TLS must be rejected.
    tls_client.send_line("STARTTLS").await;
    assert!(tls_client.read_reply().await.starts_with("503 "));

    tls_client.send_line("MAIL FROM:<tls@example.com>").await;
    assert!(tls_client.read_reply().await.starts_with("250 "));
    tls_client.send_line("RCPT TO:<user@example.com>").await;
    assert!(tls_client.read_reply().await.starts_with("250 "));
    tls_client.send_line("DATA").await;
    assert!(tls_client.read_reply().await.starts_with("354 "));
    tls_client.send_line("Subject: Encrypted Hello").await;
    tls_client.send_line("").await;
    tls_client.send_line("over starttls").await;
    tls_client.send_line(".").await;
    assert!(tls_client.read_reply().await.starts_with("250 "));
    tls_client.send_line("QUIT").await;
    assert!(tls_client.read_reply().await.starts_with("221 "));
    drop(tls_client);
    handle.await.unwrap();

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].subject, "Encrypted Hello");
    assert_eq!(messages[0].from, "tls@example.com");
}

#[tokio::test]
async fn smtp_starttls_with_pem_files_verifies_hostname_and_rejects_untrusted_ca() {
    let pki = issue_ca_cert("verify");
    let other_pki = issue_ca_cert("other");
    let tls = mail_tls_from_config(false, &pki.cert(), &pki.key(), false)
        .unwrap()
        .unwrap();

    let store = Arc::new(RecordingStore::new());
    let service = Arc::new(Service::new(store));
    let server = SmtpServer::new(SmtpConfig::default(), service).with_tls(tls);

    // Trusted CA + matching name succeeds for both localhost and 127.0.0.1.
    for host in ["localhost", "127.0.0.1"] {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let handle = tokio::spawn(server.clone().handle_conn(server_io));
        let mut c = LineClient::new(client_io);
        assert!(c.read_reply().await.starts_with("220 "));
        c.send_line("STARTTLS").await;
        assert!(c.read_reply().await.starts_with("220 "));
        let name = ServerName::try_from(host.to_string()).unwrap();
        let tls_conn = connector_trusting(&[pki.ca.clone()])
            .connect(name, c.into_inner())
            .await
            .expect("verified handshake");
        let mut tc = LineClient::new(tls_conn);
        tc.send_line("QUIT").await;
        assert!(tc.read_reply().await.starts_with("221 "));
        drop(tc);
        handle.await.unwrap();
    }

    // Wrong hostname fails handshake.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let handle = tokio::spawn(server.clone().handle_conn(server_io));
        let mut c = LineClient::new(client_io);
        assert!(c.read_reply().await.starts_with("220 "));
        c.send_line("STARTTLS").await;
        assert!(c.read_reply().await.starts_with("220 "));
        let wrong = ServerName::try_from("wrong.example".to_string()).unwrap();
        assert!(connector_trusting(&[pki.ca.clone()])
            .connect(wrong, c.into_inner())
            .await
            .is_err());
        handle.await.unwrap();
    }

    // Untrusted CA fails handshake.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let handle = tokio::spawn(server.clone().handle_conn(server_io));
        let mut c = LineClient::new(client_io);
        assert!(c.read_reply().await.starts_with("220 "));
        c.send_line("STARTTLS").await;
        assert!(c.read_reply().await.starts_with("220 "));
        let name = ServerName::try_from("localhost".to_string()).unwrap();
        assert!(connector_trusting(&[other_pki.ca.clone()])
            .connect(name, c.into_inner())
            .await
            .is_err());
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn smtp_tls_require_refuses_plaintext_mail_and_auth() {
    let store = Arc::new(RecordingStore::new());
    let service = Arc::new(Service::new(store));
    let tls = MailTls::self_signed(true).unwrap();
    let connector = connector_trusting(tls.certs_der());

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let server = SmtpServer::new(
        SmtpConfig {
            auth_mode: "relaxed".to_string(),
            ..Default::default()
        },
        service,
    )
    .with_tls(tls);
    let handle = tokio::spawn(server.handle_conn(server_io));

    let mut plain = LineClient::new(client_io);
    assert!(plain.read_reply().await.starts_with("220 "));
    plain.send_line("EHLO localhost").await;
    let ehlo = plain.read_reply().await;
    assert!(ehlo.contains("STARTTLS"), "ehlo = {ehlo:?}");
    assert!(
        !ehlo.contains("AUTH"),
        "AUTH must not be advertised before required TLS: {ehlo:?}"
    );

    plain.send_line("MAIL FROM:<a@example.com>").await;
    assert!(plain.read_reply().await.starts_with("530 "));
    let creds = BASE64.encode(b"\x00dev\x00dev");
    plain.send_line(&format!("AUTH PLAIN {creds}")).await;
    assert!(plain.read_reply().await.starts_with("530 "));

    // Upgrading to TLS unlocks AUTH and MAIL.
    plain.send_line("STARTTLS").await;
    assert!(plain.read_reply().await.starts_with("220 "));
    let name = ServerName::try_from("localhost".to_string()).unwrap();
    let tls_stream = connector.connect(name, plain.into_inner()).await.unwrap();
    let mut tc = LineClient::new(tls_stream);
    tc.send_line("EHLO localhost").await;
    assert!(tc.read_reply().await.contains("AUTH PLAIN LOGIN"));
    tc.send_line(&format!("AUTH PLAIN {creds}")).await;
    assert!(tc.read_reply().await.starts_with("235 "));
    tc.send_line("QUIT").await;
    assert!(tc.read_reply().await.starts_with("221 "));
    drop(tc);
    handle.await.unwrap();
}

#[tokio::test]
async fn smtp_starttls_rejects_pipelined_commands_and_unconfigured_starttls() {
    // 1. Unconfigured TLS returns 502.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let service = Arc::new(Service::new(Arc::new(RecordingStore::new())));
        let server = SmtpServer::new(SmtpConfig::default(), service);
        let handle = tokio::spawn(server.handle_conn(server_io));
        let mut c = LineClient::new(client_io);
        assert!(c.read_reply().await.starts_with("220 "));
        c.send_line("STARTTLS").await;
        assert!(c.read_reply().await.starts_with("502 "));
        c.send_line("QUIT").await;
        assert!(c.read_reply().await.starts_with("221 "));
        drop(c);
        handle.await.unwrap();
    }

    // 2. Pipelined command after STARTTLS is refused (CVE-2011-0411).
    {
        let (mut client_io, server_io) = tokio::io::duplex(64 * 1024);
        let service = Arc::new(Service::new(Arc::new(RecordingStore::new())));
        let tls = MailTls::self_signed(false).unwrap();
        let server = SmtpServer::new(SmtpConfig::default(), service).with_tls(tls);
        let handle = tokio::spawn(server.handle_conn(server_io));
        let mut buf = [0u8; 256];
        let n = client_io.read(&mut buf).await.unwrap();
        assert!(std::str::from_utf8(&buf[..n]).unwrap().starts_with("220 "));
        client_io.write_all(b"STARTTLS\r\nNOOP\r\n").await.unwrap();
        client_io.flush().await.unwrap();
        let mut reply = Vec::new();
        client_io.read_to_end(&mut reply).await.unwrap();
        let reply_str = String::from_utf8_lossy(&reply);
        assert!(
            reply_str.starts_with("501 "),
            "expected 501 pipelining error, got {reply_str:?}"
        );
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn smtps_implicit_tls_accepts_strict_auth_and_delivers_message() {
    let store = Arc::new(RecordingStore::new());
    let service = Arc::new(Service::new(store.clone()));
    let tls = MailTls::self_signed(false).unwrap();
    let connector = connector_trusting(tls.certs_der());

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let server = SmtpServer::new(
        SmtpConfig {
            auth_mode: "strict".to_string(),
            username: "alice".to_string(),
            password: "secret".to_string(),
            ..Default::default()
        },
        service,
    )
    .with_tls(tls);
    let handle = tokio::spawn(server.handle_tls_conn(server_io));

    let name = ServerName::try_from("localhost".to_string()).unwrap();
    let tls_stream = connector.connect(name, client_io).await.unwrap();
    let mut c = LineClient::new(tls_stream);

    assert!(c.read_reply().await.starts_with("220 "));
    c.send_line("EHLO localhost").await;
    let ehlo = c.read_reply().await;
    assert!(!ehlo.contains("STARTTLS"));
    assert!(ehlo.contains("AUTH PLAIN LOGIN"));

    let bad = BASE64.encode(b"\x00alice\x00wrong");
    c.send_line(&format!("AUTH PLAIN {bad}")).await;
    assert!(c.read_reply().await.starts_with("535 "));

    let good = BASE64.encode(b"\x00alice\x00secret");
    c.send_line(&format!("AUTH PLAIN {good}")).await;
    assert!(c.read_reply().await.starts_with("235 "));

    c.send_line("MAIL FROM:<alice@example.com>").await;
    assert!(c.read_reply().await.starts_with("250 "));
    c.send_line("RCPT TO:<bob@example.com>").await;
    assert!(c.read_reply().await.starts_with("250 "));
    c.send_line("DATA").await;
    assert!(c.read_reply().await.starts_with("354 "));
    c.send_line("Subject: Implicit SMTPS").await;
    c.send_line("").await;
    c.send_line("hello over smtps").await;
    c.send_line(".").await;
    assert!(c.read_reply().await.starts_with("250 "));
    c.send_line("QUIT").await;
    assert!(c.read_reply().await.starts_with("221 "));
    drop(c);
    handle.await.unwrap();

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].subject, "Implicit SMTPS");
}

#[test]
fn mail_tls_config_validation() {
    assert!(mail_tls_from_config(false, "", "", false)
        .unwrap()
        .is_none());
    assert!(mail_tls_from_config(true, "", "", false).unwrap().is_some());
    assert!(mail_tls_from_config(false, "", "", true).unwrap().is_some());
    assert!(mail_tls_from_config(false, "/x/cert.pem", "", false).is_err());
    assert!(mail_tls_from_config(false, "", "/x/key.pem", false).is_err());
    let err = mail_tls_from_config(false, "/no/cert.pem", "/no/key.pem", false)
        .err()
        .unwrap();
    assert!(err.contains("/no/cert.pem"), "{err}");
}
