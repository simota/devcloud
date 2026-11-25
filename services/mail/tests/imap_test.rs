//! IMAP4rev1 + STARTTLS + IMAPS integration tests backed by the shared `FileStore`.

use std::path::PathBuf;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use devcloud_mail::{
    Envelope, FileBlobStore, FileStore, ImapConfig, ImapServer, MailTls, Service, Store,
};
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "devcloud-mail-imap-{tag}-{}-{}",
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

struct ImapClient<S> {
    stream: S,
    buf: Vec<u8>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ImapClient<S> {
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

    async fn send_raw(&mut self, data: &[u8]) {
        self.stream.write_all(data).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn send(&mut self, line: &str) {
        self.send_raw(format!("{line}\r\n").as_bytes()).await;
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
            let mut tmp = [0u8; 4096];
            let n = self.stream.read(&mut tmp).await.unwrap();
            if n == 0 {
                let rem = std::mem::take(&mut self.buf);
                return String::from_utf8_lossy(&rem).into_owned();
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn read_exact(&mut self, len: usize) -> Vec<u8> {
        while self.buf.len() < len {
            let mut tmp = [0u8; 4096];
            let n = self.stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "unexpected EOF reading literal");
            self.buf.extend_from_slice(&tmp[..n]);
        }
        self.buf.drain(..len).collect()
    }

    /// Reads untagged responses (including `{N}` literals) until the tagged
    /// completion line `<tag> ...` arrives. Returns `(untagged_combined, tagged)`.
    async fn read_until_tag(&mut self, tag: &str) -> (String, String) {
        let prefix = format!("{tag} ");
        let mut untagged = String::new();
        loop {
            let line = self.read_line().await;
            if line.starts_with(&prefix) {
                return (untagged, line);
            }
            untagged.push_str(&line);
            untagged.push('\n');
            if let Some(open) = line.rfind('{') {
                if let Some(close) = line[open..].find('}') {
                    let digits = &line[open + 1..open + close];
                    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                        let lit_len = digits.parse::<usize>().unwrap();
                        let lit = self.read_exact(lit_len).await;
                        untagged.push_str(&String::from_utf8_lossy(&lit));
                        untagged.push('\n');
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn imap_select_fetch_store_search_expunge_close_and_cross_session_flags() {
    let tmp = TempDir::new("core");
    let service = new_service(&tmp);

    let m1 = service
        .receive(
            Envelope {
                from: "alice@example.com".into(),
                to: vec!["bob@example.com".into()],
            },
            b"From: Alice <alice@example.com>\r\nTo: bob@example.com\r\nSubject: Alpha Report\r\nX-Tag: urgent\r\n\r\nFirst message body\r\n",
        )
        .unwrap();
    let m2 = service
        .receive(
            Envelope {
                from: "carol@example.com".into(),
                to: vec!["bob@example.com".into()],
            },
            b"From: Carol <carol@example.com>\r\nTo: bob@example.com\r\nSubject: Beta Update\r\n\r\nSecond message body\r\n",
        )
        .unwrap();
    let m3 = service
        .receive(
            Envelope {
                from: "dave@example.com".into(),
                to: vec!["bob@example.com".into()],
            },
            b"From: Dave <dave@example.com>\r\nTo: bob@example.com\r\nSubject: Gamma Close\r\n\r\nThird\r\n",
        )
        .unwrap();

    // Shared ImapServer clone preserves the in-process FlagMap across sessions.
    let server = ImapServer::new(
        ImapConfig {
            auth_mode: "relaxed".into(),
            ..Default::default()
        },
        Arc::clone(&service),
    );

    // Session 1: login, LIST, STATUS, SELECT, FETCH/PEEK, SEARCH, STORE.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let handle = tokio::spawn(server.clone().handle_conn(server_io));
        let mut c = ImapClient::new(client_io);

        let greeting = c.read_line().await;
        assert!(greeting.starts_with("* OK [CAPABILITY IMAP4rev1"));

        c.send("A1 LOGIN dev anything").await;
        let (_, done) = c.read_until_tag("A1").await;
        assert!(done.starts_with("A1 OK"));

        c.send("A2 LIST \"\" \"\"").await;
        let (untagged, done) = c.read_until_tag("A2").await;
        assert!(untagged.contains("* LIST (\\Noselect) \"/\" \"\""));
        assert!(done.starts_with("A2 OK"));

        c.send("A3 LIST \"\" \"*\"").await;
        let (untagged, done) = c.read_until_tag("A3").await;
        assert!(untagged.contains("\"INBOX\""));
        assert!(done.starts_with("A3 OK"));

        c.send("A4 STATUS INBOX (MESSAGES UNSEEN UIDNEXT UIDVALIDITY)")
            .await;
        let (untagged, done) = c.read_until_tag("A4").await;
        assert!(untagged.contains("MESSAGES 3"));
        assert!(untagged.contains("UNSEEN 3"));
        assert!(untagged.contains("UIDNEXT 4"));
        assert!(untagged.contains("UIDVALIDITY 1"));
        assert!(done.starts_with("A4 OK"));

        c.send("A5 SELECT INBOX").await;
        let (untagged, done) = c.read_until_tag("A5").await;
        assert!(untagged.contains("* 3 EXISTS"));
        assert!(untagged.contains("[UIDNEXT 4]"));
        assert!(done.contains("[READ-WRITE]"));

        // BODY.PEEK[] must NOT mark \Seen.
        c.send("A6 FETCH 1 (UID FLAGS BODY.PEEK[HEADER.FIELDS (Subject X-Tag)])")
            .await;
        let (untagged, done) = c.read_until_tag("A6").await;
        assert!(untagged.contains("UID 1"));
        assert!(untagged.contains("FLAGS ()"));
        assert!(untagged.contains("Subject: Alpha Report"));
        assert!(untagged.contains("X-Tag: urgent"));
        assert!(!untagged.contains("First message body"));
        assert!(done.starts_with("A6 OK"));

        // Non-PEEK BODY[TEXT] partial MUST mark \Seen on message 1.
        c.send("A7 FETCH 1 (BODY[TEXT]<0.5>)").await;
        let (untagged, done) = c.read_until_tag("A7").await;
        assert!(untagged.contains("BODY[TEXT]<0> {5}"));
        assert!(untagged.contains("First"));
        assert!(done.starts_with("A7 OK"));

        // SEARCH criteria checks.
        c.send("A8 SEARCH SEEN").await;
        let (untagged, _) = c.read_until_tag("A8").await;
        assert_eq!(untagged.trim(), "* SEARCH 1");

        c.send("A9 SEARCH UNSEEN SUBJECT \"Beta\"").await;
        let (untagged, _) = c.read_until_tag("A9").await;
        assert_eq!(untagged.trim(), "* SEARCH 2");

        c.send("A10 UID SEARCH OR FROM \"alice\" BODY \"Second\"")
            .await;
        let (untagged, _) = c.read_until_tag("A10").await;
        assert_eq!(untagged.trim(), "* SEARCH 1 2");

        c.send("A10B SEARCH HEADER X-Tag \"urgent\" NOT FLAGGED")
            .await;
        let (untagged, _) = c.read_until_tag("A10B").await;
        assert_eq!(untagged.trim(), "* SEARCH 1");

        // ENVELOPE and FULL macro check.
        c.send("A11 UID FETCH 2 (ENVELOPE RFC822.SIZE INTERNALDATE)")
            .await;
        let (untagged, done) = c.read_until_tag("A11").await;
        assert!(untagged.contains("UID 2"));
        assert!(untagged.contains("\"Beta Update\""));
        assert!(untagged.contains("RFC822.SIZE "));
        assert!(untagged.contains("INTERNALDATE \""));
        assert!(done.starts_with("A11 OK"));

        // Flag message 2 as \Flagged and message 1 as \Deleted.
        c.send("A12 STORE 2 +FLAGS (\\Flagged)").await;
        let (untagged, done) = c.read_until_tag("A12").await;
        assert!(untagged.contains("FLAGS (\\Flagged)"));
        assert!(done.starts_with("A12 OK"));

        c.send("A13 UID STORE 1 +FLAGS.SILENT (\\Deleted)").await;
        let (untagged, done) = c.read_until_tag("A13").await;
        assert!(untagged.is_empty());
        assert!(done.starts_with("A13 OK"));

        c.send("A14 LOGOUT").await;
        let (untagged, done) = c.read_until_tag("A14").await;
        assert!(untagged.contains("* BYE"));
        assert!(done.starts_with("A14 OK"));
        drop(c);
        handle.await.unwrap();
    }

    // Session 2: verify flags persisted across sessions, EXAMINE refuses STORE/EXPUNGE,
    // EXPUNGE deletes message 1 while preserving UID 2 & 3, and CLOSE expunges message 3 silently.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let handle = tokio::spawn(server.clone().handle_conn(server_io));
        let mut c = ImapClient::new(client_io);
        let _ = c.read_line().await;

        let auth = BASE64.encode(b"\x00dev\x00any");
        c.send(&format!("B1 AUTHENTICATE PLAIN {auth}")).await;
        let (_, done) = c.read_until_tag("B1").await;
        assert!(done.starts_with("B1 OK"));

        // EXAMINE is read-only.
        c.send("B2 EXAMINE INBOX").await;
        let (_, done) = c.read_until_tag("B2").await;
        assert!(done.contains("[READ-ONLY]"));
        c.send("B3 EXPUNGE").await;
        let (_, done) = c.read_until_tag("B3").await;
        assert!(done.starts_with("B3 NO"));

        // SELECT read-write, verify \Flagged persisted on message 2, then EXPUNGE.
        c.send("B4 SELECT INBOX").await;
        let (_, done) = c.read_until_tag("B4").await;
        assert!(done.contains("[READ-WRITE]"));

        c.send("B5 SEARCH FLAGGED").await;
        let (untagged, _) = c.read_until_tag("B5").await;
        assert_eq!(untagged.trim(), "* SEARCH 2");

        c.send("B6 EXPUNGE").await;
        let (untagged, done) = c.read_until_tag("B6").await;
        assert_eq!(untagged.trim(), "* 1 EXPUNGE");
        assert!(done.starts_with("B6 OK"));

        // Remaining message 2 is now seq 1, but its UID is STILL 2!
        c.send("B7 FETCH 1 (UID FLAGS)").await;
        let (untagged, _) = c.read_until_tag("B7").await;
        assert!(untagged.contains("UID 2"), "untagged = {untagged}");
        assert!(untagged.contains("\\Flagged"), "untagged = {untagged}");

        // Mark seq 2 (UID 3) \Deleted and run CLOSE (must expunge silently).
        c.send("B8 STORE 2 +FLAGS.SILENT (\\Deleted)").await;
        let (_, done) = c.read_until_tag("B8").await;
        assert!(done.starts_with("B8 OK"));

        c.send("B9 CLOSE").await;
        let (untagged, done) = c.read_until_tag("B9").await;
        assert!(untagged.is_empty(), "CLOSE must not emit untagged EXPUNGE");
        assert!(done.starts_with("B9 OK"));

        c.send("B10 LOGOUT").await;
        let _ = c.read_until_tag("B10").await;
        drop(c);
        handle.await.unwrap();
    }

    assert!(service.get(&m1.id).unwrap().is_none());
    assert!(service.get(&m2.id).unwrap().is_some());
    assert!(service.get(&m3.id).unwrap().is_none());
}

#[tokio::test]
async fn imap_strict_auth_starttls_require_literal_and_imaps() {
    let tmp = TempDir::new("tls");
    let service = new_service(&tmp);
    service
        .receive(
            Envelope {
                from: "s@example.com".into(),
                to: vec!["r@example.com".into()],
            },
            b"Subject: Encrypted IMAP\r\n\r\nsecret content\r\n",
        )
        .unwrap();

    let tls = MailTls::self_signed(true).unwrap();
    let connector = connector_trusting(tls.certs_der());

    // 1. Plaintext IMAP with tls.require=true + strict auth + literal password.
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let server = ImapServer::new(
            ImapConfig {
                auth_mode: "strict".into(),
                username: "dev".into(),
                password: "p@ss".into(),
                ..Default::default()
            },
            Arc::clone(&service),
        )
        .with_tls(tls.clone());
        let handle = tokio::spawn(server.handle_conn(server_io));

        let mut plain = ImapClient::new(client_io);
        let greeting = plain.read_line().await;
        assert!(greeting.contains("STARTTLS"));
        assert!(greeting.contains("LOGINDISABLED"));

        // LOGIN before STARTTLS must be rejected with PRIVACYREQUIRED.
        plain.send("T1 LOGIN dev p@ss").await;
        let (_, done) = plain.read_until_tag("T1").await;
        assert!(done.starts_with("T1 NO") && done.contains("PRIVACYREQUIRED"));

        // Upgrade via STARTTLS.
        plain.send("T2 STARTTLS").await;
        let (_, done) = plain.read_until_tag("T2").await;
        assert!(done.starts_with("T2 OK"));

        let name = ServerName::try_from("localhost".to_string()).unwrap();
        let tls_stream = connector.connect(name, plain.into_inner()).await.unwrap();
        let mut tc = ImapClient::new(tls_stream);

        tc.send("T3 CAPABILITY").await;
        let (caps, done) = tc.read_until_tag("T3").await;
        assert!(!caps.contains("STARTTLS"));
        assert!(!caps.contains("LOGINDISABLED"));
        assert!(caps.contains("AUTH=PLAIN"));
        assert!(done.starts_with("T3 OK"));

        // Wrong password fails in strict mode.
        tc.send("T4 LOGIN dev wrong").await;
        let (_, done) = tc.read_until_tag("T4").await;
        assert!(done.starts_with("T4 NO"));

        // Synchronizing literal `{4}` for password succeeds.
        tc.send("T5 LOGIN dev {4}").await;
        let cont = tc.read_line().await;
        assert!(cont.starts_with('+'), "cont = {cont}");
        tc.send("p@ss").await;
        let (_, done) = tc.read_until_tag("T5").await;
        assert!(done.starts_with("T5 OK"), "done = {done}");

        tc.send("T6 SELECT INBOX").await;
        let (untagged, done) = tc.read_until_tag("T6").await;
        assert!(untagged.contains("* 1 EXISTS"));
        assert!(done.starts_with("T6 OK"));

        tc.send("T7 FETCH 1 (BODY[])").await;
        let (untagged, done) = tc.read_until_tag("T7").await;
        assert!(untagged.contains("secret content"));
        assert!(done.starts_with("T7 OK"));

        tc.send("T8 LOGOUT").await;
        let _ = tc.read_until_tag("T8").await;
        drop(tc);
        handle.await.unwrap();
    }

    // 2. Implicit TLS (IMAPS).
    {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let imaps = ImapServer::new(
            ImapConfig {
                auth_mode: "relaxed".into(),
                ..Default::default()
            },
            Arc::clone(&service),
        )
        .with_tls(tls);
        let handle = tokio::spawn(imaps.handle_tls_conn(server_io));

        let name = ServerName::try_from("localhost".to_string()).unwrap();
        let tls_stream = connector.connect(name, client_io).await.unwrap();
        let mut sc = ImapClient::new(tls_stream);
        let greeting = sc.read_line().await;
        assert!(greeting.starts_with("* OK"));
        assert!(!greeting.contains("STARTTLS"));
        sc.send("I1 LOGIN any any").await;
        let (_, done) = sc.read_until_tag("I1").await;
        assert!(done.starts_with("I1 OK"));
        sc.send("I2 STATUS INBOX (MESSAGES)").await;
        let (untagged, done) = sc.read_until_tag("I2").await;
        assert!(untagged.contains("MESSAGES 1"));
        assert!(done.starts_with("I2 OK"));
        sc.send("I3 LOGOUT").await;
        let _ = sc.read_until_tag("I3").await;
        drop(sc);
        handle.await.unwrap();
    }
}
