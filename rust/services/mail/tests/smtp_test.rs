//! 1:1 parity port of `internal/services/mail/smtp_test.go`.
//!
//! `net.Pipe` + goroutine → `tokio::io::duplex` + spawned task. The client
//! helper mirrors the Go `smtpTestClient`, including multiline-reply assembly.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use devcloud_mail::{RecordingStore, Service, SmtpConfig, SmtpServer};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tokio::task::JoinHandle;

fn relaxed_cfg(max: i64) -> SmtpConfig {
    SmtpConfig {
        max_message_bytes: max,
        ..Default::default()
    }
}

struct TestClient {
    stream: DuplexStream,
    buf: Vec<u8>,
}

impl TestClient {
    async fn send_line(&mut self, line: &str) {
        let s = format!("{}\r\n", line);
        self.stream.write_all(s.as_bytes()).await.expect("write");
    }

    /// Reads one logical reply line (CRLF stripped). Reads more bytes as needed.
    async fn read_raw_line(&mut self) -> String {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
                line.pop(); // '\n'
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return String::from_utf8_lossy(&line).into_owned();
            }
            let mut tmp = [0u8; 1024];
            let n = self.stream.read(&mut tmp).await.expect("read");
            if n == 0 {
                // EOF: return whatever remains.
                let line = std::mem::take(&mut self.buf);
                return String::from_utf8_lossy(&line).into_owned();
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Mirrors `smtpTestClient.readReply`: assemble multiline replies (`NNN-`).
    async fn read_reply(&mut self) -> String {
        let mut line = self.read_raw_line().await;
        let mut lines = vec![line.clone()];
        while line.len() >= 4 && line.as_bytes()[3] == b'-' {
            line = self.read_raw_line().await;
            lines.push(line.clone());
        }
        lines.join("\n")
    }

    async fn expect_reply(&mut self, prefix: &str) {
        let line = self.read_reply().await;
        assert!(
            line.starts_with(prefix),
            "reply = {:?}, want prefix {:?}",
            line,
            prefix
        );
    }

    async fn expect_reply_containing(&mut self, prefix: &str, want: &str) {
        let line = self.read_reply().await;
        assert!(
            line.starts_with(prefix) && line.contains(want),
            "reply = {:?}, want prefix {:?} and substring {:?}",
            line,
            prefix,
            want
        );
    }

    async fn close(self) {
        drop(self.stream);
    }
}

fn start_session(cfg: SmtpConfig, store: Arc<RecordingStore>) -> (TestClient, JoinHandle<()>) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let store = store as Arc<dyn devcloud_mail::Store>;
    let service = Arc::new(Service::new(store));
    let server = SmtpServer::new(cfg, service);
    let handle = tokio::spawn(async move {
        server.handle_conn(server_io).await;
    });
    (
        TestClient {
            stream: client_io,
            buf: Vec::new(),
        },
        handle,
    )
}

#[tokio::test]
async fn smtp_session_accepts_and_persists_message() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: Hello").await;
    c.send_line("").await;
    c.send_line("hello").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, raw) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].from, "sender@example.com");
    assert_eq!(messages[0].to, vec!["user@example.com".to_string()]);
    assert_eq!(messages[0].subject, "Hello");
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0], b"Subject: Hello\r\n\r\nhello\r\n");
}

#[tokio::test]
async fn smtp_session_accepts_helo_and_multiple_recipients() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("HELO localhost").await;
    c.expect_reply("250").await;
    c.send_line("NOOP").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<first@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<second@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: Team").await;
    c.send_line("").await;
    c.send_line("hello team").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].to,
        vec![
            "first@example.com".to_string(),
            "second@example.com".to_string()
        ]
    );
}

#[tokio::test]
async fn smtp_session_ehlo_advertises_size_limit() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(2048), store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply_containing("250", "SIZE 2048").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}

#[tokio::test]
async fn smtp_session_rejects_bad_sequence() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("503").await;
    c.send_line("DATA").await;
    c.expect_reply("503").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn smtp_session_rejects_oversize_message() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(8), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("0123456789").await;
    c.send_line(".").await;
    c.expect_reply("552").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn smtp_session_rejects_advertised_oversize_message() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(8), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com> SIZE=9").await;
    c.expect_reply("552").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("503").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn smtp_session_rejects_malformed_mail_size_parameter() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(8), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>SIZE=100").await;
    c.expect_reply("500").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("503").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn smtp_session_accepts_advertised_size_within_limit() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(64), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com> SIZE=16").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: Sized").await;
    c.send_line("").await;
    c.send_line("ok").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].subject, "Sized");
}

#[tokio::test]
async fn smtp_session_rejects_malformed_recipient_path() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com> extra").await;
    c.expect_reply("500").await;
    c.send_line("DATA").await;
    c.expect_reply("503").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn smtp_session_accepts_null_reverse_path() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: Bounce").await;
    c.send_line("").await;
    c.send_line("delivery status").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].from, "", "want null reverse-path");
    assert_eq!(messages[0].subject, "Bounce");
}

#[tokio::test]
async fn smtp_session_rset_clears_envelope() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<discarded@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RSET").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("503").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<kept@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: After reset").await;
    c.send_line("").await;
    c.send_line("body").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].to, vec!["kept@example.com".to_string()]);
}

#[tokio::test]
async fn smtp_session_rset_clears_null_reverse_path_state() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<>").await;
    c.expect_reply("250").await;
    c.send_line("RSET").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("503").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn smtp_session_rejects_rset_argument_without_clearing_envelope() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<kept@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RSET now").await;
    c.expect_reply("500").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: Kept").await;
    c.send_line("").await;
    c.send_line("body").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].to, vec!["kept@example.com".to_string()]);
}

#[tokio::test]
async fn smtp_session_data_resets_envelope() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<first@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: First").await;
    c.send_line("").await;
    c.send_line("body").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("503").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<second@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: Second").await;
    c.send_line("").await;
    c.send_line("body").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (messages, _) = store.snapshot();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].subject, "First");
    assert_eq!(messages[1].subject, "Second");
    assert_eq!(messages[1].to, vec!["second@example.com".to_string()]);
}

#[tokio::test]
async fn smtp_session_unstuffs_data_lines() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store.clone());

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("MAIL FROM:<sender@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("RCPT TO:<user@example.com>").await;
    c.expect_reply("250").await;
    c.send_line("DATA").await;
    c.expect_reply("354").await;
    c.send_line("Subject: Dot").await;
    c.send_line("").await;
    c.send_line("..starts with dot").await;
    c.send_line(".").await;
    c.expect_reply("250").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;

    let (_, raw) = store.snapshot();
    assert_eq!(raw.len(), 1);
    let body = String::from_utf8_lossy(&raw[0]);
    assert!(
        body.contains("\r\n.starts with dot\r\n"),
        "raw = {:?}",
        body
    );
}

#[tokio::test]
async fn smtp_ehlo_advertises_auth_when_enabled() {
    let store = Arc::new(RecordingStore::new());
    let cfg = SmtpConfig {
        max_message_bytes: 1024,
        auth_mode: "relaxed".to_string(),
        username: "dev".to_string(),
        password: "dev".to_string(),
        ..Default::default()
    };
    let (mut c, done) = start_session(cfg, store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply_containing("250", "AUTH PLAIN LOGIN").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}

#[tokio::test]
async fn smtp_ehlo_does_not_advertise_auth_when_off() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    let reply = c.read_reply().await;
    assert!(reply.starts_with("250"), "EHLO reply = {:?}", reply);
    assert!(
        !reply.contains("AUTH"),
        "EHLO advertised AUTH when mode is off: {:?}",
        reply
    );
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}

#[tokio::test]
async fn smtp_auth_off_rejects_auth_command() {
    let store = Arc::new(RecordingStore::new());
    let (mut c, done) = start_session(relaxed_cfg(1024), store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    let creds = BASE64.encode(b"\x00dev\x00dev");
    c.send_line(&format!("AUTH PLAIN {}", creds)).await;
    c.expect_reply("502").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}

#[tokio::test]
async fn smtp_auth_relaxed_accepts_any_credentials() {
    let store = Arc::new(RecordingStore::new());
    let cfg = SmtpConfig {
        max_message_bytes: 1024,
        auth_mode: "relaxed".to_string(),
        ..Default::default()
    };
    let (mut c, done) = start_session(cfg, store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    let plain = BASE64.encode(b"\x00anyone@example.com\x00anything-goes");
    c.send_line(&format!("AUTH PLAIN {}", plain)).await;
    c.expect_reply("235").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}

#[tokio::test]
async fn smtp_auth_strict_accepts_configured_credentials() {
    let store = Arc::new(RecordingStore::new());
    let cfg = SmtpConfig {
        max_message_bytes: 1024,
        auth_mode: "strict".to_string(),
        username: "configured".to_string(),
        password: "secret".to_string(),
        ..Default::default()
    };
    let (mut c, done) = start_session(cfg, store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    let plain = BASE64.encode(b"\x00configured\x00secret");
    c.send_line(&format!("AUTH PLAIN {}", plain)).await;
    c.expect_reply("235").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}

#[tokio::test]
async fn smtp_auth_strict_rejects_wrong_credentials() {
    let store = Arc::new(RecordingStore::new());
    let cfg = SmtpConfig {
        max_message_bytes: 1024,
        auth_mode: "strict".to_string(),
        username: "configured".to_string(),
        password: "secret".to_string(),
        ..Default::default()
    };
    let (mut c, done) = start_session(cfg, store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    let plain = BASE64.encode(b"\x00configured\x00wrong");
    c.send_line(&format!("AUTH PLAIN {}", plain)).await;
    c.expect_reply("535").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}

#[tokio::test]
async fn smtp_auth_login_challenge_response_flow() {
    let store = Arc::new(RecordingStore::new());
    let cfg = SmtpConfig {
        max_message_bytes: 1024,
        auth_mode: "strict".to_string(),
        username: "alice".to_string(),
        password: "pa55".to_string(),
        ..Default::default()
    };
    let (mut c, done) = start_session(cfg, store);

    c.expect_reply("220").await;
    c.send_line("EHLO localhost").await;
    c.expect_reply("250").await;
    c.send_line("AUTH LOGIN").await;
    c.expect_reply_containing("334", "VXNlcm5hbWU6").await; // base64("Username:")
    c.send_line(&BASE64.encode(b"alice")).await;
    c.expect_reply_containing("334", "UGFzc3dvcmQ6").await; // base64("Password:")
    c.send_line(&BASE64.encode(b"pa55")).await;
    c.expect_reply("235").await;
    c.send_line("QUIT").await;
    c.expect_reply("221").await;
    c.close().await;
    let _ = done.await;
}
