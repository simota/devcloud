use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use devcloud_mail::{
    Envelope, FileBlobStore, FileStore, ImapConfig, ImapServer, Pop3Config, Pop3Server, Service,
    SmtpConfig, SmtpServer,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

const ACCOUNTS: &str = r#"[
    {"username":"alice","password":"alice-pass","address":"alice@example.test",
     "allowedSenders":["billing@example.test"]},
    {"username":"bob","password":"bob-pass","address":"bob@example.test"}
]"#;

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!("mail-accounts-{}-{nanos}", std::process::id())))
    }

    fn store(&self) -> Arc<FileStore> {
        Arc::new(FileStore::new(
            self.0.join("mail"),
            Arc::new(FileBlobStore::new(self.0.join("blobs"))),
        ))
    }

    fn service(&self, mode: &str) -> Arc<Service> {
        Arc::new(Service::with_accounts(self.store(), mode, ACCOUNTS).unwrap())
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Client(BufReader<DuplexStream>);

impl Client {
    fn pair() -> (Self, DuplexStream) {
        let (client, server) = tokio::io::duplex(8192);
        (Self(BufReader::new(client)), server)
    }

    async fn send(&mut self, command: &str) {
        self.0
            .get_mut()
            .write_all(format!("{command}\r\n").as_bytes())
            .await
            .unwrap();
    }

    async fn line(&mut self) -> String {
        let mut line = String::new();
        let bytes = tokio::time::timeout(Duration::from_secs(5), self.0.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(bytes > 0, "connection closed before reply");
        line
    }

    async fn command(&mut self, command: &str, prefix: &str) -> String {
        self.send(command).await;
        let line = self.line().await;
        assert!(
            line.starts_with(prefix),
            "expected {prefix:?}, received {line:?}"
        );
        line
    }

    async fn until(&mut self, prefix: &str) -> String {
        let mut response = String::new();
        loop {
            let line = self.line().await;
            response.push_str(&line);
            if line.starts_with(prefix) {
                return response;
            }
        }
    }

    async fn imap(&mut self, tag: &str, command: &str) -> String {
        self.send(&format!("{tag} {command}")).await;
        self.until(&format!("{tag} ")).await
    }
}

async fn smtp(service: Arc<Service>, mode: &str) -> Client {
    let server = SmtpServer::new(
        SmtpConfig {
            auth_mode: mode.into(),
            username: "legacy".into(),
            password: "legacy-pass".into(),
            ..Default::default()
        },
        service,
    );
    let (mut client, io) = Client::pair();
    tokio::spawn(async move { server.handle_conn(io).await });
    assert!(client.line().await.starts_with("220 "));
    client.command("HELO localhost", "250 ").await;
    client
}

async fn imap(service: Arc<Service>) -> Client {
    let server = ImapServer::new(
        ImapConfig {
            auth_mode: "strict".into(),
            ..Default::default()
        },
        service,
    );
    let (mut client, io) = Client::pair();
    tokio::spawn(async move { server.handle_conn(io).await });
    assert!(client.line().await.starts_with("* OK "));
    client
}

async fn pop3(service: Arc<Service>) -> Client {
    let server = Pop3Server::new(
        Pop3Config {
            auth_mode: "strict".into(),
            ..Default::default()
        },
        service,
    );
    let (mut client, io) = Client::pair();
    tokio::spawn(async move { server.handle_conn(io).await });
    assert!(client.line().await.starts_with("+OK"));
    client
}

#[tokio::test]
async fn strict_sender_permissions_follow_the_authenticated_smtp_identity() {
    let tmp = Workspace::new();
    let service = tmp.service("strict");
    let mut alice = smtp(service.clone(), " StRiCt ").await;
    alice
        .command("MAIL FROM:<alice@example.test>", "530 5.7.0")
        .await;
    for credentials in [
        b"bob\0alice\0alice-pass".as_slice(),
        b"\0legacy\0legacy-pass",
        b"\0alice\0wrong",
    ] {
        alice
            .command(
                &format!("AUTH PLAIN {}", BASE64.encode(credentials)),
                "535 ",
            )
            .await;
    }
    alice
        .command(
            &format!("AUTH PLAIN {}", BASE64.encode(b"alice\0alice\0alice-pass")),
            "235 ",
        )
        .await;
    alice
        .command("MAIL FROM:<billing@EXAMPLE.TEST> SIZE=22", "250 ")
        .await;
    alice.command("RCPT TO:<bob@example.test>", "250 ").await;
    alice
        .command("MAIL FROM:<bob@example.test>", "550 5.7.1")
        .await;
    alice.command("DATA", "503 ").await;
    alice
        .command("MAIL FROM:<Alice@example.test>", "550 5.7.1")
        .await;
    alice.command("MAIL FROM:<>", "250 ").await;
    alice.command("RSET", "250 ").await;
    alice
        .command("MAIL FROM:<alice@example.test>", "250 ")
        .await;

    let mut bob = smtp(service, "strict").await;
    bob.command("AUTH LOGIN", "334 ").await;
    bob.command(&BASE64.encode("bob"), "334 ").await;
    bob.command(&BASE64.encode("bob-pass"), "235 ").await;
    bob.command("MAIL FROM:<billing@example.test>", "550 5.7.1")
        .await;
    bob.command("MAIL FROM:<bob@example.test>", "250 ").await;
}

#[tokio::test]
async fn smtp_delivery_and_imap_pop3_mutations_are_isolated_across_restarts() {
    let tmp = Workspace::new();
    let service = tmp.service("strict");
    service
        .receive(Envelope::default(), b"Subject: Legacy\r\n\r\nshared")
        .unwrap();
    let mut submission = smtp(service.clone(), "strict").await;
    submission
        .command(
            &format!("AUTH PLAIN {}", BASE64.encode(b"\0alice\0alice-pass")),
            "235 ",
        )
        .await;
    submission
        .command("MAIL FROM:<alice@example.test>", "250 ")
        .await;
    submission
        .command("RCPT TO:<missing@example.test>", "550 5.1.1")
        .await;
    submission
        .command("RCPT TO:<bob@EXAMPLE.TEST>", "250 ")
        .await;
    submission
        .command("RCPT TO:<bob@example.test>", "250 ")
        .await;
    submission.command("DATA", "354 ").await;
    submission
        .command(
            "To: alice@example.test\r\nSubject: Bob only\r\n\r\nprivate\r\n.",
            "250 ",
        )
        .await;
    submission
        .command("MAIL FROM:<alice@example.test>", "250 ")
        .await;
    for recipient in ["alice@example.test", "bob@example.test"] {
        submission
            .command(&format!("RCPT TO:<{recipient}>"), "250 ")
            .await;
    }
    submission.command("DATA", "354 ").await;
    submission
        .command("Subject: Two copies\r\n\r\nshared delivery\r\n.", "250 ")
        .await;
    submission.command("QUIT", "221 ").await;
    assert_eq!(service.list_all().unwrap().len(), 1);
    service.delete_all().unwrap();
    drop(service);
    let reopened = tmp.service("strict");

    let mut alice = imap(reopened.clone()).await;
    assert!(alice
        .imap("a1", "LOGIN alice alice-pass")
        .await
        .contains("a1 OK"));
    assert!(alice
        .imap("a2", "SELECT INBOX")
        .await
        .contains("* 1 EXISTS"));
    let fetched = alice.imap("a3", "UID FETCH 1:* (BODY.PEEK[])").await;
    assert!(fetched.contains("Two copies") && !fetched.contains("Bob only"));
    assert!(alice.imap("a4", "CREATE Private").await.contains("a4 OK"));
    assert!(alice
        .imap("a5", r"UID STORE 1 +FLAGS (\Deleted)")
        .await
        .contains("a5 OK"));
    assert!(alice.imap("a6", "EXPUNGE").await.contains("* 1 EXPUNGE"));

    let mut bob = imap(reopened.clone()).await;
    let other = BASE64.encode(b"alice\0bob\0bob-pass");
    assert!(bob
        .imap("b1", &format!("AUTHENTICATE PLAIN {other}"))
        .await
        .contains("b1 NO"));
    let own = BASE64.encode(b"\0bob\0bob-pass");
    assert!(bob
        .imap("b2", &format!("AUTHENTICATE PLAIN {own}"))
        .await
        .contains("b2 OK"));
    assert!(!bob.imap("b3", r#"LIST "" "*""#).await.contains("Private"));
    assert!(bob.imap("b4", "SELECT INBOX").await.contains("* 2 EXISTS"));

    let mut bob_pop = pop3(reopened.clone()).await;
    bob_pop
        .command(&format!("AUTH PLAIN {other}"), "-ERR")
        .await;
    bob_pop.command("USER bob", "+OK").await;
    bob_pop
        .command("PASS bob-pass", "+OK mailbox ready (2 messages)")
        .await;
    bob_pop.command("RETR 1", "+OK").await;
    assert!(bob_pop.until(".\r\n").await.contains("Subject: Bob only"));
    bob_pop.command("DELE 1", "+OK").await;
    bob_pop.command("QUIT", "+OK").await;
    assert!(bob.imap("b5", "NOOP").await.contains("* 1 EXPUNGE"));

    let mut alice_pop = pop3(reopened).await;
    let plain = BASE64.encode(b"\0alice\0alice-pass");
    alice_pop
        .command(
            &format!("AUTH PLAIN {plain}"),
            "+OK mailbox ready (0 messages)",
        )
        .await;
}

#[tokio::test]
async fn relaxed_and_off_modes_keep_the_shared_unrestricted_inbox() {
    for mode in ["relaxed", "off"] {
        let tmp = Workspace::new();
        let service = tmp.service(mode);
        let mut c = smtp(service.clone(), mode).await;
        if mode == "relaxed" {
            c.command(
                &format!("AUTH PLAIN {}", BASE64.encode(b"ignored\0unknown\0wrong")),
                "235 ",
            )
            .await;
        }
        c.command("MAIL FROM:<anywhere@example.test>", "250 ").await;
        c.command("RCPT TO:<unknown@example.test>", "250 ").await;
        c.command("DATA", "354 ").await;
        c.command("Subject: Shared\r\n\r\nbody\r\n.", "250 ").await;
        assert_eq!(service.list_mailbox().unwrap().messages.len(), 1);
    }
}

#[test]
fn strict_account_validation_does_not_echo_configuration_values() {
    let tmp = Workspace::new();
    let duplicate = ACCOUNTS.replace("bob@example.test", "alice@EXAMPLE.TEST");
    for invalid in [duplicate, "secret-sentinel".into()] {
        let error = Service::with_accounts(tmp.store(), "strict", &invalid)
            .err()
            .unwrap();
        assert!(!error.contains("alice-pass") && !error.contains("secret-sentinel"));
        assert!(Service::with_accounts(tmp.store(), "relaxed", &invalid).is_ok());
    }
}
