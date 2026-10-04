use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_mail::{Envelope, FileBlobStore, FileStore, Service};
use devcloud_mailbox::config::Config;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        // Parallel tests can read the same clock value (macOS is microsecond
        // granular), so a per-process counter keeps the names unique.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "mailbox-ephemeral-tests-{}-{nonce}-{n}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn service(root: &std::path::Path) -> Service {
    Service::new(Arc::new(FileStore::new(
        root.join("mail"),
        Arc::new(FileBlobStore::new(root.join("blobs"))),
    )))
}

fn receive(service: &Service) {
    service
        .receive(
            Envelope {
                from: "sender@example.test".into(),
                to: vec!["recipient@example.test".into()],
            },
            b"Subject: ephemeral\r\n\r\nprivate-body-marker\r\n",
        )
        .unwrap();
}

#[test]
fn ephemeral_storage_is_private_fresh_and_removed_without_touching_persistent_storage() {
    let directory = TestDirectory::new();
    let persistent = directory.0.join("persistent");
    std::fs::create_dir_all(persistent.join("mail")).unwrap();
    let metadata = persistent.join("mail/messages.jsonl");
    std::fs::write(&metadata, b"corrupt persistent metadata\n").unwrap();
    let config = Config::from_lookup(|key| match key {
        "DEVCLOUD_MAILBOX_STORAGE" => Some(persistent.to_string_lossy().into_owned()),
        "DEVCLOUD_MAILBOX_EPHEMERAL" => Some("true".into()),
        _ => None,
    })
    .unwrap();
    let storage = config.prepare_storage().unwrap();
    let first_root = storage.path().to_path_buf();
    assert!(!first_root.starts_with(&directory.0));
    assert!(first_root.join("mail").is_dir());
    assert!(first_root.join("blobs").is_dir());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&first_root).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    let inbox = service(&first_root);
    receive(&inbox);
    assert_eq!(inbox.list_all().unwrap().len(), 1);
    assert!(first_root.join("mail/messages.jsonl").is_file());
    assert_eq!(
        std::fs::read_dir(first_root.join("blobs")).unwrap().count(),
        1
    );
    assert_eq!(
        std::fs::read(&metadata).unwrap(),
        b"corrupt persistent metadata\n"
    );
    assert!(!persistent.join("blobs").exists());

    let second = config.prepare_storage().unwrap();
    let second_root = second.path().to_path_buf();
    assert_ne!(first_root, second_root);
    assert!(service(&second_root).list_all().unwrap().is_empty());
    drop(storage);
    assert!(!first_root.exists());
    second.cleanup().unwrap();
    assert!(!second_root.exists());
}

#[test]
fn persistent_storage_survives_cleanup_and_reload() {
    let directory = TestDirectory::new();
    let config = Config::from_lookup(|key| {
        (key == "DEVCLOUD_MAILBOX_STORAGE").then(|| directory.0.to_string_lossy().into_owned())
    })
    .unwrap();
    let storage = config.prepare_storage().unwrap();
    assert_eq!(storage.path(), directory.0);
    receive(&service(storage.path()));
    storage.cleanup().unwrap();
    let reloaded = config.prepare_storage().unwrap();
    assert_eq!(service(reloaded.path()).list_all().unwrap().len(), 1);
    drop(reloaded);
    assert!(directory.0.join("mail/messages.jsonl").is_file());
}

#[cfg(unix)]
mod signals {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    use super::TestDirectory;

    struct Mailbox(Child);

    impl Drop for Mailbox {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn messages(addr: SocketAddr) -> std::io::Result<serde_json::Value> {
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500))?;
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        stream.set_write_timeout(Some(Duration::from_secs(1)))?;
        stream.write_all(b"GET /api/v2/messages HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
        let mut wire = String::new();
        stream.read_to_string(&mut wire)?;
        assert!(wire.starts_with("HTTP/1.1 200 "));
        Ok(serde_json::from_str(wire.split_once("\r\n\r\n").unwrap().1).unwrap())
    }

    fn start(temp: &Path, persistent: &Path, log: &Path) -> (Mailbox, SocketAddr, SocketAddr) {
        let smtp = TcpListener::bind("127.0.0.1:0").unwrap();
        let http = TcpListener::bind("127.0.0.1:0").unwrap();
        let smtp_addr = smtp.local_addr().unwrap();
        let http_addr = http.local_addr().unwrap();
        drop((smtp, http));
        let mut mailbox = Mailbox(
            Command::new(env!("CARGO_BIN_EXE_devcloud-mailbox"))
                .env_clear()
                .env("TMPDIR", temp)
                .env("TMP", temp)
                .env("TEMP", temp)
                .env("DEVCLOUD_MAILBOX_EPHEMERAL", "true")
                .env("DEVCLOUD_MAILBOX_STORAGE", persistent)
                .env("DEVCLOUD_MAILBOX_SMTP_ADDR", smtp_addr.to_string())
                .env("DEVCLOUD_MAILBOX_HTTP_ADDR", http_addr.to_string())
                .env("DEVCLOUD_MAILBOX_USERNAME", "private-username-marker")
                .env("DEVCLOUD_MAILBOX_PASSWORD", "private-password-marker")
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(log).unwrap())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                mailbox.0.try_wait().unwrap().is_none(),
                "mailbox exited during startup"
            );
            if let Ok(value) = messages(http_addr) {
                assert_eq!(value["total"], 0);
                return (mailbox, smtp_addr, http_addr);
            }
            assert!(Instant::now() < deadline, "mailbox startup timed out");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn send_mail(addr: SocketAddr) {
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).unwrap();
        assert!(line.starts_with("220 "));
        for (command, code) in [
            ("HELO client.example\r\n", "250 "),
            ("MAIL FROM:<sender@example.test>\r\n", "250 "),
            ("RCPT TO:<recipient@example.test>\r\n", "250 "),
            ("DATA\r\n", "354 "),
            (
                "Subject: ephemeral\r\n\r\nprivate-body-marker\r\n.\r\n",
                "250 ",
            ),
            ("QUIT\r\n", "221 "),
        ] {
            stream.get_mut().write_all(command.as_bytes()).unwrap();
            line.clear();
            stream.read_line(&mut line).unwrap();
            assert!(line.starts_with(code), "unexpected SMTP reply: {line}");
        }
    }

    fn stop(mailbox: &mut Mailbox, signal: &str) {
        assert!(Command::new("kill")
            .args([&format!("-{signal}"), &mailbox.0.id().to_string()])
            .status()
            .unwrap()
            .success());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = mailbox.0.try_wait().unwrap() {
                assert_eq!(status.code(), Some(0));
                return;
            }
            assert!(Instant::now() < deadline, "mailbox shutdown timed out");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn binary_uses_temp_storage_and_removes_it_on_sigterm_and_sigint() {
        let directory = TestDirectory::new();
        let temp = directory.0.join("temporary");
        std::fs::create_dir(&temp).unwrap();
        let persistent = directory.0.join("configured-storage");
        std::fs::write(&persistent, b"persistent sentinel").unwrap();
        let log = directory.0.join("startup.log");
        let mut previous_root = None;
        for signal in ["TERM", "INT"] {
            let (mut mailbox, smtp, http) = start(&temp, &persistent, &log);
            let roots: Vec<_> = std::fs::read_dir(&temp)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect();
            assert_eq!(roots.len(), 1);
            let root = &roots[0];
            assert_ne!(previous_root.as_ref(), Some(root));
            send_mail(smtp);
            assert_eq!(messages(http).unwrap()["total"], 1);
            assert!(root.join("mail/messages.jsonl").is_file());
            assert_eq!(std::fs::read_dir(root.join("blobs")).unwrap().count(), 1);
            stop(&mut mailbox, signal);
            assert!(!root.exists());
            assert_eq!(std::fs::read_dir(&temp).unwrap().count(), 0);
            assert_eq!(std::fs::read(&persistent).unwrap(), b"persistent sentinel");
            let log = std::fs::read_to_string(&log).unwrap();
            assert_eq!(log.lines().count(), 1);
            assert!(log.contains("ephemeral=true"));
            assert!(log.contains("username=<masked> password=<masked>"));
            for omitted in [
                "private-username-marker",
                "private-password-marker",
                "private-body-marker",
                root.to_str().unwrap(),
            ] {
                assert!(!log.contains(omitted));
            }
            previous_root = Some(root.clone());
        }
    }
}
