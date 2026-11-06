use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_mail::http::HttpAuth;
use devcloud_mail::{Message, SmtpConfig};

pub struct Config {
    pub smtp_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub storage: PathBuf,
    pub ephemeral: bool,
    pub max_bytes: i64,
    pub auth: HttpAuth,
    pub hostname: String,
    pub allowed_hosts: Vec<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let var = |suffix: &str, default: &str| {
            get(&format!("DEVCLOUD_MAILBOX_{suffix}")).unwrap_or_else(|| default.into())
        };
        let smtp_addr = var("SMTP_ADDR", "127.0.0.1:1025")
            .parse()
            .map_err(|_| "Invalid DEVCLOUD_MAILBOX_SMTP_ADDR")?;
        let http_addr = var("HTTP_ADDR", "127.0.0.1:8025")
            .parse()
            .map_err(|_| "Invalid DEVCLOUD_MAILBOX_HTTP_ADDR")?;
        let max_bytes = var("MAX_BYTES", "10485760")
            .parse::<i64>()
            .ok()
            .filter(|&n| n >= 0)
            .ok_or("Invalid DEVCLOUD_MAILBOX_MAX_BYTES")?;
        let mode = var("AUTH_MODE", "relaxed").trim().to_ascii_lowercase();
        if !matches!(mode.as_str(), "off" | "relaxed" | "strict") {
            return Err("Invalid DEVCLOUD_MAILBOX_AUTH_MODE; use off, relaxed or strict".into());
        }
        let auth = HttpAuth {
            auth_mode: mode,
            username: var("USERNAME", ""),
            password: var("PASSWORD", ""),
        };
        if auth.is_strict() && (auth.username.is_empty() || auth.password.is_empty()) {
            return Err(
                "Strict mode requires DEVCLOUD_MAILBOX_USERNAME and DEVCLOUD_MAILBOX_PASSWORD"
                    .into(),
            );
        }
        let hostname = var("HOSTNAME", "mailhog.example");
        if hostname.is_empty()
            || hostname.chars().count() > 255
            || hostname
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
        {
            return Err("Invalid DEVCLOUD_MAILBOX_HOSTNAME".into());
        }
        let allowed_hosts = var("ALLOWED_HOSTS", "")
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        Ok(Self {
            smtp_addr,
            http_addr,
            storage: PathBuf::from(var("STORAGE", "/data")),
            ephemeral: matches!(
                var("EPHEMERAL", "").to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            ),
            max_bytes,
            auth,
            hostname,
            allowed_hosts,
        })
    }

    pub fn smtp_config(&self) -> SmtpConfig {
        SmtpConfig {
            addr: self.smtp_addr.to_string(),
            max_message_bytes: self.max_bytes,
            auth_mode: self.auth.auth_mode.clone(),
            username: self.auth.username.clone(),
            password: self.auth.password.clone(),
        }
    }

    pub fn prepare_storage(&self) -> Result<PreparedStorage, String> {
        let storage = PreparedStorage {
            root: if self.ephemeral {
                create_temp_storage()?
            } else {
                self.storage.clone()
            },
            ephemeral: self.ephemeral,
        };
        probe(&storage.root.join("mail"))?;
        probe(&storage.root.join("blobs"))?;
        check_log(&storage.root.join("mail/messages.jsonl"))?;
        Ok(storage)
    }
}

#[derive(Debug)]
pub struct PreparedStorage {
    root: PathBuf,
    ephemeral: bool,
}

impl PreparedStorage {
    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn cleanup(mut self) -> Result<(), String> {
        if !self.ephemeral {
            return Ok(());
        }
        self.ephemeral = false;
        fs::remove_dir_all(&self.root).map_err(|_| {
            "Cannot remove ephemeral storage; check temporary directory permissions".into()
        })
    }
}

impl Drop for PreparedStorage {
    fn drop(&mut self) {
        // Also clean up when startup fails before the runtime is serving mail.
        if self.ephemeral && fs::remove_dir_all(&self.root).is_err() {
            eprintln!("devcloud-mailbox: cannot remove ephemeral storage; check temporary directory permissions");
        }
    }
}

fn create_temp_storage() -> Result<PathBuf, String> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let failure =
        || "Cannot create private temporary storage; check temporary directory permissions";
    for _ in 0..100 {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "devcloud-mailbox-{}-{nonce}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let builder = fs::DirBuilder::new();
        #[cfg(unix)]
        let builder = {
            use std::os::unix::fs::DirBuilderExt;
            let mut builder = builder;
            builder.mode(0o700);
            builder
        };
        match builder.create(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(failure().into()),
        }
    }
    Err(failure().into())
}

fn uid() -> String {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn probe(directory: &Path) -> Result<(), String> {
    let failure = || {
        format!("Storage directory {} is not writable by uid {}; make the bind mount writable and owned by this uid (Docker uid 10001), or choose DEVCLOUD_MAILBOX_STORAGE", directory.display(), uid())
    };
    fs::create_dir_all(directory).map_err(|_| failure())?;
    let path = directory.join(format!(".mailbox-write-probe-{}", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|_| failure())?;
    let result = file.write_all(b"probe").and_then(|_| file.sync_all());
    drop(file);
    let cleanup = fs::remove_file(path);
    result.and(cleanup).map_err(|_| failure())
}

pub fn check_log(path: &Path) -> Result<(), String> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("Cannot read mail/messages.jsonl; check storage permissions".into()),
    };
    let lines: Vec<&[u8]> = data.split(|&b| b == b'\n').collect();
    let last = lines.len() - 1;
    for (line_number, line) in lines.into_iter().enumerate() {
        if line.is_empty() {
            continue;
        }
        // An unterminated last line is a torn append, which the store
        // ignores and repairs; only complete lines must parse.
        if line_number == last {
            continue;
        }
        if serde_json::from_slice::<Message>(line).is_err() {
            return Err(format!(
                "Corrupt mail/messages.jsonl at line {}; restore or remove the metadata log",
                line_number + 1
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_torn_last_record_does_not_block_startup_but_a_corrupt_one_does() {
        let dir = std::env::temp_dir().join(format!(
            "devcloud-mailbox-checklog-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("messages.jsonl");
        let ok = r#"{"id":"m1","from":"a@x","to":[],"subject":"s","raw":"r"}"#;
        std::fs::write(&path, format!("{ok}\n{{\"id\":\"torn")).unwrap();
        assert!(super::check_log(&path).is_ok());
        std::fs::write(&path, format!("{{broken\n{ok}\n")).unwrap();
        assert!(super::check_log(&path).unwrap_err().contains("line 1"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    use super::Config;

    #[test]
    fn ephemeral_accepts_only_case_insensitive_truthy_values() {
        for value in ["1", "true", "TRUE", "tRuE", "yes", "YES", "yEs"] {
            let config = Config::from_lookup(|key| {
                (key == "DEVCLOUD_MAILBOX_EPHEMERAL").then(|| value.into())
            })
            .unwrap();
            assert!(config.ephemeral, "{value}");
        }
    }

    #[test]
    fn ephemeral_defaults_to_persistent_for_every_other_value() {
        assert!(!Config::from_lookup(|_| None).unwrap().ephemeral);
        for value in ["", "0", "false", "FALSE", "no", "on", "2", " true", "yes "] {
            let config = Config::from_lookup(|key| {
                (key == "DEVCLOUD_MAILBOX_EPHEMERAL").then(|| value.into())
            })
            .unwrap();
            assert!(!config.ephemeral, "{value}");
        }
    }
}
