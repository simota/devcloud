//! redis data plane: manages a local `redis-server` child process in-process.
//!
//! Mirrors the legacy `startRedisBackend` + `redisServerConfig` managed path and the
//! `devcloud-redis` crate's binary. `redis-server` is a genuinely external
//! program (RESP parity is delegated upstream), so this is the one allowed
//! subprocess in the single-binary orchestrator. The redis HTTP control surface
//! is a separate listener (see `services::redis_control`).

use std::future::Future;
use std::process::Stdio;

use devcloud_redis::Config;
use tokio::process::Command;

use crate::config::Config as AppConfig;
use crate::services::util::scoped_data_dir;

/// legacy `redisMode`: explicit mode wins; else external when an external URL is set;
/// else managed.
fn redis_mode(cfg: &AppConfig) -> String {
    let mode = config_text(&cfg.services.redis.mode).to_lowercase();
    if !mode.is_empty() {
        return mode;
    }
    if !config_text(&cfg.services.redis.external_url).is_empty() {
        return "external".to_string();
    }
    "managed".to_string()
}

/// One resolved upstream, shared by lifecycle, control-plane wiring and display.
/// The upstream password is independent of the control HTTP Basic password.
pub struct ConnectionTarget {
    pub mode: String,
    pub addr: String,
    pub password: String,
    pub database: i64,
}

impl ConnectionTarget {
    pub fn display_endpoint(&self) -> String {
        if self.mode == "external" {
            format!("redis://{}/{}", self.addr, self.database)
        } else {
            format!("redis://{}", self.addr)
        }
    }
}

fn config_text(value: &str) -> &str {
    let value = value.trim();
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value)
}

pub fn connection_target(cfg: &AppConfig) -> Result<ConnectionTarget, String> {
    match redis_mode(cfg).as_str() {
        "external" => parse_external_url(config_text(&cfg.services.redis.external_url)),
        "managed" => Ok(ConnectionTarget {
            mode: "managed".into(),
            addr: format!("127.0.0.1:{}", cfg.server.redis_port),
            password: if cfg.auth.redis.mode.trim().eq_ignore_ascii_case("strict") {
                cfg.auth.redis.password.clone()
            } else {
                String::new()
            },
            database: 0,
        }),
        _ => Err("redis: services.redis.mode must be managed or external".into()),
    }
}

/// Redis's supported local URL form: redis://[:password@]host[:port][/db].
/// Non-default ACL users and TLS are outside this emulator's Redis contract;
/// reject them explicitly rather than connecting with different credentials.
fn parse_external_url(raw: &str) -> Result<ConnectionTarget, String> {
    let invalid = |reason: &str| format!("redis: invalid services.redis.externalUrl: {reason}");
    let rest = raw
        .strip_prefix("redis://")
        .ok_or_else(|| invalid("expected a redis:// URL"))?;
    if rest.contains(['?', '#']) {
        return Err(invalid("query parameters and fragments are unsupported"));
    }
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let database = if path.is_empty() {
        0
    } else {
        if !path.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid("database must be a non-negative integer"));
        }
        path.parse::<i64>()
            .map_err(|_| invalid("database is out of range"))?
    };
    let (authority, password) = match authority.rsplit_once('@') {
        Some((userinfo, authority)) => {
            let (username, password) = userinfo
                .split_once(':')
                .ok_or_else(|| invalid("credentials must use :password@host"))?;
            let username = decode_url_component(username)
                .ok_or_else(|| invalid("credentials contain an invalid percent escape"))?;
            if !username.is_empty() && username != "default" {
                return Err(invalid("only the default Redis ACL user is supported"));
            }
            let password = decode_url_component(password)
                .ok_or_else(|| invalid("credentials contain an invalid percent escape"))?;
            (authority, password)
        }
        None => (authority, String::new()),
    };
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| invalid("IPv6 hosts must be enclosed in brackets"))?;
        host.parse::<std::net::Ipv6Addr>()
            .map_err(|_| invalid("invalid IPv6 host"))?;
        let port = if suffix.is_empty() {
            "6379"
        } else {
            suffix
                .strip_prefix(':')
                .ok_or_else(|| invalid("invalid host or port"))?
        };
        (format!("[{host}]"), port)
    } else {
        let (host, port) = authority.split_once(':').unwrap_or((authority, "6379"));
        if host.is_empty()
            || host.chars().any(|c| {
                c.is_whitespace() || c.is_control() || matches!(c, '@' | '[' | ']' | '\\' | '%')
            })
        {
            return Err(invalid("invalid host"));
        }
        (host.to_string(), port)
    };
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| invalid("port must be an integer between 1 and 65535"))?;
    Ok(ConnectionTarget {
        mode: "external".into(),
        addr: format!("{host}:{port}"),
        password,
        database,
    })
}

fn decode_url_component(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let high = (*bytes.get(i + 1)? as char).to_digit(16)?;
            let low = (*bytes.get(i + 2)? as char).to_digit(16)?;
            output.push((high * 16 + low) as u8);
            i += 3;
        } else {
            output.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(output).ok()
}

pub async fn run(cfg: &AppConfig, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    // External mode: the orchestrator does not own a redis process; the control
    // surface talks to the user-supplied URL. Nothing to manage here.
    let target = connection_target(cfg)?;
    if target.mode == "external" {
        let result = devcloud_redis_control::validate_upstream(
            &target.addr,
            &target.password,
            target.database,
        )
        .await;
        result.map_err(|error| {
            let error = if target.password.is_empty() {
                error
            } else {
                error.replace(&target.password, "redacted")
            };
            format!("redis: validate external Redis connection: {error}")
        })?;
        shutdown.await;
        return Ok(());
    }

    let rc = Config {
        addr: format!("127.0.0.1:{}", cfg.server.redis_port),
        data_dir: scoped_data_dir(&cfg.storage.path, &cfg.services.redis.data_dir, "redis").into(),
        binary: cfg.services.redis.binary_path.clone(),
        max_memory_mb: cfg.services.redis.max_memory_mb.max(0) as u64,
        append_only: cfg.services.redis.append_only,
        auth_mode: cfg.auth.redis.mode.clone(),
        password: cfg.auth.redis.password.clone(),
    }
    .normalized();
    rc.validate()?;

    if let Err(e) = std::fs::create_dir_all(&rc.data_dir) {
        return Err(format!("redis: create data directory: {e}"));
    }
    let args = rc.args()?;

    let mut child = match Command::new(&rc.binary)
        .args(&args)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            // Mirror the legacy daemon: a missing redis-server disables Redis with a
            // warning rather than taking the whole daemon down.
            eprintln!(
                "devcloud: Redis disabled: start redis-server {}: {e}",
                rc.binary
            );
            shutdown.await;
            return Ok(());
        }
    };

    tokio::select! {
        status = child.wait() => match status {
            Ok(s) if s.success() => Ok(()),
            Ok(s) => Err(format!("redis: redis-server exited with {s}")),
            Err(e) => Err(format!("redis: wait for redis-server: {e}")),
        },
        _ = shutdown => {
            devcloud_redis::stop_child(&mut child)
                .await
                .map_err(|e| format!("redis: stop redis-server: {e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_target_uses_url_credentials_and_database() {
        let mut cfg = AppConfig::default();
        cfg.services.redis.mode = "external".into();
        cfg.services.redis.external_url = "\"redis://default:p%40ss%3Aword@[::1]:6380/3\"".into();
        cfg.auth.redis.mode = "strict".into();
        cfg.auth.redis.password = "control-only-secret".into();
        let target = connection_target(&cfg).unwrap();
        assert_eq!(target.addr, "[::1]:6380");
        assert_eq!(target.password, "p@ss:word");
        assert_eq!(target.database, 3);
        assert_eq!(target.display_endpoint(), "redis://[::1]:6380/3");

        cfg.services.redis.mode.clear();
        cfg.services.redis.external_url = "redis://localhost".into();
        let target = connection_target(&cfg).unwrap();
        assert_eq!(target.mode, "external");
        assert_eq!(target.addr, "localhost:6379");
        assert!(target.password.is_empty());
        assert_eq!(target.database, 0);
    }

    #[test]
    fn external_target_rejects_invalid_urls_without_disclosing_credentials() {
        for raw in [
            "",
            "https://:secret@example.test:6379",
            "rediss://:secret@example.test:6379",
            "redis://:secret@",
            "redis://:secret@host:0",
            "redis://:secret@host:65536",
            "redis://:secret@host/-1",
            "redis://:secret@host/9223372036854775808",
            "redis://:secret@host/0?db=1",
            "redis://:secret@host/0#fragment",
            "redis://other:secret@host/0",
            "redis://:secret%あ@host/0",
            "redis://:secret%zz@host/0",
            "redis://:secret@[not-an-ip]/0",
        ] {
            let error = match parse_external_url(raw) {
                Ok(_) => panic!("invalid external URL was accepted"),
                Err(error) => error,
            };
            assert!(!error.contains("secret"), "credential leaked in error");
        }
    }

    #[test]
    fn managed_target_auth_matches_the_managed_server() {
        let mut cfg = AppConfig::default();
        cfg.auth.redis.password = "control-secret".into();
        let target = connection_target(&cfg).unwrap();
        assert_eq!(target.addr, "127.0.0.1:16379");
        assert!(
            target.password.is_empty(),
            "relaxed Redis has no requirepass"
        );
        cfg.auth.redis.mode = "strict".into();
        assert_eq!(connection_target(&cfg).unwrap().password, "control-secret");
        cfg.services.redis.mode = "typo".into();
        assert!(connection_target(&cfg).is_err());
    }

    #[cfg(unix)]
    mod lifecycle {
        use super::*;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::path::PathBuf;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Duration;
        use tokio::sync::oneshot;

        extern "C" {
            fn kill(pid: i32, signal: i32) -> i32;
        }

        struct FakeRedis {
            root: PathBuf,
            binary: PathBuf,
        }

        impl FakeRedis {
            fn new(term_handler: &str) -> Self {
                static NEXT_ID: AtomicU64 = AtomicU64::new(0);
                let root = std::env::temp_dir().join(format!(
                    "devcloud-redis-lifecycle-{}-{}",
                    std::process::id(),
                    NEXT_ID.fetch_add(1, Ordering::Relaxed)
                ));
                fs::create_dir_all(&root).unwrap();
                let binary = root.join("redis-server");
                let body = if term_handler.is_empty() {
                    // Ignored signals survive exec, retaining this PID without
                    // busy-looping throughout the five-second shutdown grace.
                    "exec sleep 30"
                } else {
                    "while :; do :; done"
                };
                fs::write(
                    &binary,
                    format!(
                        "#!/bin/sh\ntrap '{term_handler}' TERM\nprintf '%s\\n' \"$$\" > \"$0.pid\"\n{body}\n"
                    ),
                )
                .unwrap();
                fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
                Self { root, binary }
            }

            fn config(&self) -> AppConfig {
                let mut cfg = AppConfig::default();
                cfg.storage.path = self.root.join("data").to_string_lossy().into_owned();
                cfg.services.redis.binary_path = self.binary.to_string_lossy().into_owned();
                cfg
            }

            fn marker(&self, name: &str) -> PathBuf {
                self.binary.with_extension(name)
            }

            async fn ready(&self) -> i32 {
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        if let Some(pid) = fs::read_to_string(self.marker("pid"))
                            .ok()
                            .and_then(|value| value.trim().parse::<i32>().ok())
                        {
                            return pid;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("fake redis never became ready")
            }

            async fn assert_reaped(&self, pid: i32) {
                tokio::time::timeout(Duration::from_secs(3), async {
                    // SAFETY: signal 0 only checks the process we just spawned.
                    while unsafe { kill(pid, 0) } == 0 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("managed redis child was not stopped and reaped");
                // Disarm emergency cleanup once the PID is no longer ours.
                fs::remove_file(self.marker("pid")).unwrap();
            }
        }

        impl Drop for FakeRedis {
            fn drop(&mut self) {
                if let Some(pid) = fs::read_to_string(self.marker("pid"))
                    .ok()
                    .and_then(|value| value.trim().parse::<i32>().ok())
                {
                    // SAFETY: emergency cleanup of this test's child after a
                    // failed assertion; successful checks remove the PID file.
                    unsafe { kill(pid, 9) };
                }
                let _ = fs::remove_dir_all(&self.root);
            }
        }

        #[tokio::test]
        async fn managed_shutdown_sends_sigterm_and_reaps() {
            let fake = FakeRedis::new("printf saved > \"$0.saved\"; exit 0");
            let cfg = fake.config();
            let (tx, rx) = oneshot::channel();
            let mut running = Box::pin(run(&cfg, async {
                let _ = rx.await;
            }));
            let pid = tokio::select! {
                pid = fake.ready() => pid,
                result = &mut running => panic!("redis exited before readiness: {result:?}"),
            };
            tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), running)
                .await
                .expect("graceful redis shutdown timed out")
                .unwrap();
            assert_eq!(fs::read(fake.marker("saved")).unwrap(), b"saved");
            fake.assert_reaped(pid).await;
        }

        #[tokio::test]
        async fn managed_shutdown_kills_and_reaps_a_term_ignoring_child() {
            let fake = FakeRedis::new("");
            let cfg = fake.config();
            let (tx, rx) = oneshot::channel();
            let mut running = Box::pin(run(&cfg, async {
                let _ = rx.await;
            }));
            let pid = tokio::select! {
                pid = fake.ready() => pid,
                result = &mut running => panic!("redis exited before readiness: {result:?}"),
            };
            tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(8), running)
                .await
                .expect("unresponsive redis blocked shutdown")
                .unwrap();
            fake.assert_reaped(pid).await;
        }

        #[tokio::test]
        async fn cancelling_managed_redis_stops_and_reaps_its_child() {
            // Check both cancellation during service operation and while its
            // SIGTERM grace period is in progress.
            for during_shutdown in [false, true] {
                let fake = FakeRedis::new("printf received > \"$0.term\"");
                let cfg = fake.config();
                let (tx, rx) = oneshot::channel();
                let mut running = Box::pin(run(&cfg, async {
                    let _ = rx.await;
                }));
                let pid = tokio::select! {
                    pid = fake.ready() => pid,
                    result = &mut running => panic!("redis exited before readiness: {result:?}"),
                };
                if during_shutdown {
                    tx.send(()).unwrap();
                    let received_term = async {
                        while !fake.marker("term").exists() {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    };
                    tokio::select! {
                        result = tokio::time::timeout(Duration::from_secs(3), received_term) => {
                            result.expect("redis never received SIGTERM");
                        }
                        result = &mut running => panic!("redis exited during its grace period: {result:?}"),
                    }
                }
                drop(running);
                fake.assert_reaped(pid).await;
            }
        }
    }
}
