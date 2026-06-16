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
    let mode = cfg.services.redis.mode.trim().to_lowercase();
    if !mode.is_empty() {
        return mode;
    }
    if !cfg.services.redis.external_url.trim().is_empty() {
        return "external".to_string();
    }
    "managed".to_string()
}

pub async fn run(cfg: &AppConfig, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    // External mode: the orchestrator does not own a redis process; the control
    // surface talks to the user-supplied URL. Nothing to manage here.
    if redis_mode(cfg) == "external" {
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
            let _ = child.start_kill();
            let _ = child.wait().await;
            Ok(())
        }
    }
}
