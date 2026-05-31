//! `devcloud-redis` binary: owns a local `redis-server` child process for the
//! Go daemon's opt-in Rust seam.
//!
//! This intentionally does not implement RESP. Protocol parity stays delegated
//! to upstream `redis-server`, matching the Go managed Redis design.

use std::process::Stdio;

use devcloud_redis::Config;
use tokio::process::Command;

fn main() {
    let config = match Config::from_env() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("devcloud-redis: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = config.validate() {
        eprintln!("devcloud-redis: {e}");
        std::process::exit(2);
    }
    if let Err(e) = std::fs::create_dir_all(&config.data_dir) {
        eprintln!("devcloud-redis: create data directory: {e}");
        std::process::exit(1);
    }
    let args = match config.args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("devcloud-redis: {e}");
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    runtime.block_on(async move {
        let mut child = match Command::new(&config.binary)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                eprintln!("devcloud-redis: start redis-server {}: {e}", config.binary);
                std::process::exit(1);
            }
        };

        tokio::select! {
            status = child.wait() => {
                match status {
                    Ok(status) if status.success() => {}
                    Ok(status) => {
                        eprintln!("devcloud-redis: redis-server exited with {status}");
                        std::process::exit(1);
                    }
                    Err(e) => {
                        eprintln!("devcloud-redis: wait for redis-server: {e}");
                        std::process::exit(1);
                    }
                }
            }
            _ = shutdown_signal() => {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
        }
    });
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
