//! `devcloud-dynamodb` binary: serves the AWS JSON 1.0 DynamoDB protocol on the
//! loopback address the Go daemon would have used, reading the same storage dir
//! and writing a byte-compatible `state.json`.
//!
//! Configuration comes entirely from environment variables (set by the Go daemon
//! seam in `internal/app/dynamodb_rust.go`):
//!
//!   DEVCLOUD_DYNAMODB_ADDR          listen address (host:port)
//!   DEVCLOUD_DYNAMODB_STORAGE       storage directory (state.json lives here)
//!   DEVCLOUD_DYNAMODB_REGION        AWS region
//!   DEVCLOUD_DYNAMODB_AUTH_MODE     relaxed | signed-relaxed | strict
//!   DEVCLOUD_DYNAMODB_ACCESS_KEY    SigV4 access key id
//!   DEVCLOUD_DYNAMODB_SECRET_KEY    SigV4 secret access key
//!   DEVCLOUD_DYNAMODB_MAX_ITEM_BYTES, DEVCLOUD_DYNAMODB_MAX_TABLES

use std::sync::{Arc, Mutex};

use devcloud_dynamodb::server::{Config, Server};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn env_i64(name: &str) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn main() {
    let addr = env("DEVCLOUD_DYNAMODB_ADDR");
    if addr.is_empty() {
        eprintln!("devcloud-dynamodb: DEVCLOUD_DYNAMODB_ADDR is required");
        std::process::exit(2);
    }
    let auth_mode = env("DEVCLOUD_DYNAMODB_AUTH_MODE");
    let config = Config {
        addr: addr.clone(),
        region: env("DEVCLOUD_DYNAMODB_REGION"),
        auth_mode: auth_mode.clone(),
        access_key_id: env("DEVCLOUD_DYNAMODB_ACCESS_KEY"),
        secret_access_key: env("DEVCLOUD_DYNAMODB_SECRET_KEY"),
        storage_path: env("DEVCLOUD_DYNAMODB_STORAGE"),
        max_item_bytes: env_i64("DEVCLOUD_DYNAMODB_MAX_ITEM_BYTES"),
        max_tables: env_i64("DEVCLOUD_DYNAMODB_MAX_TABLES"),
    };

    let server = Server::new(config);
    if let Some(err) = server.load_err() {
        eprintln!("devcloud-dynamodb: failed to load state: {err}");
        std::process::exit(1);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-dynamodb: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        let shared = Arc::new(Mutex::new(server));
        let shutdown = async {
            // Graceful shutdown on SIGTERM (sent by the Go daemon) or Ctrl-C.
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut term = signal(SignalKind::terminate()).expect("sigterm");
                let mut int = signal(SignalKind::interrupt()).expect("sigint");
                tokio::select! {
                    _ = term.recv() => {},
                    _ = int.recv() => {},
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
        };
        if let Err(e) = devcloud_dynamodb::http::serve(listener, shared, auth_mode, shutdown).await
        {
            eprintln!("devcloud-dynamodb: serve error: {e}");
            std::process::exit(1);
        }
    });
}
