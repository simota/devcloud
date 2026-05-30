//! `devcloud-pubsub` binary: serves the Pub/Sub **REST** protocol on the
//! loopback address the Go daemon would have used, reading the same storage dirs
//! and writing byte-compatible `resources.json` / `pubsub.json`. The gRPC
//! protocol stays on the Go engine.
//!
//! Configuration comes from environment variables (set by the Go daemon seam in
//! `internal/app/pubsub_rust.go`):
//!
//!   DEVCLOUD_PUBSUB_REST_ADDR        REST listen address (host:port)
//!   DEVCLOUD_PUBSUB_PROJECT          default project
//!   DEVCLOUD_PUBSUB_AUTH_MODE        relaxed | oauth-relaxed | bearer-dev | strict
//!   DEVCLOUD_PUBSUB_BEARER_TOKEN     expected bearer token (strict/bearer-dev)
//!   DEVCLOUD_PUBSUB_STORAGE          resource storage dir (resources.json)
//!   DEVCLOUD_PUBSUB_MESSAGE_STORAGE  message storage dir (pubsub.json)
//!   DEVCLOUD_PUBSUB_DEFAULT_ACK_DEADLINE, _MAX_ACK_DEADLINE,
//!   DEVCLOUD_PUBSUB_MESSAGE_RETENTION, _MAX_PULL_MESSAGES

use std::sync::{Arc, Mutex};

use devcloud_pubsub::server::{Config, Server};

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
    let addr = env("DEVCLOUD_PUBSUB_REST_ADDR");
    if addr.is_empty() {
        eprintln!("devcloud-pubsub: DEVCLOUD_PUBSUB_REST_ADDR is required");
        std::process::exit(2);
    }
    let config = Config {
        project: env("DEVCLOUD_PUBSUB_PROJECT"),
        auth_mode: env("DEVCLOUD_PUBSUB_AUTH_MODE"),
        bearer_token: env("DEVCLOUD_PUBSUB_BEARER_TOKEN"),
        storage_path: env("DEVCLOUD_PUBSUB_STORAGE"),
        message_storage_path: env("DEVCLOUD_PUBSUB_MESSAGE_STORAGE"),
        default_ack_deadline_seconds: env_i64("DEVCLOUD_PUBSUB_DEFAULT_ACK_DEADLINE"),
        message_retention_seconds: env_i64("DEVCLOUD_PUBSUB_MESSAGE_RETENTION"),
        max_ack_deadline_seconds: env_i64("DEVCLOUD_PUBSUB_MAX_ACK_DEADLINE"),
        max_pull_messages: env_i64("DEVCLOUD_PUBSUB_MAX_PULL_MESSAGES"),
    };

    let server = Server::new(config);
    if let Some(err) = server.load_err() {
        eprintln!("devcloud-pubsub: failed to load state: {err}");
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
                eprintln!("devcloud-pubsub: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        let shared = Arc::new(Mutex::new(server));
        let shutdown = async {
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
        if let Err(e) = devcloud_pubsub::http::serve(listener, shared, shutdown).await {
            eprintln!("devcloud-pubsub: serve error: {e}");
            std::process::exit(1);
        }
    });
}
