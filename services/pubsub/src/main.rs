//! `devcloud-pubsub` binary: serves BOTH the Pub/Sub **gRPC** and **REST**
//! protocols, sharing one in-memory state behind an `Arc<Mutex<Server>>` — the
//! same way the legacy daemon shares one `*Server` between its gRPC adapter and REST
//! handlers. State persists to byte-compatible `resources.json` / `pubsub.json`.
//!
//! Configuration comes from environment variables (set by the legacy daemon seam in
//! `internal/app/pubsub_rust.rs`):
//!
//!   DEVCLOUD_PUBSUB_REST_ADDR        REST listen address (host:port)
//!   DEVCLOUD_PUBSUB_GRPC_ADDR        gRPC listen address (host:port)
//!   DEVCLOUD_PUBSUB_PROJECT          default project
//!   DEVCLOUD_PUBSUB_AUTH_MODE        relaxed | oauth-relaxed | bearer-dev | strict
//!   DEVCLOUD_PUBSUB_BEARER_TOKEN     expected bearer token (strict/bearer-dev)
//!   DEVCLOUD_PUBSUB_STORAGE          resource storage dir (resources.json)
//!   DEVCLOUD_PUBSUB_MESSAGE_STORAGE  message storage dir (pubsub.json)
//!   DEVCLOUD_PUBSUB_DEFAULT_ACK_DEADLINE, _MAX_ACK_DEADLINE,
//!   DEVCLOUD_PUBSUB_MESSAGE_RETENTION, _MAX_PULL_MESSAGES

use std::sync::{Arc, Mutex};

use devcloud_pubsub::grpc::PubSubGrpc;
use devcloud_pubsub::proto::pubsub::{
    publisher_server::PublisherServer, schema_service_server::SchemaServiceServer,
    subscriber_server::SubscriberServer,
};
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
    let rest_addr = env("DEVCLOUD_PUBSUB_REST_ADDR");
    let grpc_addr = env("DEVCLOUD_PUBSUB_GRPC_ADDR");
    if rest_addr.is_empty() && grpc_addr.is_empty() {
        eprintln!(
            "devcloud-pubsub: at least one of DEVCLOUD_PUBSUB_REST_ADDR / \
             DEVCLOUD_PUBSUB_GRPC_ADDR is required"
        );
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
        streaming_pull_disabled: env("DEVCLOUD_PUBSUB_STREAMING_PULL_DISABLED") == "1",
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
        // The gRPC and REST tasks share ONE state (mirrors the legacy single *Server).
        let shared = Arc::new(Mutex::new(server));

        let rest_handle = if rest_addr.is_empty() {
            None
        } else {
            let listener = match tokio::net::TcpListener::bind(&rest_addr).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("devcloud-pubsub: bind REST {rest_addr}: {e}");
                    std::process::exit(1);
                }
            };
            let state = shared.clone();
            Some(tokio::spawn(async move {
                let shutdown = wait_for_signal();
                devcloud_pubsub::http::serve(listener, state, shutdown)
                    .await
                    .map_err(|e| format!("REST serve error: {e}"))
            }))
        };

        let grpc_handle = if grpc_addr.is_empty() {
            None
        } else {
            let addr: std::net::SocketAddr = match grpc_addr.parse() {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("devcloud-pubsub: parse gRPC addr {grpc_addr}: {e}");
                    std::process::exit(1);
                }
            };
            let adapter = PubSubGrpc::new(shared.clone());
            Some(tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(PublisherServer::new(adapter.clone()))
                    .add_service(SubscriberServer::new(adapter.clone()))
                    .add_service(SchemaServiceServer::new(adapter))
                    .serve_with_shutdown(addr, wait_for_signal())
                    .await
                    .map_err(|e| format!("gRPC serve error: {e}"))
            }))
        };

        // Wait for whichever task finishes/errors first.
        let result = match (rest_handle, grpc_handle) {
            (Some(rest), Some(grpc)) => {
                tokio::select! {
                    r = rest => r.unwrap_or_else(|e| Err(e.to_string())),
                    g = grpc => g.unwrap_or_else(|e| Err(e.to_string())),
                }
            }
            (Some(rest), None) => rest.await.unwrap_or_else(|e| Err(e.to_string())),
            (None, Some(grpc)) => grpc.await.unwrap_or_else(|e| Err(e.to_string())),
            (None, None) => Ok(()),
        };
        if let Err(e) = result {
            eprintln!("devcloud-pubsub: {e}");
            std::process::exit(1);
        }
    });
}

/// Resolves when SIGTERM/SIGINT (or Ctrl-C on non-unix) arrives.
async fn wait_for_signal() {
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
}
