//! Standalone SQS server binary for the strangler-fig seam (JSON protocol).
//!
//! Config via environment (set by the legacy daemon):
//!   DEVCLOUD_SQS_ADDR            listen address, e.g. 127.0.0.1:19324 (required)
//!   DEVCLOUD_SQS_STORAGE         state.json directory                (required)
//!   DEVCLOUD_SQS_REGION          AWS region (default us-east-1)
//!   DEVCLOUD_SQS_ACCOUNT_ID      account id for ARNs (default 000000000000)
//!   DEVCLOUD_SQS_QUEUE_URL_HOST  host[:port] used in queue URLs
//!   DEVCLOUD_SQS_AUTH_MODE       relaxed | strict
//!   DEVCLOUD_SQS_ACCESS_KEY / DEVCLOUD_SQS_SECRET_KEY  strict-mode credentials
//!   DEVCLOUD_SQS_MAX_*           numeric limits / defaults (optional)
//!
//! Exits 0 on SIGINT/SIGTERM after a graceful shutdown.

use std::sync::{Arc, Mutex};

use devcloud_sqs::{http, Config, Server};

fn env_required(key: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("devcloud-sqs: missing required env var {key}");
            std::process::exit(2);
        }
    }
}

fn env_i64(key: &str) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn main() {
    let addr = env_required("DEVCLOUD_SQS_ADDR");
    let config = Config {
        addr: addr.clone(),
        region: std::env::var("DEVCLOUD_SQS_REGION").unwrap_or_default(),
        account_id: std::env::var("DEVCLOUD_SQS_ACCOUNT_ID").unwrap_or_default(),
        queue_url_host: std::env::var("DEVCLOUD_SQS_QUEUE_URL_HOST").unwrap_or_default(),
        auth_mode: std::env::var("DEVCLOUD_SQS_AUTH_MODE").unwrap_or_default(),
        access_key_id: std::env::var("DEVCLOUD_SQS_ACCESS_KEY").unwrap_or_default(),
        secret_access_key: std::env::var("DEVCLOUD_SQS_SECRET_KEY").unwrap_or_default(),
        storage_path: env_required("DEVCLOUD_SQS_STORAGE"),
        max_queues: env_i64("DEVCLOUD_SQS_MAX_QUEUES"),
        max_message_bytes: env_i64("DEVCLOUD_SQS_MAX_MESSAGE_BYTES"),
        max_receive_batch_size: env_i64("DEVCLOUD_SQS_MAX_RECEIVE_BATCH_SIZE"),
        default_visibility_timeout_seconds: env_i64("DEVCLOUD_SQS_DEFAULT_VISIBILITY_TIMEOUT"),
        default_delay_seconds: env_i64("DEVCLOUD_SQS_DEFAULT_DELAY"),
        default_message_retention_seconds: env_i64("DEVCLOUD_SQS_DEFAULT_RETENTION"),
        default_receive_wait_time_seconds: env_i64("DEVCLOUD_SQS_DEFAULT_WAIT_TIME"),
    };

    let server = Server::new(config);
    if let Some(err) = server.load_err() {
        eprintln!("devcloud-sqs: load state failed: {err}");
        std::process::exit(1);
    }
    let server = Arc::new(Mutex::new(server));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-sqs: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        if let Err(e) = http::serve(listener, server, shutdown_signal()).await {
            eprintln!("devcloud-sqs: server error: {e}");
            std::process::exit(1);
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
