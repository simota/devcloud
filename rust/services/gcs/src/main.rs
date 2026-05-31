//! `devcloud-gcs` binary: serves the GCS JSON API on the loopback address the Go
//! daemon would have used, reading/writing the shared S3 bucket store.
//!
//! Configuration comes from environment variables:
//!
//!   DEVCLOUD_GCS_ADDR          listen address (host:port)
//!   DEVCLOUD_GCS_PROJECT       default project
//!   DEVCLOUD_GCS_LOCATION      default bucket location
//!   DEVCLOUD_GCS_AUTH_MODE     relaxed | oauth-relaxed | bearer-dev
//!   DEVCLOUD_GCS_BEARER_TOKEN  expected bearer token for bearer-dev
//!   DEVCLOUD_GCS_STORAGE       shared bucket storage root
//!   DEVCLOUD_GCS_UPLOAD_SESSIONS resumable upload session storage

use std::sync::{Arc, Mutex};

use devcloud_gcs::http::{Config, Server};
use devcloud_s3::store::FileBucketStore;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn main() {
    let addr = env("DEVCLOUD_GCS_ADDR");
    if addr.is_empty() {
        eprintln!("devcloud-gcs: DEVCLOUD_GCS_ADDR is required");
        std::process::exit(2);
    }
    let storage = env("DEVCLOUD_GCS_STORAGE");
    if storage.is_empty() {
        eprintln!("devcloud-gcs: DEVCLOUD_GCS_STORAGE is required");
        std::process::exit(2);
    }
    let server = Server::new(
        Config {
            project: env("DEVCLOUD_GCS_PROJECT"),
            location: env("DEVCLOUD_GCS_LOCATION"),
            auth_mode: env("DEVCLOUD_GCS_AUTH_MODE"),
            bearer_token: env("DEVCLOUD_GCS_BEARER_TOKEN"),
            upload_session_path: env("DEVCLOUD_GCS_UPLOAD_SESSIONS"),
        },
        FileBucketStore::new(storage),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-gcs: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        let shared = Arc::new(Mutex::new(server));
        if let Err(e) = devcloud_gcs::http::serve(listener, shared, shutdown_signal()).await {
            eprintln!("devcloud-gcs: serve error: {e}");
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
