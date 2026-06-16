//! `devcloud-s3` binary: serves the current S3 Rust increment over HTTP.
//!
//! Configuration comes from environment variables:
//!
//!   DEVCLOUD_S3_ADDR     listen address (host:port), default 127.0.0.1:0
//!   DEVCLOUD_S3_STORAGE  bucket storage root, required
//!   DEVCLOUD_S3_AUTH_MODE relaxed (default) or strict
//!   DEVCLOUD_S3_ACCESS_KEY_ID / DEVCLOUD_S3_SECRET_ACCESS_KEY strict-mode creds
//!   DEVCLOUD_S3_REGION    SigV4 region

use std::sync::{Arc, Mutex};

use devcloud_s3::http::AuthConfig;
use devcloud_s3::store::FileBucketStore;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn main() {
    let addr = std::env::var("DEVCLOUD_S3_ADDR").unwrap_or_else(|_| "127.0.0.1:0".to_string());
    let storage = env("DEVCLOUD_S3_STORAGE");
    if storage.is_empty() {
        eprintln!("devcloud-s3: DEVCLOUD_S3_STORAGE is required");
        std::process::exit(2);
    }
    let store = Arc::new(Mutex::new(FileBucketStore::new(storage)));
    let auth = AuthConfig {
        auth_mode: env("DEVCLOUD_S3_AUTH_MODE"),
        access_key_id: env("DEVCLOUD_S3_ACCESS_KEY_ID"),
        secret_access_key: env("DEVCLOUD_S3_SECRET_ACCESS_KEY"),
        region: env("DEVCLOUD_S3_REGION"),
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-s3: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        if let Err(e) =
            devcloud_s3::http::serve_with_auth(listener, store, auth, shutdown_signal()).await
        {
            eprintln!("devcloud-s3: serve error: {e}");
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
