//! Standalone Application Auto Scaling server binary for the strangler-fig seam.
//!
//! Config via environment (set by the legacy daemon):
//!   DEVCLOUD_AAS_ADDR        listen address, e.g. 127.0.0.1:8030 (required)
//!   DEVCLOUD_AAS_STORAGE     state.json directory                (required)
//!   DEVCLOUD_AAS_REGION      AWS region (default us-east-1)
//!   DEVCLOUD_AAS_ACCOUNT_ID  account id for ARNs (default 000000000000)
//!   DEVCLOUD_AAS_AUTH_MODE   relaxed | signed-relaxed | strict
//!   DEVCLOUD_AAS_ACCESS_KEY  strict-mode access key id
//!   DEVCLOUD_AAS_SECRET_KEY  strict-mode secret access key
//!
//! Exits 0 on SIGINT/SIGTERM after a graceful shutdown.

use std::sync::Arc;

use devcloud_applicationautoscaling::{http::serve, Config, Server};

fn env_required(key: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("devcloud-applicationautoscaling: missing required env var {key}");
            std::process::exit(2);
        }
    }
}

fn main() {
    let addr = env_required("DEVCLOUD_AAS_ADDR");
    let config = Config {
        addr: addr.clone(),
        region: std::env::var("DEVCLOUD_AAS_REGION").unwrap_or_default(),
        account_id: std::env::var("DEVCLOUD_AAS_ACCOUNT_ID").unwrap_or_default(),
        auth_mode: std::env::var("DEVCLOUD_AAS_AUTH_MODE").unwrap_or_default(),
        access_key_id: std::env::var("DEVCLOUD_AAS_ACCESS_KEY").unwrap_or_default(),
        secret_access_key: std::env::var("DEVCLOUD_AAS_SECRET_KEY").unwrap_or_default(),
        storage_path: env_required("DEVCLOUD_AAS_STORAGE"),
    };

    let server = Arc::new(Server::new(config));
    if let Some(err) = server.load_err() {
        eprintln!("devcloud-applicationautoscaling: load state failed: {err}");
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
                eprintln!("devcloud-applicationautoscaling: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        if let Err(e) = serve(listener, server, shutdown_signal()).await {
            eprintln!("devcloud-applicationautoscaling: server error: {e}");
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
