//! Standalone dashboard server binary for the legacy-to-Rust dashboard migration
//! (Phase 2 foundation). Opt-in: launched by the legacy daemon seam
//! (`internal/app/dashboard_rust.rs`) only when
//! `DEVCLOUD_DASHBOARD_ENGINE=rust`, fully replacing the in-process legacy dashboard.
//!
//! Config via environment (set by the legacy daemon):
//!   DEVCLOUD_DASHBOARD_ADDR          listen address, e.g. 127.0.0.1:18025 (required)
//!   DEVCLOUD_DASHBOARD_EVENT_RELAY   ws base for the event relay (e.g. ws://127.0.0.1:18027)
//!   DEVCLOUD_DASHBOARD_SQS_BASE      SQS service HTTP base (http://127.0.0.1:19324)
//!   DEVCLOUD_DASHBOARD_<SVC>_BASE    other service bases (later increments)
//!   DEVCLOUD_DASHBOARD_<SVC>_ENDPOINT/_STORAGE/...  registry display metadata
//!
//! Exits 0 on SIGINT/SIGTERM after a graceful shutdown.

use std::sync::Arc;

use devcloud_dashboard::{http, Config};

fn main() {
    let config = Config::from_env();
    if config.addr.is_empty() {
        eprintln!("devcloud-dashboard: missing required env var DEVCLOUD_DASHBOARD_ADDR");
        std::process::exit(2);
    }
    let addr = config.addr.clone();
    let config = Arc::new(config);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-dashboard: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        if let Err(e) = http::serve(listener, config, shutdown_signal()).await {
            eprintln!("devcloud-dashboard: server error: {e}");
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
