//! Standalone SMTP server binary for the strangler-fig seam.
//!
//! The legacy daemon launches this as a subprocess on the same `127.0.0.1:<port>`
//! the in-process legacy SMTP server would have used, pointed at the same storage
//! paths. Because the Rust `FileStore`/`FileBlobStore` write a byte-compatible
//! on-disk format, the legacy dashboard keeps reading messages transparently.
//!
//! Configuration is passed via environment variables (set by the daemon):
//!   DEVCLOUD_MAIL_ADDR        listen address, e.g. 127.0.0.1:11025 (required)
//!   DEVCLOUD_MAIL_STORAGE     mail metadata dir (messages.jsonl)   (required)
//!   DEVCLOUD_MAIL_BLOBS       blob store dir                       (required)
//!   DEVCLOUD_MAIL_MAX_BYTES   max message bytes (i64, 0 = no limit)
//!   DEVCLOUD_MAIL_AUTH_MODE   off | relaxed | strict
//!   DEVCLOUD_MAIL_USERNAME    strict-mode username
//!   DEVCLOUD_MAIL_PASSWORD    strict-mode password
//!
//! On SIGINT/SIGTERM the accept loop ends and the process exits 0, mirroring the
//! legacy server's context-cancellation shutdown.

use std::sync::Arc;

use devcloud_mail::{BlobStore, FileBlobStore, FileStore, Service, SmtpConfig, SmtpServer, Store};

fn env_required(key: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("devcloud-mail: missing required env var {key}");
            std::process::exit(2);
        }
    }
}

fn main() {
    let addr = env_required("DEVCLOUD_MAIL_ADDR");
    let storage = env_required("DEVCLOUD_MAIL_STORAGE");
    let blobs_dir = env_required("DEVCLOUD_MAIL_BLOBS");
    let max_bytes: i64 = std::env::var("DEVCLOUD_MAIL_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let auth_mode = std::env::var("DEVCLOUD_MAIL_AUTH_MODE").unwrap_or_default();
    let username = std::env::var("DEVCLOUD_MAIL_USERNAME").unwrap_or_default();
    let password = std::env::var("DEVCLOUD_MAIL_PASSWORD").unwrap_or_default();

    let blobs: Arc<dyn BlobStore> = Arc::new(FileBlobStore::new(blobs_dir));
    let store: Arc<dyn Store> = Arc::new(FileStore::new(storage, blobs));
    let service = Arc::new(Service::new(store));
    let server = Arc::new(SmtpServer::new(
        SmtpConfig {
            addr: addr.clone(),
            max_message_bytes: max_bytes,
            auth_mode,
            username,
            password,
        },
        service,
    ));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        tokio::select! {
            res = server.run() => {
                if let Err(e) = res {
                    eprintln!("devcloud-mail: server error: {e}");
                    std::process::exit(1);
                }
            }
            _ = shutdown_signal() => {
                // Graceful: drop the accept loop and exit 0.
            }
        }
    });
}

/// Resolves when SIGINT or SIGTERM arrives, mirroring the legacy daemon's
/// context-cancellation shutdown.
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
