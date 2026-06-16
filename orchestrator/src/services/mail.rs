//! mail service, run in-process as a supervisor task.
//!
//! Runs BOTH the SMTP server (`smtp_port`) and the dashboard HTTP
//! introspect/control surface (`mail_http_port`) over one shared `Arc<Service>`,
//! mirroring the legacy daemon (`mail.NewServer` SMTP + `mail.NewHTTPServer`). The
//! HTTP surface is what the dashboard mail page reads from. Storage layout
//! matches the legacy seam: metadata under `<storage>/mail`, blobs under
//! `<storage>/blobs`, byte-compatible with the legacy on-disk format.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_mail::{BlobStore, FileBlobStore, FileStore, Service, SmtpConfig, SmtpServer, Store};
use tokio::net::TcpListener;

use crate::config::Config;
use crate::supervisor::shutdown_future;

pub async fn run(
    cfg: &Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    let root = Path::new(&cfg.storage.path);
    let blobs: Arc<dyn BlobStore> = Arc::new(FileBlobStore::new(root.join("blobs")));
    let store: Arc<dyn Store> = Arc::new(FileStore::new(root.join("mail"), blobs));
    let service = Arc::new(Service::new(store));

    let smtp = Arc::new(SmtpServer::new(
        SmtpConfig {
            addr: format!("127.0.0.1:{}", cfg.server.smtp_port),
            max_message_bytes: cfg.services.mail.max_message_bytes,
            auth_mode: cfg.auth.smtp.mode.clone(),
            username: cfg.auth.smtp.username.clone(),
            password: cfg.auth.smtp.password.clone(),
        },
        service.clone(),
    ));

    // One inner shutdown fanned out to the SMTP loop, the HTTP loop, and the
    // outer select.
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = tx.send(true);
    });

    // SMTP task (binds internally from SmtpConfig.addr; no shutdown arg — select
    // it against the inner shutdown below).
    let smtp_task = {
        let s = smtp.clone();
        tokio::spawn(async move { s.run().await.map_err(|e| format!("mail SMTP: {e}")) })
    };

    // Dashboard HTTP introspect/control surface.
    let http_addr = format!("127.0.0.1:{}", cfg.server.mail_http_port);
    let listener = TcpListener::bind(&http_addr)
        .await
        .map_err(|e| format!("mail: bind http {http_addr}: {e}"))?;
    let http_sd = shutdown_future(rx.clone());
    let svc = service.clone();
    let auth_mode = cfg.auth.smtp.mode.clone();
    let username = cfg.auth.smtp.username.clone();
    let password = cfg.auth.smtp.password.clone();
    let http_task = tokio::spawn(async move {
        devcloud_mail::serve_http(listener, svc, auth_mode, username, password, http_sd).await
    });

    tokio::select! {
        _ = shutdown_future(rx.clone()) => Ok(()),
        r = smtp_task => r.map_err(|e| e.to_string()).and_then(|x| x),
        r = http_task => r.map_err(|e| e.to_string()).and_then(|x| x),
    }
}
