//! mail service, run in-process as a supervisor task.
//!
//! Runs SMTP (`smtp_port`), optional implicit-TLS SMTPS (`smtps_port`), IMAP
//! (`imap_port`), optional IMAPS (`imaps_port`), POP3 (`pop3_port`), optional
//! POP3S (`pop3s_port`), and the dashboard HTTP introspect/control surface
//! (`mail_http_port`) over one shared `Arc<Service>`. Storage layout matches
//! the legacy seam: metadata under `<storage>/mail`, blobs under
//! `<storage>/blobs`.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_mail::{
    mail_tls_from_config, BlobStore, FileBlobStore, FileStore, ImapConfig, ImapServer, Pop3Config,
    Pop3Server, Service, SmtpConfig, SmtpServer, Store,
};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

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

    let any_implicit_tls =
        cfg.server.smtps_port > 0 || cfg.server.imaps_port > 0 || cfg.server.pop3s_port > 0;
    let tls_cfg = &cfg.services.mail.tls;
    let tls = mail_tls_from_config(
        tls_cfg.enabled || any_implicit_tls,
        &tls_cfg.cert_file,
        &tls_cfg.key_file,
        tls_cfg.require,
    )
    .map_err(|e| format!("mail: {e}"))?;

    // Bind every enabled listener before spawning so port collisions fail fast.
    let smtp_addr = format!("127.0.0.1:{}", cfg.server.smtp_port);
    let smtp_listener = TcpListener::bind(&smtp_addr)
        .await
        .map_err(|e| format!("mail: bind smtp {smtp_addr}: {e}"))?;

    let smtps_listener = if cfg.server.smtps_port > 0 {
        let addr = format!("127.0.0.1:{}", cfg.server.smtps_port);
        Some(
            TcpListener::bind(&addr)
                .await
                .map_err(|e| format!("mail: bind smtps {addr}: {e}"))?,
        )
    } else {
        None
    };

    let imap_listener = if cfg.server.imap_port > 0 {
        let addr = format!("127.0.0.1:{}", cfg.server.imap_port);
        Some(
            TcpListener::bind(&addr)
                .await
                .map_err(|e| format!("mail: bind imap {addr}: {e}"))?,
        )
    } else {
        None
    };

    let imaps_listener = if cfg.server.imaps_port > 0 {
        let addr = format!("127.0.0.1:{}", cfg.server.imaps_port);
        Some(
            TcpListener::bind(&addr)
                .await
                .map_err(|e| format!("mail: bind imaps {addr}: {e}"))?,
        )
    } else {
        None
    };

    let pop3_listener = if cfg.server.pop3_port > 0 {
        let addr = format!("127.0.0.1:{}", cfg.server.pop3_port);
        Some(
            TcpListener::bind(&addr)
                .await
                .map_err(|e| format!("mail: bind pop3 {addr}: {e}"))?,
        )
    } else {
        None
    };

    let pop3s_listener = if cfg.server.pop3s_port > 0 {
        let addr = format!("127.0.0.1:{}", cfg.server.pop3s_port);
        Some(
            TcpListener::bind(&addr)
                .await
                .map_err(|e| format!("mail: bind pop3s {addr}: {e}"))?,
        )
    } else {
        None
    };

    let http_addr = format!("127.0.0.1:{}", cfg.server.mail_http_port);
    let http_listener = TcpListener::bind(&http_addr)
        .await
        .map_err(|e| format!("mail: bind http {http_addr}: {e}"))?;

    let smtp = Arc::new(
        SmtpServer::new(
            SmtpConfig {
                addr: smtp_addr,
                max_message_bytes: cfg.services.mail.max_message_bytes,
                auth_mode: cfg.auth.smtp.mode.clone(),
                username: cfg.auth.smtp.username.clone(),
                password: cfg.auth.smtp.password.clone(),
            },
            Arc::clone(&service),
        )
        .with_optional_tls(tls.clone()),
    );

    let imap = Arc::new(
        ImapServer::new(
            ImapConfig {
                addr: format!("127.0.0.1:{}", cfg.server.imap_port),
                auth_mode: cfg.auth.smtp.mode.clone(),
                username: cfg.auth.smtp.username.clone(),
                password: cfg.auth.smtp.password.clone(),
            },
            Arc::clone(&service),
        )
        .with_optional_tls(tls.clone()),
    );

    let pop3 = Arc::new(
        Pop3Server::new(
            Pop3Config {
                addr: format!("127.0.0.1:{}", cfg.server.pop3_port),
                auth_mode: cfg.auth.smtp.mode.clone(),
                username: cfg.auth.smtp.username.clone(),
                password: cfg.auth.smtp.password.clone(),
            },
            Arc::clone(&service),
        )
        .with_optional_tls(tls),
    );

    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = tx.send(true);
    });

    let mut tasks: JoinSet<Result<(), String>> = JoinSet::new();

    {
        let s = Arc::clone(&smtp);
        let sd = shutdown_future(rx.clone());
        tasks.spawn(async move {
            s.serve(smtp_listener, sd)
                .await
                .map_err(|e| format!("mail SMTP: {e}"))
        });
    }
    if let Some(listener) = smtps_listener {
        let s = Arc::clone(&smtp);
        let sd = shutdown_future(rx.clone());
        tasks.spawn(async move {
            s.serve_tls(listener, sd)
                .await
                .map_err(|e| format!("mail SMTPS: {e}"))
        });
    }
    if let Some(listener) = imap_listener {
        let s = Arc::clone(&imap);
        let sd = shutdown_future(rx.clone());
        tasks.spawn(async move {
            s.serve(listener, sd)
                .await
                .map_err(|e| format!("mail IMAP: {e}"))
        });
    }
    if let Some(listener) = imaps_listener {
        let s = Arc::clone(&imap);
        let sd = shutdown_future(rx.clone());
        tasks.spawn(async move {
            s.serve_tls(listener, sd)
                .await
                .map_err(|e| format!("mail IMAPS: {e}"))
        });
    }
    if let Some(listener) = pop3_listener {
        let s = Arc::clone(&pop3);
        let sd = shutdown_future(rx.clone());
        tasks.spawn(async move {
            s.serve(listener, sd)
                .await
                .map_err(|e| format!("mail POP3: {e}"))
        });
    }
    if let Some(listener) = pop3s_listener {
        let s = Arc::clone(&pop3);
        let sd = shutdown_future(rx.clone());
        tasks.spawn(async move {
            s.serve_tls(listener, sd)
                .await
                .map_err(|e| format!("mail POP3S: {e}"))
        });
    }

    {
        let http_sd = shutdown_future(rx.clone());
        let svc = Arc::clone(&service);
        let auth_mode = cfg.auth.smtp.mode.clone();
        let username = cfg.auth.smtp.username.clone();
        let password = cfg.auth.smtp.password.clone();
        tasks.spawn(async move {
            devcloud_mail::serve_http(http_listener, svc, auth_mode, username, password, http_sd)
                .await
        });
    }

    let mut first_err: Option<String> = None;
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
                tasks.abort_all();
            }
            Err(join_err) => {
                if first_err.is_none() {
                    first_err = Some(format!("mail task failed: {join_err}"));
                }
                tasks.abort_all();
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
