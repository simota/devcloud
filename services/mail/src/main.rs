//! Standalone Mail server binary for the strangler-fig seam.
//!
//! Configuration is passed via environment variables:
//!   DEVCLOUD_MAIL_ADDR          SMTP listen address, e.g. 127.0.0.1:11025 (required)
//!   DEVCLOUD_MAIL_STORAGE       mail metadata dir (messages.jsonl)        (required)
//!   DEVCLOUD_MAIL_BLOBS         blob store dir                            (required)
//!   DEVCLOUD_MAIL_MAX_BYTES     max message bytes (i64, 0 = no limit)
//!   DEVCLOUD_MAIL_AUTH_MODE     off | relaxed | strict
//!   DEVCLOUD_MAIL_USERNAME      strict-mode username
//!   DEVCLOUD_MAIL_PASSWORD      strict-mode password
//!   DEVCLOUD_MAIL_TLS_ENABLED   true/1 to enable TLS (self-signed if no cert/key)
//!   DEVCLOUD_MAIL_TLS_CERT_FILE PEM certificate chain file
//!   DEVCLOUD_MAIL_TLS_KEY_FILE  PEM private key file
//!   DEVCLOUD_MAIL_TLS_REQUIRE   true/1 to require STARTTLS/STLS before auth/mail
//!   DEVCLOUD_MAIL_SMTPS_ADDR    optional implicit-TLS SMTP listen address
//!   DEVCLOUD_MAIL_IMAP_ADDR     optional IMAP listen address
//!   DEVCLOUD_MAIL_IMAPS_ADDR    optional implicit-TLS IMAP listen address
//!   DEVCLOUD_MAIL_POP3_ADDR     optional POP3 listen address
//!   DEVCLOUD_MAIL_POP3S_ADDR    optional implicit-TLS POP3 listen address

use std::sync::Arc;

use devcloud_mail::{
    mail_tls_from_config, BlobStore, FileBlobStore, FileStore, ImapConfig, ImapServer, Pop3Config,
    Pop3Server, Service, SmtpConfig, SmtpServer, Store,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

fn env_required(key: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("devcloud-mail: missing required env var {key}");
            std::process::exit(2);
        }
    }
}

fn env_bool(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "t" | "yes"
    )
}

fn env_optional(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
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

    let smtps_addr = env_optional("DEVCLOUD_MAIL_SMTPS_ADDR");
    let imap_addr = env_optional("DEVCLOUD_MAIL_IMAP_ADDR");
    let imaps_addr = env_optional("DEVCLOUD_MAIL_IMAPS_ADDR");
    let pop3_addr = env_optional("DEVCLOUD_MAIL_POP3_ADDR");
    let pop3s_addr = env_optional("DEVCLOUD_MAIL_POP3S_ADDR");

    let any_implicit_tls = smtps_addr.is_some() || imaps_addr.is_some() || pop3s_addr.is_some();
    let tls_enabled = env_bool("DEVCLOUD_MAIL_TLS_ENABLED") || any_implicit_tls;
    let tls_cert = std::env::var("DEVCLOUD_MAIL_TLS_CERT_FILE").unwrap_or_default();
    let tls_key = std::env::var("DEVCLOUD_MAIL_TLS_KEY_FILE").unwrap_or_default();
    let tls_require = env_bool("DEVCLOUD_MAIL_TLS_REQUIRE");
    let tls = match mail_tls_from_config(tls_enabled, &tls_cert, &tls_key, tls_require) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("devcloud-mail: TLS config error: {e}");
            std::process::exit(2);
        }
    };

    let blobs: Arc<dyn BlobStore> = Arc::new(FileBlobStore::new(blobs_dir));
    let store: Arc<dyn Store> = Arc::new(FileStore::new(storage, blobs));
    let service = Arc::new(Service::new(store));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let (tx, rx) = watch::channel(false);
        let mut tasks: JoinSet<Result<(), String>> = JoinSet::new();

        let smtp = Arc::new(
            SmtpServer::new(
                SmtpConfig {
                    addr: addr.clone(),
                    max_message_bytes: max_bytes,
                    auth_mode: auth_mode.clone(),
                    username: username.clone(),
                    password: password.clone(),
                },
                Arc::clone(&service),
            )
            .with_optional_tls(tls.clone()),
        );

        let smtp_listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-mail: bind SMTP {addr}: {e}");
                std::process::exit(1);
            }
        };
        {
            let s = Arc::clone(&smtp);
            let sd = wait_watch(rx.clone());
            tasks.spawn(async move { s.serve(smtp_listener, sd).await.map_err(|e| e.to_string()) });
        }

        if let Some(a) = smtps_addr {
            let l = TcpListener::bind(&a).await.unwrap_or_else(|e| {
                eprintln!("devcloud-mail: bind SMTPS {a}: {e}");
                std::process::exit(1);
            });
            let s = Arc::clone(&smtp);
            let sd = wait_watch(rx.clone());
            tasks.spawn(async move { s.serve_tls(l, sd).await.map_err(|e| e.to_string()) });
        }

        if imap_addr.is_some() || imaps_addr.is_some() {
            let imap = Arc::new(
                ImapServer::new(
                    ImapConfig {
                        addr: imap_addr.clone().unwrap_or_default(),
                        auth_mode: auth_mode.clone(),
                        username: username.clone(),
                        password: password.clone(),
                    },
                    Arc::clone(&service),
                )
                .with_optional_tls(tls.clone()),
            );
            if let Some(a) = imap_addr {
                let l = TcpListener::bind(&a).await.unwrap_or_else(|e| {
                    eprintln!("devcloud-mail: bind IMAP {a}: {e}");
                    std::process::exit(1);
                });
                let s = Arc::clone(&imap);
                let sd = wait_watch(rx.clone());
                tasks.spawn(async move { s.serve(l, sd).await.map_err(|e| e.to_string()) });
            }
            if let Some(a) = imaps_addr {
                let l = TcpListener::bind(&a).await.unwrap_or_else(|e| {
                    eprintln!("devcloud-mail: bind IMAPS {a}: {e}");
                    std::process::exit(1);
                });
                let s = Arc::clone(&imap);
                let sd = wait_watch(rx.clone());
                tasks.spawn(async move { s.serve_tls(l, sd).await.map_err(|e| e.to_string()) });
            }
        }

        if pop3_addr.is_some() || pop3s_addr.is_some() {
            let pop3 = Arc::new(
                Pop3Server::new(
                    Pop3Config {
                        addr: pop3_addr.clone().unwrap_or_default(),
                        auth_mode,
                        username,
                        password,
                    },
                    Arc::clone(&service),
                )
                .with_optional_tls(tls),
            );
            if let Some(a) = pop3_addr {
                let l = TcpListener::bind(&a).await.unwrap_or_else(|e| {
                    eprintln!("devcloud-mail: bind POP3 {a}: {e}");
                    std::process::exit(1);
                });
                let s = Arc::clone(&pop3);
                let sd = wait_watch(rx.clone());
                tasks.spawn(async move { s.serve(l, sd).await.map_err(|e| e.to_string()) });
            }
            if let Some(a) = pop3s_addr {
                let l = TcpListener::bind(&a).await.unwrap_or_else(|e| {
                    eprintln!("devcloud-mail: bind POP3S {a}: {e}");
                    std::process::exit(1);
                });
                let s = Arc::clone(&pop3);
                let sd = wait_watch(rx.clone());
                tasks.spawn(async move { s.serve_tls(l, sd).await.map_err(|e| e.to_string()) });
            }
        }

        tokio::select! {
            _ = shutdown_signal() => {
                let _ = tx.send(true);
            }
            res = tasks.join_next() => {
                let _ = tx.send(true);
                if let Some(Ok(Err(e))) = res {
                    eprintln!("devcloud-mail: server error: {e}");
                    std::process::exit(1);
                }
            }
        }
        while tasks.join_next().await.is_some() {}
    });
}

async fn wait_watch(mut rx: watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
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
