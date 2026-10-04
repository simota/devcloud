use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use devcloud_mail::{FileBlobStore, FileStore, Service, SmtpLimits, SmtpServer};
use devcloud_mailbox::{config::Config, http, routes::App};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Semaphore};

fn main() {
    install_panic_hook();
    if let Err(error) = run_with_runtime() {
        eprintln!("devcloud-mailbox: {error}");
        std::process::exit(1);
    }
}

fn run_with_runtime() -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot initialize runtime".to_string())?;
    let config = Arc::new(Config::from_env()?);
    let storage = config.prepare_storage()?;
    let result = runtime.block_on(run(config, storage.path()));
    // A synchronous MIME projection may already be running on the blocking
    // pool. Runtime drop would wait forever for it with MAX_BYTES=0.
    runtime.shutdown_timeout(Duration::from_secs(2));
    result.and(storage.cleanup())
}

fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        if let Some(location) = info.location() {
            eprintln!("devcloud-mailbox: internal panic at {location}");
        } else {
            eprintln!("devcloud-mailbox: internal panic");
        }
    }));
}

async fn run(config: Arc<Config>, storage: &Path) -> Result<(), String> {
    let store = Arc::new(FileStore::new(
        storage.join("mail"),
        Arc::new(FileBlobStore::new(storage.join("blobs"))),
    ));
    let service = Arc::new(Service::new(store));
    service
        .list_all()
        .map_err(|_| "Cannot load mail/messages.jsonl".to_string())?;
    let smtp = TcpListener::bind(config.smtp_addr)
        .await
        .map_err(|e| format!("SMTP bind failed: {e}"))?;
    let http_listener = TcpListener::bind(config.http_addr)
        .await
        .map_err(|e| format!("HTTP bind failed: {e}"))?;
    let app = Arc::new(App::new(
        service.clone(),
        config.auth.clone(),
        config.hostname.clone(),
        config.allowed_hosts.clone(),
    ));
    let (cancel, shutdown) = watch::channel(false);
    let (tx, mut rx) = mpsc::unbounded_channel();
    devcloud_mail::set_event_sink(tx);
    let event_app = app.clone();
    let mut event_shutdown = shutdown.clone();
    let events = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = event_shutdown.changed() => return,
                event = rx.recv() => {
                    let Some(event) = event else { return; };
                    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&event) {
                        if value["type"] == "mail.received" && event_app.events.receiver_count() > 0 {
                            if let Some(id) = value["payload"]["messageID"].as_str() {
                                let app = event_app.clone();
                                let id = id.to_string();
                                let _ = tokio::task::spawn_blocking(move || app.publish(&id)).await;
                            }
                        }
                    }
                }
            }
        }
    });
    // Credential values, headers, queries and message contents never reach logs.
    eprintln!("devcloud-mailbox: smtp={} http={} auth={} username=<masked> password=<masked> max_bytes={} ephemeral={}", config.smtp_addr, config.http_addr, config.auth.auth_mode, config.max_bytes, config.ephemeral);
    let mut smtp_task = tokio::spawn(serve_smtp(smtp, config, service, shutdown.clone()));
    let mut http_task = tokio::spawn(http::serve(http_listener, app, shutdown));
    let result = tokio::select! {
        _ = shutdown_signal() => Ok(()),
        result = &mut smtp_task => result.map_err(|_| "SMTP task failed".to_string()).and_then(|r| r.map_err(|e| format!("SMTP listener failed: {e}"))),
        result = &mut http_task => result.map_err(|_| "HTTP task failed".to_string()).and_then(|r| r.map_err(|e| format!("HTTP listener failed: {e}"))),
    };
    let _ = cancel.send(true);
    // SSE tasks are detached and watch cancellation; never wait for their next
    // heartbeat. Bound the accept-loop drain even if a listener has failed.
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        if !smtp_task.is_finished() {
            let _ = (&mut smtp_task).await;
        }
        if !http_task.is_finished() {
            let _ = (&mut http_task).await;
        }
    })
    .await;
    events.abort();
    result
}

async fn serve_smtp(
    listener: TcpListener,
    config: Arc<Config>,
    service: Arc<Service>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let slots = Arc::new(Semaphore::new(512));
    loop {
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(connection) => connection,
                    Err(_) => {
                        eprintln!("devcloud-mailbox: SMTP accept failed; retrying");
                        tokio::select! {
                            _ = shutdown.changed() => return Ok(()),
                            _ = tokio::time::sleep(Duration::from_millis(75)) => {},
                        }
                        continue;
                    }
                };
                let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                let server = SmtpServer::new(config.smtp_config(), service.clone()).with_envelope_capture().with_limits(SmtpLimits {
                    max_line_bytes: 1024 * 1024, idle_timeout: Some(Duration::from_secs(300)), max_recipients: 1000,
                });
                let mut shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    tokio::select! { _ = shutdown.changed() => {}, _ = server.handle_conn(stream) => {} }
                });
            }
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    #[test]
    fn panic_hook_omits_payload() {
        if std::env::var_os("MAILBOX_PANIC_HOOK_PROBE").is_some() {
            super::install_panic_hook();
            panic!("private-panic-payload-marker");
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::panic_hook_omits_payload", "--nocapture"])
            .env("MAILBOX_PANIC_HOOK_PROBE", "1")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let log = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            log.contains("devcloud-mailbox: internal panic at ") && log.contains("src/main.rs:")
        );
        assert!(!log.contains("private-panic-payload-marker"));
    }
}
