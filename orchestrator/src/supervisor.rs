//! Supervisor — the in-process equivalent of the legacy `internal/app` daemon.
//!
//! Launches every enabled service as a tokio task sharing one process, fans the
//! first error back, and drives a single graceful shutdown (SIGINT/SIGTERM)
//! across all tasks via a `watch` broadcast — mirroring `Daemon.Run`'s
//! context-cancellation fan-out, minus the subprocess spawning.

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::config::Config;
use crate::services;

/// Awaits the process shutdown signal: SIGINT/SIGTERM on unix, Ctrl-C elsewhere.
async fn wait_os_signal() {
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

/// A per-service shutdown future: resolves once the supervisor flips the watch
/// channel to `true`. Each service `select!`s its serve loop against this.
pub async fn shutdown_future(mut rx: watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
}

/// Starts all enabled services and blocks until they all exit (clean shutdown)
/// or the first one errors. Returns the first service error, if any.
pub async fn run(cfg: Config) -> Result<(), String> {
    let (tx, rx) = watch::channel(false);
    let mut set: JoinSet<Result<(), String>> = JoinSet::new();

    // ---- dashboard live-event pipeline ----
    // One in-process channel collects events from every emitting service crate
    // (installed BEFORE the services start, replacing the old subprocess stdout
    // bridge); the relay fans them out over WebSocket on :event_relay_port. The
    // relay always runs, mirroring the legacy daemon.
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    devcloud_mail::set_event_sink(event_tx.clone());
    devcloud_s3::set_event_sink(event_tx.clone());
    devcloud_gcs::set_event_sink(event_tx.clone());
    devcloud_bigquery::set_event_sink(event_tx.clone());
    devcloud_redshift::set_event_sink(event_tx.clone());
    {
        let addr = format!("127.0.0.1:{}", cfg.server.event_relay_port);
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { devcloud_event_relay::run(addr, event_rx, sd).await });
    }

    // ---- enabled services as in-process tasks (wired incrementally) ----
    if cfg.services.mail.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::mail::run(&c, sd).await });
    }
    if cfg.services.s3.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::s3::run(&c, sd).await });
    }
    if cfg.services.gcs.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::gcs::run(&c, sd).await });
    }
    if cfg.services.dynamodb.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::dynamodb::run(&c, sd).await });
    }
    if cfg.services.bigquery.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::bigquery::run(&c, sd).await });
    }
    if cfg.services.sqs.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::sqs::run(&c, sd).await });
    }
    if cfg.services.app_auto_scaling.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::applicationautoscaling::run(&c, sd).await });
    }
    if cfg.services.pubsub.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::pubsub::run(&c, sd).await });
    }
    if cfg.services.redshift.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::redshift::run(&c, sd).await });
    }
    if cfg.services.redis.enabled {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::redis::run(&c, sd).await });

        // Redis control/introspection HTTP surface (dashboard redis page). Stays
        // in-process even when the upstream redis-server is absent (reads/mutations
        // surface 502), mirroring the legacy listener which always runs when redis is
        // enabled. Feeds redis mutation events into the same relay channel.
        let redis_mode = {
            let m = cfg.services.redis.mode.trim().to_lowercase();
            if !m.is_empty() {
                m
            } else if !cfg.services.redis.external_url.trim().is_empty() {
                "external".to_string()
            } else {
                "managed".to_string()
            }
        };
        let rc_cfg = devcloud_redis_control::Config {
            http_addr: format!("127.0.0.1:{}", cfg.server.redis_http_port),
            redis_addr: format!("127.0.0.1:{}", cfg.server.redis_port),
            redis_password: cfg.auth.redis.password.clone(),
            control_auth_mode: cfg.auth.redis.mode.clone(),
            control_password: cfg.auth.redis.password.clone(),
            mode: redis_mode,
            database_count: 0,
            event_sink: Some(event_tx.clone()),
        };
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { devcloud_redis_control::run(rc_cfg, sd).await });
    }
    // Dashboard always starts (independent of services.*.enabled), mirroring the
    // legacy daemon. It forwards to each service's network endpoint.
    {
        let c = cfg.clone();
        let sd = shutdown_future(rx.clone());
        set.spawn(async move { services::dashboard::run(&c, sd).await });
    }
    // TODO(agent): wire event relay (:18027) + redis control surface (tasks #8/#9).

    if set.is_empty() {
        return Ok(());
    }

    print_banner(&cfg);

    // ---- OS signal listener -> flip the shutdown watch ----
    let (sig_tx, mut sig_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        wait_os_signal().await;
        let _ = sig_tx.send(()).await;
    });

    let mut first_err: Option<String> = None;
    let mut signaled = false;
    loop {
        tokio::select! {
            // Disable this branch once we have already started shutting down so
            // the dropped sender does not busy-loop the select.
            _ = sig_rx.recv(), if !signaled => {
                signaled = true;
                let _ = tx.send(true);
            }
            res = set.join_next() => {
                match res {
                    None => break,
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(e))) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                        let _ = tx.send(true);
                    }
                    Some(Err(join_err)) => {
                        if first_err.is_none() {
                            first_err = Some(format!("service task failed: {join_err}"));
                        }
                        let _ = tx.send(true);
                    }
                }
            }
        }
    }

    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Minimal startup banner.
//
// TODO(agent): port `internal/app/banner.rs` for byte-parity stdout once the
// full service set + dashboard are wired.
fn print_banner(cfg: &Config) {
    println!(
        "devcloud up — dashboard: http://localhost:{}",
        cfg.server.dashboard_port
    );
}
