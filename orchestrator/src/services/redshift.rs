//! Redshift service: pgwire SQL listener + Data API HTTP listener over one
//! shared `Server`, mirroring the `devcloud-redshift` binary and the legacy daemon's
//! backend selection (`redshiftRustBackendDSN`). The orchestrator owns the
//! managed-PostgreSQL lifecycle (`services::managed_postgres`) and hands the DSN
//! to the postgres backend.

use std::future::Future;
use std::sync::Arc;

use devcloud_redshift::backend::SqlBackend;
use devcloud_redshift::backend_postgres::{self, Backend as PostgresBackend};
use devcloud_redshift::server::{Config, Server};
use devcloud_redshift::translator::{RedshiftToPostgres, RedshiftTranslator};
use devcloud_s3::store::FileBucketStore;
use tokio::net::TcpListener;

use crate::config::{Config as AppConfig, RedshiftBackendConfig};
use crate::services::managed_postgres::{self, ManagedPostgres};
use crate::services::util::{default_string, scoped_data_dir};
use crate::supervisor::shutdown_future;

/// legacy `redshiftBackendKind`: lowercased kind, defaulting to `postgres`.
fn backend_kind(b: &RedshiftBackendConfig) -> String {
    default_string(&b.kind, "postgres").to_lowercase()
}

/// legacy `redshiftBackendMode`: explicit mode wins; external when a DSN is set;
/// memory when kind is memory; otherwise managed.
fn backend_mode(b: &RedshiftBackendConfig) -> String {
    let mode = b.mode.trim().to_lowercase();
    if !mode.is_empty() {
        return mode;
    }
    if !b.external_dsn.trim().is_empty() {
        return "external".to_string();
    }
    if backend_kind(b) == "memory" {
        return "memory".to_string();
    }
    "managed".to_string()
}

pub async fn run(
    cfg: &AppConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    let backend_cfg = &cfg.services.redshift.backend;
    let kind = backend_kind(backend_cfg);

    // ---- backend DSN selection (mirror redshiftRustBackendDSN) ----
    let (dsn, mut managed): (String, Option<ManagedPostgres>) = match kind.as_str() {
        "" | "memory" => (String::new(), None),
        "postgres" | "postgresql" => {
            let mode = backend_mode(backend_cfg);
            if mode == "managed"
                || (backend_cfg.managed
                    && backend_cfg.mode.is_empty()
                    && backend_cfg.external_dsn.is_empty())
            {
                let owned = cfg.clone();
                let pg = tokio::task::spawn_blocking(move || {
                    managed_postgres::start_managed_redshift_postgres(&owned)
                })
                .await
                .map_err(|e| format!("redshift: managed postgres join: {e}"))??;
                (pg.dsn(), Some(pg))
            } else if mode == "external" {
                (backend_cfg.external_dsn.clone(), None)
            } else {
                return Err(format!(
                    "unsupported redshift postgres backend mode: {}",
                    backend_cfg.mode
                ));
            }
        }
        other => return Err(format!("unsupported redshift backend kind: {other}")),
    };

    // ---- backend + translator (mirror devcloud-redshift main.rs) ----
    let (sql_backend, translator): (
        Option<Arc<dyn SqlBackend>>,
        Option<Arc<dyn RedshiftTranslator>>,
    ) = match kind.as_str() {
        "postgres" | "postgresql" => {
            // `PostgresBackend::open` drives its own runtime with `block_on`, which
            // panics ("runtime within runtime") if called on a tokio worker thread.
            // Open it on a blocking thread (no runtime entered); later queries use
            // the backend's `drive()` which `block_in_place`s safely on workers.
            let open_dsn = dsn.clone();
            let pg = tokio::task::spawn_blocking(move || {
                PostgresBackend::open(backend_postgres::Config {
                    dsn: open_dsn,
                    ..backend_postgres::Config::default()
                })
            })
            .await
            .map_err(|e| format!("redshift: backend open join: {e}"))?
            .map_err(|e| format!("redshift: open postgres backend: {e}"))?;
            (Some(Arc::new(pg)), Some(Arc::new(RedshiftToPostgres)))
        }
        _ => (None, None),
    };

    // ObjectStore exists whenever S3 or GCS is enabled (shared bucket root).
    let object_store = if cfg.services.s3.enabled || cfg.services.gcs.enabled {
        Some(Arc::new(FileBucketStore::new(format!(
            "{}/s3/buckets",
            cfg.storage.path
        ))))
    } else {
        None
    };

    let sql_addr = format!("127.0.0.1:{}", cfg.server.redshift_port);
    let api_addr = format!("127.0.0.1:{}", cfg.server.redshift_api_port);

    let server = Arc::new(Server::new(Config {
        sql_addr: sql_addr.clone(),
        api_addr: api_addr.clone(),
        region: cfg.services.redshift.region.clone(),
        cluster_identifier: cfg.services.redshift.cluster_identifier.clone(),
        database: cfg.services.redshift.database.clone(),
        node_type: cfg.services.redshift.node_type.clone(),
        number_of_nodes: cfg.services.redshift.number_of_nodes as i64,
        storage_path: scoped_data_dir(
            &cfg.storage.path,
            &cfg.services.redshift.data_dir,
            "redshift",
        ),
        max_statement_bytes: cfg.services.redshift.max_statement_bytes,
        max_copy_input_bytes: cfg.services.redshift.copy_unload.max_input_row_bytes,
        auth_mode: cfg.auth.redshift.mode.clone(),
        user: cfg.auth.redshift.user.clone(),
        password: cfg.auth.redshift.password.clone(),
        account_id: cfg.auth.redshift.account_id.clone(),
        backend_kind: kind,
        backend_mode: backend_mode(backend_cfg),
        object_store,
        sql_backend,
        translator,
        events_enabled: true,
    }));

    let sql_listener = TcpListener::bind(&sql_addr)
        .await
        .map_err(|e| format!("redshift: bind sql {sql_addr}: {e}"))?;
    let api_listener = TcpListener::bind(&api_addr)
        .await
        .map_err(|e| format!("redshift: bind api {api_addr}: {e}"))?;

    // One inner shutdown fanned out to the HTTP serve loop + the outer select.
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = tx.send(true);
    });

    let sql_server = Arc::clone(&server);
    let sql_task = tokio::spawn(async move { sql_server.serve_sql(sql_listener).await });
    let http_server = Arc::clone(&server);
    let http_sd = shutdown_future(rx.clone());
    let http_task = tokio::spawn(async move {
        devcloud_redshift::http::serve(api_listener, http_server, http_sd).await
    });

    let result = tokio::select! {
        _ = shutdown_future(rx.clone()) => Ok(()),
        r = sql_task => r
            .map_err(|e| e.to_string())
            .and_then(|x| x.map_err(|e| format!("redshift sql serve error: {e}"))),
        r = http_task => r
            .map_err(|e| e.to_string())
            .and_then(|x| x.map_err(|e| format!("redshift http serve error: {e}"))),
    };

    if let Some(pg) = managed.as_mut() {
        let _ = pg.close();
    }
    result
}
