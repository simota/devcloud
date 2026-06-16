//! `devcloud-redshift` binary: serves the Redshift control-plane + Data API
//! over HTTP and the pgwire SQL protocol over TCP on the loopback addresses the
//! legacy daemon would have used, persisting state to the same storage root and
//! reaching COPY/UNLOAD `s3://` URIs through the shared S3 bucket store.
//!
//! Configuration comes from environment variables (set by
//! `internal/app/redshift_rust.rs`):
//!
//!   DEVCLOUD_REDSHIFT_ADDR              pgwire (SQL) listen address (host:port)
//!   DEVCLOUD_REDSHIFT_API_ADDR         control-plane + Data API HTTP address
//!   DEVCLOUD_REDSHIFT_REGION           AWS region
//!   DEVCLOUD_REDSHIFT_CLUSTER          cluster identifier
//!   DEVCLOUD_REDSHIFT_DATABASE         default database
//!   DEVCLOUD_REDSHIFT_NODE_TYPE        cluster node type
//!   DEVCLOUD_REDSHIFT_NUMBER_OF_NODES  cluster node count
//!   DEVCLOUD_REDSHIFT_STORAGE          state.json storage root
//!   DEVCLOUD_REDSHIFT_OBJECT_STORE     shared S3 bucket root (optional)
//!   DEVCLOUD_REDSHIFT_MAX_STATEMENT_BYTES  SQL statement size cap (0 = none)
//!   DEVCLOUD_REDSHIFT_MAX_COPY_INPUT_BYTES COPY input row size cap (0 = none)
//!   DEVCLOUD_REDSHIFT_AUTH_MODE        relaxed | strict
//!   DEVCLOUD_REDSHIFT_USER             expected user
//!   DEVCLOUD_REDSHIFT_PASSWORD         expected password
//!   DEVCLOUD_REDSHIFT_ACCOUNT_ID       account id
//!   DEVCLOUD_REDSHIFT_BACKEND_KIND     memory | postgres
//!   DEVCLOUD_REDSHIFT_BACKEND_MODE     managed | external | memory | embedded
//!   DEVCLOUD_REDSHIFT_DSN              postgres DSN (managed: daemon-provided)
//!
//! The daemon owns the managed-PostgreSQL lifecycle (managed_postgres.rs); this
//! binary only connects to the DSN. Successful Data API mutations print
//! `DEVCLOUD_EVENT {json}` lines on stdout for the daemon's dashboard bridge.

use std::sync::Arc;

use devcloud_redshift::backend::SqlBackend;
use devcloud_redshift::backend_postgres::{self, Backend as PostgresBackend};
use devcloud_redshift::server::{Config, Server};
use devcloud_redshift::translator::{RedshiftToPostgres, RedshiftTranslator};
use devcloud_s3::store::FileBucketStore;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn env_i64(name: &str) -> i64 {
    env(name).trim().parse().unwrap_or(0)
}

fn main() {
    let sql_addr = env("DEVCLOUD_REDSHIFT_ADDR");
    if sql_addr.is_empty() {
        eprintln!("devcloud-redshift: DEVCLOUD_REDSHIFT_ADDR is required");
        std::process::exit(2);
    }
    let api_addr = env("DEVCLOUD_REDSHIFT_API_ADDR");
    if api_addr.is_empty() {
        eprintln!("devcloud-redshift: DEVCLOUD_REDSHIFT_API_ADDR is required");
        std::process::exit(2);
    }
    let storage = env("DEVCLOUD_REDSHIFT_STORAGE");
    if storage.is_empty() {
        eprintln!("devcloud-redshift: DEVCLOUD_REDSHIFT_STORAGE is required");
        std::process::exit(2);
    }

    let backend_kind = env("DEVCLOUD_REDSHIFT_BACKEND_KIND");
    let backend_mode = env("DEVCLOUD_REDSHIFT_BACKEND_MODE");
    let dsn = env("DEVCLOUD_REDSHIFT_DSN");

    // Mirror the daemon's backend selection: postgres kind connects to the DSN
    // (managed or external) and rewrites Redshift SQL via the translator;
    // memory kind runs the in-process engine with the passthrough translator.
    let (sql_backend, translator): (
        Option<Arc<dyn SqlBackend>>,
        Option<Arc<dyn RedshiftTranslator>>,
    ) = match backend_kind.to_lowercase().as_str() {
        "postgres" | "postgresql" => {
            let pg = match PostgresBackend::open(backend_postgres::Config {
                dsn,
                ..backend_postgres::Config::default()
            }) {
                Ok(b) => b,
                Err(err) => {
                    eprintln!("devcloud-redshift: open postgres backend: {err}");
                    std::process::exit(1);
                }
            };
            (Some(Arc::new(pg)), Some(Arc::new(RedshiftToPostgres)))
        }
        _ => (None, None),
    };

    let object_store_root = env("DEVCLOUD_REDSHIFT_OBJECT_STORE");
    let object_store = if object_store_root.is_empty() {
        None
    } else {
        Some(Arc::new(FileBucketStore::new(object_store_root)))
    };

    let server = Server::new(Config {
        sql_addr: sql_addr.clone(),
        api_addr: api_addr.clone(),
        region: env("DEVCLOUD_REDSHIFT_REGION"),
        cluster_identifier: env("DEVCLOUD_REDSHIFT_CLUSTER"),
        database: env("DEVCLOUD_REDSHIFT_DATABASE"),
        node_type: env("DEVCLOUD_REDSHIFT_NODE_TYPE"),
        number_of_nodes: env_i64("DEVCLOUD_REDSHIFT_NUMBER_OF_NODES"),
        storage_path: storage,
        max_statement_bytes: env_i64("DEVCLOUD_REDSHIFT_MAX_STATEMENT_BYTES"),
        max_copy_input_bytes: env_i64("DEVCLOUD_REDSHIFT_MAX_COPY_INPUT_BYTES"),
        auth_mode: env("DEVCLOUD_REDSHIFT_AUTH_MODE"),
        user: env("DEVCLOUD_REDSHIFT_USER"),
        password: env("DEVCLOUD_REDSHIFT_PASSWORD"),
        account_id: env("DEVCLOUD_REDSHIFT_ACCOUNT_ID"),
        backend_kind,
        backend_mode,
        object_store,
        sql_backend,
        translator,
        events_enabled: true,
    });

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let sql_listener = match tokio::net::TcpListener::bind(&sql_addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-redshift: bind sql {sql_addr}: {e}");
                std::process::exit(1);
            }
        };
        let api_listener = match tokio::net::TcpListener::bind(&api_addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-redshift: bind api {api_addr}: {e}");
                std::process::exit(1);
            }
        };

        let shared = Arc::new(server);
        let sql_server = Arc::clone(&shared);
        let sql_task = tokio::spawn(async move { sql_server.serve_sql(sql_listener).await });
        let http_server = Arc::clone(&shared);
        let http_task = tokio::spawn(async move {
            devcloud_redshift::http::serve(api_listener, http_server, std::future::pending()).await
        });

        tokio::select! {
            _ = shutdown_signal() => {}
            res = sql_task => {
                if let Ok(Err(e)) = res {
                    eprintln!("devcloud-redshift: sql serve error: {e}");
                }
            }
            res = http_task => {
                if let Ok(Err(e)) = res {
                    eprintln!("devcloud-redshift: http serve error: {e}");
                }
            }
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
