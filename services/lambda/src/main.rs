//! `devcloud-lambda` binary: serves the Lambda REST API without the orchestrator.
//!
//! Configuration comes from environment variables:
//!
//!   DEVCLOUD_LAMBDA_ADDR       listen address (host:port), default 127.0.0.1:0
//!   DEVCLOUD_LAMBDA_STORAGE    function state + package root, required
//!   DEVCLOUD_LAMBDA_ENDPOINT   public base URL for `Code.Location`, default http://<addr>
//!   DEVCLOUD_LAMBDA_REGION     default us-east-1
//!   DEVCLOUD_LAMBDA_ACCOUNT_ID default 000000000000
//!   DEVCLOUD_LAMBDA_AUTH_MODE  relaxed (default), signed-relaxed, or strict
//!   DEVCLOUD_LAMBDA_ACCESS_KEY_ID / DEVCLOUD_LAMBDA_SECRET_ACCESS_KEY strict-mode creds
//!   DEVCLOUD_LAMBDA_S3_STORAGE devcloud-s3 storage root, enabling Code.S3Bucket/S3Key (optional)
//!   DEVCLOUD_LAMBDA_FUNCTION_ACCESS_KEY_ID / DEVCLOUD_LAMBDA_FUNCTION_SECRET_ACCESS_KEY /
//!   DEVCLOUD_LAMBDA_FUNCTION_SESSION_TOKEN credentials passed to every handler (optional;
//!                              both the key id and the secret are needed)
//!   DEVCLOUD_LAMBDA_OPT_DIR    stand-in for Lambda's /opt (layer contents), default /opt
//!   DEVCLOUD_LAMBDA_IDLE_TIMEOUT_SECONDS how long an idle execution environment stays
//!                              warm, default 300; 0 = a cold start for every invocation

use std::path::PathBuf;
use std::sync::Arc;

use devcloud_lambda::runtime::FunctionCredentials;
use devcloud_lambda::{Config, Server};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn main() {
    let addr = env_or("DEVCLOUD_LAMBDA_ADDR", "127.0.0.1:0");
    let storage = env("DEVCLOUD_LAMBDA_STORAGE");
    if storage.is_empty() {
        eprintln!("devcloud-lambda: DEVCLOUD_LAMBDA_STORAGE is required");
        std::process::exit(2);
    }
    let s3_storage = env("DEVCLOUD_LAMBDA_S3_STORAGE");
    let opt_dir = env("DEVCLOUD_LAMBDA_OPT_DIR");
    let idle_timeout = match env("DEVCLOUD_LAMBDA_IDLE_TIMEOUT_SECONDS").as_str() {
        "" => None,
        v => match v.parse::<u64>() {
            Ok(secs) => Some(std::time::Duration::from_secs(secs)),
            Err(_) => {
                eprintln!("devcloud-lambda: DEVCLOUD_LAMBDA_IDLE_TIMEOUT_SECONDS must be a whole number of seconds");
                std::process::exit(2);
            }
        },
    };
    let function_credentials = FunctionCredentials {
        access_key_id: env("DEVCLOUD_LAMBDA_FUNCTION_ACCESS_KEY_ID"),
        secret_access_key: env("DEVCLOUD_LAMBDA_FUNCTION_SECRET_ACCESS_KEY"),
        session_token: env("DEVCLOUD_LAMBDA_FUNCTION_SESSION_TOKEN"),
    };
    let function_credentials = match (
        function_credentials.access_key_id.is_empty(),
        function_credentials.secret_access_key.is_empty(),
    ) {
        (false, false) => Some(function_credentials),
        (true, true) => None,
        _ => {
            eprintln!("devcloud-lambda: set both DEVCLOUD_LAMBDA_FUNCTION_ACCESS_KEY_ID and DEVCLOUD_LAMBDA_FUNCTION_SECRET_ACCESS_KEY, or neither");
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-lambda: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        // Resolved after bind so port 0 reports the real port.
        let bound = listener.local_addr().map(|a| a.to_string()).unwrap_or(addr);
        let config = Config {
            endpoint: env_or("DEVCLOUD_LAMBDA_ENDPOINT", &format!("http://{bound}")),
            addr: bound,
            region: env_or("DEVCLOUD_LAMBDA_REGION", "us-east-1"),
            account_id: env_or("DEVCLOUD_LAMBDA_ACCOUNT_ID", "000000000000"),
            auth_mode: env("DEVCLOUD_LAMBDA_AUTH_MODE"),
            access_key_id: env("DEVCLOUD_LAMBDA_ACCESS_KEY_ID"),
            secret_access_key: env("DEVCLOUD_LAMBDA_SECRET_ACCESS_KEY"),
            storage_path: storage,
            object_store_root: (!s3_storage.is_empty()).then(|| PathBuf::from(s3_storage)),
            interpreters: Default::default(),
            function_credentials,
            opt_dir: (!opt_dir.is_empty()).then(|| PathBuf::from(opt_dir)),
            idle_timeout,
        };
        let server = Arc::new(Server::new(config));
        if let Some(err) = server.load_err() {
            eprintln!("devcloud-lambda: failed to load state: {err}");
            std::process::exit(1);
        }
        if let Err(e) = devcloud_lambda::http::serve(listener, server, shutdown_signal()).await {
            eprintln!("devcloud-lambda: serve error: {e}");
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
