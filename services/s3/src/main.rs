//! `devcloud-s3` binary: serves the current S3 Rust increment over HTTP.
//!
//! Configuration comes from environment variables:
//!
//!   DEVCLOUD_S3_ADDR     listen address (host:port), default 127.0.0.1:0
//!   DEVCLOUD_S3_STORAGE  bucket storage root, required
//!   DEVCLOUD_S3_AUTH_MODE relaxed (default) or strict
//!   DEVCLOUD_S3_ACCESS_KEY_ID / DEVCLOUD_S3_SECRET_ACCESS_KEY strict-mode creds
//!   DEVCLOUD_S3_REGION    SigV4 region
//!   DEVCLOUD_S3_BUCKETS   comma-separated buckets to create at startup (existing ones are kept)
//!   DEVCLOUD_S3_CORS_ALLOW_ORIGINS comma-separated origins for buckets without
//!                         their own CORS configuration; unset allows none
//!   DEVCLOUD_S3_CONSOLE_ADDR web console listen address; unset disables it
//!   DEVCLOUD_S3_CONSOLE_ALLOWED_HOSTS extra comma-separated console Host names

use std::sync::{Arc, Mutex};

use devcloud_s3::console::{self, ConsoleConfig};
use devcloud_s3::cors;
use devcloud_s3::http::AuthConfig;
use devcloud_s3::store::FileBucketStore;
use tokio::net::TcpListener;
use tokio::sync::watch;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn env_list(name: &str) -> Vec<String> {
    env(name)
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect()
}

fn fail(code: i32, message: &str) -> ! {
    eprintln!("devcloud-s3: {message}");
    std::process::exit(code);
}

/// A mistyped mode must not silently disable signature checks, and strict mode
/// must not silently fall back to the built-in development credentials.
fn validate_auth(auth: &AuthConfig) {
    let mode = auth.auth_mode.to_ascii_lowercase();
    if !matches!(mode.as_str(), "" | "relaxed" | "strict") {
        fail(2, "DEVCLOUD_S3_AUTH_MODE must be relaxed or strict");
    }
    if mode == "strict" && (auth.access_key_id.is_empty() || auth.secret_access_key.is_empty()) {
        fail(
            2,
            "strict auth requires DEVCLOUD_S3_ACCESS_KEY_ID and DEVCLOUD_S3_SECRET_ACCESS_KEY",
        );
    }
}

async fn bind(addr: &str) -> TcpListener {
    match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => fail(1, &format!("bind {addr}: {e}")),
    }
}

fn main() {
    let addr = std::env::var("DEVCLOUD_S3_ADDR").unwrap_or_else(|_| "127.0.0.1:0".to_string());
    let storage = env("DEVCLOUD_S3_STORAGE");
    if storage.is_empty() {
        fail(2, "DEVCLOUD_S3_STORAGE is required");
    }
    let store = FileBucketStore::new(storage);
    if let Err(e) = create_initial_buckets(&store, &env("DEVCLOUD_S3_BUCKETS")) {
        eprintln!("devcloud-s3: DEVCLOUD_S3_BUCKETS: {e}");
        std::process::exit(2);
    }
    let store = Arc::new(Mutex::new(store));
    let auth = AuthConfig {
        auth_mode: env("DEVCLOUD_S3_AUTH_MODE"),
        access_key_id: env("DEVCLOUD_S3_ACCESS_KEY_ID"),
        secret_access_key: env("DEVCLOUD_S3_SECRET_ACCESS_KEY"),
        region: env("DEVCLOUD_S3_REGION"),
    };
    validate_auth(&auth);
    let default_cors = cors::default_rule(&env_list("DEVCLOUD_S3_CORS_ALLOW_ORIGINS"));
    let console_addr = env("DEVCLOUD_S3_CONSOLE_ADDR");
    let console_config = ConsoleConfig {
        auth: auth.clone(),
        allowed_hosts: env_list("DEVCLOUD_S3_CONSOLE_ALLOWED_HOSTS"),
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = bind(&addr).await;
        let console_listener = if console_addr.is_empty() {
            None
        } else {
            Some(bind(&console_addr).await)
        };

        let (stop, stopped) = watch::channel(false);
        tokio::spawn(async move {
            shutdown_signal().await;
            let _ = stop.send(true);
        });
        let wait_for_stop = |mut stopped: watch::Receiver<bool>| async move {
            let _ = stopped.changed().await;
        };

        let s3 = devcloud_s3::http::serve_with_cors(
            listener,
            Arc::clone(&store),
            auth,
            default_cors,
            wait_for_stop(stopped.clone()),
        );
        let console = async {
            match console_listener {
                Some(listener) => {
                    console::serve(listener, store, console_config, wait_for_stop(stopped)).await
                }
                None => Ok(()),
            }
        };
        // Either listener failing ends the process, so a restart policy can act
        // instead of the container looking healthy with one server gone.
        if let Err(e) = tokio::try_join!(s3, console) {
            fail(1, &format!("serve error: {e}"));
        }
    });
}

/// Creates each bucket named in the comma-separated `list`. Blank entries are
/// skipped and buckets that already exist are left untouched.
fn create_initial_buckets(store: &FileBucketStore, list: &str) -> Result<(), String> {
    for name in list.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        store
            .create_bucket(name)
            .map_err(|e| format!("create bucket {name:?}: {e:?}"))?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store(tag: &str) -> (std::path::PathBuf, FileBucketStore) {
        let root =
            std::env::temp_dir().join(format!("devcloud-s3-main-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = FileBucketStore::new(root.to_string_lossy().into_owned());
        (root, store)
    }

    #[test]
    fn creates_listed_buckets_and_skips_blanks() {
        let (root, store) = temp_store("create");
        create_initial_buckets(&store, " alpha, ,beta ,").unwrap();
        let names: Vec<String> = store
            .list_buckets()
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        assert_eq!(names, ["alpha", "beta"]);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn keeps_existing_bucket_metadata() {
        let (root, store) = temp_store("existing");
        let (first, _) = store.create_bucket("alpha").unwrap();
        create_initial_buckets(&store, "alpha").unwrap();
        let again = store.get_bucket("alpha").unwrap().unwrap();
        assert_eq!(again.created_at, first.created_at);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn empty_list_is_a_no_op() {
        let (root, store) = temp_store("empty");
        create_initial_buckets(&store, "").unwrap();
        assert!(store.list_buckets().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_invalid_bucket_name() {
        let (root, store) = temp_store("invalid");
        let err = create_initial_buckets(&store, "ok-bucket,Bad_Name").unwrap_err();
        assert!(err.contains("Bad_Name"), "{err}");
        let _ = std::fs::remove_dir_all(root);
    }
}
