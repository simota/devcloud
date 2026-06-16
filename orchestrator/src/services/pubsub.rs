//! Pub/Sub service: gRPC (always) + REST (when enabled) sharing one in-memory
//! `Server` (`Arc<Mutex<Server>>`), mirroring the legacy single `*Server` and the
//! `devcloud-pubsub` binary. The two listeners share one inner shutdown fanned
//! out from the supervisor's single shutdown future.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use devcloud_pubsub::grpc::PubSubGrpc;
use devcloud_pubsub::proto::pubsub::{
    publisher_server::PublisherServer, schema_service_server::SchemaServiceServer,
    subscriber_server::SubscriberServer,
};
use devcloud_pubsub::server::{Config, Server};
use tokio::net::TcpListener;

use crate::config::Config as AppConfig;
use crate::services::util::default_string;
use crate::supervisor::shutdown_future;

pub async fn run(
    cfg: &AppConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    let config = Config {
        project: default_string(&cfg.services.pubsub.project, &cfg.auth.pubsub.project_id),
        auth_mode: cfg.auth.pubsub.mode.clone(),
        bearer_token: cfg.auth.pubsub.bearer_token.clone(),
        storage_path: default_string(
            &cfg.services.pubsub.data_dir,
            &format!("{}/pubsub", cfg.storage.path),
        ),
        message_storage_path: default_string(
            &cfg.services.pubsub.message_data_dir,
            &format!("{}/message", cfg.storage.path),
        ),
        default_ack_deadline_seconds: cfg.services.pubsub.default_ack_deadline_seconds as i64,
        message_retention_seconds: cfg.services.pubsub.message_retention_seconds as i64,
        max_ack_deadline_seconds: cfg.services.pubsub.max_ack_deadline_seconds as i64,
        max_pull_messages: cfg.services.pubsub.max_pull_messages as i64,
        streaming_pull_disabled: !cfg.services.pubsub.enable_streaming_pull,
    };

    let server = Server::new(config);
    if let Some(err) = server.load_err() {
        return Err(format!("pubsub: failed to load state: {err}"));
    }
    let shared = Arc::new(Mutex::new(server));

    // One inner shutdown, fanned out to both listeners.
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = tx.send(true);
    });

    // REST task — only when enabled (an absent REST addr disables it, as in legacy).
    let rest = if cfg.services.pubsub.enable_rest {
        let addr = format!("127.0.0.1:{}", cfg.server.pubsub_rest_port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("pubsub: bind REST {addr}: {e}"))?;
        let state = shared.clone();
        let sd = shutdown_future(rx.clone());
        Some(tokio::spawn(async move {
            devcloud_pubsub::http::serve(listener, state, sd)
                .await
                .map_err(|e| format!("pubsub REST serve error: {e}"))
        }))
    } else {
        None
    };

    // gRPC task — always on; tonic binds the addr itself.
    let grpc_addr: SocketAddr = format!("127.0.0.1:{}", cfg.server.pubsub_grpc_port)
        .parse()
        .map_err(|e| format!("pubsub: parse gRPC addr: {e}"))?;
    let adapter = PubSubGrpc::new(shared.clone());
    let sd = shutdown_future(rx.clone());
    let grpc = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PublisherServer::new(adapter.clone()))
            .add_service(SubscriberServer::new(adapter.clone()))
            .add_service(SchemaServiceServer::new(adapter))
            .serve_with_shutdown(grpc_addr, sd)
            .await
            .map_err(|e| format!("pubsub gRPC serve error: {e}"))
    });

    // Return whichever finishes/errors first.
    match rest {
        Some(rest) => tokio::select! {
            r = rest => r.map_err(|e| e.to_string()).and_then(|x| x),
            g = grpc => g.map_err(|e| e.to_string()).and_then(|x| x),
        },
        None => grpc.await.map_err(|e| e.to_string()).and_then(|x| x),
    }
}
