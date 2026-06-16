//! Redis control/introspection HTTP surface — Rust port of
//! `internal/services/redis` (`http.rs` + `server.rs`).
//!
//! The legacy server is a CLIENT of the upstream `redis-server` via Redis client; here
//! the RESP client is hand-rolled over tokio TCP (precedent: the redshift pgwire
//! client in `devcloud-redshift`), keeping the crate std-first with deps limited
//! to tokio/serde/serde_json.
//!
//! IMPORTANT asymmetry (carried from the legacy comment): this listener stays in the
//! single binary even under the Rust redis engine — the redis *data plane* is
//! the upstream `redis-server` process, and this surface is only its dashboard
//! control client. It binds loopback only (the orchestrator passes a loopback
//! addr), the primary blast-radius control for the arbitrary-command Exec
//! surface.
//!
//! Public API: [`Config`] + [`run`]. The orchestrator wires it from its own
//! config (`cfg.server.redis_http_port`, `cfg.server.redis_port`,
//! `cfg.auth.redis.mode`, `cfg.auth.redis.password`, and the redis mode for the
//! status display field).
//!
//! SAFETY: no credential, password, Authorization header, or key VALUE is ever
//! logged. `audit_mutation` (in [`http`]) logs command + key only; the RESP
//! client ([`resp`]) and auth parser never log.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;

mod command_allowlist;
mod http;
mod resp;
mod server;

pub use command_allowlist::CommandClass;

/// Configuration for the redis control surface, mirroring the inputs legacy
/// `HTTPConfig` + `Config` carry. The orchestrator builds this from its own
/// `Config`:
///
/// | field            | orchestrator source                                  |
/// |------------------|------------------------------------------------------|
/// | `http_addr`      | loopback + `cfg.server.redis_http_port`              |
/// | `redis_addr`     | loopback + `cfg.server.redis_port` (data plane)     |
/// | `redis_password` | `cfg.auth.redis.password`                            |
/// | `control_auth_mode` | `cfg.auth.redis.mode` ("relaxed" | "strict")     |
/// | `control_password`  | `cfg.auth.redis.password` (legacy reuses it)         |
/// | `mode`           | redis mode display string ("managed" | "external")  |
/// | `database_count` | 0 = probe via `CONFIG GET databases` (default 16)   |
#[derive(Debug, Clone)]
pub struct Config {
    /// `host:port` the control HTTP listener binds (loopback).
    pub http_addr: String,
    /// `host:port` of the upstream redis data plane.
    pub redis_addr: String,
    /// Upstream redis password (sent via `AUTH`; never logged). Empty in relaxed.
    pub redis_password: String,
    /// Control-surface auth mode: "relaxed" (open) or "strict" (HTTP Basic).
    pub control_auth_mode: String,
    /// Control-surface password checked in strict mode (HTTP Basic). The legacy
    /// reference reuses the redis password; the orchestrator passes the same.
    pub control_password: String,
    /// Display-only mode string for the status response ("managed"/"external").
    pub mode: String,
    /// Database count; 0 probes the upstream (`CONFIG GET databases`, default 16).
    pub database_count: i64,
    /// Optional in-process dashboard event sink. When set, mutation events are
    /// forwarded as `{"type","service","payload"}` JSON strings (parity with
    /// the legacy `redis.command.mutation` events emitted by Server mutations).
    pub event_sink: Option<UnboundedSender<String>>,
}

impl Config {
    fn auth(&self) -> http::HttpAuth {
        http::HttpAuth {
            auth_mode: self.control_auth_mode.clone(),
            password: self.control_password.clone(),
        }
    }
}

/// Runs the redis control/introspection HTTP surface until `shutdown` resolves.
///
/// Binds `cfg.http_addr`, then serves only `/_introspect/` (reads) and
/// `/_control/` (mutations + Exec) over a hand-rolled HTTP/1.1 transport. Every
/// operation is a thin transport over the RESP client against `cfg.redis_addr`,
/// never any other path.
pub async fn run(
    cfg: Config,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), String> {
    let listener = TcpListener::bind(&cfg.http_addr)
        .await
        .map_err(|e| format!("bind redis control listener {}: {e}", cfg.http_addr))?;

    let auth = cfg.auth();
    let emit = build_emitter(cfg.event_sink.clone());
    let server = Arc::new(server::Server::new(
        cfg.redis_addr.clone(),
        cfg.redis_password.clone(),
        normalize_mode(&cfg.mode),
        cfg.database_count,
        emit,
    ));

    // Best-effort: probe the upstream database count + warm the db-0 connection,
    // mirroring legacy Run (which pings + CONFIG GET databases at startup). A dead
    // upstream is not fatal here — reads/mutations surface 502 per request, same
    // as the legacy listener which always runs regardless of upstream liveness.
    server.refresh_database_count().await;

    http::serve(listener, server, auth, shutdown)
        .await
        .map_err(|e| format!("redis control listener: {e}"))
}

/// Builds the mutation event emitter. Mirrors legacy `emitMutation`: the payload
/// carries `command` and (when non-empty) `key`, NEVER a value. When no sink is
/// installed it is a no-op (the legacy path also only emits when a publisher is set).
fn build_emitter(sink: Option<UnboundedSender<String>>) -> Box<dyn Fn(&str, &str) + Send + Sync> {
    Box::new(move |command: &str, key: &str| {
        let Some(tx) = sink.as_ref() else {
            return;
        };
        let mut payload = serde_json::json!({ "command": command });
        if !key.is_empty() {
            payload["key"] = serde_json::Value::String(key.to_string());
        }
        let event = serde_json::json!({
            "type": "redis.command.mutation",
            "service": "redis",
            "payload": payload,
        })
        .to_string();
        let _ = tx.send(event);
    })
}

fn normalize_mode(mode: &str) -> String {
    let mode = mode.trim().to_ascii_lowercase();
    if mode.is_empty() {
        "managed".to_string()
    } else {
        mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emitter_omits_key_when_empty_and_never_carries_value() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let emit = build_emitter(Some(tx));

        emit("FLUSHDB", "");
        let event = rx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&event).unwrap();
        assert_eq!(parsed["type"], "redis.command.mutation");
        assert_eq!(parsed["service"], "redis");
        assert_eq!(parsed["payload"]["command"], "FLUSHDB");
        assert!(parsed["payload"].get("key").is_none());

        emit("DEL", "user:1");
        let event = rx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&event).unwrap();
        assert_eq!(parsed["payload"]["command"], "DEL");
        assert_eq!(parsed["payload"]["key"], "user:1");
    }

    #[test]
    fn emitter_without_sink_is_noop() {
        let emit = build_emitter(None);
        emit("SET", "k"); // must not panic
    }

    #[test]
    fn mode_normalizes() {
        assert_eq!(normalize_mode(""), "managed");
        assert_eq!(normalize_mode(" External "), "external");
    }
}
