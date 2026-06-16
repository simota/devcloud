//! Redis control client operations — port of `internal/services/redis/server.rs`.
//!
//! legacy `*Server` is a Redis client client; here the same operations run over the
//! hand-rolled RESP client in [`crate::resp`]. The behaviors reproduced:
//!
//! - `Status`   → INFO parse (server/clients/memory/keyspace) + currentDB/count.
//! - `Keys`     → SCAN cursor/match/count, then per-key TYPE + TTL.
//! - `KeyDetail`→ TYPE + TTL, then a per-type value read (string/list/hash/set/zset).
//! - `SetCurrentDB`, `DatabaseCount`, `ensure_client_for_db` → per-db connections.
//! - `DeleteKey` / `ExpireKey` / `FlushDB` → mutations + `redis.command.mutation` events.
//! - `Exec`     → arbitrary allowlisted command + reply-row formatting.
//!
//! Connections are opened lazily per database and cached, mirroring legacy
//! `dbClients` map guarded by a mutex.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::command_allowlist::{command_allowed, CommandClass};
use crate::resp::{RespConn, Value};

/// Default database count when redis does not report `databases` via CONFIG GET,
/// mirroring `defaultDatabaseCount`.
const DEFAULT_DATABASE_COUNT: i64 = 16;

/// Status snapshot, mirroring legacy `Status` struct.
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub mode: String,
    pub address: String,
    pub server_version: String,
    pub connected_clients: i64,
    pub used_memory_human: String,
    pub current_db: i64,
    pub database_count: i64,
    pub current_db_keys: i64,
}

/// A single key summary, mirroring `KeySummary`.
#[derive(Debug, Clone)]
pub struct KeySummary {
    pub key: String,
    pub key_type: String,
    pub ttl_seconds: i64,
}

/// Result of a SCAN page, mirroring `KeysSnapshot`.
#[derive(Debug, Clone, Default)]
pub struct KeysSnapshot {
    pub cursor: u64,
    pub next_cursor: u64,
    pub keys: Vec<KeySummary>,
}

/// Key detail, mirroring `KeyDetail`.
#[derive(Debug, Clone)]
pub struct KeyDetail {
    pub key: String,
    pub key_type: String,
    pub ttl_seconds: i64,
    pub preview: Vec<String>,
}

/// Result of an Exec, mirroring `CommandResult`.
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub command: String,
    pub class: CommandClass,
    pub rows: Vec<String>,
}

/// Sentinel returned when a command is not allowlisted, mirroring
/// `ErrCommandNotAllowed`. Carried as the `Err` string so the handler can map it
/// to 403.
pub const ERR_COMMAND_NOT_ALLOWED: &str = "redis command is not allowlisted";

/// The control server's client over the upstream redis, mirroring `*Server`.
/// `mode`/`address` are display-only (the dashboard status fields); the data
/// plane connection target is `addr` + `password`.
pub struct Server {
    addr: String,
    password: String,
    mode: String,
    state: Mutex<State>,
    /// Emits a `redis.command.mutation` event carrying command + optional key,
    /// never values. Mirrors legacy `emitMutation`.
    emit: Box<dyn Fn(&str, &str) + Send + Sync>,
}

struct State {
    clients: HashMap<i64, RespConn>,
    current_db: i64,
    database_count: i64,
}

impl Server {
    /// Builds a server. `mode` and `address` populate the display fields of the
    /// status response (`managed`/`external` and the redis address). `emit` is
    /// invoked on every mutation with `(command, key)` — `key` empty when none.
    pub fn new(
        addr: String,
        password: String,
        mode: String,
        database_count: i64,
        emit: Box<dyn Fn(&str, &str) + Send + Sync>,
    ) -> Server {
        let database_count = if database_count > 0 {
            database_count
        } else {
            DEFAULT_DATABASE_COUNT
        };
        Server {
            addr,
            password,
            mode,
            state: Mutex::new(State {
                clients: HashMap::new(),
                current_db: 0,
                database_count,
            }),
            emit,
        }
    }

    pub fn current_db(&self) -> i64 {
        self.state.lock().unwrap().current_db
    }

    pub fn database_count(&self) -> i64 {
        self.state.lock().unwrap().database_count
    }

    /// On first connect, refreshes `database_count` from `CONFIG GET databases`
    /// (mirrors legacy Run). Best-effort: a failure leaves the default in place.
    pub async fn refresh_database_count(&self) {
        let mut conn = match RespConn::connect(&self.addr, &self.password, 0).await {
            Ok(conn) => conn,
            Err(_) => return,
        };
        if let Ok(Value::Array(Some(items))) = conn.command(&["CONFIG", "GET", "databases"]).await {
            if let Some(Value::Bulk(Some(bytes))) = items.get(1) {
                if let Ok(n) = String::from_utf8_lossy(bytes).parse::<i64>() {
                    if n > 0 {
                        self.state.lock().unwrap().database_count = n;
                    }
                }
            }
        }
        // Cache the db-0 connection so the first status/keys call reuses it.
        let mut state = self.state.lock().unwrap();
        state.clients.entry(0).or_insert(conn);
    }

    /// Runs `op` with a mutable borrow of the connection for `db`, opening and
    /// caching it on first use (mirrors `ensureClientForDB` + `currentClient`).
    ///
    /// A tokio `TcpStream` is held across the await, so the connection cannot be
    /// stored behind the std `Mutex` while awaiting. Instead the connection is
    /// taken out of the map for the duration of the call and returned after,
    /// which also serializes access per db (matching Redis client' single-conn use
    /// here).
    async fn with_conn<F, Fut, T>(&self, db: i64, op: F) -> Result<T, String>
    where
        F: FnOnce(RespConn) -> Fut,
        Fut: std::future::Future<Output = (RespConn, Result<T, String>)>,
    {
        let existing = self.state.lock().unwrap().clients.remove(&db);
        let conn = match existing {
            Some(conn) => conn,
            None => RespConn::connect(&self.addr, &self.password, db).await?,
        };
        let (conn, result) = op(conn).await;
        // Only re-cache a healthy connection; on error drop it so the next call
        // reconnects (Redis client pools transparently reconnect on broken conns).
        if result.is_ok() {
            self.state.lock().unwrap().clients.insert(db, conn);
        }
        result
    }

    /// Mirrors `Status`: INFO parse for the running redis plus the local
    /// mode/address/currentDB/databaseCount fields.
    pub async fn status(&self) -> Result<Status, String> {
        let (current_db, database_count) = {
            let state = self.state.lock().unwrap();
            (state.current_db, state.database_count)
        };
        let mut status = Status {
            mode: self.mode.clone(),
            address: self.addr.clone(),
            current_db,
            database_count,
            ..Default::default()
        };
        let info = self
            .with_conn(current_db, |mut conn| async move {
                let reply = conn
                    .command(&["INFO", "server", "clients", "memory", "keyspace"])
                    .await;
                (conn, reply)
            })
            .await
            .map_err(|e| format!("read redis status: {e}"))?;
        let text = info.as_text().unwrap_or_default();
        let values = parse_redis_info(&text);
        status.server_version = values.get("redis_version").cloned().unwrap_or_default();
        status.used_memory_human = values.get("used_memory_human").cloned().unwrap_or_default();
        status.connected_clients = values
            .get("connected_clients")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        status.current_db_keys = parse_db_key_count(&values, current_db);
        Ok(status)
    }

    /// Mirrors `Keys`: SCAN one page, then TYPE + TTL per key.
    pub async fn keys(
        &self,
        cursor: u64,
        match_pattern: &str,
        count: i64,
    ) -> Result<KeysSnapshot, String> {
        let pattern = if match_pattern.trim().is_empty() {
            "*".to_string()
        } else {
            match_pattern.to_string()
        };
        let count = if count <= 0 { 100 } else { count };
        let current_db = self.current_db();
        let mut snapshot = KeysSnapshot {
            cursor,
            next_cursor: 0,
            keys: Vec::new(),
        };

        let (next_cursor, key_names) = self
            .with_conn(current_db, |mut conn| {
                let pattern = pattern.clone();
                async move {
                    let reply = conn
                        .command(&[
                            "SCAN",
                            &cursor.to_string(),
                            "MATCH",
                            &pattern,
                            "COUNT",
                            &count.to_string(),
                        ])
                        .await;
                    let parsed = reply.and_then(|value| parse_scan_reply(&value));
                    (conn, parsed)
                }
            })
            .await
            .map_err(|e| format!("scan redis keys: {e}"))?;
        snapshot.next_cursor = next_cursor;

        for key in key_names {
            let (key_type, ttl) = self
                .with_conn(current_db, |mut conn| {
                    let key = key.clone();
                    async move {
                        let typ = match conn.command(&["TYPE", &key]).await {
                            Ok(v) => v.as_text().unwrap_or_default(),
                            Err(_) => String::new(),
                        };
                        let ttl = match conn.command(&["TTL", &key]).await {
                            Ok(Value::Integer(n)) => n,
                            _ => 0,
                        };
                        (conn, Ok::<(String, i64), String>((typ, ttl)))
                    }
                })
                .await?;
            snapshot.keys.push(KeySummary {
                key,
                key_type,
                ttl_seconds: ttl,
            });
        }
        Ok(snapshot)
    }

    /// Mirrors `KeyDetail`: TYPE + TTL, then a per-type preview read. A missing
    /// key reports type "none" with ttl -2 (redis' missing-key TTL), never an
    /// error.
    pub async fn key_detail(&self, key: &str) -> Result<KeyDetail, String> {
        let current_db = self.current_db();
        let mut detail = KeyDetail {
            key: key.to_string(),
            key_type: "none".to_string(),
            ttl_seconds: -2,
            preview: Vec::new(),
        };
        let key = key.to_string();
        let result = self
            .with_conn(current_db, |mut conn| {
                let key = key.clone();
                async move {
                    let detail = read_key_detail(&mut conn, &key).await;
                    (conn, detail)
                }
            })
            .await?;
        if let Some(read) = result {
            detail = read;
        }
        Ok(detail)
    }

    /// Mirrors `SetCurrentDB`: validates the index, opens/pings the db
    /// connection, then records it as current.
    pub async fn set_current_db(&self, db: i64) -> Result<(), String> {
        let count = self.database_count();
        if db < 0 {
            return Err(format!("db must be >= 0, got {db}"));
        }
        if count > 0 && db >= count {
            return Err(format!("db {db} out of range (0..{})", count - 1));
        }
        // Ensure a connection exists and is live for this db.
        self.with_conn(db, |mut conn| async move {
            let reply = conn.command(&["PING"]).await.map(|_| ());
            (conn, reply)
        })
        .await
        .map_err(|e| format!("ping redis db {db}: {e}"))?;
        self.state.lock().unwrap().current_db = db;
        Ok(())
    }

    /// Mirrors `DeleteKey`: DEL on the current db, emit DEL mutation.
    pub async fn delete_key(&self, key: &str) -> Result<i64, String> {
        let current_db = self.current_db();
        let key = key.to_string();
        let deleted = self
            .with_conn(current_db, |mut conn| {
                let key = key.clone();
                async move {
                    let reply = match conn.command(&["DEL", &key]).await {
                        Ok(Value::Integer(n)) => Ok(n),
                        Ok(_) => Ok(0),
                        Err(e) => Err(format!("delete redis key: {e}")),
                    };
                    (conn, reply)
                }
            })
            .await?;
        (self.emit)("DEL", &key);
        Ok(deleted)
    }

    /// Mirrors `ExpireKey`: EXPIRE on the current db, emit EXPIRE mutation.
    pub async fn expire_key(&self, key: &str, ttl_seconds: i64) -> Result<bool, String> {
        let current_db = self.current_db();
        let key = key.to_string();
        let updated = self
            .with_conn(current_db, |mut conn| {
                let key = key.clone();
                async move {
                    let reply = match conn
                        .command(&["EXPIRE", &key, &ttl_seconds.to_string()])
                        .await
                    {
                        Ok(Value::Integer(n)) => Ok(n == 1),
                        Ok(_) => Ok(false),
                        Err(e) => Err(format!("expire redis key: {e}")),
                    };
                    (conn, reply)
                }
            })
            .await?;
        (self.emit)("EXPIRE", &key);
        Ok(updated)
    }

    /// Mirrors `FlushDB`: FLUSHDB on the current db, emit FLUSHDB mutation.
    pub async fn flush_db(&self) -> Result<String, String> {
        let current_db = self.current_db();
        let result = self
            .with_conn(current_db, |mut conn| async move {
                let reply = match conn.command(&["FLUSHDB"]).await {
                    Ok(value) => Ok(value.as_text().unwrap_or_else(|| "OK".to_string())),
                    Err(e) => Err(format!("flush redis db: {e}")),
                };
                (conn, reply)
            })
            .await?;
        (self.emit)("FLUSHDB", "");
        Ok(result)
    }

    /// Mirrors `Exec`: allowlist gate, run the arbitrary command, format reply
    /// rows, emit a mutation event for mutation-class commands.
    pub async fn exec(&self, command: &str, args: &[String]) -> Result<CommandResult, String> {
        let name = command.trim().to_ascii_uppercase();
        let class = match command_allowed(&name) {
            Some(class) => class,
            None => return Err(ERR_COMMAND_NOT_ALLOWED.to_string()),
        };
        let current_db = self.current_db();
        let mut parts: Vec<String> = Vec::with_capacity(1 + args.len());
        parts.push(name.clone());
        parts.extend(args.iter().cloned());

        let rows = self
            .with_conn(current_db, |mut conn| {
                let parts = parts.clone();
                let name = name.clone();
                async move {
                    let reply = conn.command_owned(&parts).await;
                    let rows = match reply {
                        Ok(value) => Ok(redis_result_rows(&value)),
                        Err(e) => Err(format!("execute redis command {name}: {e}")),
                    };
                    (conn, rows)
                }
            })
            .await?;

        if class == CommandClass::Mutation {
            let key = args.first().map(String::as_str).unwrap_or("");
            (self.emit)(&name, key);
        }
        Ok(CommandResult {
            command: name,
            class,
            rows,
        })
    }
}

/// Reads TYPE + TTL + per-type preview for `key`. Returns `None` only when the
/// type/ttl reads themselves fail (so the caller keeps the default "none"
/// detail); a value read miss yields a detail with an empty preview, matching
/// legacy behavior for `redis.Nil` on the string GET.
async fn read_key_detail(conn: &mut RespConn, key: &str) -> Result<Option<KeyDetail>, String> {
    let key_type = conn
        .command(&["TYPE", key])
        .await
        .map_err(|e| format!("read redis key type: {e}"))?
        .as_text()
        .unwrap_or_default();
    let ttl = match conn.command(&["TTL", key]).await {
        Ok(Value::Integer(n)) => n,
        Ok(_) => -2,
        Err(e) => return Err(format!("read redis key ttl: {e}")),
    };
    let mut detail = KeyDetail {
        key: key.to_string(),
        key_type: key_type.clone(),
        ttl_seconds: ttl,
        preview: Vec::new(),
    };
    match key_type.as_str() {
        "string" => {
            match conn.command(&["GET", key]).await {
                Ok(Value::Bulk(Some(bytes))) => {
                    detail.preview = vec![String::from_utf8_lossy(&bytes).into_owned()];
                }
                Ok(Value::Bulk(None)) => {} // redis.Nil → empty preview
                Ok(other) => {
                    if let Some(text) = other.as_text() {
                        detail.preview = vec![text];
                    }
                }
                Err(e) => return Err(format!("read redis string key: {e}")),
            }
        }
        "list" => {
            let values = read_string_array(conn, &["LRANGE", key, "0", "49"]).await?;
            detail.preview = values;
        }
        "hash" => {
            let pairs = read_string_array(conn, &["HGETALL", key]).await?;
            detail.preview = map_preview(&pairs);
        }
        "set" => {
            let values = read_string_array(conn, &["SMEMBERS", key]).await?;
            detail.preview = values;
        }
        "zset" => {
            let values = read_string_array(conn, &["ZRANGE", key, "0", "49", "WITHSCORES"]).await?;
            detail.preview = zset_preview(&values);
        }
        _ => {}
    }
    Ok(Some(detail))
}

async fn read_string_array(conn: &mut RespConn, args: &[&str]) -> Result<Vec<String>, String> {
    match conn.command(args).await {
        Ok(Value::Array(Some(items))) => Ok(items
            .iter()
            .map(|v| v.as_text().unwrap_or_default())
            .collect()),
        Ok(Value::Array(None)) => Ok(Vec::new()),
        Ok(_) => Ok(Vec::new()),
        Err(e) => Err(format!("read redis key: {e}")),
    }
}

/// Mirrors `parseRedisInfo`: `key:value` lines, skipping blanks and `#` sections.
fn parse_redis_info(info: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for line in info.split('\n') {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            values.insert(key.to_string(), value.to_string());
        }
    }
    values
}

/// Mirrors `parseDBKeyCount`: parse `keys=N` out of the `dbN:` keyspace line.
fn parse_db_key_count(values: &HashMap<String, String>, db: i64) -> i64 {
    let Some(value) = values.get(&format!("db{db}")) else {
        return 0;
    };
    for part in value.split(',') {
        if let Some((key, raw)) = part.split_once('=') {
            if key == "keys" {
                return raw.parse().unwrap_or(0);
            }
        }
    }
    0
}

/// Parses a SCAN reply: `[next_cursor_bulk, [keys...]]`.
fn parse_scan_reply(value: &Value) -> Result<(u64, Vec<String>), String> {
    let Value::Array(Some(items)) = value else {
        return Err("unexpected SCAN reply".to_string());
    };
    if items.len() < 2 {
        return Err("short SCAN reply".to_string());
    }
    let next_cursor = items[0].as_text().unwrap_or_default().parse().unwrap_or(0);
    let keys = match &items[1] {
        Value::Array(Some(elems)) => elems
            .iter()
            .map(|v| v.as_text().unwrap_or_default())
            .collect(),
        _ => Vec::new(),
    };
    Ok((next_cursor, keys))
}

/// Mirrors `mapPreview`: `"key: value"` lines, sorted (matches legacy
/// `sort.Strings`). Input is a flat HGETALL field/value array.
fn map_preview(pairs: &[String]) -> Vec<String> {
    if pairs.is_empty() {
        return Vec::new();
    }
    let mut preview = Vec::with_capacity(pairs.len() / 2);
    let mut iter = pairs.iter();
    while let (Some(field), Some(value)) = (iter.next(), iter.next()) {
        preview.push(format!("{field}: {value}"));
    }
    preview.sort();
    preview
}

/// Mirrors `zsetPreview`: `"member: score"` from a flat ZRANGE WITHSCORES array
/// (member, score, member, score, ...). The score is rendered with legacy `%g`.
fn zset_preview(values: &[String]) -> Vec<String> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut preview = Vec::with_capacity(values.len() / 2);
    let mut iter = values.iter();
    while let (Some(member), Some(score)) = (iter.next(), iter.next()) {
        preview.push(format!("{member}: {}", format_score(score)));
    }
    preview
}

/// Renders a redis zset score string the way legacy `%g` would for a float64,
/// so `42` stays `42` and `1.5` stays `1.5`. redis returns the score as a
/// string already; re-parse to normalize trailing `.0`-style forms.
fn format_score(raw: &str) -> String {
    match raw.parse::<f64>() {
        Ok(n) => format_g(n),
        Err(_) => raw.to_string(),
    }
}

/// Approximates legacy `%g` for the score values this surface sees: integral
/// floats render without a decimal point, others use the shortest round-trip
/// representation Rust produces (which matches `%g` for these magnitudes).
fn format_g(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

/// Mirrors `redisResultRows`: flattens a reply into string rows. Nil → empty,
/// scalars → one row, arrays → one row per element (`fmt.Sprint` semantics).
fn redis_result_rows(value: &Value) -> Vec<String> {
    match value {
        Value::Bulk(None) | Value::Array(None) => Vec::new(),
        Value::Simple(s) => vec![s.clone()],
        Value::Integer(n) => vec![n.to_string()],
        Value::Bulk(Some(bytes)) => vec![String::from_utf8_lossy(bytes).into_owned()],
        Value::Array(Some(items)) => items
            .iter()
            .map(|v| v.as_text().unwrap_or_default())
            .collect(),
        Value::Error(message) => vec![message.clone()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_info_skips_sections_and_blanks() {
        let info = "# Server\r\nredis_version:7.2.0\r\n\r\n# Clients\r\nconnected_clients:3\r\n";
        let values = parse_redis_info(info);
        assert_eq!(values.get("redis_version").unwrap(), "7.2.0");
        assert_eq!(values.get("connected_clients").unwrap(), "3");
    }

    #[test]
    fn parse_db_key_count_reads_keyspace_line() {
        let mut values = HashMap::new();
        values.insert("db0".to_string(), "keys=12,expires=1,avg_ttl=0".to_string());
        assert_eq!(parse_db_key_count(&values, 0), 12);
        assert_eq!(parse_db_key_count(&values, 1), 0);
    }

    #[test]
    fn map_preview_sorts_pairs() {
        let pairs = vec![
            "b".to_string(),
            "2".to_string(),
            "a".to_string(),
            "1".to_string(),
        ];
        assert_eq!(map_preview(&pairs), vec!["a: 1", "b: 2"]);
    }

    #[test]
    fn zset_preview_pairs_member_and_score() {
        let values = vec![
            "alice".to_string(),
            "1".to_string(),
            "bob".to_string(),
            "2.5".to_string(),
        ];
        assert_eq!(zset_preview(&values), vec!["alice: 1", "bob: 2.5"]);
    }

    #[test]
    fn result_rows_flatten_kinds() {
        assert_eq!(redis_result_rows(&Value::Bulk(None)), Vec::<String>::new());
        assert_eq!(redis_result_rows(&Value::Integer(7)), vec!["7"]);
        assert_eq!(
            redis_result_rows(&Value::Bulk(Some(b"hi".to_vec()))),
            vec!["hi"]
        );
        assert_eq!(
            redis_result_rows(&Value::Array(Some(vec![
                Value::Bulk(Some(b"a".to_vec())),
                Value::Integer(2),
            ]))),
            vec!["a", "2"]
        );
    }

    #[test]
    fn scan_reply_parsed() {
        let value = Value::Array(Some(vec![
            Value::Bulk(Some(b"5".to_vec())),
            Value::Array(Some(vec![
                Value::Bulk(Some(b"k1".to_vec())),
                Value::Bulk(Some(b"k2".to_vec())),
            ])),
        ]));
        let (cursor, keys) = parse_scan_reply(&value).unwrap();
        assert_eq!(cursor, 5);
        assert_eq!(keys, vec!["k1", "k2"]);
    }
}
