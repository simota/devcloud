//! Minimal hand-rolled RESP2 client over a tokio `TcpStream`.
//!
//! The legacy reference (`internal/services/redis/server.rs`) is a CLIENT of the
//! upstream `redis-server` via the Redis client library. There is no Rust RESP
//! client in the workspace, so this hand-rolls RESP2 the same way the redshift
//! backend hand-rolls a pgwire client (`devcloud-redshift`
//! `src/backend_postgres.rs`): a std-first wire client on a single tokio
//! `TcpStream`, no third-party redis crate.
//!
//! Only the subset the control surface needs is implemented: connect (+ optional
//! `AUTH` and `SELECT`), command send as a RESP array of bulk strings, and reply
//! parse for the five RESP2 kinds (simple string `+`, error `-`, integer `:`,
//! bulk string `$`, array `*`, plus null bulk/array).
//!
//! SAFETY: this module never logs. In particular it never logs the AUTH password
//! or any command arguments or values — the audit discipline lives in `http.rs`
//! (`audit_mutation`: command + key only).

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Matches the Redis client dial/read/write timeouts the legacy `newClientForDB` sets
/// (2s each). Applied to connect and to every command round-trip.
const IO_TIMEOUT: Duration = Duration::from_secs(2);

/// A parsed RESP2 reply value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Simple string (`+OK`).
    Simple(String),
    /// Error (`-ERR ...`). Carried as the error text (without the leading `-`).
    Error(String),
    /// Integer (`:42`).
    Integer(i64),
    /// Bulk string (`$3\r\nfoo`). `None` is the null bulk string (`$-1`).
    Bulk(Option<Vec<u8>>),
    /// Array (`*2\r\n...`). `None` is the null array (`*-1`).
    Array(Option<Vec<Value>>),
}

impl Value {
    /// Renders a scalar value to its string form, mirroring how Redis client
    /// stringifies a single reply element. Null and arrays return `None`.
    pub fn as_text(&self) -> Option<String> {
        match self {
            Value::Simple(s) => Some(s.clone()),
            Value::Integer(n) => Some(n.to_string()),
            Value::Bulk(Some(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
            Value::Bulk(None) => None,
            Value::Error(_) => None,
            Value::Array(_) => None,
        }
    }
}

/// A single RESP2 connection. Owns the `TcpStream` after a completed handshake
/// (AUTH + SELECT). One connection is opened per redis database, mirroring the
/// legacy `dbClients` map keyed by db index.
pub struct RespConn {
    stream: TcpStream,
}

impl RespConn {
    /// Connects to `addr` (host:port), then issues `AUTH` when a password is set
    /// and `SELECT db`. Mirrors Redis client `Options{Addr, Password, DB}`.
    pub async fn connect(addr: &str, password: &str, db: i64) -> Result<RespConn, String> {
        let stream = timeout(IO_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| "connect redis: timed out".to_string())?
            .map_err(|e| format!("connect redis: {e}"))?;
        let mut conn = RespConn { stream };
        if !password.is_empty() {
            // AUTH carries the password as a bulk-string argument; it is never
            // logged here or anywhere in the crate.
            conn.command(&["AUTH", password]).await?;
        }
        if db != 0 {
            conn.command(&["SELECT", &db.to_string()]).await?;
        } else {
            // PING confirms liveness even on db 0 (legacy Run pings after connect).
            conn.command(&["PING"]).await?;
        }
        Ok(conn)
    }

    /// Sends a command as a RESP array of bulk strings and reads one reply.
    /// A RESP error reply is surfaced as `Err` carrying the redis error text.
    pub async fn command(&mut self, args: &[&str]) -> Result<Value, String> {
        self.send(args).await?;
        let value = self.read_value().await?;
        if let Value::Error(message) = value {
            return Err(message);
        }
        Ok(value)
    }

    /// Like [`command`], but accepts owned/borrowed string args (the Exec path
    /// builds an args vec at runtime).
    pub async fn command_owned(&mut self, args: &[String]) -> Result<Value, String> {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        self.command(&refs).await
    }

    async fn send(&mut self, args: &[&str]) -> Result<(), String> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        timeout(IO_TIMEOUT, self.stream.write_all(&buf))
            .await
            .map_err(|_| "write redis command: timed out".to_string())?
            .map_err(|e| format!("write redis command: {e}"))?;
        timeout(IO_TIMEOUT, self.stream.flush())
            .await
            .map_err(|_| "flush redis command: timed out".to_string())?
            .map_err(|e| format!("flush redis command: {e}"))
    }

    async fn read_value(&mut self) -> Result<Value, String> {
        timeout(IO_TIMEOUT, self.read_value_inner())
            .await
            .map_err(|_| "read redis reply: timed out".to_string())?
    }

    async fn read_value_inner(&mut self) -> Result<Value, String> {
        let line = self.read_line().await?;
        if line.is_empty() {
            return Err("empty redis reply".to_string());
        }
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => Ok(Value::Simple(rest.to_string())),
            "-" => Ok(Value::Error(rest.to_string())),
            ":" => rest
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| "invalid redis integer reply".to_string()),
            "$" => {
                let len: i64 = rest
                    .parse()
                    .map_err(|_| "invalid redis bulk length".to_string())?;
                if len < 0 {
                    return Ok(Value::Bulk(None));
                }
                let bytes = self.read_exact_bytes(len as usize).await?;
                Ok(Value::Bulk(Some(bytes)))
            }
            "*" => {
                let count: i64 = rest
                    .parse()
                    .map_err(|_| "invalid redis array length".to_string())?;
                if count < 0 {
                    return Ok(Value::Array(None));
                }
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    items.push(Box::pin(self.read_value_inner()).await?);
                }
                Ok(Value::Array(Some(items)))
            }
            other => Err(format!("unsupported redis reply prefix {other:?}")),
        }
    }

    /// Reads one `\r\n`-terminated line (without the terminator).
    async fn read_line(&mut self) -> Result<String, String> {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = self
                .stream
                .read(&mut byte)
                .await
                .map_err(|e| format!("read redis reply: {e}"))?;
            if n == 0 {
                return Err("redis connection closed".to_string());
            }
            if byte[0] == b'\n' {
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
                break;
            }
            buf.push(byte[0]);
        }
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    /// Reads exactly `len` bytes plus the trailing `\r\n`.
    async fn read_exact_bytes(&mut self, len: usize) -> Result<Vec<u8>, String> {
        let mut bytes = vec![0u8; len];
        self.stream
            .read_exact(&mut bytes)
            .await
            .map_err(|e| format!("read redis bulk: {e}"))?;
        let mut crlf = [0u8; 2];
        self.stream
            .read_exact(&mut crlf)
            .await
            .map_err(|e| format!("read redis bulk terminator: {e}"))?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_as_text_renders_scalars() {
        assert_eq!(Value::Simple("OK".into()).as_text().as_deref(), Some("OK"));
        assert_eq!(Value::Integer(42).as_text().as_deref(), Some("42"));
        assert_eq!(
            Value::Bulk(Some(b"hello".to_vec())).as_text().as_deref(),
            Some("hello")
        );
        assert_eq!(Value::Bulk(None).as_text(), None);
        assert_eq!(Value::Array(None).as_text(), None);
    }
}
