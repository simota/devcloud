//! Minimal hand-rolled RESP2 client over TCP or TLS.
//!
//! Only the subset the control surface needs is implemented: connect (+ optional
//! `AUTH` and `SELECT`), command send as a RESP array of bulk strings, and reply
//! parse for the five RESP2 kinds (simple string `+`, error `-`, integer `:`,
//! bulk string `$`, array `*`, plus null bulk/array).
//!
//! SAFETY: this module never logs. In particular it never logs the AUTH password
//! or any command arguments or values — the audit discipline lives in `http.rs`
//! (`audit_mutation`: command + key only).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls_pki_types::{pem::PemObject, CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

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

#[derive(Clone)]
pub struct Upstream {
    pub addr: String,
    pub username: String,
    pub password: String,
    tls: Option<(ServerName<'static>, TlsConnector)>,
}

impl Upstream {
    pub fn new(addr: String, username: String, password: String) -> Self {
        Self {
            addr,
            username,
            password,
            tls: None,
        }
    }

    /// Verify the URL host against the peer certificate and trust the supplied PEM roots.
    pub fn enable_tls(&mut self, ca_file: &Path) -> Result<(), String> {
        let host = self
            .addr
            .rsplit_once(':')
            .map(|(host, _)| host.trim_matches(['[', ']']))
            .ok_or("redis TLS requires a host and port")?;
        let server_name =
            ServerName::try_from(host.to_owned()).map_err(|_| "invalid redis TLS server name")?;
        let certs =
            CertificateDer::pem_file_iter(ca_file).map_err(|_| "cannot open Redis TLS CA file")?;
        let mut roots = RootCertStore::empty();
        for cert in certs {
            roots
                .add(cert.map_err(|_| "invalid PEM in Redis TLS CA file")?)
                .map_err(|_| "invalid certificate in Redis TLS CA file")?;
        }
        if roots.is_empty() {
            return Err("Redis TLS CA file has no certificates".into());
        }
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|_| "cannot configure Redis TLS protocol versions")?
                .with_root_certificates(roots)
                .with_no_client_auth();
        self.tls = Some((server_name, TlsConnector::from(Arc::new(config))));
        Ok(())
    }
}

trait RespStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> RespStream for T {}

/// A single RESP2 connection. Owns the stream after a completed handshake
/// (AUTH + SELECT). One connection is opened per redis database, mirroring the
/// legacy `dbClients` map keyed by db index.
pub struct RespConn {
    stream: Box<dyn RespStream>,
}

impl RespConn {
    /// Connects, negotiates TLS when configured, then authenticates and selects the DB.
    pub async fn connect(upstream: &Upstream, db: i64) -> Result<RespConn, String> {
        let tcp = timeout(IO_TIMEOUT, TcpStream::connect(&upstream.addr))
            .await
            .map_err(|_| "connect redis: timed out".to_string())?
            .map_err(|e| format!("connect redis: {e}"))?;
        let stream: Box<dyn RespStream> = if let Some((name, connector)) = &upstream.tls {
            let tls = timeout(IO_TIMEOUT, connector.connect(name.clone(), tcp))
                .await
                .map_err(|_| "redis TLS handshake: timed out")?
                .map_err(|e| format!("redis TLS handshake: {e}"))?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        let mut conn = RespConn { stream };
        if !upstream.username.is_empty() {
            conn.expect_reply(&["AUTH", &upstream.username, &upstream.password], "OK")
                .await?;
        } else if !upstream.password.is_empty() {
            conn.expect_reply(&["AUTH", &upstream.password], "OK")
                .await?;
        }
        if db != 0 {
            conn.expect_reply(&["SELECT", &db.to_string()], "OK")
                .await?;
        } else {
            conn.ping().await?;
        }
        Ok(conn)
    }

    pub async fn ping(&mut self) -> Result<(), String> {
        self.expect_reply(&["PING"], "PONG").await
    }

    async fn expect_reply(&mut self, args: &[&str], expected: &str) -> Result<(), String> {
        match self.command(args).await {
            Ok(Value::Simple(reply)) if reply == expected => Ok(()),
            // An upstream can include credentials in a handshake error.
            _ => Err(format!("redis {} failed", args[0])),
        }
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
    use tokio::net::TcpListener;
    use tokio_rustls::{rustls::ServerConfig, TlsAcceptor};

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

    #[tokio::test]
    async fn acl_auth_sends_username_and_password_and_sanitizes_upstream_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = Upstream::new(
            listener.local_addr().unwrap().to_string(),
            "reader".into(),
            "test-password".into(),
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            const EXPECTED: &[u8] = b"*3\r\n$4\r\nAUTH\r\n$6\r\nreader\r\n$13\r\ntest-password\r\n";
            let mut received = [0; EXPECTED.len()];
            stream.read_exact(&mut received).await.unwrap();
            assert!(received == EXPECTED, "expected ACL authentication frame");
            stream
                .write_all(b"-WRONGPASS reader test-password\r\n")
                .await
                .unwrap();
        });
        assert_eq!(
            crate::validate_upstream(&upstream, 1).await.unwrap_err(),
            "redis AUTH failed"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_rejects_an_untrusted_certificate_and_a_wrong_hostname() {
        let directory = tempfile::tempdir().unwrap();
        let ca_file = directory.path().join("redis-ca.pem");
        for trusted in [false, true] {
            let names = if trusted { "localhost" } else { "127.0.0.1" };
            let cert = rcgen::generate_simple_self_signed(vec![names.into()]).unwrap();
            let unrelated = rcgen::generate_simple_self_signed(vec![names.into()]).unwrap();
            let root = if trusted { &cert.cert } else { &unrelated.cert };
            std::fs::write(&ca_file, root.pem()).unwrap();
            let config = ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls_pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
            )
            .unwrap();
            let acceptor = TlsAcceptor::from(Arc::new(config));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut upstream = Upstream::new(
                listener.local_addr().unwrap().to_string(),
                "reader".into(),
                "test-password".into(),
            );
            upstream.enable_tls(&ca_file).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                assert!(acceptor.accept(stream).await.is_err());
            });
            let error = crate::validate_upstream(&upstream, 0).await.unwrap_err();
            assert!(error.starts_with("redis TLS handshake:"), "{error}");
            server.await.unwrap();
        }
    }
}
