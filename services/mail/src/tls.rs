//! TLS support for the Mail endpoints (SMTP STARTTLS/SMTPS, IMAP STARTTLS/IMAPS,
//! POP3 STLS/POP3S).
//!
//! Supports either user-supplied PEM certificate/key files (matching the
//! Redshift TLS configuration model) or a locally generated self-signed
//! certificate (via `rcgen`, in memory with zero network access).

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rcgen::{CertificateParams, DnType, KeyPair, SanType};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

/// TLS configuration shared across Mail listeners.
#[derive(Clone)]
pub struct MailTls {
    pub acceptor: TlsAcceptor,
    /// Refuse plaintext authentication / mail commands until TLS is negotiated.
    pub require: bool,
    certs_der: Arc<Vec<CertificateDer<'static>>>,
}

impl MailTls {
    /// Builds a TLS configuration from a PEM certificate chain (leaf first)
    /// and a PEM private key (PKCS#8, PKCS#1, or SEC1).
    pub fn from_pem_files(cert_file: &str, key_file: &str, require: bool) -> Result<Self, String> {
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_file)
            .map_err(|e| format!("read TLS certificate {cert_file}: {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("parse TLS certificate {cert_file}: {e}"))?;
        if certs.is_empty() {
            return Err(format!("TLS certificate {cert_file} holds no certificate"));
        }
        let key = PrivateKeyDer::from_pem_file(key_file)
            .map_err(|e| format!("read TLS private key {key_file}: {e}"))?;
        build_mail_tls(certs, key, require)
            .map_err(|e| format!("TLS certificate {cert_file} and key {key_file}: {e}"))
    }

    /// Generates an in-memory self-signed certificate for local development
    /// (`localhost`, `devcloud.local`, `127.0.0.1`, `::1`). No disk or network I/O.
    pub fn self_signed(require: bool) -> Result<Self, String> {
        let key_pair = KeyPair::generate().map_err(|e| format!("generate self-signed key: {e}"))?;
        let mut params =
            CertificateParams::new(vec!["localhost".to_string(), "devcloud.local".to_string()])
                .map_err(|e| format!("build self-signed params: {e}"))?;
        params
            .distinguished_name
            .push(DnType::CommonName, "devcloud-mail");
        params
            .subject_alt_names
            .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
        params
            .subject_alt_names
            .push(SanType::IpAddress("::1".parse().unwrap()));
        let cert = params
            .self_signed(&key_pair)
            .map_err(|e| format!("self-sign certificate: {e}"))?;
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::try_from(key_pair.serialize_der())
            .map_err(|e| format!("serialize self-signed key: {e}"))?;
        build_mail_tls(vec![cert_der], key_der, require)
    }

    /// Returns the DER-encoded server certificate chain (leaf first).
    pub fn certs_der(&self) -> &[CertificateDer<'static>] {
        &self.certs_der
    }
}

fn build_mail_tls(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    require: bool,
) -> Result<MailTls, String> {
    let certs_arc = Arc::new(certs.clone());
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("TLS protocol versions: {e}"))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("TLS server config: {e}"))?;
    Ok(MailTls {
        acceptor: TlsAcceptor::from(Arc::new(config)),
        require,
        certs_der: certs_arc,
    })
}

/// Resolves Mail TLS configuration from config fields.
///
/// - If both `cert_file` and `key_file` are provided, loads them from disk.
/// - If only one of `cert_file` / `key_file` is provided, returns a config error.
/// - If neither is provided and `enabled || require`, generates a local self-signed certificate.
/// - Otherwise returns `Ok(None)`.
pub fn mail_tls_from_config(
    enabled: bool,
    cert_file: &str,
    key_file: &str,
    require: bool,
) -> Result<Option<MailTls>, String> {
    let cert = cert_file.trim();
    let key = key_file.trim();
    match (cert.is_empty(), key.is_empty()) {
        (false, false) => Ok(Some(MailTls::from_pem_files(cert, key, require)?)),
        (true, true) if enabled || require => Ok(Some(MailTls::self_signed(require)?)),
        (true, true) => Ok(None),
        _ => Err("mail TLS needs both a certificate file and a key file".to_string()),
    }
}

/// Unified transport stream supporting seamless upgrade from plaintext to TLS.
pub(crate) enum MaybeTls<S> {
    Plain(S),
    Tls(Box<TlsStream<S>>),
}

impl<S> MaybeTls<S> {
    pub(crate) fn into_plain(self) -> Option<S> {
        match self {
            MaybeTls::Plain(s) => Some(s),
            MaybeTls::Tls(_) => None,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeTls<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeTls<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Runs a TLS server handshake, optionally bounded by `timeout`.
pub(crate) async fn accept_tls<S>(
    stream: S,
    acceptor: &TlsAcceptor,
    timeout: Option<Duration>,
) -> io::Result<MaybeTls<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tls = match timeout {
        Some(limit) => tokio::time::timeout(limit, acceptor.accept(stream))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timeout"))??,
        None => acceptor.accept(stream).await?,
    };
    Ok(MaybeTls::Tls(Box::new(tls)))
}

/// Constant-time byte comparison for credential checks across SMTP/IMAP/POP3.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

/// Returns `true` if a `TcpListener::accept` error is transient and the accept
/// loop should back off and continue rather than terminate the service.
pub(crate) fn is_transient_accept_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
    ) || matches!(e.raw_os_error(), Some(12 | 23 | 24))
}
