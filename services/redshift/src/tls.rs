//! TLS for the PostgreSQL wire endpoint.
//!
//! devcloud never issues certificates: the user supplies a PEM certificate
//! chain and private key (for example from a local CA its clients trust), and
//! clients that ask for SSL (`SSLRequest`) get a TLS 1.2+ session with them.

use std::sync::Arc;

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_rustls::TlsAcceptor;

/// TLS settings for the SQL endpoint.
#[derive(Clone)]
pub struct SqlTls {
    pub acceptor: TlsAcceptor,
    /// Refuse clients that do not negotiate TLS.
    pub require: bool,
}

/// Builds an acceptor from a PEM certificate chain (leaf first) and a PEM
/// private key (PKCS#8, PKCS#1, or SEC1).
pub fn acceptor_from_pem_files(cert_file: &str, key_file: &str) -> Result<TlsAcceptor, String> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_file)
        .map_err(|e| format!("read TLS certificate {cert_file}: {e}"))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parse TLS certificate {cert_file}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("TLS certificate {cert_file} holds no certificate"));
    }
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|e| format!("read TLS private key {key_file}: {e}"))?;
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            // rustls offers only TLS 1.2 and 1.3.
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("TLS protocol versions: {e}"))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("TLS certificate {cert_file} and key {key_file}: {e}"))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// The SQL endpoint's TLS settings from configuration: both files or
/// neither. `require` without a certificate is a configuration error.
pub fn sql_tls_from_files(
    cert_file: &str,
    key_file: &str,
    require: bool,
) -> Result<Option<SqlTls>, String> {
    match (cert_file.trim().is_empty(), key_file.trim().is_empty()) {
        (true, true) if require => {
            Err("redshift TLS is required but no certificate and key are configured".to_string())
        }
        (true, true) => Ok(None),
        (false, false) => Ok(Some(SqlTls {
            acceptor: acceptor_from_pem_files(cert_file.trim(), key_file.trim())?,
            require,
        })),
        _ => Err("redshift TLS needs both a certificate file and a key file".to_string()),
    }
}
