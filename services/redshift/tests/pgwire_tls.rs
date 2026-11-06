//! TLS on the PostgreSQL wire endpoint: SSLRequest negotiation, certificate
//! verification by name, plain connections alongside TLS, and `require`.
//! A throwaway CA and server certificate are issued per test with rcgen.

use std::path::PathBuf;
use std::sync::Arc;

use devcloud_redshift::tls::sql_tls_from_files;
use devcloud_redshift::{Config, Server};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType};
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

struct Pki {
    dir: PathBuf,
    ca: CertificateDer<'static>,
}

impl Drop for Pki {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Pki {
    fn cert(&self) -> String {
        self.dir.join("server.pem").to_string_lossy().into_owned()
    }
    fn key(&self) -> String {
        self.dir.join("server.key").to_string_lossy().into_owned()
    }
}

/// A CA, and a server certificate it signs for `localhost` and `127.0.0.1`.
fn issue(tag: &str) -> Pki {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-redshift-tls-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "devcloud test CA");
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let server = params.signed_by(&server_key, &ca, &ca_key).unwrap();
    std::fs::write(dir.join("server.pem"), server.pem()).unwrap();
    std::fs::write(dir.join("server.key"), server_key.serialize_pem()).unwrap();
    Pki {
        dir,
        ca: ca.der().clone(),
    }
}

async fn start(sql_tls: Option<devcloud_redshift::tls::SqlTls>) -> u16 {
    let server = Arc::new(Server::new(Config {
        sql_tls,
        ..Config::default()
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { server.serve_sql(listener).await });
    port
}

fn connector(ca: &CertificateDer<'static>) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    roots.add(ca.clone()).unwrap();
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

async fn read_message<S: AsyncRead + Unpin>(s: &mut S) -> (u8, Vec<u8>) {
    let mut tag = [0u8; 1];
    s.read_exact(&mut tag).await.unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).await.unwrap();
    let mut payload = vec![0u8; u32::from_be_bytes(len) as usize - 4];
    s.read_exact(&mut payload).await.unwrap();
    (tag[0], payload)
}

async fn send(s: &mut (impl AsyncWrite + Unpin), tag: u8, payload: &[u8]) {
    let mut msg = vec![tag];
    msg.extend(((payload.len() + 4) as u32).to_be_bytes());
    msg.extend(payload);
    s.write_all(&msg).await.unwrap();
}

/// Startup, cleartext password, `select 1`; returns the first message after
/// the startup (an ErrorResponse when the server refuses the session).
async fn login_and_select_one<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S) -> Result<(), String> {
    let mut body = Vec::new();
    body.extend(196608i32.to_be_bytes());
    body.extend(b"user\0dev\0database\0dev\0\0");
    let mut startup = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    startup.extend(body);
    s.write_all(&startup).await.unwrap();
    let (tag, payload) = read_message(s).await;
    if tag == b'E' {
        return Err(String::from_utf8_lossy(&payload).into_owned());
    }
    assert_eq!(tag, b'R', "auth request");
    send(s, b'p', b"dev\0").await;
    loop {
        if read_message(s).await.0 == b'Z' {
            break;
        }
    }
    send(s, b'Q', b"select 1\0").await;
    let mut saw_row = false;
    loop {
        match read_message(s).await {
            (b'D', payload) if payload.ends_with(b"1") => saw_row = true,
            (b'Z', _) => break,
            _ => {}
        }
    }
    assert!(saw_row);
    send(s, b'X', &[]).await;
    Ok(())
}

async fn ssl_request(stream: &mut TcpStream) -> u8 {
    stream.write_all(&SSL_REQUEST).await.unwrap();
    let mut answer = [0u8; 1];
    stream.read_exact(&mut answer).await.unwrap();
    answer[0]
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_session_verified_by_name() {
    let pki = issue("verified");
    let port = start(sql_tls_from_files(&pki.cert(), &pki.key(), false).unwrap()).await;
    for name in ["localhost", "127.0.0.1"] {
        let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(ssl_request(&mut tcp).await, b'S');
        let server_name = ServerName::try_from(name.to_string()).unwrap();
        let mut tls = connector(&pki.ca).connect(server_name, tcp).await.unwrap();
        login_and_select_one(&mut tls).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mismatched_name_or_untrusted_ca_fails_the_handshake() {
    let pki = issue("mismatch");
    let port = start(sql_tls_from_files(&pki.cert(), &pki.key(), false).unwrap()).await;

    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(ssl_request(&mut tcp).await, b'S');
    let wrong_name = ServerName::try_from("host.docker.internal".to_string()).unwrap();
    assert!(connector(&pki.ca).connect(wrong_name, tcp).await.is_err());

    let other = issue("other-ca");
    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(ssl_request(&mut tcp).await, b'S');
    let name = ServerName::try_from("localhost".to_string()).unwrap();
    assert!(connector(&other.ca).connect(name, tcp).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn plain_connections_still_work_unless_required() {
    let pki = issue("plain");
    let port = start(sql_tls_from_files(&pki.cert(), &pki.key(), false).unwrap()).await;
    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    login_and_select_one(&mut tcp).await.unwrap();

    let port = start(sql_tls_from_files(&pki.cert(), &pki.key(), true).unwrap()).await;
    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let refused = login_and_select_one(&mut tcp).await.unwrap_err();
    assert!(refused.contains("requires TLS"), "{refused}");
    // TLS still works when required.
    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(ssl_request(&mut tcp).await, b'S');
    let name = ServerName::try_from("localhost".to_string()).unwrap();
    let mut tls = connector(&pki.ca).connect(name, tcp).await.unwrap();
    login_and_select_one(&mut tls).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn without_tls_ssl_request_is_declined() {
    let port = start(None).await;
    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(ssl_request(&mut tcp).await, b'N');
    login_and_select_one(&mut tcp).await.unwrap();
}

#[test]
fn configuration_errors() {
    assert!(sql_tls_from_files("", "", false).unwrap().is_none());
    assert!(sql_tls_from_files("", "", true).is_err());
    assert!(sql_tls_from_files("/x/cert.pem", "", false).is_err());
    let err = sql_tls_from_files("/nonexistent/cert.pem", "/nonexistent/key.pem", false)
        .err()
        .unwrap();
    assert!(err.contains("/nonexistent/cert.pem"), "{err}");
}
