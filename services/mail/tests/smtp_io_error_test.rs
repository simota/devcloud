use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use devcloud_mail::{RecordingStore, Service, SmtpConfig, SmtpLimits, SmtpServer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct ReadErrorStream(Arc<Mutex<Vec<u8>>>);

impl AsyncRead for ReadErrorStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionReset)))
    }
}

impl AsyncWrite for ReadErrorStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn read_error_closes_silently_even_when_idle_timeout_is_configured() {
    let output = Arc::new(Mutex::new(Vec::new()));
    SmtpServer::new(
        SmtpConfig::default(),
        Arc::new(Service::new(Arc::new(RecordingStore::new()))),
    )
    .with_limits(SmtpLimits {
        idle_timeout: Some(std::time::Duration::from_secs(1)),
        ..Default::default()
    })
    .handle_conn(ReadErrorStream(output.clone()))
    .await;
    assert_eq!(*output.lock().unwrap(), b"220 devcloud ESMTP ready\r\n");
}
