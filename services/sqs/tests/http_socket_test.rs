//! Regression test for PERF-1: a `ReceiveMessage` long poll on one queue must
//! not serialize other queues' operations behind it. Unlike the other
//! `http_*_test.rs` files (which drive `dispatch_json`/`dispatch_query`
//! directly against an owned `Server`), this test runs the real socket
//! server (`devcloud_sqs::http::serve`) so the server-lock contention the bug
//! was about is actually exercised.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use devcloud_sqs::{Config, Server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn cfg() -> Config {
    Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        queue_url_host: "127.0.0.1:9324".to_string(),
        ..Default::default()
    }
}

fn json_request(target: &str, body: &str) -> Vec<u8> {
    format!(
        "POST / HTTP/1.1\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// Reads a full HTTP/1.1 response (status + `Content-Length`-bounded body).
async fn read_http_response(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.unwrap();
        if n == 0 {
            break buf.len();
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status: u16 = status_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = buf[(header_end + 4).min(buf.len())..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_on_one_queue_does_not_block_another_queue() {
    let mut server = Server::new(cfg());
    let url_a = server
        .create_queue("QueueA", &Default::default(), &Default::default())
        .unwrap()
        .url;
    let url_b = server
        .create_queue("QueueB", &Default::default(), &Default::default())
        .unwrap()
        .url;
    let server = Arc::new(Mutex::new(server));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(devcloud_sqs::http::serve(
        listener,
        Arc::clone(&server),
        std::future::pending(),
    ));

    // `t0` anchors both measurements. Timing must be read relative to this
    // single fixed point rather than an `Instant::now()` captured later in
    // the test task: if a synchronous, lock-holding long poll ever stalls the
    // whole executor (not just the mutex), even the test's own `.await`
    // points downstream of it get delayed, which would silently hide the
    // regression from a "restart the clock, then measure" style assertion.
    let t0 = Instant::now();

    // Start a 1s long poll on QueueA (empty — no message is ever sent there).
    let mut stream_a = TcpStream::connect(addr).await.unwrap();
    let body_a = format!(r#"{{"QueueUrl":"{url_a}","WaitTimeSeconds":1}}"#);
    stream_a
        .write_all(&json_request("AmazonSQS.ReceiveMessage", &body_a))
        .await
        .unwrap();

    // Let the server accept the connection and get into the poll loop (first
    // attempt + first 100ms sleep) before issuing the second request.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A SendMessage on the unrelated QueueB must complete quickly — it must
    // not queue up behind QueueA's still-running long poll.
    let mut stream_b = TcpStream::connect(addr).await.unwrap();
    let body_b = format!(r#"{{"QueueUrl":"{url_b}","MessageBody":"hello"}}"#);
    stream_b
        .write_all(&json_request("AmazonSQS.SendMessage", &body_b))
        .await
        .unwrap();
    let (status_b, resp_b) = read_http_response(&mut stream_b).await;
    let elapsed_b = t0.elapsed();

    let (status_a, resp_a) = read_http_response(&mut stream_a).await;
    let elapsed_a = t0.elapsed();

    assert_eq!(status_b, 200, "{}", String::from_utf8_lossy(&resp_b));
    assert!(
        elapsed_b < Duration::from_millis(500),
        "QueueB's SendMessage finished {elapsed_b:?} after the test began — \
         appears blocked behind QueueA's long poll (expected well under \
         QueueA's 1s WaitTimeSeconds)"
    );

    assert_eq!(status_a, 200, "{}", String::from_utf8_lossy(&resp_a));
    let parsed_a: serde_json::Value = serde_json::from_slice(&resp_a).unwrap();
    assert_eq!(parsed_a["Messages"].as_array().unwrap().len(), 0);
    // Sanity: QueueA's poll actually ran close to its full WaitTimeSeconds
    // (i.e. this isn't passing merely because the poll was skipped/short).
    assert!(
        elapsed_a >= Duration::from_millis(900),
        "QueueA's long poll returned too early: {elapsed_a:?}"
    );
}

fn query_form_request(body: &str) -> Vec<u8> {
    format!(
        "POST / HTTP/1.1\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// Regression test for the Query-protocol counterpart of PERF-1: a
/// `ReceiveMessage` long poll issued via the legacy form-encoded/XML protocol
/// must not serialize other queues' operations behind it either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_long_poll_on_one_queue_does_not_block_another_queue() {
    let mut server = Server::new(cfg());
    let url_a = server
        .create_queue("QueueA", &Default::default(), &Default::default())
        .unwrap()
        .url;
    let url_b = server
        .create_queue("QueueB", &Default::default(), &Default::default())
        .unwrap()
        .url;
    let server = Arc::new(Mutex::new(server));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(devcloud_sqs::http::serve(
        listener,
        Arc::clone(&server),
        std::future::pending(),
    ));

    let t0 = Instant::now();

    // Start a 1s long poll on QueueA (empty — no message is ever sent there)
    // via the Query/XML protocol.
    let mut stream_a = TcpStream::connect(addr).await.unwrap();
    let body_a = format!(
        "Action=ReceiveMessage&Version=2012-11-05&QueueUrl={url_a}&WaitTimeSeconds=1",
        url_a = urlencoding_lite(&url_a)
    );
    stream_a
        .write_all(&query_form_request(&body_a))
        .await
        .unwrap();

    // Let the server accept the connection and get into the poll loop (first
    // attempt + first 100ms sleep) before issuing the second request.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A SendMessage on the unrelated QueueB (also Query protocol) must
    // complete quickly — it must not queue up behind QueueA's still-running
    // long poll.
    let mut stream_b = TcpStream::connect(addr).await.unwrap();
    let body_b = format!(
        "Action=SendMessage&Version=2012-11-05&QueueUrl={url_b}&MessageBody=hello",
        url_b = urlencoding_lite(&url_b)
    );
    stream_b
        .write_all(&query_form_request(&body_b))
        .await
        .unwrap();
    let (status_b, resp_b) = read_http_response(&mut stream_b).await;
    let elapsed_b = t0.elapsed();

    let (status_a, resp_a) = read_http_response(&mut stream_a).await;
    let elapsed_a = t0.elapsed();

    let resp_b_str = String::from_utf8_lossy(&resp_b);
    assert_eq!(status_b, 200, "{resp_b_str}");
    assert!(resp_b_str.contains("<SendMessageResponse"), "{resp_b_str}");
    assert!(
        elapsed_b < Duration::from_millis(500),
        "QueueB's SendMessage finished {elapsed_b:?} after the test began — \
         appears blocked behind QueueA's long poll (expected well under \
         QueueA's 1s WaitTimeSeconds)"
    );

    let resp_a_str = String::from_utf8_lossy(&resp_a);
    assert_eq!(status_a, 200, "{resp_a_str}");
    assert!(
        resp_a_str.contains("<ReceiveMessageResponse"),
        "{resp_a_str}"
    );
    assert!(
        !resp_a_str.contains("<Message>"),
        "expected no messages: {resp_a_str}"
    );
    // Sanity: QueueA's poll actually ran close to its full WaitTimeSeconds
    // (i.e. this isn't passing merely because the poll was skipped/short).
    assert!(
        elapsed_a >= Duration::from_millis(900),
        "QueueA's long poll returned too early: {elapsed_a:?}"
    );
}

/// Regression test: a long-polling `ReceiveMessage` must not silently switch
/// to a same-named queue that replaced the original mid-poll. If the queue is
/// deleted and recreated while the poll is in flight, the in-flight request
/// must fail as if the (original) queue no longer exists, rather than
/// transparently continuing against the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_does_not_follow_delete_and_recreate_of_same_queue_name() {
    let mut server = Server::new(cfg());
    let url = server
        .create_queue("QueueA", &Default::default(), &Default::default())
        .unwrap()
        .url;
    let server = Arc::new(Mutex::new(server));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(devcloud_sqs::http::serve(
        listener,
        Arc::clone(&server),
        std::future::pending(),
    ));

    // Start a 2s long poll (JSON protocol) on the queue — empty, so it will
    // keep polling until the wait elapses or the queue identity check fails.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let body = format!(r#"{{"QueueUrl":"{url}","WaitTimeSeconds":2}}"#);
    stream
        .write_all(&json_request("AmazonSQS.ReceiveMessage", &body))
        .await
        .unwrap();

    // Let the poll get past its first attempt.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Delete and recreate the same-named queue directly against the shared
    // server (out from under the in-flight poll).
    {
        let mut guard = server.lock().unwrap();
        assert!(guard.delete_queue(&url));
        let recreated = guard
            .create_queue("QueueA", &Default::default(), &Default::default())
            .unwrap();
        assert_eq!(recreated.url, url, "recreated queue must reuse the URL");
    }

    let (status, resp) = read_http_response(&mut stream).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert_eq!(status, 400, "{resp_str}");
    assert!(
        resp_str.contains("QueueDoesNotExist"),
        "expected a QueueDoesNotExist error once the queue was replaced mid-poll: {resp_str}"
    );
}

/// Minimal `application/x-www-form-urlencoded` value encoder sufficient for
/// the queue URLs used in these tests (only `:` and `/` need escaping).
fn urlencoding_lite(s: &str) -> String {
    s.replace(':', "%3A").replace('/', "%2F")
}
