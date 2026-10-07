//! Container image functions (`PackageType: Image`) under Docker.
//!
//! An image environment is one `docker run` of the function's image with the
//! port of the image's Runtime Interface Emulator (RIE, shipped in every AWS
//! Lambda base image and started by its entrypoint when
//! `AWS_LAMBDA_RUNTIME_API` is unset) published on a loopback port.
//! Invocations are POSTed to the RIE invoke endpoint, one at a time. The
//! container's output is the environment's log; RIE's own lines are dropped
//! and devcloud frames the log like every other invocation.

use std::collections::HashMap;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Where the RIE listens inside the container.
pub const RIE_PORT: u16 = 8080;

const INVOKE_PATH: &str = "/2015-03-31/functions/function/invocations";

/// Largest response devcloud accepts from the RIE (Lambda's 6 MB limit, with
/// room for the HTTP framing of an error document).
const MAX_RESPONSE_BYTES: usize = 7 * 1024 * 1024;

/// What runs: the image plus the `ImageConfig` overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageSpec {
    pub uri: String,
    pub entry_point: Vec<String>,
    pub command: Vec<String>,
    pub working_directory: String,
}

/// How devcloud reaches the RIE in a function's container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reach {
    /// The RIE port is published on this loopback port of the Docker host
    /// (devcloud runs on the host).
    Loopback(u16),
    /// The container joins this Docker network and is reached by name
    /// (devcloud itself runs in a container on that network).
    Network(String),
}

/// Where to connect for one container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Reach {
    pub fn endpoint(&self, container_name: &str) -> Endpoint {
        match self {
            Reach::Loopback(port) => Endpoint {
                host: "127.0.0.1".into(),
                port: *port,
            },
            Reach::Network(_) => Endpoint {
                host: container_name.into(),
                port: RIE_PORT,
            },
        }
    }
}

/// `docker run` arguments. Environment variables come from `env_file`, never
/// the command line, where any local user could read them.
pub fn run_args(
    spec: &ImageSpec,
    name: &str,
    reach: &Reach,
    memory_mb: i64,
    env_file: &std::path::Path,
) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        name.to_string(),
    ];
    match reach {
        Reach::Loopback(port) => {
            args.push("-p".to_string());
            args.push(format!("127.0.0.1:{port}:{RIE_PORT}"));
        }
        Reach::Network(network) => {
            args.push("--network".to_string());
            args.push(network.clone());
        }
    }
    // MemorySize is a real limit here, unlike for local processes.
    args.push("--memory".to_string());
    args.push(format!("{memory_mb}m"));
    args.push("--env-file".to_string());
    args.push(env_file.to_string_lossy().into_owned());
    if let Some(entry) = spec.entry_point.first() {
        args.push("--entrypoint".to_string());
        args.push(entry.clone());
    }
    if !spec.working_directory.is_empty() {
        args.push("-w".to_string());
        args.push(spec.working_directory.clone());
    }
    // `--` ends docker's own options: an image such as `--privileged` is a
    // (bad) image name, never a flag.
    args.push("--".to_string());
    args.push(spec.uri.clone());
    args.extend(spec.entry_point.iter().skip(1).cloned());
    args.extend(spec.command.iter().cloned());
    args
}

/// Writes `vars` as a `docker run --env-file` (owner-only: it holds the
/// function's variables and credentials). The format has no quoting, so a
/// value with a line break cannot be passed.
pub fn write_env_file(path: &std::path::Path, vars: &[(String, String)]) -> Result<(), String> {
    use std::io::Write;
    let mut text = String::new();
    for (k, v) in vars {
        // The API validates names; the file format must not be bent anyway.
        if k.is_empty() || k.contains(['=', '\n', '\r', '\0']) {
            return Err(format!("invalid environment variable name {k:?}"));
        }
        if v.contains(['\n', '\r']) {
            return Err(format!(
                "environment variable {k} contains a line break, which Docker cannot pass to a container"
            ));
        }
        text.push_str(&format!("{k}={v}\n"));
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .and_then(|mut f| f.write_all(text.as_bytes()))
        .map_err(|e| format!("write {}: {e}", path.display()))
}

/// One HTTP response from the RIE.
#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

/// True once something answers HTTP on `port` (docker publishes the port
/// before the RIE inside listens, so a TCP connect alone proves nothing).
pub async fn probe(at: &Endpoint) -> bool {
    let attempt = async {
        let mut s = TcpStream::connect((at.host.as_str(), at.port)).await.ok()?;
        s.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .ok()?;
        let mut head = [0u8; 8];
        s.read_exact(&mut head).await.ok()?;
        head.starts_with(b"HTTP/1.").then_some(())
    };
    tokio::time::timeout(Duration::from_secs(2), attempt)
        .await
        .ok()
        .flatten()
        .is_some()
}

/// POSTs one invocation payload to the RIE.
pub async fn invoke(at: &Endpoint, payload: &[u8]) -> std::io::Result<Response> {
    let mut s = TcpStream::connect((at.host.as_str(), at.port)).await?;
    let head = format!(
        "POST {INVOKE_PATH} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    s.write_all(head.as_bytes()).await?;
    s.write_all(payload).await?;
    read_response(&mut s).await
}

async fn read_response(s: &mut TcpStream) -> std::io::Result<Response> {
    let bad = |msg: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string());
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > 64 * 1024 {
            return Err(bad("response head too large"));
        }
        let mut chunk = [0u8; 8192];
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before a response",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let leftover = buf[head_end + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| bad("malformed status line"))?;
    let headers: HashMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let body =
        if headers.contains_key("transfer-encoding") || headers.contains_key("content-length") {
            crate::body::read_body(s, leftover, &headers, MAX_RESPONSE_BYTES)
                .await?
                .map_err(|e| bad(&format!("response body: {e:?}")))?
        } else {
            // Close-delimited.
            let mut body = leftover;
            s.take(MAX_RESPONSE_BYTES as u64)
                .read_to_end(&mut body)
                .await?;
            body
        };
    Ok(Response { status, body })
}

/// Whether an RIE response body is a function error. The RIE answers 200
/// either way, so the error document is recognised by its shape: an object
/// with `errorType` and `errorMessage` and nothing but error fields.
pub fn is_error_document(body: &[u8]) -> bool {
    const ERROR_FIELDS: &[&str] = &[
        "errorMessage",
        "errorType",
        "stackTrace",
        "requestId",
        "cause",
        "trace",
    ];
    let Ok(serde_json::Value::Object(map)) = serde_json::from_slice::<serde_json::Value>(body)
    else {
        return false;
    };
    map.get("errorType")
        .is_some_and(serde_json::Value::is_string)
        && map.contains_key("errorMessage")
        && map.keys().all(|k| ERROR_FIELDS.contains(&k.as_str()))
}

/// The function's own output from a slice of container log: the RIE's
/// diagnostics and START/END/REPORT framing are dropped. When the slice
/// covers the runtime's init (a cold start), it is split into the init
/// output, the invoke output, and the init duration the RIE measured.
pub struct ContainerLog {
    pub init: Option<(Duration, String)>,
    pub body: String,
}

pub fn split_log(raw: &str) -> ContainerLog {
    let mut lines = Vec::new();
    let mut init = None;
    for line in raw.lines() {
        if let Some((_, rest)) = line.split_once("(rapid) INIT REPORT(durationMs: ") {
            // The container's output is also the handler's: a look-alike line
            // with NaN, inf, or a negative number is just output.
            let Some(duration) = rest
                .split(')')
                .next()
                .and_then(|v| v.trim().parse::<f64>().ok())
                .and_then(|ms| Duration::try_from_secs_f64(ms / 1000.0).ok())
            else {
                lines.push(line);
                continue;
            };
            init = Some((duration, std::mem::take(&mut lines).join("\n")));
            continue;
        }
        if line.contains("(rapid)")
            || line.starts_with("START RequestId:")
            || line.starts_with("END RequestId:")
            || line.starts_with("REPORT RequestId:")
        {
            continue;
        }
        lines.push(line);
    }
    ContainerLog {
        init,
        body: lines.join("\n"),
    }
}

/// True once the RIE has logged the end of an invocation.
pub fn invocation_logged(raw: &[u8]) -> bool {
    raw.windows(b"REPORT RequestId:".len())
        .any(|w| w == b"REPORT RequestId:")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_args_publish_loopback_and_keep_values_off_the_command_line() {
        let spec = ImageSpec {
            uri: "my-fn:latest".into(),
            entry_point: vec!["/lambda-entrypoint.sh".into(), "--debug".into()],
            command: vec!["app.handler".into()],
            working_directory: "/var/task".into(),
        };
        let args = run_args(
            &spec,
            "devcloud-lambda-x",
            &Reach::Loopback(45000),
            512,
            std::path::Path::new("/w/env"),
        );
        let joined = args.join(" ");
        assert!(joined.starts_with("run --rm --name devcloud-lambda-x -p 127.0.0.1:45000:8080 --memory 512m --env-file /w/env --entrypoint /lambda-entrypoint.sh -w /var/task -- my-fn:latest --debug app.handler"), "{joined}");
    }

    #[test]
    fn env_file_refuses_names_that_bend_the_format() {
        let path = std::env::temp_dir().join(format!("devcloud-envfile-{}", std::process::id()));
        for name in ["A=B", "X\nAWS_LAMBDA_FUNCTION_NAME", ""] {
            assert!(
                write_env_file(&path, &[(name.into(), "v".into())]).is_err(),
                "{name:?}"
            );
        }
        write_env_file(&path, &[("GOOD".into(), "a=b".into())]).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "GOOD=a=b\n");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn split_log_keeps_bogus_init_reports_as_output() {
        for bogus in ["NaN", "inf", "-5", "-inf", "1e400"] {
            let line = format!("[INFO] (rapid) INIT REPORT(durationMs: {bogus})");
            let log = split_log(&format!("{line}\nhello\n"));
            assert!(log.init.is_none(), "{bogus}");
            assert_eq!(log.body, format!("{line}\nhello"), "{bogus}");
        }
    }

    #[test]
    fn network_reach_joins_the_network_instead_of_publishing() {
        let spec = ImageSpec {
            uri: "img".into(),
            ..ImageSpec::default()
        };
        let reach = Reach::Network("app_default".into());
        let args = run_args(&spec, "c1", &reach, 128, std::path::Path::new("/e")).join(" ");
        assert!(args.contains("--network app_default"), "{args}");
        assert!(!args.contains(" -p "), "{args}");
        assert_eq!(
            reach.endpoint("c1"),
            Endpoint {
                host: "c1".into(),
                port: RIE_PORT
            }
        );
        assert_eq!(Reach::Loopback(4000).endpoint("c1").host, "127.0.0.1");
    }

    #[test]
    fn error_documents_are_recognised_by_shape() {
        assert!(is_error_document(
            br#"{"errorMessage":"boom","errorType":"TestError"}"#
        ));
        assert!(is_error_document(
            br#"{"errorMessage":"x","errorType":"E","stackTrace":["a"],"requestId":"r"}"#
        ));
        assert!(!is_error_document(
            br#"{"errorType":"E","errorMessage":"x","status":1}"#
        ));
        assert!(!is_error_document(br#"{"errorMessage":"x"}"#));
        assert!(!is_error_document(br#""errorType""#));
    }

    #[test]
    fn split_log_drops_rie_lines_and_finds_init() {
        let raw = "06 Oct 2026 09:13:17,920 [INFO] (rapid) exec '/var/runtime/bootstrap'\nSTART RequestId: a Version: $LATEST\n06 Oct 2026 [INFO] (rapid) INIT START(type: on-demand, phase: init)\ninit pid=16\n06 Oct 2026 [INFO] (rapid) INIT REPORT(durationMs: 1.757000)\n06 Oct 2026 [INFO] (rapid) INVOKE START(requestId: a)\ninvocation 1\nEND RequestId: a\nREPORT RequestId: a\tInit Duration: 0.10 ms\tDuration: 6.12 ms\n";
        let log = split_log(raw);
        let (d, init_out) = log.init.unwrap();
        assert_eq!(init_out, "init pid=16");
        assert!((d.as_secs_f64() * 1000.0 - 1.757).abs() < 1e-6);
        assert_eq!(log.body, "invocation 1");
        assert!(invocation_logged(raw.as_bytes()));

        let warm =
            split_log("START RequestId: b Version: $LATEST\ninvocation 2\nEND RequestId: b\n");
        assert!(warm.init.is_none());
        assert_eq!(warm.body, "invocation 2");
    }
}
