//! Container image functions (`PackageType: Image`): control plane, and
//! execution through a fake `docker` CLI that emulates the AWS Runtime
//! Interface Emulator (RIE) — so these run without Docker. A real-Docker
//! check runs only with `DEVCLOUD_LAMBDA_DOCKER_E2E=1`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use devcloud_lambda::http::{process, Request};
use devcloud_lambda::runtime::{FunctionCredentials, Interpreters};
use devcloud_lambda::zip::build_stored;
use devcloud_lambda::{Config, Server};
use serde_json::{json, Value};

/// `docker run ... -p 127.0.0.1:<port>:8080 --env-file <f> ... -- <image> <args>`
/// serves an RIE look-alike in the foreground; `docker rm -f <name>` kills it.
/// Every `run` records its argv and env file next to the image path
/// (`<image>.run.json`), every `rm` touches `<state>/<name>.removed`.
const FAKE_DOCKER: &str = r#"#!/usr/bin/env python3
import json, os, signal, sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state")
os.makedirs(STATE, exist_ok=True)
args = sys.argv[1:]
if args[:2] == ["image", "inspect"]:
    print(json.dumps(["default.handler"]))
    sys.exit(0)
if args[:2] == ["rm", "-f"]:
    name = args[2]
    # Slow on purpose: callers must wait for the removal, not just start it.
    time.sleep(0.3)
    open(os.path.join(STATE, name + ".removed"), "w").close()
    try:
        os.kill(int(open(os.path.join(STATE, name + ".pid")).read()), signal.SIGKILL)
    except Exception:
        pass
    sys.exit(0)
assert args[0] == "run", args
sep = args.index("--")
opts, image, rest = args[1:sep], args[sep + 1], args[sep + 2:]
get = lambda flag: opts[opts.index(flag) + 1] if flag in opts else None
port = int(get("-p").split(":")[1])
name = get("--name")
env = dict(line.rstrip("\n").split("=", 1) for line in open(get("--env-file")) if line.strip())
mode = oct(os.stat(get("--env-file")).st_mode & 0o777)
with open(image + ".run.json", "a") as f:
    f.write(json.dumps({"argv": args, "env": env, "envFileMode": mode, "name": name}) + "\n")
if os.path.basename(image) == "broken":
    print("Unable to find image 'broken:latest' locally", file=sys.stderr)
    sys.exit(125)
open(os.path.join(STATE, name + ".pid"), "w").write(str(os.getpid()))
print("06 Oct 2026 00:00:00,000 [INFO] (rapid) exec '/var/runtime/bootstrap'", flush=True)
state = {"count": 0}

class RIE(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def reply(self, code, body):
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        self.reply(404, "404 page not found")
    def do_POST(self):
        event = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
        if state["count"] == 0:
            print("[INFO] (rapid) INIT START(type: on-demand, phase: init)")
            print("loading model")
            print("06 Oct 2026 [INFO] (rapid) INIT REPORT(durationMs: 1234.500000)")
        state["count"] += 1
        print("START RequestId: rie-%d Version: $LATEST" % state["count"])
        print("handling %d" % state["count"], flush=True)
        if event.get("crash"):
            os._exit(1)
        time.sleep(event.get("sleep", 0))
        if event.get("fail"):
            body = json.dumps({"errorMessage": "boom", "errorType": "TestError", "stackTrace": []})
        else:
            body = json.dumps({"pid": os.getpid(), "count": state["count"], "argv": rest,
                               "env": env})
        print("END RequestId: rie-%d" % state["count"])
        print("REPORT RequestId: rie-%d\tDuration: 1.00 ms" % state["count"], flush=True)
        self.reply(200, body)

HTTPServer(("127.0.0.1", port), RIE).serve_forever()
"#;

struct Env {
    server: Arc<Server>,
    dir: PathBuf,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-image-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn has(bin: &str) -> bool {
    std::process::Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// A server whose `docker` is the fake above (or none).
fn env(tag: &str, docker: Option<&str>) -> Env {
    let dir = temp_dir(tag);
    let docker = docker.map(|d| {
        if d != "fake" {
            return d.to_string();
        }
        let bin = dir.join("fake-docker");
        std::fs::write(&bin, FAKE_DOCKER).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        bin.to_string_lossy().into_owned()
    });
    let server = Arc::new(Server::new(Config {
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
        interpreters: Interpreters {
            docker,
            ..Interpreters::default()
        },
        function_credentials: Some(FunctionCredentials {
            access_key_id: "AKIDLOCAL".into(),
            secret_access_key: "local-secret".into(),
            session_token: String::new(),
        }),
        ..Config::default()
    }));
    Env { server, dir }
}

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn log(&self) -> String {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(self.header("X-Amz-Log-Result").unwrap_or_default())
            .unwrap();
        String::from_utf8(raw).unwrap()
    }
}

async fn call(
    server: &Arc<Server>,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Resp {
    let req = Request {
        method: method.into(),
        raw_path: path.into(),
        query: String::new(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        body: body.to_vec(),
    };
    let r = process(server, &req).await;
    Resp {
        status: r.status,
        headers: r.headers,
        body: r.body,
    }
}

async fn create_image(server: &Arc<Server>, name: &str, image: &str, timeout: i64) -> Resp {
    let body = json!({
        "FunctionName": name,
        "PackageType": "Image",
        "Role": "r",
        "Timeout": timeout,
        "MemorySize": 512,
        "Environment": { "Variables": { "MY_VAR": "hello", "PATH": "/custom/bin" } },
        "ImageConfig": { "Command": ["app.handler"] },
        "Code": { "ImageUri": image },
    });
    call(
        server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await
}

async fn invoke(server: &Arc<Server>, name: &str, payload: &[u8]) -> Resp {
    call(
        server,
        "POST",
        &format!("/2015-03-31/functions/{name}/invocations"),
        &[("X-Amz-Log-Type", "Tail")],
        payload,
    )
    .await
}

fn runs(image: &Path) -> Vec<Value> {
    std::fs::read_to_string(image.with_extension("run.json"))
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

async fn removed(dir: &Path, name: &str) -> bool {
    for _ in 0..60 {
        if dir.join("state").join(format!("{name}.removed")).exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn image_functions_have_their_own_control_plane_shape() {
    let e = env("ctl", None);
    let r = create_image(&e.server, "img-fn", "my-repo/fn:1", 3).await;
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    let conf = r.json();
    assert_eq!(conf["PackageType"], "Image");
    assert!(
        conf.get("Runtime").is_none() && conf.get("Handler").is_none(),
        "{conf}"
    );
    assert_eq!(
        conf["ImageConfigResponse"]["ImageConfig"]["Command"],
        json!(["app.handler"])
    );

    let got = call(&e.server, "GET", "/2015-03-31/functions/img-fn", &[], b"")
        .await
        .json();
    assert_eq!(got["Code"]["RepositoryType"], "ECR");
    assert_eq!(got["Code"]["ImageUri"], "my-repo/fn:1");

    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/img-fn/code",
        &[],
        json!({ "ImageUri": "my-repo/fn:2" }).to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_ne!(r.json()["CodeSha256"], conf["CodeSha256"]);
    let got = call(&e.server, "GET", "/2015-03-31/functions/img-fn", &[], b"")
        .await
        .json();
    assert_eq!(got["Code"]["ImageUri"], "my-repo/fn:2");

    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/img-fn/configuration",
        &[],
        json!({ "ImageConfig": { "EntryPoint": ["/bin/sh"], "WorkingDirectory": "/w" } })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(
        r.json()["ImageConfigResponse"]["ImageConfig"]["WorkingDirectory"],
        "/w"
    );

    for (body, why) in [
        (
            json!({ "FunctionName": "x", "PackageType": "Image", "Role": "r", "Runtime": "python3.12", "Code": { "ImageUri": "i" } }),
            "Runtime on Image",
        ),
        (
            json!({ "FunctionName": "x", "PackageType": "Image", "Role": "r", "Code": { "ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==" } }),
            "zip on Image",
        ),
        (
            json!({ "FunctionName": "x", "PackageType": "Image", "Role": "r", "Code": {} }),
            "no ImageUri",
        ),
        (
            json!({ "FunctionName": "x", "Runtime": "python3.12", "Handler": "a.b", "Role": "r", "Code": { "ImageUri": "i" } }),
            "ImageUri on Zip",
        ),
        (
            json!({ "FunctionName": "x", "Runtime": "python3.12", "Handler": "a.b", "Role": "r", "ImageConfig": {}, "Code": { "ZipFile": base64::engine::general_purpose::STANDARD.encode(build_stored(&[("a.py", b"")])) } }),
            "ImageConfig on Zip",
        ),
        (
            json!({ "FunctionName": "x", "PackageType": "Docker", "Role": "r", "Code": { "ImageUri": "i" } }),
            "unknown PackageType",
        ),
    ] {
        let r = call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes(),
        )
        .await;
        assert_eq!(r.status, 400, "{why}: {}", String::from_utf8_lossy(&r.body));
    }
    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/img-fn/code",
        &[],
        json!({ "ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==" })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400, "the package type cannot change");

    let r = invoke(&e.server, "img-fn", b"{}").await;
    assert_eq!(r.status, 502);
    assert!(String::from_utf8_lossy(&r.body).contains("services.lambda.docker"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn image_functions_run_warm_in_their_container() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("run", Some("fake"));
    let image = e.dir.join("my-image");
    let image_uri = image.to_string_lossy().into_owned();
    assert_eq!(
        create_image(&e.server, "img-fn", &image_uri, 2)
            .await
            .status,
        201
    );

    let first = invoke(&e.server, "img-fn", br#"{"n":1}"#).await;
    assert_eq!(
        first.status,
        200,
        "{}",
        String::from_utf8_lossy(&first.body)
    );
    assert_eq!(
        first.header("X-Amz-Function-Error"),
        None,
        "{}",
        first.log()
    );
    let a = first.json();
    assert_eq!(a["count"], 1);
    assert_eq!(
        a["argv"],
        json!(["app.handler"]),
        "ImageConfig.Command is the container command"
    );
    assert_eq!(a["env"]["MY_VAR"], "hello");
    assert_eq!(a["env"]["PATH"], "/custom/bin");
    assert_eq!(a["env"]["AWS_LAMBDA_FUNCTION_NAME"], "img-fn");
    assert_eq!(a["env"]["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"], "512");
    assert_eq!(a["env"]["AWS_LAMBDA_FUNCTION_TIMEOUT"], "2");
    assert_eq!(a["env"]["AWS_ACCESS_KEY_ID"], "AKIDLOCAL");
    let log = first.log();
    assert!(log.starts_with("INIT_START"), "{log}");
    assert!(log.contains("loading model"), "{log}");
    assert!(log.contains("Init Duration: 1234.50 ms"), "{log}");
    assert!(log.contains("handling 1"), "{log}");
    assert!(
        !log.contains("(rapid)") && !log.contains("rie-1"),
        "RIE lines are dropped: {log}"
    );

    let run = &runs(&image)[0];
    let argv = run["argv"].to_string();
    assert!(argv.contains(r#""--memory","512m""#), "{argv}");
    assert!(argv.contains("127.0.0.1:"), "{argv}");
    assert!(
        !argv.contains("local-secret"),
        "secrets stay off the command line: {argv}"
    );
    assert_eq!(run["envFileMode"], "0o600");

    let second = invoke(&e.server, "img-fn", b"{}").await;
    let b = second.json();
    assert_eq!(
        (b["pid"].clone(), b["count"].clone()),
        (a["pid"].clone(), json!(2)),
        "warm container reused"
    );
    assert!(!second.log().contains("Init Duration"), "{}", second.log());
    assert_eq!(runs(&image).len(), 1);

    let failed = invoke(&e.server, "img-fn", br#"{"fail":true}"#).await;
    assert_eq!(failed.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(failed.json()["errorType"], "TestError");
    assert_eq!(
        e.server.idle_environments("img-fn"),
        1,
        "a function error keeps the container"
    );

    let name = run["name"].as_str().unwrap().to_string();
    let timed_out = invoke(&e.server, "img-fn", br#"{"sleep":5}"#).await;
    assert_eq!(timed_out.json()["errorType"], "Sandbox.Timedout");
    assert!(
        removed(&e.dir, &name).await,
        "a timed-out container is removed"
    );
    assert_eq!(e.server.idle_environments("img-fn"), 0);

    let crashed = invoke(&e.server, "img-fn", br#"{"crash":true}"#).await;
    assert_eq!(
        crashed.json()["errorType"],
        "Runtime.ExitError",
        "{}",
        crashed.log()
    );
    assert_eq!(runs(&image).len(), 2, "a new container after the timeout");

    let fresh = invoke(&e.server, "img-fn", b"{}").await.json();
    assert_eq!(fresh["count"], 1);
    let last = runs(&image).last().unwrap()["name"]
        .as_str()
        .unwrap()
        .to_string();
    e.server.shutdown().await;
    assert!(
        removed(&e.dir, &last).await,
        "shutdown removes idle containers"
    );
}

#[tokio::test]
async fn image_functions_survive_a_restart() {
    let e = env("restart", None);
    assert_eq!(
        create_image(&e.server, "img-fn", "my-repo/fn:1", 3)
            .await
            .status,
        201
    );
    let restarted = Arc::new(Server::new(Config {
        storage_path: e.dir.join("lambda").to_string_lossy().into_owned(),
        ..Config::default()
    }));
    assert_eq!(restarted.load_err(), None);
    let got = call(&restarted, "GET", "/2015-03-31/functions/img-fn", &[], b"")
        .await
        .json();
    assert_eq!(got["Code"]["ImageUri"], "my-repo/fn:1");
}

#[tokio::test]
async fn an_entry_point_override_keeps_the_image_command() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("entrypoint", Some("fake"));
    let image = e.dir.join("my-image");
    let body = json!({
        "FunctionName": "ep-fn",
        "PackageType": "Image",
        "Role": "r",
        "ImageConfig": { "EntryPoint": ["/lambda-entrypoint.sh"] },
        "Code": { "ImageUri": image.to_string_lossy() },
    });
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 201);
    let r = invoke(&e.server, "ep-fn", b"{}").await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(
        r.json()["argv"],
        json!(["default.handler"]),
        "the image's CMD survives --entrypoint"
    );
}

#[tokio::test]
async fn a_container_that_cannot_start_is_a_service_error() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("broken", Some("fake"));
    let image = e.dir.join("broken");
    assert_eq!(
        create_image(&e.server, "bad-fn", &image.to_string_lossy(), 3)
            .await
            .status,
        201
    );
    let r = invoke(&e.server, "bad-fn", b"{}").await;
    assert_eq!(r.status, 502, "{}", String::from_utf8_lossy(&r.body));
    let msg = String::from_utf8_lossy(&r.body).into_owned();
    assert!(msg.contains("Unable to find image"), "{msg}");
}

/// Real Docker + an AWS base image: `DEVCLOUD_LAMBDA_DOCKER_E2E=1` (needs
/// `public.ecr.aws/lambda/provided:al2023` locally or pullable).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_docker_runs_an_aws_base_image() {
    if std::env::var("DEVCLOUD_LAMBDA_DOCKER_E2E").as_deref() != Ok("1") {
        eprintln!("skipping: set DEVCLOUD_LAMBDA_DOCKER_E2E=1 to run against Docker");
        return;
    }
    let dir = temp_dir("real");
    // A bash Runtime API client: the base image has neither curl nor python.
    let bootstrap = r#"#!/bin/bash
host=${AWS_LAMBDA_RUNTIME_API%:*}; port=${AWS_LAMBDA_RUNTIME_API#*:}
echo "init pid=$$"
count=0
request() {
  exec 3<>/dev/tcp/$host/$port
  printf '%s %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s' "$1" "$2" "$host" "${#3}" "$3" >&3
  RESP_HEADERS=""; len=0
  while IFS= read -r line <&3; do line=${line%$'\r'}; [ -z "$line" ] && break; RESP_HEADERS+="$line"$'\n'
    case "${line,,}" in content-length:*) len=${line#*: };; esac; done
  RESP_BODY=""; [ "$len" -gt 0 ] && read -r -N "$len" RESP_BODY <&3
  exec 3>&-
}
while true; do
  request GET /2018-06-01/runtime/invocation/next ""
  id=$(printf '%s' "$RESP_HEADERS" | grep -i '^lambda-runtime-aws-request-id:' | cut -d' ' -f2)
  count=$((count+1))
  echo "invocation $count"
  if [[ "$RESP_BODY" == *fail* ]]; then
    request POST /2018-06-01/runtime/invocation/$id/error '{"errorMessage":"boom","errorType":"TestError"}'
  else
    request POST /2018-06-01/runtime/invocation/$id/response "{\"pid\":$$,\"count\":$count,\"var\":\"$MY_VAR\"}"
  fi
done
"#;
    std::fs::write(dir.join("bootstrap"), bootstrap).unwrap();
    std::fs::write(
        dir.join("Dockerfile"),
        "FROM public.ecr.aws/lambda/provided:al2023\nCOPY --chmod=755 bootstrap /var/runtime/bootstrap\nCMD [\"handler\"]\n",
    )
    .unwrap();
    let tag = format!("devcloud-lambda-e2e:{}", std::process::id());
    let built = std::process::Command::new("docker")
        .args(["build", "-q", "-t", &tag])
        .arg(&dir)
        .status()
        .unwrap();
    assert!(built.success());

    let e = env("realrun", Some("docker"));
    let body = json!({
        "FunctionName": "real-fn",
        "PackageType": "Image",
        "Role": "r",
        "Timeout": 10,
        "Environment": { "Variables": { "MY_VAR": "hello" } },
        "Code": { "ImageUri": tag },
    });
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 201);
    let first = invoke(&e.server, "real-fn", b"{}").await;
    assert_eq!(
        first.status,
        200,
        "{}",
        String::from_utf8_lossy(&first.body)
    );
    let a = first.json();
    assert_eq!(
        (a["count"].clone(), a["var"].clone()),
        (json!(1), json!("hello")),
        "{}",
        first.log()
    );
    assert!(first.log().contains("init pid="), "{}", first.log());
    assert!(first.log().contains("Init Duration: "), "{}", first.log());
    let second = invoke(&e.server, "real-fn", b"{}").await.json();
    assert_eq!(
        (second["pid"].clone(), second["count"].clone()),
        (a["pid"].clone(), json!(2))
    );
    let failed = invoke(&e.server, "real-fn", br#"{"fail":1}"#).await;
    assert_eq!(failed.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(failed.json()["errorType"], "TestError");
    // EntryPoint alone: the image's CMD (the handler name the base image's
    // entrypoint requires) must survive.
    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/real-fn/configuration",
        &[],
        json!({ "ImageConfig": { "EntryPoint": ["/lambda-entrypoint.sh"] } })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200);
    let r = invoke(&e.server, "real-fn", b"{}").await;
    assert_eq!(
        r.json()["count"],
        1,
        "{} {}",
        String::from_utf8_lossy(&r.body),
        r.log()
    );
    e.server.shutdown().await;
    let left = std::process::Command::new("docker")
        .args(["ps", "-q", "--filter", &format!("ancestor={tag}")])
        .output()
        .unwrap();
    assert!(left.stdout.is_empty(), "no container left behind");
    let _ = std::process::Command::new("docker")
        .args(["rmi", "-f", &tag])
        .status();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_containers_of_cancelled_invocations() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("cancel", Some("fake"));
    let image = e.dir.join("my-image");
    assert_eq!(
        create_image(&e.server, "img-fn", &image.to_string_lossy(), 30)
            .await
            .status,
        201
    );
    // Like `http::serve` on SIGTERM: the in-flight request is aborted and
    // awaited, then the server shuts down.
    let server = Arc::clone(&e.server);
    let running = tokio::spawn(async move { invoke(&server, "img-fn", br#"{"sleep":10}"#).await });
    let name = loop {
        if let Some(run) = runs(&image).first() {
            break run["name"].as_str().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    running.abort();
    let _ = running.await;
    e.server.shutdown().await;
    assert!(
        e.dir.join("state").join(format!("{name}.removed")).exists(),
        "shutdown returned before the cancelled invocation's container was removed"
    );
}

#[tokio::test]
async fn a_missing_docker_cli_is_explained() {
    let e = env("nocli", Some("/nonexistent/devcloud-test/docker"));
    assert_eq!(
        create_image(&e.server, "img-fn", "my-repo/fn:1", 3)
            .await
            .status,
        201
    );
    let body = json!({
        "FunctionName": "ep-fn", "PackageType": "Image", "Role": "r",
        "ImageConfig": { "EntryPoint": ["/lambda-entrypoint.sh"] },
        "Code": { "ImageUri": "my-repo/fn:1" },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    // Both the plain run and the EntryPoint-only path (image inspect first).
    for name in ["img-fn", "ep-fn"] {
        let r = invoke(&e.server, name, b"{}").await;
        assert_eq!(r.status, 502);
        let msg = String::from_utf8_lossy(&r.body).into_owned();
        assert!(
            msg.contains("was not found on devcloud's PATH"),
            "{name}: {msg}"
        );
        assert!(
            msg.contains("run devcloud-lambda (or devcloud) on the host"),
            "{name}: {msg}"
        );
    }
}
