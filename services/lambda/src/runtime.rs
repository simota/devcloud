//! Local execution of Lambda invocations in reusable execution environments.
//!
//! An environment is one interpreter process (`python3` / `node`) running a
//! small embedded bootstrap (`bootstrap/`): it loads the configured handler
//! once (the init phase, reported as `Init Duration`), then serves invocations
//! one at a time — requests arrive as JSON lines on a private pipe (fd 3; the
//! handler's own stdin is empty, as on AWS), each outcome goes to
//! a per-request result file, and a marker line on stdout ends the invocation
//! and its slice of the log. Idle environments are kept warm in a [`Pool`].
//! The child runs with a cleared environment (only `PATH`, Lambda's reserved
//! variables, and the function's own `Environment.Variables`), so host
//! credentials never leak into handler code; the only credentials a handler
//! sees are the ones configured for functions ([`FunctionCredentials`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::container::{self, ImageSpec};

/// Per-stream log bytes kept while an invocation runs (the API tail is 4 KiB).
const LOG_CAPTURE_BYTES: usize = 64 * 1024;

/// Default for how long an idle environment is kept warm.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// How long log readers may keep draining after the process group is killed.
const LOG_DRAIN_GRACE: Duration = Duration::from_millis(500);

const PYTHON_BOOTSTRAP: &str = include_str!("bootstrap/python.py");
const NODE_BOOTSTRAP: &str = include_str!("bootstrap/node.js");

/// Interpreter family a runtime identifier maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Python,
    Node,
}

/// Maps a Lambda `Runtime` identifier (`python3.12`, `nodejs20.x`) to the
/// interpreter family devcloud can execute locally.
pub fn family(runtime: &str) -> Option<Family> {
    if runtime.starts_with("python3") {
        Some(Family::Python)
    } else if runtime.starts_with("nodejs") {
        Some(Family::Node)
    } else {
        None
    }
}

/// The language version a runtime identifier names, as the interpreter reports
/// it: `python3.12` → `3.12`, `nodejs20.x` → `20`. `None` when the identifier
/// carries no usable version.
pub fn version(runtime: &str) -> Option<String> {
    let v = match runtime.strip_prefix("python") {
        Some(rest) => rest,
        None => {
            let rest = runtime.strip_prefix("nodejs")?;
            rest.strip_suffix(".x").unwrap_or(rest)
        }
    };
    let numeric = !v.is_empty()
        && v.split('.')
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    numeric.then(|| v.to_string())
}

/// Host interpreter binaries (overridable for tests / non-standard installs).
///
/// With the default `python3`, a Python runtime first looks for the matching
/// versioned binary (`python3.12` for `python3.12`) on devcloud's `PATH`, so a
/// host with several Pythons runs each function on the version it asked for.
#[derive(Debug, Clone)]
pub struct Interpreters {
    pub python: String,
    pub node: String,
    /// Docker CLI for container image functions; `None` disables them.
    pub docker: Option<String>,
    /// Docker network devcloud runs on, when devcloud itself is a container
    /// sharing the Docker daemon: function containers join it and are
    /// reached by name instead of through a loopback port.
    pub docker_network: Option<String>,
}

const DEFAULT_PYTHON: &str = "python3";

impl Default for Interpreters {
    fn default() -> Self {
        Interpreters {
            python: DEFAULT_PYTHON.to_string(),
            node: "node".to_string(),
            docker: None,
            docker_network: None,
        }
    }
}

impl Interpreters {
    /// The interpreter to start for `runtime`, resolved to an absolute path.
    fn resolve(&self, family: Family, runtime: &str) -> Option<PathBuf> {
        match family {
            Family::Python => {
                let versioned = (self.python == DEFAULT_PYTHON)
                    .then(|| version(runtime))
                    .flatten()
                    .and_then(|v| resolve_program(&format!("python{v}")));
                versioned.or_else(|| resolve_program(&self.python))
            }
            Family::Node => resolve_program(&self.node),
        }
    }
}

/// Credentials handed to every handler as `AWS_ACCESS_KEY_ID` /
/// `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, standing in for the
/// execution role's credentials Lambda injects.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct FunctionCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Omitted from the environment when empty.
    pub session_token: String,
}

impl std::fmt::Debug for FunctionCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionCredentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

/// Everything one invocation needs; assembled by the server from the stored
/// function configuration.
pub struct Invocation {
    pub runtime: String,
    pub handler: String,
    pub code_dir: PathBuf,
    pub function_name: String,
    pub function_arn: String,
    pub memory_size: i64,
    pub timeout_seconds: i64,
    pub region: String,
    pub request_id: String,
    pub environment: BTreeMap<String, String>,
    pub payload: Vec<u8>,
    /// Stand-in for Lambda's `/opt`, where layers are extracted: its
    /// `python/` and `nodejs/node_modules/` trees are on the default
    /// `PYTHONPATH` / `NODE_PATH`.
    pub opt_dir: PathBuf,
    pub credentials: Option<FunctionCredentials>,
    /// The function configuration an environment is started from (its
    /// revision): only an idle environment with the same key is reused.
    pub env_key: String,
    /// Set for a container image function, which runs under Docker instead
    /// of a local interpreter (`runtime` and `handler` are then empty).
    pub image: Option<ImageSpec>,
}

/// The handler's outcome as Lambda reports it on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Handler returned; the body is its JSON-encoded result.
    Success(Vec<u8>),
    /// Handler raised/rejected, timed out, or the runtime crashed. The body is
    /// the Lambda error document (`errorMessage`, `errorType`, ...).
    FunctionError(Vec<u8>),
}

pub struct Completed {
    pub outcome: Outcome,
    /// Combined log output, framed with START/END/REPORT lines like CloudWatch.
    pub log: String,
    /// The invoke phase only; a cold start's init is in `init_duration`.
    pub duration: Duration,
    /// Set on a cold start: how long the environment took to load the handler.
    pub init_duration: Option<Duration>,
}

/// A failure to run the function at all (as opposed to a function error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// The runtime identifier has no local interpreter mapping.
    Unsupported(String),
    /// The interpreter binary (or the function's container) could not be
    /// started.
    Spawn(String),
}

/// How long a container image environment may take to start (including an
/// image pull) before its first invocation fails.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_secs(120);

/// Bound on `docker rm -f` when the pool closes, so an unresponsive daemon
/// cannot hang shutdown.
const DOCKER_RM_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
struct BootstrapResult {
    result: Option<String>,
    error: Option<serde_json::Value>,
}

/// Execution environments kept warm between invocations, per function.
///
/// An environment serves one invocation at a time; concurrent invocations of
/// the same function each get their own. After an invocation the environment
/// goes back to the pool and is stopped once it has been idle for
/// `idle_timeout` (zero: never reused, every invocation is a cold start), when
/// its function is deleted or reconfigured, or when the pool closes.
pub struct Pool {
    work_root: PathBuf,
    /// This devcloud instance's id, labelled onto its function containers.
    owner: String,
    idle_timeout: Duration,
    seq: AtomicU64,
    state: Mutex<PoolState>,
    /// Container removals started where nothing could await them (an
    /// environment dropped mid-invocation); [`Pool::close`] waits for them.
    removals: Removals,
}

type Removals = Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>;

#[derive(Default)]
struct PoolState {
    idle: HashMap<String, Vec<Environment>>,
    /// Keys (function revisions) that must never serve again. Revision ids
    /// are never reused, so entries stay valid for the process lifetime.
    retired: HashSet<String>,
    /// `function/runtime` pairs whose interpreter-version mismatch has
    /// already been reported on stderr.
    version_warned: HashSet<String>,
    closed: bool,
}

impl Pool {
    /// `work_root` holds per-environment scratch directories; `owner`
    /// identifies this devcloud instance on the containers it starts.
    pub fn new(work_root: PathBuf, idle_timeout: Duration, owner: String) -> Arc<Self> {
        Arc::new(Pool {
            work_root,
            owner,
            idle_timeout,
            seq: AtomicU64::new(0),
            state: Mutex::new(PoolState::default()),
            removals: Removals::default(),
        })
    }

    /// This devcloud instance's id (see [`container::OWNER_LABEL`]).
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Number of idle environments of `function` (introspection and tests).
    pub fn idle_count(&self, function: &str) -> usize {
        let st = self.state.lock().unwrap();
        st.idle.get(function).map_or(0, Vec::len)
    }

    /// Repeats the bootstrap's runtime-version warning (in `init_log`) on
    /// devcloud's stderr, once per function and runtime: the invocation log
    /// only carries it on a cold start, where it is easy to miss.
    fn version_mismatch(&self, inv: &Invocation, init_log: &[u8]) -> Option<String> {
        const PREFIX: &str = "[WARNING] devcloud: ";
        let text = String::from_utf8_lossy(init_log);
        let warning = text
            .lines()
            .find_map(|l| l.strip_prefix(PREFIX))
            .filter(|w| w.starts_with(&format!("runtime {} ", inv.runtime)))?;
        let key = format!("{}/{}", inv.function_name, inv.runtime);
        self.state
            .lock()
            .unwrap()
            .version_warned
            .insert(key)
            .then(|| format!("function {}: {warning}", inv.function_name))
    }

    /// Retires configuration `key` of `function` (updated or deleted): its
    /// idle environments stop now, and environments still running an
    /// invocation stop when it ends instead of returning to the pool.
    pub fn retire(&self, function: &str, key: &str) {
        let stopped = {
            let mut st = self.state.lock().unwrap();
            st.retired.insert(key.to_string());
            st.idle.remove(function)
        };
        drop(stopped);
    }

    /// Stops every idle environment and refuses to keep any from now on.
    /// Containers are removed before this returns — idle ones and those of
    /// environments dropped earlier (e.g. invocations cancelled by shutdown).
    pub async fn close(&self) {
        let stopped = {
            let mut st = self.state.lock().unwrap();
            st.closed = true;
            std::mem::take(&mut st.idle)
        };
        for mut env in stopped.into_values().flatten() {
            if let Some(c) = env.container.take() {
                env.kill_group();
                c.remove().await;
            }
        }
        // Each removal is bounded by DOCKER_RM_TIMEOUT. Removals started
        // while waiting (environments dropped meanwhile) are waited for too.
        loop {
            let pending = std::mem::take(&mut *self.removals.lock().unwrap());
            if pending.is_empty() {
                break;
            }
            let _ = tokio::task::spawn_blocking(move || {
                for handle in pending {
                    let _ = handle.join();
                }
            })
            .await;
        }
    }

    /// An idle environment of `function` started from `key`, if one is still
    /// healthy. Environments of another configuration are stopped: they can
    /// never be used again.
    fn take(&self, function: &str, key: &str) -> Option<Environment> {
        let mut stopped = Vec::new();
        let mut found = None;
        {
            let mut st = self.state.lock().unwrap();
            let list = st.idle.get_mut(function)?;
            while let Some(mut env) = list.pop() {
                if env.key == key && env.healthy() {
                    found = Some(env);
                    break;
                }
                stopped.push(env);
            }
            if list.is_empty() {
                st.idle.remove(function);
            }
        }
        drop(stopped);
        found
    }

    fn give_back(self: &Arc<Self>, function: &str, mut env: Environment) {
        if self.idle_timeout.is_zero() {
            return;
        }
        env.idle_since = Instant::now();
        let id = env.id;
        {
            let mut st = self.state.lock().unwrap();
            if st.closed || st.retired.contains(&env.key) {
                drop(st);
                drop(env);
                return;
            }
            st.idle.entry(function.to_string()).or_default().push(env);
        }
        let pool = Arc::downgrade(self);
        let function = function.to_string();
        let idle_timeout = self.idle_timeout;
        tokio::spawn(async move {
            tokio::time::sleep(idle_timeout).await;
            if let Some(pool) = pool.upgrade() {
                pool.expire(&function, id);
            }
        });
    }

    /// Stops environment `id` if it has been idle for the whole timeout (it
    /// may have served another invocation since this timer was armed).
    fn expire(&self, function: &str, id: u64) {
        let stopped = {
            let mut st = self.state.lock().unwrap();
            let Some(list) = st.idle.get_mut(function) else {
                return;
            };
            let Some(pos) = list
                .iter()
                .position(|e| e.id == id && e.idle_since.elapsed() >= self.idle_timeout)
            else {
                return;
            };
            let env = list.swap_remove(pos);
            if list.is_empty() {
                st.idle.remove(function);
            }
            env
        };
        drop(stopped);
    }
}

/// What the stdout reader reports: a bootstrap marker line (with the output
/// that preceded it), or end of stream (with whatever output remained).
enum StreamEvent {
    Marker { tag: String, log: Vec<u8> },
    Eof,
}

/// Splits a stdout stream at `<prefix><tag>\n` marker lines, keeping only
/// the newest [`LOG_CAPTURE_BYTES`] of output between markers.
struct Scanner {
    prefix: Vec<u8>,
    buf: Vec<u8>,
    scan_from: usize,
}

impl Scanner {
    fn new(prefix: Vec<u8>) -> Self {
        Scanner {
            prefix,
            buf: Vec::new(),
            scan_from: 0,
        }
    }

    /// Appends `data` and returns every marker it completed, in order.
    fn push(&mut self, data: &[u8]) -> Vec<(String, Vec<u8>)> {
        self.buf.extend_from_slice(data);
        let mut markers = Vec::new();
        loop {
            let Some(i) = find(&self.buf[self.scan_from..], &self.prefix) else {
                self.scan_from = self.buf.len().saturating_sub(self.prefix.len() - 1);
                break;
            };
            let at = self.scan_from + i;
            let after = at + self.prefix.len();
            let Some(nl) = self.buf[after..].iter().position(|&b| b == b'\n') else {
                // The rest of the marker line is still on its way.
                self.scan_from = at;
                break;
            };
            let tag = String::from_utf8_lossy(&self.buf[after..after + nl]).into_owned();
            let log = self.buf[..at].to_vec();
            self.buf.drain(..after + nl + 1);
            self.scan_from = 0;
            markers.push((tag, log));
        }
        if self.buf.len() > LOG_CAPTURE_BYTES {
            let excess = (self.buf.len() - LOG_CAPTURE_BYTES).min(self.scan_from);
            self.buf.drain(..excess);
            self.scan_from -= excess;
        }
        markers
    }

    /// The output after the last marker.
    fn take_rest(&mut self) -> Vec<u8> {
        self.scan_from = 0;
        std::mem::take(&mut self.buf)
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// One running bootstrap process: a Lambda execution environment.
struct Environment {
    id: u64,
    key: String,
    child: tokio::process::Child,
    pgid: Option<u32>,
    /// Requests to the bootstrap; `None` for a container (invoked over HTTP).
    control: Option<ControlWriter>,
    /// Set when the process is a `docker run` of an image function.
    container: Option<Container>,
    events: mpsc::UnboundedReceiver<StreamEvent>,
    stdout: Arc<Mutex<Scanner>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    tasks: Vec<tokio::task::AbortHandle>,
    work_dir: PathBuf,
    idle_since: Instant,
    /// Keeps the code tree the environment runs from in place.
    _code: Box<dyn Send>,
}

/// How waiting on an environment ended.
enum Wait {
    Marker {
        tag: String,
        log: Vec<u8>,
    },
    /// The process ended; `crashed` describes a non-zero exit or signal.
    Exited {
        crashed: Option<String>,
        log: Vec<u8>,
    },
    TimedOut {
        log: Vec<u8>,
        /// Peak memory, read before the process group was killed.
        max_memory_mb: Option<u64>,
    },
}

impl Environment {
    fn healthy(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None)) && self.events.is_empty()
    }

    /// Waits for the next marker, the process ending, or `deadline`.
    async fn wait(&mut self, deadline: tokio::time::Instant) -> Wait {
        enum Woke {
            Event(Option<StreamEvent>),
            Exit(std::io::Result<std::process::ExitStatus>),
            Deadline,
        }
        let woke = tokio::select! {
            ev = self.events.recv() => Woke::Event(ev),
            status = self.child.wait() => Woke::Exit(status),
            _ = tokio::time::sleep_until(deadline) => Woke::Deadline,
        };
        let status = match woke {
            Woke::Event(Some(StreamEvent::Marker { tag, log })) => {
                let mut log = log;
                log.extend(self.take_stderr());
                return Wait::Marker { tag, log };
            }
            // stdout closed: the process is ending (or already gone).
            Woke::Event(_) => match tokio::time::timeout_at(deadline, self.child.wait()).await {
                Ok(status) => status,
                Err(_) => {
                    let max_memory_mb = self.child.id().and_then(peak_rss_mb);
                    self.kill_group();
                    return Wait::TimedOut {
                        log: self.drain().await,
                        max_memory_mb,
                    };
                }
            },
            Woke::Exit(status) => status,
            Woke::Deadline => {
                let max_memory_mb = self.child.id().and_then(peak_rss_mb);
                self.kill_group();
                return Wait::TimedOut {
                    log: self.drain().await,
                    max_memory_mb,
                };
            }
        };
        let crashed = match status {
            Ok(s) if s.success() => None,
            Ok(s) => Some(exit_status_text(s)),
            Err(e) => Some(e.to_string()),
        };
        // Descendants still holding the log pipes go down with the group.
        self.kill_group();
        Wait::Exited {
            crashed,
            log: self.drain().await,
        }
    }

    /// The output still buffered once the process group is gone: readers get
    /// a bounded grace period (a descendant that escaped the group can keep a
    /// pipe open forever), then whatever they collected is kept.
    async fn drain(&mut self) -> Vec<u8> {
        let mut log = Vec::new();
        let deadline = tokio::time::Instant::now() + LOG_DRAIN_GRACE;
        while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, self.events.recv()).await {
            match ev {
                StreamEvent::Marker { log: part, .. } => log.extend(part),
                StreamEvent::Eof => break,
            }
        }
        log.extend(self.stdout.lock().unwrap().take_rest());
        log.extend(self.take_stderr());
        log
    }

    fn take_stderr(&self) -> Vec<u8> {
        std::mem::take(&mut *self.stderr.lock().unwrap())
    }

    /// Kills the group once; later calls (and the drop) are no-ops, so a
    /// recycled process-group id is never signalled.
    fn kill_group(&mut self) {
        if let Some(pgid) = self.pgid.take() {
            kill_process_group(pgid);
        }
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        self.kill_group();
        for task in &self.tasks {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&self.work_dir);
        if let Some(c) = self.container.take() {
            // Killing the CLI leaves the container running: remove it from a
            // thread (no runtime to await on here), tracked so the pool's
            // close waits for it.
            drop(c.spawn_removal());
        }
    }
}

/// The Docker side of a container environment.
struct Container {
    docker: String,
    name: String,
    /// Where the RIE answers.
    endpoint: container::Endpoint,
    removals: Removals,
}

impl Container {
    /// `docker rm -f`, bounded by [`DOCKER_RM_TIMEOUT`].
    fn remove_blocking(self) {
        let Ok(mut rm) = std::process::Command::new(&self.docker)
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        else {
            return;
        };
        let deadline = Instant::now() + DOCKER_RM_TIMEOUT;
        while Instant::now() < deadline {
            if !matches!(rm.try_wait(), Ok(None)) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = rm.kill();
        let _ = rm.wait();
    }

    /// Starts `docker rm -f` on a thread tracked in `removals` (so the
    /// pool's close waits for it); the receiver fires once it is done.
    fn spawn_removal(self) -> tokio::sync::oneshot::Receiver<()> {
        let (done, finished) = tokio::sync::oneshot::channel();
        let removals = Arc::clone(&self.removals);
        let handle = std::thread::spawn(move || {
            self.remove_blocking();
            let _ = done.send(());
        });
        let mut pending = removals.lock().unwrap();
        pending.retain(|h| !h.is_finished());
        pending.push(handle);
        finished
    }

    /// Removes the container and waits for it. Cancellation-safe: if this
    /// future is dropped (shutdown aborting an invocation, a client hanging
    /// up), the removal still completes and the pool's close still waits.
    async fn remove(self) {
        let _ = self.spawn_removal().await;
    }
}

/// Runs one invocation, in a warm environment from `pool` when there is one,
/// otherwise in a new one (a cold start). `code` keeps the invocation's code
/// tree in place; a new environment holds on to it for its whole life.
pub async fn run(
    inv: &Invocation,
    interpreters: &Interpreters,
    pool: &Arc<Pool>,
    code: Box<dyn Send>,
) -> Result<Completed, RuntimeError> {
    if let Some(image) = &inv.image {
        return run_container(inv, image, interpreters, pool, code).await;
    }
    let family =
        family(&inv.runtime).ok_or_else(|| RuntimeError::Unsupported(inv.runtime.clone()))?;
    let timeout = Duration::from_secs(inv.timeout_seconds.max(1) as u64);

    let (mut env, init) = match pool.take(&inv.function_name, &inv.env_key) {
        Some(env) => (env, None),
        None => {
            let started = Instant::now();
            let mut env = spawn(family, inv, interpreters, pool, code)?;
            // Init gets its own budget of one function timeout.
            match env.wait(tokio::time::Instant::now() + timeout).await {
                Wait::Marker { tag, log } if tag == "ready" => {
                    if let Some(warning) = pool.version_mismatch(inv, &log) {
                        eprintln!("devcloud-lambda: warning: {warning}");
                    }
                    (env, Some((started.elapsed(), log)))
                }
                other => return Ok(failed_init(inv, other, started.elapsed())),
            }
        }
    };

    let result_path = env.work_dir.join(format!("{}.json", inv.request_id));
    let deadline_ms = now_millis() + timeout.as_millis();
    let request = serde_json::json!({
        "requestId": inv.request_id,
        "deadlineMs": deadline_ms as u64,
        "resultPath": result_path,
        "event": String::from_utf8_lossy(&inv.payload),
    });
    let mut line = serde_json::to_vec(&request).unwrap_or_default();
    line.push(b'\n');

    let pid = env.child.id();
    // Max Memory Used is per invocation in a warm environment; a cold start
    // keeps what init used.
    if let (Some(pid), None) = (pid, &init) {
        reset_peak_rss(pid);
    }
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    // A dead environment fails the write; `wait` then reports how it ended.
    if let Some(control) = env.control.as_mut() {
        let _ = tokio::time::timeout_at(deadline, control.write_all(&line)).await;
    }
    let waited = env.wait(deadline).await;
    let duration = started.elapsed();

    let mut reusable = false;
    let mut max_memory_mb = None;
    let (outcome, body, timed_out) = match waited {
        Wait::Marker { tag, log } => {
            reusable = tag == "done";
            max_memory_mb = pid.and_then(peak_rss_mb);
            (read_result(&result_path, &inv.request_id), log, false)
        }
        Wait::Exited {
            crashed: Some(status),
            log,
        } => (
            Outcome::FunctionError(error_doc(
                &format!(
                    "RequestId: {} Error: Runtime exited with error: {status}",
                    inv.request_id
                ),
                "Runtime.ExitError",
            )),
            log,
            false,
        ),
        // A clean exit can still have recorded the result (a Node callback
        // whose event loop had already drained).
        Wait::Exited { crashed: None, log } => {
            (read_result(&result_path, &inv.request_id), log, false)
        }
        Wait::TimedOut {
            log,
            max_memory_mb: peak,
        } => {
            max_memory_mb = peak;
            (timed_out_error(inv), log, true)
        }
    };
    let _ = std::fs::remove_file(&result_path);

    let log = frame_log(
        inv,
        init.as_ref()
            .map(|(d, log)| (*d, String::from_utf8_lossy(log).into_owned())),
        &String::from_utf8_lossy(&body),
        duration,
        timed_out,
        max_memory_mb,
    );
    if reusable {
        pool.give_back(&inv.function_name, env);
    }
    Ok(Completed {
        outcome,
        log,
        duration,
        init_duration: init.map(|(d, _)| d),
    })
}

/// The invocation's result when the environment never became ready.
fn failed_init(inv: &Invocation, waited: Wait, init: Duration) -> Completed {
    let (outcome, log, timed_out) = match waited {
        Wait::TimedOut { log, .. } => (timed_out_error(inv), log, true),
        Wait::Exited { crashed, log } => (
            Outcome::FunctionError(error_doc(
                &format!(
                    "RequestId: {} Error: Runtime exited with error: {}",
                    inv.request_id,
                    crashed.unwrap_or_else(|| "exit status 0".to_string())
                ),
                "Runtime.ExitError",
            )),
            log,
            false,
        ),
        Wait::Marker { log, .. } => (
            Outcome::FunctionError(error_doc(
                "runtime produced an invalid result",
                "Runtime.InvalidResult",
            )),
            log,
            false,
        ),
    };
    Completed {
        outcome,
        log: frame_log(
            inv,
            Some((init, String::from_utf8_lossy(&log).into_owned())),
            "",
            Duration::ZERO,
            timed_out,
            None,
        ),
        duration: Duration::ZERO,
        init_duration: Some(init),
    }
}

fn timed_out_error(inv: &Invocation) -> Outcome {
    Outcome::FunctionError(error_doc(
        &format!(
            "{} {} Task timed out after {}.00 seconds",
            crate::time_fmt::now_rfc3339(),
            inv.request_id,
            inv.timeout_seconds.max(1)
        ),
        "Sandbox.Timedout",
    ))
}

/// Starts a bootstrap process for `inv`'s function.
fn spawn(
    family: Family,
    inv: &Invocation,
    interpreters: &Interpreters,
    pool: &Pool,
    code: Box<dyn Send>,
) -> Result<Environment, RuntimeError> {
    let id = pool.seq.fetch_add(1, Ordering::Relaxed);
    let work_dir = pool.work_root.join(format!("env-{id}"));
    std::fs::create_dir_all(&work_dir).map_err(|e| RuntimeError::Spawn(e.to_string()))?;
    let marker = new_marker(id);

    let bin = match family {
        Family::Python => &interpreters.python,
        Family::Node => &interpreters.node,
    };
    // Resolve the interpreter against devcloud's own PATH: the function may set
    // its own PATH, which applies to the handler, not to finding python/node.
    let program = interpreters.resolve(family, &inv.runtime).ok_or_else(|| {
        let _ = std::fs::remove_dir_all(&work_dir);
        RuntimeError::Spawn(format!(
            "start {bin} for runtime {}: interpreter not found on devcloud's PATH",
            inv.runtime
        ))
    })?;
    let mut cmd = tokio::process::Command::new(&program);
    match family {
        // Unbuffered: output written before a timeout's SIGKILL must still
        // reach the log (a pipe would otherwise be block-buffered).
        Family::Python => cmd.arg("-u").arg("-c").arg(PYTHON_BOOTSTRAP),
        Family::Node => cmd.arg("-e").arg(NODE_BOOTSTRAP),
    };
    // Precedence: devcloud defaults < the function's Environment.Variables <
    // Lambda-reserved variables (which CreateFunction refuses to accept).
    cmd.env_clear()
        .envs(default_env(family, inv))
        .envs(&inv.environment)
        .envs(reserved_env(inv, id))
        .env("_DEVCLOUD_MARKER", &marker)
        .current_dir(&inv.code_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Own process group, so stopping the environment takes down anything the
    // handler spawned along with the interpreter.
    #[cfg(unix)]
    cmd.process_group(0);
    // fd 2 shares the stdout pipe, so output written straight to it (native
    // code, subprocesses, warnings) stays ordered before the invocation's
    // marker instead of racing it through a second pipe into the next log.
    #[cfg(unix)]
    {
        cmd.stderr(Stdio::null());
        // SAFETY: dup2 is async-signal-safe; std has already installed the
        // piped stdout on fd 1 when pre_exec runs.
        unsafe {
            cmd.pre_exec(|| {
                if libc::dup2(1, 2) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let control = attach_control_pipe(&mut cmd).map_err(|e| {
        let _ = std::fs::remove_dir_all(&work_dir);
        RuntimeError::Spawn(format!("start {bin} for runtime {}: {e}", inv.runtime))
    })?;

    let mut child = cmd.spawn().map_err(|e| {
        let _ = std::fs::remove_dir_all(&work_dir);
        RuntimeError::Spawn(format!("start {bin} for runtime {}: {e}", inv.runtime))
    })?;
    // The child has its copy of the read end now.
    drop(cmd);
    let (tx, events) = mpsc::unbounded_channel();
    let stdout = Arc::new(Mutex::new(Scanner::new(format!("{marker} ").into_bytes())));
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let out_task = tokio::spawn(scan(
        child.stdout.take().expect("piped stdout"),
        Arc::clone(&stdout),
        tx,
    ));
    let mut tasks = vec![out_task.abort_handle()];
    if let Some(err) = child.stderr.take() {
        tasks.push(tokio::spawn(collect(err, Arc::clone(&stderr))).abort_handle());
    }
    Ok(Environment {
        id,
        key: inv.env_key.clone(),
        pgid: child.id(),
        child,
        control: Some(control),
        container: None,
        events,
        stdout,
        stderr,
        tasks,
        work_dir,
        idle_since: Instant::now(),
        _code: code,
    })
}

/// [`run`] for a container image function: the environment is a `docker run`
/// of the image, invoked through the RIE inside it.
async fn run_container(
    inv: &Invocation,
    image: &ImageSpec,
    interpreters: &Interpreters,
    pool: &Arc<Pool>,
    code: Box<dyn Send>,
) -> Result<Completed, RuntimeError> {
    let docker = interpreters.docker.as_deref().ok_or_else(|| {
        RuntimeError::Spawn("container image functions need Docker, which is disabled".into())
    })?;
    let timeout = Duration::from_secs(inv.timeout_seconds.max(1) as u64);
    let mut env = match pool.take(&inv.function_name, &inv.env_key) {
        Some(env) => env,
        None => {
            start_container(
                docker,
                interpreters.docker_network.as_deref(),
                image,
                inv,
                pool,
                code,
            )
            .await?
        }
    };
    let endpoint = env
        .container
        .as_ref()
        .map(|c| c.endpoint.clone())
        .expect("container environment");

    enum Done {
        Response(std::io::Result<container::Response>),
        Exited(String),
        TimedOut,
    }
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let done = tokio::select! {
        r = container::invoke(&endpoint, &inv.payload) => Done::Response(r),
        status = env.child.wait() => Done::Exited(match status {
            Ok(s) => s.to_string(),
            Err(e) => e.to_string(),
        }),
        _ = tokio::time::sleep_until(deadline) => Done::TimedOut,
    };
    let duration = started.elapsed();
    let mut max_memory_mb = None;
    if matches!(done, Done::Response(Ok(_))) {
        // The response can overtake the container's output: wait (briefly)
        // for the RIE to log the end of the invocation, while reading the
        // container's memory peak.
        let peak = async {
            match env.container.as_ref() {
                Some(c) => container_peak_mb(&c.docker, &c.name).await,
                None => None,
            }
        };
        let logged = async {
            let grace = tokio::time::Instant::now() + LOG_DRAIN_GRACE;
            while !container::invocation_logged(&env.stdout.lock().unwrap().buf)
                && tokio::time::Instant::now() < grace
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        (max_memory_mb, ()) = tokio::join!(peak, logged);
    }
    let mut raw = env.stdout.lock().unwrap().take_rest();
    raw.extend(env.take_stderr());
    let log = container::split_log(&String::from_utf8_lossy(&raw));

    let exit_error = |status: &str| {
        Outcome::FunctionError(error_doc(
            &format!(
                "RequestId: {} Error: Runtime exited with error: {status}",
                inv.request_id
            ),
            "Runtime.ExitError",
        ))
    };
    let (outcome, reusable, timed_out) = match done {
        Done::Response(Ok(r)) if r.status == 200 => {
            if container::is_error_document(&r.body) {
                (Outcome::FunctionError(r.body), true, false)
            } else {
                (Outcome::Success(r.body), true, false)
            }
        }
        Done::Response(Ok(r)) => (
            exit_error(&format!(
                "runtime interface emulator answered HTTP {}: {}",
                r.status,
                String::from_utf8_lossy(&r.body).trim()
            )),
            false,
            false,
        ),
        Done::Response(Err(e)) => (exit_error(&e.to_string()), false, false),
        Done::Exited(status) => (exit_error(&status), false, false),
        Done::TimedOut => (timed_out_error(inv), false, true),
    };
    let init_duration = log.init.as_ref().map(|(d, _)| *d);
    let framed = frame_log(inv, log.init, &log.body, duration, timed_out, max_memory_mb);
    if reusable {
        pool.give_back(&inv.function_name, env);
    } else if let Some(c) = env.container.take() {
        env.kill_group();
        c.remove().await;
    }
    Ok(Completed {
        outcome,
        log: framed,
        duration,
        init_duration,
    })
}

/// Starts `docker run` for `image` and waits until the RIE inside answers.
async fn start_container(
    docker: &str,
    network: Option<&str>,
    image: &ImageSpec,
    inv: &Invocation,
    pool: &Pool,
    code: Box<dyn Send>,
) -> Result<Environment, RuntimeError> {
    let id = pool.seq.fetch_add(1, Ordering::Relaxed);
    let work_dir = pool.work_root.join(format!("env-{id}"));
    std::fs::create_dir_all(&work_dir).map_err(|e| RuntimeError::Spawn(e.to_string()))?;
    let cleanup = |msg: String| {
        let _ = std::fs::remove_dir_all(&work_dir);
        RuntimeError::Spawn(msg)
    };
    // Function variables first, Lambda's own last (later lines win).
    let mut vars: BTreeMap<String, String> = inv.environment.clone();
    vars.extend(container_env(inv, id));
    let vars: Vec<(String, String)> = vars.into_iter().collect();
    let env_file = work_dir.join("env");
    container::write_env_file(&env_file, &vars).map_err(cleanup)?;
    let reach = match network {
        Some(network) => container::Reach::Network(network.to_string()),
        None => container::Reach::Loopback(free_port().map_err(cleanup)?),
    };
    // Docker names are daemon-wide: the marker hash keeps devcloud processes
    // (and workspaces) sharing a daemon apart.
    let name = format!(
        "devcloud-lambda-{}-{id}",
        &new_marker(id)["devcloud-".len()..][..12]
    );
    // One budget for the whole start: image inspect/pull and the RIE coming up.
    let deadline = Instant::now() + CONTAINER_START_TIMEOUT;

    // `--entrypoint` also clears the image's CMD; Lambda overrides the two
    // independently, so an EntryPoint-only override keeps the image's CMD.
    let resolved;
    let image = if !image.entry_point.is_empty() && image.command.is_empty() {
        let command = image_cmd(docker, &image.uri, deadline)
            .await
            .map_err(cleanup)?;
        resolved = ImageSpec {
            command,
            ..image.clone()
        };
        &resolved
    } else {
        image
    };
    let mut cmd = tokio::process::Command::new(docker);
    cmd.args(container::run_args(
        image,
        &name,
        &reach,
        inv.memory_size,
        &env_file,
        &pool.owner,
    ))
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd.spawn().map_err(|e| {
        cleanup(docker_start_error(
            docker,
            &format!("start {docker} for image {}", image.uri),
            &e,
        ))
    })?;

    let (tx, events) = mpsc::unbounded_channel();
    // No bootstrap markers in a container: the scanner only buffers output.
    let stdout = Arc::new(Mutex::new(Scanner::new(new_marker(id).into_bytes())));
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let out_task = tokio::spawn(scan(
        child.stdout.take().expect("piped stdout"),
        Arc::clone(&stdout),
        tx,
    ));
    let err_task = tokio::spawn(collect(
        child.stderr.take().expect("piped stderr"),
        Arc::clone(&stderr),
    ));
    let mut env = Environment {
        id,
        key: inv.env_key.clone(),
        pgid: child.id(),
        child,
        control: None,
        container: Some(Container {
            docker: docker.to_string(),
            endpoint: reach.endpoint(&name),
            name,
            removals: Arc::clone(&pool.removals),
        }),
        events,
        stdout,
        stderr,
        tasks: vec![out_task.abort_handle(), err_task.abort_handle()],
        work_dir,
        idle_since: Instant::now(),
        _code: code,
    };

    let endpoint = reach.endpoint(&env.container.as_ref().expect("container").name);
    loop {
        if container::probe(&endpoint).await {
            return Ok(env);
        }
        if let Ok(Some(status)) = env.child.try_wait() {
            let mut out = env.stdout.lock().unwrap().take_rest();
            out.extend(env.take_stderr());
            return Err(RuntimeError::Spawn(format!(
                "docker run {} exited ({status}): {}",
                image.uri,
                String::from_utf8_lossy(&out).trim()
            )));
        }
        if Instant::now() >= deadline {
            if let Some(c) = env.container.take() {
                env.kill_group();
                c.remove().await;
            }
            return Err(RuntimeError::Spawn(format!(
                "container for image {} did not start within {} s",
                image.uri,
                CONTAINER_START_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Removes the function containers a previous run of devcloud instance
/// `owner` left behind (it was killed before it could remove them). Returns
/// how many were removed. Containers of other instances are not touched.
pub async fn remove_orphaned_containers(docker: &str, owner: &str) -> Result<usize, String> {
    let deadline = Instant::now() + DOCKER_RM_TIMEOUT;
    let filter = format!("label={}={owner}", container::OWNER_LABEL);
    let listed = docker_output(docker, &["ps", "-aq", "--filter", &filter], deadline).await?;
    if !listed.status.success() {
        return Err(format!(
            "docker ps: {}",
            String::from_utf8_lossy(&listed.stderr).trim()
        ));
    }
    let ids: Vec<String> = String::from_utf8_lossy(&listed.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect();
    if ids.is_empty() {
        return Ok(0);
    }
    let mut args = vec!["rm", "-f"];
    args.extend(ids.iter().map(String::as_str));
    let removed = docker_output(docker, &args, deadline).await?;
    if !removed.status.success() {
        return Err(format!(
            "docker rm: {}",
            String::from_utf8_lossy(&removed.stderr).trim()
        ));
    }
    Ok(ids.len())
}

/// Peak memory of a function's container in MB, for REPORT's Max Memory
/// Used: the cgroup's high-water mark since the container started (cgroup v2
/// `memory.peak`, else v1 `memory.max_usage_in_bytes`), page cache included.
/// `None` when it cannot be read (e.g. an image without `cat`).
async fn container_peak_mb(docker: &str, name: &str) -> Option<u64> {
    const PEAK_FILES: [&str; 2] = [
        "/sys/fs/cgroup/memory.peak",
        "/sys/fs/cgroup/memory/memory.max_usage_in_bytes",
    ];
    let deadline = Instant::now() + Duration::from_secs(2);
    for file in PEAK_FILES {
        let Ok(out) = docker_output(docker, &["exec", name, "cat", file], deadline).await else {
            return None;
        };
        if out.status.success() {
            return parse_bytes_as_mb(&String::from_utf8_lossy(&out.stdout));
        }
    }
    None
}

/// A byte count (as cgroup files print it) in whole MB, rounded up.
fn parse_bytes_as_mb(text: &str) -> Option<u64> {
    let bytes: u64 = text.trim().parse().ok()?;
    Some(bytes.div_ceil(1024 * 1024))
}

/// The image's default CMD, pulling the image first if it is not local.
async fn image_cmd(docker: &str, uri: &str, deadline: Instant) -> Result<Vec<String>, String> {
    let inspect = [
        "image",
        "inspect",
        "--format",
        "{{json .Config.Cmd}}",
        "--",
        uri,
    ];
    let mut out = docker_output(docker, &inspect, deadline).await?;
    if !out.status.success() {
        let pulled = docker_output(docker, &["pull", "--", uri], deadline).await?;
        if !pulled.status.success() {
            return Err(format!(
                "docker pull {uri}: {}",
                String::from_utf8_lossy(&pulled.stderr).trim()
            ));
        }
        out = docker_output(docker, &inspect, deadline).await?;
        if !out.status.success() {
            return Err(format!(
                "docker image inspect {uri}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
    }
    // `null` when the image has no CMD.
    serde_json::from_slice::<Option<Vec<String>>>(&out.stdout)
        .map(Option::unwrap_or_default)
        .map_err(|e| format!("docker image inspect {uri}: unexpected output: {e}"))
}

/// Runs a docker CLI command to completion, bounded by `deadline`.
async fn docker_output(
    docker: &str,
    args: &[&str],
    deadline: Instant,
) -> Result<std::process::Output, String> {
    let run = tokio::process::Command::new(docker)
        .args(args)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output();
    tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), run)
        .await
        .map_err(|_| format!("docker {}: timed out", args.join(" ")))?
        .map_err(|e| docker_start_error(docker, &format!("docker {}", args.join(" ")), &e))
}

/// Why `docker` could not be started. A missing CLI gets an explanation:
/// the usual cause is running devcloud-lambda's own Docker image, which has
/// no Docker inside.
fn docker_start_error(docker: &str, what: &str, e: &std::io::Error) -> String {
    if e.kind() == std::io::ErrorKind::NotFound {
        format!(
            "{what}: the Docker CLI ({docker}) was not found on devcloud's PATH. Container image functions need it; the devcloud-lambda Docker image does not include Docker, so run devcloud-lambda (or devcloud) on the host to invoke them"
        )
    } else {
        format!("{what}: {e}")
    }
}

/// The variables Lambda sets in an image function's environment. The RIE
/// reads `AWS_LAMBDA_FUNCTION_TIMEOUT` and the memory size for its REPORT.
fn container_env(inv: &Invocation, env_id: u64) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = vec![
        ("AWS_LAMBDA_FUNCTION_NAME".into(), inv.function_name.clone()),
        ("AWS_LAMBDA_FUNCTION_VERSION".into(), "$LATEST".into()),
        (
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE".into(),
            inv.memory_size.to_string(),
        ),
        (
            "AWS_LAMBDA_FUNCTION_TIMEOUT".into(),
            inv.timeout_seconds.max(1).to_string(),
        ),
        (
            "AWS_LAMBDA_LOG_GROUP_NAME".into(),
            format!("/aws/lambda/{}", inv.function_name),
        ),
        (
            "AWS_LAMBDA_LOG_STREAM_NAME".into(),
            format!("devcloud/[$LATEST]env-{env_id}"),
        ),
        ("AWS_REGION".into(), inv.region.clone()),
        ("AWS_DEFAULT_REGION".into(), inv.region.clone()),
        ("AWS_LAMBDA_INITIALIZATION_TYPE".into(), "on-demand".into()),
    ];
    if let Some(creds) = &inv.credentials {
        env.push(("AWS_ACCESS_KEY_ID".into(), creds.access_key_id.clone()));
        env.push((
            "AWS_SECRET_ACCESS_KEY".into(),
            creds.secret_access_key.clone(),
        ));
        if !creds.session_token.is_empty() {
            env.push(("AWS_SESSION_TOKEN".into(), creds.session_token.clone()));
        }
    }
    env
}

fn free_port() -> Result<u16, String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .map_err(|e| format!("allocate a port for the container: {e}"))
}

/// Where devcloud writes invocation requests to a bootstrap.
#[cfg(unix)]
type ControlWriter = tokio::net::unix::pipe::Sender;
#[cfg(not(unix))]
type ControlWriter = tokio::process::ChildStdin;

/// Fd the bootstrap reads requests from.
#[cfg(unix)]
const CONTROL_FD: i32 = 3;

/// Gives `cmd`'s child the read end of a new pipe as [`CONTROL_FD`] and
/// returns the write end. Requests must not travel on stdin: a handler that
/// reads its stdin would consume them (or block forever waiting for EOF).
#[cfg(unix)]
fn attach_control_pipe(cmd: &mut tokio::process::Command) -> std::io::Result<ControlWriter> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: `fds` is a valid two-element buffer; on success both
    // descriptors are new and owned here. FD_CLOEXEC keeps them out of other
    // children; the dup2 below gives only this child its copy. Linux sets it
    // atomically, so a concurrent spawn on another thread cannot inherit the
    // pipe; macOS has no pipe2 and sets it right after.
    #[cfg(target_os = "linux")]
    let created = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    #[cfg(not(target_os = "linux"))]
    let created = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if created != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: both descriptors were just created and are owned by nobody else.
    let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    #[cfg(not(target_os = "linux"))]
    for fd in [&read, &write] {
        // SAFETY: plain fcntl on a descriptor we own.
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
    }
    let read_fd = read.as_raw_fd();
    // SAFETY: the closure only calls async-signal-safe functions (dup2,
    // fcntl) between fork and exec. `read` outlives the spawn: it moves into
    // the closure, which `cmd` keeps until it is dropped.
    unsafe {
        cmd.pre_exec(move || {
            let _keep = &read;
            if read_fd == CONTROL_FD {
                // dup2 onto itself would keep FD_CLOEXEC set.
                if libc::fcntl(read_fd, libc::F_SETFD, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            } else if libc::dup2(read_fd, CONTROL_FD) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    tokio::net::unix::pipe::Sender::from_owned_fd(write)
}

#[cfg(not(unix))]
fn attach_control_pipe(_cmd: &mut tokio::process::Command) -> std::io::Result<ControlWriter> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "local handler execution needs a Unix host",
    ))
}

/// A marker prefix the handler's own output will not contain by accident.
fn new_marker(id: u64) -> String {
    use sha2::{Digest, Sha256};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let digest = Sha256::digest(format!("{nanos}:{id}:{}", std::process::id()).as_bytes());
    format!("devcloud-{}", hex::encode(&digest[..16]))
}

/// Feeds stdout through the marker scanner, reporting markers and the end of
/// the stream. Output between markers stays in the shared scanner, so a
/// reader abandoned after the drain deadline still leaves it behind.
async fn scan<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    scanner: Arc<Mutex<Scanner>>,
    tx: mpsc::UnboundedSender<StreamEvent>,
) {
    let mut chunk = [0u8; 8192];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => {
                let _ = tx.send(StreamEvent::Eof);
                return;
            }
            Ok(n) => {
                let markers = scanner.lock().unwrap().push(&chunk[..n]);
                for (tag, log) in markers {
                    let _ = tx.send(StreamEvent::Marker { tag, log });
                }
            }
        }
    }
}

/// Appends everything `reader` yields into `buf` as it arrives, so a reader
/// abandoned after the drain deadline still leaves its partial output behind.
async fn collect<R: tokio::io::AsyncRead + Unpin>(mut reader: R, buf: Arc<Mutex<Vec<u8>>>) {
    let mut chunk = [0u8; 8192];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => append_bounded(&mut buf.lock().unwrap(), &chunk[..n]),
        }
    }
}

/// Appends `data`, keeping only the newest [`LOG_CAPTURE_BYTES`]: callers only
/// ever use the log tail, and a chatty handler must not grow devcloud's memory
/// without bound.
fn append_bounded(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(data);
    if buf.len() > LOG_CAPTURE_BYTES {
        let excess = buf.len() - LOG_CAPTURE_BYTES;
        buf.drain(..excess);
    }
}

#[cfg(unix)]
fn kill_process_group(pid: u32) {
    // SAFETY: killpg only sends a signal; ESRCH (group already gone) is fine.
    unsafe {
        libc::killpg(pid as libc::pid_t, libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_process_group(_pid: u32) {}

/// Absolute path of `bin`: as given when it contains a `/`, otherwise the
/// first executable match on devcloud's `PATH`.
///
/// The result is always absolute (relative to devcloud's working directory):
/// the child starts in the function's code directory, where a relative path
/// such as `.venv/bin/python3` would point somewhere else.
fn resolve_program(bin: &str) -> Option<PathBuf> {
    let found = if bin.contains('/') {
        PathBuf::from(bin)
    } else {
        let path = std::env::var_os("PATH")?;
        std::env::split_paths(&path)
            .map(|dir| dir.join(bin))
            .find(|candidate| is_executable(candidate))?
    };
    std::path::absolute(found).ok()
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.is_file()
    }
}

/// Values Lambda provides but lets functions override (`TZ`, `LANG`, `PATH`,
/// and the `/opt` layer paths in `PYTHONPATH` / `NODE_PATH`).
fn default_env(family: Family, inv: &Invocation) -> Vec<(String, String)> {
    let mut env = vec![
        ("TZ".to_string(), "UTC".to_string()),
        ("LANG".to_string(), "en_US.UTF-8".to_string()),
        ("PYTHONDONTWRITEBYTECODE".to_string(), "1".to_string()),
    ];
    if let Ok(path) = std::env::var("PATH") {
        env.push(("PATH".to_string(), path));
    }
    let opt = inv.opt_dir.to_string_lossy();
    let v = version(&inv.runtime);
    let layer_path = match (family, v) {
        (Family::Python, Some(v)) => (
            "PYTHONPATH",
            format!("{opt}/python/lib/python{v}/site-packages:{opt}/python"),
        ),
        (Family::Python, None) => ("PYTHONPATH", format!("{opt}/python")),
        (Family::Node, Some(v)) => (
            "NODE_PATH",
            format!("{opt}/nodejs/node{v}/node_modules:{opt}/nodejs/node_modules"),
        ),
        (Family::Node, None) => ("NODE_PATH", format!("{opt}/nodejs/node_modules")),
    };
    env.push((layer_path.0.to_string(), layer_path.1));
    env
}

fn reserved_env(inv: &Invocation, env_id: u64) -> Vec<(String, String)> {
    let task_root = inv.code_dir.to_string_lossy().into_owned();
    let mut env: Vec<(String, String)> = vec![
        ("AWS_LAMBDA_FUNCTION_NAME".into(), inv.function_name.clone()),
        ("AWS_LAMBDA_FUNCTION_VERSION".into(), "$LATEST".into()),
        (
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE".into(),
            inv.memory_size.to_string(),
        ),
        (
            "AWS_LAMBDA_LOG_GROUP_NAME".into(),
            format!("/aws/lambda/{}", inv.function_name),
        ),
        (
            "AWS_LAMBDA_LOG_STREAM_NAME".into(),
            format!("devcloud/[$LATEST]env-{env_id}"),
        ),
        ("AWS_REGION".into(), inv.region.clone()),
        ("AWS_DEFAULT_REGION".into(), inv.region.clone()),
        (
            "AWS_EXECUTION_ENV".into(),
            format!("AWS_Lambda_{}", inv.runtime),
        ),
        ("LAMBDA_TASK_ROOT".into(), task_root.clone()),
        ("LAMBDA_RUNTIME_DIR".into(), task_root),
        ("_HANDLER".into(), inv.handler.clone()),
        ("AWS_LAMBDA_INITIALIZATION_TYPE".into(), "on-demand".into()),
        ("_DEVCLOUD_FUNCTION_ARN".into(), inv.function_arn.clone()),
        (
            "_DEVCLOUD_RUNTIME_VERSION".into(),
            version(&inv.runtime).unwrap_or_default(),
        ),
    ];
    if let Some(creds) = &inv.credentials {
        env.push(("AWS_ACCESS_KEY_ID".into(), creds.access_key_id.clone()));
        env.push((
            "AWS_SECRET_ACCESS_KEY".into(),
            creds.secret_access_key.clone(),
        ));
        if !creds.session_token.is_empty() {
            env.push(("AWS_SESSION_TOKEN".into(), creds.session_token.clone()));
        }
    }
    env
}

/// An exit status the way the Lambda runtime reports it (Go's
/// `ProcessState.String`): `exit status 1`, `signal: killed`.
fn exit_status_text(status: std::process::ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            let name = match sig {
                libc::SIGHUP => "hangup",
                libc::SIGINT => "interrupt",
                libc::SIGQUIT => "quit",
                libc::SIGILL => "illegal instruction",
                libc::SIGABRT => "aborted",
                libc::SIGBUS => "bus error",
                libc::SIGFPE => "floating point exception",
                libc::SIGKILL => "killed",
                libc::SIGSEGV => "segmentation fault",
                libc::SIGPIPE => "broken pipe",
                libc::SIGTERM => "terminated",
                _ => return format!("signal: {sig}"),
            };
            return format!("signal: {name}");
        }
    }
    match status.code() {
        Some(code) => format!("exit status {code}"),
        None => status.to_string(),
    }
}

fn read_result(path: &Path, request_id: &str) -> Outcome {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => {
            return Outcome::FunctionError(error_doc(
                &format!(
                    "RequestId: {request_id} Error: Runtime exited without providing a reason"
                ),
                "Runtime.ExitError",
            ))
        }
    };
    match serde_json::from_slice::<BootstrapResult>(&data) {
        Ok(BootstrapResult {
            result: Some(body), ..
        }) => Outcome::Success(body.into_bytes()),
        Ok(BootstrapResult {
            error: Some(err), ..
        }) => Outcome::FunctionError(serde_json::to_vec(&err).unwrap_or_default()),
        _ => Outcome::FunctionError(error_doc(
            "runtime produced an invalid result",
            "Runtime.InvalidResult",
        )),
    }
}

fn error_doc(message: &str, error_type: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "errorMessage": message,
        "errorType": error_type,
    }))
    .unwrap_or_default()
}

/// CloudWatch-style framing. A cold start (`init`: duration and the output
/// of the init phase) adds an `INIT_START` section and `Init Duration`.
fn frame_log(
    inv: &Invocation,
    init: Option<(Duration, String)>,
    body: &str,
    duration: Duration,
    timed_out: bool,
    max_memory_mb: Option<u64>,
) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    let billed = (ms.ceil() as u64).max(1);
    let mut log = String::new();
    let push_block = |log: &mut String, text: &str| {
        log.push_str(text);
        if !text.is_empty() && !text.ends_with('\n') {
            log.push('\n');
        }
    };
    if let Some((_, init_log)) = &init {
        // An image function has no managed runtime: name its image instead.
        match &inv.image {
            Some(image) => log.push_str(&format!("INIT_START Image: {}\n", image.uri)),
            None => log.push_str(&format!("INIT_START Runtime Version: {}\n", inv.runtime)),
        }
        push_block(&mut log, init_log);
    }
    log.push_str(&format!(
        "START RequestId: {} Version: $LATEST\n",
        inv.request_id
    ));
    push_block(&mut log, body);
    if timed_out {
        log.push_str(&format!(
            "{} {} Task timed out after {}.00 seconds\n",
            crate::time_fmt::now_rfc3339(),
            inv.request_id,
            inv.timeout_seconds.max(1)
        ));
    }
    if let Some(used) = max_memory_mb.filter(|&m| m > inv.memory_size.max(0) as u64) {
        log.push_str(&format!(
            "[WARNING] devcloud: Max Memory Used ({used} MB) exceeds MemorySize ({} MB); Lambda would have stopped this invocation\n",
            inv.memory_size
        ));
    }
    log.push_str(&format!("END RequestId: {}\n", inv.request_id));
    log.push_str(&format!(
        "REPORT RequestId: {}\tDuration: {:.2} ms\tBilled Duration: {} ms\tMemory Size: {} MB",
        inv.request_id, ms, billed, inv.memory_size
    ));
    if let Some(used) = max_memory_mb {
        log.push_str(&format!("\tMax Memory Used: {used} MB"));
    }
    if let Some((init_duration, _)) = init {
        log.push_str(&format!(
            "\tInit Duration: {:.2} ms",
            init_duration.as_secs_f64() * 1000.0
        ));
    }
    log.push('\n');
    log
}

/// Resets the peak resident set size of `pid` (Linux `clear_refs`), so the
/// next [`peak_rss_mb`] covers only what follows. Elsewhere: nothing to do.
#[cfg(target_os = "linux")]
fn reset_peak_rss(pid: u32) {
    let _ = std::fs::write(format!("/proc/{pid}/clear_refs"), "5");
}

#[cfg(not(target_os = "linux"))]
fn reset_peak_rss(_pid: u32) {}

/// Peak resident set size of `pid` in MB (Linux `VmHWM`), for REPORT's Max
/// Memory Used. It covers the interpreter process, not processes it spawned.
#[cfg(target_os = "linux")]
fn peak_rss_mb(pid: u32) -> Option<u64> {
    parse_vm_hwm(&std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?)
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_mb(_pid: u32) -> Option<u64> {
    None
}

/// `VmHWM:   123456 kB` → MB, rounded up like Lambda's report.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_vm_hwm(status: &str) -> Option<u64> {
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kb.div_ceil(1024))
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn family_mapping() {
        assert_eq!(family("python3.12"), Some(Family::Python));
        assert_eq!(family("nodejs20.x"), Some(Family::Node));
        assert_eq!(family("java21"), None);
        assert_eq!(family("provided.al2023"), None);
    }

    #[test]
    fn version_mapping() {
        assert_eq!(version("python3.12").as_deref(), Some("3.12"));
        assert_eq!(version("python3.9").as_deref(), Some("3.9"));
        assert_eq!(version("nodejs20.x").as_deref(), Some("20"));
        assert_eq!(version("nodejs").as_deref(), None);
        assert_eq!(version("python3.x").as_deref(), None);
        assert_eq!(version("java21").as_deref(), None);
    }

    #[test]
    fn default_python_prefers_the_runtime_versioned_binary() {
        let defaults = Interpreters::default();
        let pinned = Interpreters {
            python: resolve_program("python3")
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| "python3".into()),
            ..Interpreters::default()
        };
        for minor in 8..=20 {
            let runtime = format!("python3.{minor}");
            let Some(versioned) = resolve_program(&runtime) else {
                continue;
            };
            assert_eq!(defaults.resolve(Family::Python, &runtime), Some(versioned));
            if pinned.python != DEFAULT_PYTHON {
                assert_eq!(
                    pinned.resolve(Family::Python, &runtime),
                    resolve_program(&pinned.python),
                    "an explicit interpreter is used as configured"
                );
            }
        }
        assert_eq!(
            defaults.resolve(Family::Python, "python3.99"),
            resolve_program("python3"),
            "falls back to python3 when the versioned binary is missing"
        );
    }

    #[test]
    fn scanner_splits_output_at_markers_across_chunks() {
        let mut s = Scanner::new(b"devcloud-ab ".to_vec());
        assert!(s.push(b"hello devcl").is_empty());
        assert!(
            s.push(b"oud-ab do").is_empty(),
            "marker line not complete yet"
        );
        assert_eq!(
            s.push(b"ne\nnext devcloud-ab ready\ntail"),
            vec![
                ("done".to_string(), b"hello ".to_vec()),
                ("ready".to_string(), b"next ".to_vec()),
            ]
        );
        assert_eq!(s.take_rest(), b"tail");
    }

    #[test]
    fn scanner_keeps_only_the_newest_bytes_between_markers() {
        let mut s = Scanner::new(b"devcloud-ab ".to_vec());
        for _ in 0..(LOG_CAPTURE_BYTES / 1024 + 8) {
            assert!(s.push(&[b'x'; 1024]).is_empty());
        }
        let markers = s.push(b"TAILdevcloud-ab done\n");
        assert_eq!(markers.len(), 1);
        assert!(markers[0].1.len() <= LOG_CAPTURE_BYTES + 4);
        assert!(markers[0].1.ends_with(b"TAIL"));
    }

    #[test]
    fn version_mismatch_is_reported_once_per_function_and_runtime() {
        let pool = Pool::new(std::env::temp_dir(), DEFAULT_IDLE_TIMEOUT, "t".into());
        let inv = |function: &str, runtime: &str| Invocation {
            runtime: runtime.into(),
            handler: "app.handler".into(),
            code_dir: PathBuf::new(),
            function_name: function.into(),
            function_arn: String::new(),
            memory_size: 128,
            timeout_seconds: 3,
            region: "us-east-1".into(),
            request_id: String::new(),
            environment: BTreeMap::new(),
            payload: Vec::new(),
            opt_dir: PathBuf::new(),
            credentials: None,
            env_key: String::new(),
            image: None,
        };
        let log = b"loading\n[WARNING] devcloud: runtime python3.12 is running on Python 3.11.2 (/usr/bin/python3)\n";
        assert_eq!(
            pool.version_mismatch(&inv("f", "python3.12"), log)
                .as_deref(),
            Some("function f: runtime python3.12 is running on Python 3.11.2 (/usr/bin/python3)")
        );
        assert_eq!(
            pool.version_mismatch(&inv("f", "python3.12"), log),
            None,
            "once"
        );
        assert!(
            pool.version_mismatch(&inv("g", "python3.12"), log)
                .is_some(),
            "per function"
        );
        assert_eq!(
            pool.version_mismatch(&inv("h", "python3.12"), b"no warning\n"),
            None
        );
        // A handler printing a look-alike line for another runtime is ignored.
        assert_eq!(pool.version_mismatch(&inv("i", "python3.13"), log), None);
    }

    #[test]
    fn cgroup_byte_counts_are_reported_in_whole_megabytes() {
        assert_eq!(parse_bytes_as_mb("157286400\n"), Some(150));
        assert_eq!(parse_bytes_as_mb("157286401"), Some(151));
        assert_eq!(parse_bytes_as_mb("max"), None);
    }

    #[test]
    fn vm_hwm_is_reported_in_whole_megabytes() {
        let status =
            "Name:\tpython3\nVmPeak:\t  999999 kB\nVmHWM:\t   45057 kB\nVmRSS:\t   40000 kB\n";
        assert_eq!(parse_vm_hwm(status), Some(45), "rounded up");
        assert_eq!(parse_vm_hwm("VmHWM:\t 1024 kB\n"), Some(1));
        assert_eq!(parse_vm_hwm("VmRSS:\t 1024 kB\n"), None);
    }

    #[test]
    fn credentials_debug_hides_secrets() {
        let creds = FunctionCredentials {
            access_key_id: "AKID".into(),
            secret_access_key: "s3cr3t".into(),
            session_token: "t0ken".into(),
        };
        let shown = format!("{creds:?}");
        assert!(shown.contains("AKID"));
        assert!(
            !shown.contains("s3cr3t") && !shown.contains("t0ken"),
            "{shown}"
        );
    }

    #[tokio::test]
    async fn collect_keeps_only_the_newest_bytes() {
        use tokio::io::AsyncReadExt as _;
        let reader = tokio::io::repeat(b'x')
            .take(16 * 1024 * 1024)
            .chain(&b"TAIL"[..]);
        let buf = Arc::new(Mutex::new(Vec::new()));
        collect(reader, Arc::clone(&buf)).await;
        let buf = buf.lock().unwrap();
        assert_eq!(buf.len(), LOG_CAPTURE_BYTES);
        assert!(buf.ends_with(b"TAIL"));
    }

    #[test]
    fn resolved_interpreters_are_absolute() {
        let cwd = std::env::current_dir().unwrap();
        let dir =
            std::env::temp_dir().join(format!("devcloud-lambda-relbin-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("fake-python");
        std::fs::write(&bin, "#!/bin/sh\n").unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        // `./<relative dir>/fake-python`, as a relative PATH entry would yield.
        let rel = pathdiff(&bin, &cwd);
        let resolved = resolve_program(&rel).unwrap();
        assert!(resolved.is_absolute(), "{resolved:?}");
        assert_eq!(
            std::fs::canonicalize(&resolved).unwrap(),
            std::fs::canonicalize(&bin).unwrap()
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    fn pathdiff(target: &Path, base: &Path) -> String {
        let t: Vec<_> = target.components().collect();
        let b: Vec<_> = base.components().collect();
        let common = t.iter().zip(&b).take_while(|(x, y)| x == y).count();
        let mut out = PathBuf::from(".");
        for _ in common..b.len() {
            out.push("..");
        }
        for c in &t[common..] {
            out.push(c);
        }
        out.to_string_lossy().into_owned()
    }

    #[test]
    fn missing_result_is_exit_error() {
        let out = read_result(Path::new("/nonexistent/devcloud/result.json"), "req-1");
        match out {
            Outcome::FunctionError(body) => {
                let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(v["errorType"], "Runtime.ExitError");
                assert_eq!(
                    v["errorMessage"],
                    "RequestId: req-1 Error: Runtime exited without providing a reason"
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
