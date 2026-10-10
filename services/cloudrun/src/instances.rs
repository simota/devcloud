//! Local "container instances" for Cloud Run services.
//!
//! Each service runs at most one instance: the latest revision's first
//! container, started lazily on the first request (scale-from-zero) and kept
//! until the service gets a new revision, is deleted, or devcloud shuts down.
//! A container with a `command` runs as a local process (the image is not
//! pulled); an image-only container runs under `docker run` when Docker
//! execution is enabled. Either way the instance listens on a free loopback
//! port passed in `PORT`, which the HTTP layer reverse-proxies to.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::TcpListener as StdListener;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const LOG_LINES: usize = 200;
/// Longest kept log line; longer output is split into several entries.
const MAX_LOG_LINE_BYTES: usize = 16 * 1024;
const STOP_GRACE: Duration = Duration::from_secs(2);
const DOCKER_RM_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(10)
};

/// What to run for one revision, derived from `template.containers[0]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchSpec {
    pub service: String,
    pub service_id: String,
    pub revision_id: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub working_dir: String,
    pub container_port: u16,
}

impl LaunchSpec {
    /// Builds the spec from a stored Service resource.
    pub fn from_service(service: &Value) -> Option<LaunchSpec> {
        let name = service.get("name")?.as_str()?.to_string();
        let service_id = name.rsplit('/').next()?.to_string();
        let template = service.get("template")?;
        let container = template.get("containers")?.as_array()?.first()?;
        let strings = |key: &str| -> Vec<String> {
            container
                .get(key)
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default()
        };
        let env = container
            .get("env")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|e| {
                        Some((
                            e.get("name")?.as_str()?.to_string(),
                            e.get("value")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let container_port = container
            .pointer("/ports/0/containerPort")
            .and_then(Value::as_u64)
            .and_then(|n| u16::try_from(n).ok())
            .filter(|n| *n != 0)
            .unwrap_or(8080);
        let revision_id = service
            .get("latestCreatedRevision")
            .and_then(Value::as_str)
            .and_then(|r| r.rsplit('/').next())
            .unwrap_or("")
            .to_string();
        Some(LaunchSpec {
            service: name,
            service_id,
            revision_id,
            image: container
                .get("image")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            command: strings("command"),
            args: strings("args"),
            env,
            working_dir: container
                .get("workingDir")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            container_port,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartError {
    /// Nothing runnable: image-only container while Docker execution is off.
    NotRunnable(String),
    Spawn(String),
    /// The process exited or never opened its port.
    Unhealthy(String),
    /// The service was deleted (or devcloud is stopping) while the request
    /// waited; nothing may be started for it.
    Gone,
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartError::NotRunnable(m) | StartError::Spawn(m) | StartError::Unhealthy(m) => {
                f.write_str(m)
            }
            StartError::Gone => f.write_str("service no longer exists"),
        }
    }
}

struct Instance {
    spec: LaunchSpec,
    port: u16,
    child: Child,
    /// Process (group) id captured at spawn; `child.id()` is gone once reaped.
    pid: Option<u32>,
    docker_name: Option<String>,
    started_at: String,
    requests: u64,
    /// Kills the group (and removes the container) if this instance is
    /// dropped without a completed `terminate` — e.g. a delete's connection
    /// task cancelled by devcloud shutting down mid-teardown.
    guard: TeardownGuard,
}

struct TeardownGuard {
    pgid: Option<u32>,
    /// `(docker binary, container name)`.
    container: Option<(String, String)>,
    /// Where a container removal started from `drop` is tracked, so
    /// `stop_all` waits for it before devcloud exits.
    removals: Removals,
}

/// Container removals running on their own threads.
type Removals = Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>;

impl TeardownGuard {
    fn disarm(&mut self) {
        self.pgid = None;
        self.container = None;
    }
}

impl Drop for TeardownGuard {
    fn drop(&mut self) {
        if let Some(pgid) = self.pgid.take() {
            signal_group(pgid, Signal::Kill);
        }
        if let Some((docker, name)) = self.container.take() {
            // No runtime to await on here: remove the container from a
            // thread (bounded by the CLI itself), tracked so shutdown waits
            // for it — killing the CLI alone leaves the container running.
            let handle = std::thread::spawn(move || {
                let _ = std::process::Command::new(docker)
                    .args(["rm", "-f", &name])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            });
            let mut pending = self.removals.lock().unwrap();
            pending.retain(|h| !h.is_finished());
            pending.push(handle);
        }
    }
}

impl Instance {
    /// Whether the instance's process is still running. Reaping the leader
    /// also kills what is left of its group at once and forgets the group:
    /// once empty, its id can be reused, and a later signal to it would hit
    /// an unrelated process group.
    fn alive(&mut self) -> bool {
        if matches!(self.child.try_wait(), Ok(None)) {
            return true;
        }
        if let Some(pid) = self.pid.take() {
            signal_group(pid, Signal::Kill);
        }
        self.guard.pgid = None;
        false
    }
}

/// Introspection view of one instance.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceInfo {
    pub service: String,
    pub revision: String,
    pub port: u16,
    pub pid: Option<u32>,
    pub mode: &'static str,
    pub started_at: String,
    pub request_count: u64,
}

/// One service's instance slot. Each slot has its own async lock, so a slow
/// cold start only blocks requests for that service.
#[derive(Default)]
struct Slot {
    instance: Option<Instance>,
    /// Set by `stop`: the slot left the map and must not get a new instance.
    retired: bool,
}

#[derive(Default)]
struct Slots {
    map: HashMap<String, Arc<tokio::sync::Mutex<Slot>>>,
    /// Set by `stop_all`; checked under the same lock as slot creation, so no
    /// request accepted before shutdown can create a slot `stop_all` misses.
    closed: bool,
}

pub struct Manager {
    docker_bin: Option<String>,
    /// Distinguishes this devcloud run's Docker containers from other runs'.
    run_id: String,
    /// Short critical sections only — never held across an `.await`.
    slots: Mutex<Slots>,
    logs: Arc<Mutex<HashMap<String, VecDeque<String>>>>,
    /// Ports handed to instances that are starting or running, so two cold
    /// starts never get the same one (`free_port` releases its listener).
    ports: Mutex<HashSet<u16>>,
    removals: Removals,
}

impl Manager {
    pub fn new(docker_bin: Option<String>) -> Self {
        Manager {
            docker_bin,
            run_id: new_run_id(),
            slots: Mutex::new(Slots::default()),
            logs: Arc::new(Mutex::new(HashMap::new())),
            ports: Mutex::new(HashSet::new()),
            removals: Removals::default(),
        }
    }

    /// The service's slot, or `None` once shutdown has begun.
    fn slot(&self, service: &str) -> Option<Arc<tokio::sync::Mutex<Slot>>> {
        let mut slots = self.slots.lock().unwrap();
        if slots.closed {
            return None;
        }
        Some(Arc::clone(
            slots.map.entry(service.to_string()).or_default(),
        ))
    }

    /// Returns the loopback port of a live instance of `service`, starting
    /// (or replacing) one when needed. `current` reads the service's launch
    /// spec from the control-plane state; it is consulted only after the slot
    /// lock is held, so a request that raced a delete or an update never
    /// (re)starts code the service no longer has.
    pub async fn ensure(
        &self,
        service: &str,
        current: impl Fn() -> Option<LaunchSpec>,
    ) -> Result<u16, StartError> {
        let Some(slot) = self.slot(service) else {
            return Err(StartError::Gone);
        };
        let mut guard = slot.lock().await;
        if guard.retired {
            // `stop` ran while we waited: the service was deleted.
            return Err(StartError::Gone);
        }
        let Some(spec) = current() else {
            // Deleted before we got the slot (its `stop` found no instance).
            drop(guard);
            self.discard_empty_slot(service, &slot);
            return Err(StartError::Gone);
        };
        if let Some(inst) = guard.instance.as_mut() {
            if inst.alive() && inst.spec == spec {
                inst.requests += 1;
                return Ok(inst.port);
            }
        }
        if let Some(old) = guard.instance.take() {
            self.terminate(old).await;
        }
        let mut inst = self.start(&spec).await?;
        inst.requests = 1;
        let port = inst.port;
        guard.instance = Some(inst);
        Ok(port)
    }

    /// Drops a slot this request created for a service that turned out to be
    /// gone, unless someone else has started using it meanwhile.
    fn discard_empty_slot(&self, service: &str, slot: &Arc<tokio::sync::Mutex<Slot>>) {
        let mut slots = self.slots.lock().unwrap();
        let unused = slots.map.get(service).is_some_and(|s| Arc::ptr_eq(s, slot))
            && slot.try_lock().is_ok_and(|g| g.instance.is_none());
        if unused {
            slots.map.remove(service);
        }
    }

    /// Stops the instance of `service`, if any (delete).
    pub async fn stop(&self, service: &str) {
        let removed = self.slots.lock().unwrap().map.remove(service);
        if let Some(slot) = removed {
            self.retire(slot).await;
        }
    }

    pub async fn stop_all(&self) {
        let drained: Vec<_> = {
            let mut slots = self.slots.lock().unwrap();
            slots.closed = true;
            slots.map.drain().map(|(_, v)| v).collect()
        };
        for slot in drained {
            self.retire(slot).await;
        }
        // Removals started by dropped instances (connections cancelled by
        // shutdown mid-start or mid-delete) finish before devcloud exits.
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

    /// A port no other instance of this manager holds.
    fn reserve_port(&self) -> Result<u16, String> {
        for _ in 0..32 {
            let port = free_port()?;
            if self.ports.lock().unwrap().insert(port) {
                return Ok(port);
            }
        }
        Err("allocate port: no free port".to_string())
    }

    fn release_port(&self, port: u16) {
        self.ports.lock().unwrap().remove(&port);
    }

    async fn retire(&self, slot: Arc<tokio::sync::Mutex<Slot>>) {
        let mut guard = slot.lock().await;
        guard.retired = true;
        if let Some(inst) = guard.instance.take() {
            self.terminate(inst).await;
        }
    }

    /// Running instances. Slots busy with a cold start are skipped rather than
    /// waited on.
    pub async fn list(&self) -> Vec<InstanceInfo> {
        let slots: Vec<_> = self.slots.lock().unwrap().map.values().cloned().collect();
        let mut out = Vec::new();
        for slot in slots {
            let Ok(mut guard) = slot.try_lock() else {
                continue;
            };
            let Some(i) = guard.instance.as_mut() else {
                continue;
            };
            if !i.alive() {
                continue;
            }
            out.push(InstanceInfo {
                service: i.spec.service.clone(),
                revision: i.spec.revision_id.clone(),
                port: i.port,
                pid: i.pid,
                mode: if i.docker_name.is_some() {
                    "docker"
                } else {
                    "process"
                },
                started_at: i.started_at.clone(),
                request_count: i.requests,
            });
        }
        out.sort_by(|a, b| a.service.cmp(&b.service));
        out
    }

    /// Recent stdout/stderr lines of a service's instances (newest last).
    pub fn logs(&self, service: &str) -> Vec<String> {
        self.logs
            .lock()
            .unwrap()
            .get(service)
            .map(|l| l.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn forget_logs(&self, service: &str) {
        self.logs.lock().unwrap().remove(service);
    }

    async fn start(&self, spec: &LaunchSpec) -> Result<Instance, StartError> {
        if spec.command.is_empty() && self.docker_bin.is_none() {
            return Err(not_runnable(spec));
        }
        let port = self.reserve_port().map_err(StartError::Spawn)?;
        let started = self.start_on(spec, port).await;
        if started.is_err() {
            self.release_port(port);
        }
        started
    }

    async fn start_on(&self, spec: &LaunchSpec, port: u16) -> Result<Instance, StartError> {
        let (mut cmd, docker_name) = self.command_for(spec, port)?;
        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null())
            .kill_on_drop(true);
        // Own process group: stopping the instance must also stop whatever the
        // command forked (`sh -c 'server & wait'`, npm scripts, ...).
        #[cfg(unix)]
        cmd.process_group(0);
        // If devcloud itself dies (SIGKILL, OOM, abort), its own group leaves
        // with it; the instance would not, since it has a group of its own.
        #[cfg(target_os = "linux")]
        // SAFETY: prctl is async-signal-safe and only touches this child.
        unsafe {
            cmd.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn().map_err(|e| {
            StartError::Spawn(format!("start {}: {e}", describe(spec, &docker_name)))
        })?;
        let pid = child.id();
        let mut guard = TeardownGuard {
            pgid: pid,
            container: self.docker_bin.clone().zip(docker_name.clone()),
            removals: Arc::clone(&self.removals),
        };
        if let Some(out) = child.stdout.take() {
            self.pump_logs(&spec.service, out);
        }
        if let Some(err) = child.stderr.take() {
            self.pump_logs(&spec.service, err);
        }

        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            let mut listening = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok();
            // The port was free when picked, but another program may have
            // taken it before the child could: then the child fails to bind
            // while something else answers. A local child must own the
            // listener (Docker's proxy listens for containers, so it is
            // exempt).
            #[cfg(target_os = "linux")]
            if listening && docker_name.is_none() {
                if let Some(pgid) = pid {
                    listening = listener_in_group(port, pgid) != Some(false);
                }
            }
            // Checked after the connect: a child that already exited (say it
            // lost the port to another program) must not be taken as ready
            // because something else answered.
            if let Ok(Some(status)) = child.try_wait() {
                if let Some(pid) = pid {
                    signal_group(pid, Signal::Kill);
                }
                // The CLI may die (daemon disconnect) after the container was
                // created; nothing tracks it yet, so remove it here.
                if let Some(name) = &docker_name {
                    self.remove_container(name).await;
                }
                guard.disarm();
                return Err(StartError::Unhealthy(format!(
                    "The user-provided container failed to start and listen on the port defined provided by the PORT={port} environment variable (exited with {status})"
                )));
            }
            if listening {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                let inst = Instance {
                    spec: spec.clone(),
                    port,
                    child,
                    pid,
                    docker_name,
                    started_at: String::new(),
                    requests: 0,
                    guard,
                };
                self.terminate(inst).await;
                return Err(StartError::Unhealthy(format!(
                    "The user-provided container failed to listen on PORT={port} within {}s",
                    STARTUP_TIMEOUT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(Instance {
            spec: spec.clone(),
            port,
            child,
            pid,
            docker_name,
            started_at: crate::time_fmt::now_rfc3339(),
            requests: 0,
            guard,
        })
    }

    fn command_for(
        &self,
        spec: &LaunchSpec,
        port: u16,
    ) -> Result<(Command, Option<String>), StartError> {
        let platform_env = [
            ("K_SERVICE", spec.service_id.clone()),
            ("K_REVISION", spec.revision_id.clone()),
            ("K_CONFIGURATION", spec.service_id.clone()),
        ];
        if let Some((program, rest)) = spec.command.split_first() {
            let mut cmd = Command::new(program);
            cmd.args(rest).args(&spec.args).env_clear();
            for key in ["PATH", "HOME", "USER", "LANG", "TMPDIR"] {
                if let Ok(v) = std::env::var(key) {
                    cmd.env(key, v);
                }
            }
            cmd.envs(&spec.env)
                .envs(platform_env.iter().map(|(k, v)| (*k, v.as_str())))
                .env("PORT", port.to_string());
            if !spec.working_dir.is_empty() {
                cmd.current_dir(&spec.working_dir);
            }
            return Ok((cmd, None));
        }
        let Some(docker) = &self.docker_bin else {
            return Err(not_runnable(spec));
        };
        let name = docker_container_name(spec, &self.run_id);
        let mut cmd = Command::new(docker);
        cmd.args(docker_run_args(spec, port, &name));
        Ok((cmd, Some(name)))
    }

    fn pump_logs<R>(&self, service: &str, reader: R)
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let logs = Arc::clone(&self.logs);
        let service = service.to_string();
        tokio::spawn(async move {
            let push = |line: &[u8]| {
                let mut map = logs.lock().unwrap();
                let buf = map.entry(service.clone()).or_default();
                if buf.len() >= LOG_LINES {
                    buf.pop_front();
                }
                buf.push_back(String::from_utf8_lossy(line).into_owned());
            };
            read_log_lines(reader, push).await;
        });
    }

    /// `docker rm -f <name>`, bounded: an unresponsive daemon must not hang
    /// deletes or shutdown (kill_on_drop reaps the CLI on timeout).
    async fn remove_container(&self, name: &str) {
        let Some(docker) = &self.docker_bin else {
            return;
        };
        let rm = Command::new(docker)
            .args(["rm", "-f", name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status();
        let _ = tokio::time::timeout(DOCKER_RM_TIMEOUT, rm).await;
    }

    async fn terminate(&self, mut inst: Instance) {
        self.release_port(inst.port);
        if let Some(name) = &inst.docker_name {
            self.remove_container(name).await;
        }
        // Graceful first (SIGTERM, like Cloud Run), then make sure nothing in
        // the group survives.
        if let Some(pid) = inst.pid {
            signal_group(pid, Signal::Term);
            let _ = tokio::time::timeout(STOP_GRACE, inst.child.wait()).await;
            signal_group(pid, Signal::Kill);
        }
        let _ = inst.child.kill().await;
        inst.guard.disarm();
    }
}

#[derive(Clone, Copy)]
enum Signal {
    Term,
    Kill,
}

#[cfg(unix)]
fn signal_group(pgid: u32, signal: Signal) {
    let sig = match signal {
        Signal::Term => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
    };
    // SAFETY: killpg only sends a signal; ESRCH (group already gone) is fine.
    unsafe {
        libc::killpg(pgid as libc::pid_t, sig);
    }
}

#[cfg(not(unix))]
fn signal_group(_pgid: u32, _signal: Signal) {}

/// Feeds `push` one log line at a time. Lines are split at
/// [`MAX_LOG_LINE_BYTES`], so a newline-free stream cannot grow devcloud's
/// memory without bound.
async fn read_log_lines<R>(mut reader: R, mut push: impl FnMut(&[u8]))
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut chunk = [0u8; 8192];
    let mut line = Vec::new();
    loop {
        let n = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        for &b in &chunk[..n] {
            if b == b'\n' {
                push(&line);
                line.clear();
            } else {
                line.push(b);
                if line.len() >= MAX_LOG_LINE_BYTES {
                    push(&line);
                    line.clear();
                }
            }
        }
    }
    if !line.is_empty() {
        push(&line);
    }
}

fn not_runnable(spec: &LaunchSpec) -> StartError {
    StartError::NotRunnable(format!(
        "container image {} has no command; set template.containers[0].command to run it as a local process, or enable services.cloudRun.docker",
        spec.image
    ))
}

fn describe(spec: &LaunchSpec, docker_name: &Option<String>) -> String {
    match docker_name {
        Some(_) => format!("docker image {}", spec.image),
        None => format!("command {:?}", spec.command),
    }
}

/// Docker names are daemon-wide, while `serviceId`/revision ids only need to
/// be unique per project and location: a hash of the full resource name keeps
/// `projects/a/.../web` and `projects/b/.../web` apart.
///
/// `run_id` is unique per [`Manager`] (i.e. per devcloud run), so two
/// workspaces deploying the same resource names to one Docker daemon never
/// collide either.
fn docker_container_name(spec: &LaunchSpec, run_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let scope = hex::encode(&Sha256::digest(spec.service.as_bytes())[..6]);
    format!(
        "devcloud-run-{}-{}-{scope}-{run_id}",
        spec.service_id, spec.revision_id
    )
}

/// A short identifier unique to this process and moment.
fn new_run_id() -> String {
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seed = format!(
        "{}:{nanos}:{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    hex::encode(&Sha256::digest(seed.as_bytes())[..4])
}

/// `docker run` arguments for an image-only container. The container keeps
/// its declared port (passed as `PORT`) and is published on the loopback port.
pub fn docker_run_args(spec: &LaunchSpec, host_port: u16, name: &str) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        name.to_string(),
        "-p".to_string(),
        format!("127.0.0.1:{host_port}:{}", spec.container_port),
    ];
    // User env first, platform values last: with repeated `-e` the last one
    // wins, so a container `PORT=9000` cannot desync from the published port
    // (the local-process path applies them in the same order).
    let platform = [
        format!("PORT={}", spec.container_port),
        format!("K_SERVICE={}", spec.service_id),
        format!("K_REVISION={}", spec.revision_id),
        format!("K_CONFIGURATION={}", spec.service_id),
    ];
    for kv in spec
        .env
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .chain(platform)
    {
        args.push("-e".to_string());
        args.push(kv);
    }
    if !spec.working_dir.is_empty() {
        args.push("-w".to_string());
        args.push(spec.working_dir.clone());
    }
    // `--` ends docker's own options: an image such as `--privileged` is a
    // (bad) image name, never a flag.
    args.push("--".to_string());
    args.push(spec.image.clone());
    args.extend(spec.args.iter().cloned());
    args
}

/// Whether a process in group `pgid` holds the TCP listener on `port`;
/// `None` when that cannot be told (no matching socket visible).
#[cfg(target_os = "linux")]
fn listener_in_group(port: u16, pgid: u32) -> Option<bool> {
    let mut inodes = HashSet::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(data) = std::fs::read_to_string(table) else {
            continue;
        };
        for line in data.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            // `0A` is LISTEN; the local address is `ADDR:PORT` in hex.
            if fields.len() < 10 || fields[3] != "0A" {
                continue;
            }
            let local = fields[1].rsplit(':').next();
            if local.and_then(|p| u16::from_str_radix(p, 16).ok()) == Some(port) {
                inodes.insert(format!("socket:[{}]", fields[9]));
            }
        }
    }
    if inodes.is_empty() {
        return None;
    }
    for entry in std::fs::read_dir("/proc").ok()?.flatten() {
        let name = entry.file_name();
        let Some(pid) = name
            .to_str()
            .filter(|n| n.bytes().all(|b| b.is_ascii_digit()))
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        // `pid (comm) state ppid pgrp ...`; comm may hold spaces and parens.
        let after_comm = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
        let pgrp = after_comm.split_whitespace().nth(2);
        if pgrp.and_then(|g| g.parse::<u32>().ok()) != Some(pgid) {
            continue;
        }
        let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else {
            continue;
        };
        for fd in fds.flatten() {
            if let Ok(target) = std::fs::read_link(fd.path()) {
                if target.to_str().is_some_and(|t| inodes.contains(t)) {
                    return Some(true);
                }
            }
        }
    }
    Some(false)
}

fn free_port() -> Result<u16, String> {
    let listener = StdListener::bind("127.0.0.1:0").map_err(|e| format!("allocate port: {e}"))?;
    listener
        .local_addr()
        .map(|a| a.port())
        .map_err(|e| format!("allocate port: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn service() -> Value {
        json!({
            "name": "projects/p/locations/us-central1/services/web",
            "latestCreatedRevision": "projects/p/locations/us-central1/services/web/revisions/web-00002-abc",
            "template": {
                "containers": [{
                    "image": "gcr.io/p/web:latest",
                    "args": ["--verbose"],
                    "env": [{ "name": "MODE", "value": "dev" }, { "name": "SECRET", "valueSource": {} }],
                    "ports": [{ "containerPort": 3000 }],
                }]
            }
        })
    }

    #[test]
    fn launch_spec_from_service() {
        let spec = LaunchSpec::from_service(&service()).unwrap();
        assert_eq!(spec.service_id, "web");
        assert_eq!(spec.revision_id, "web-00002-abc");
        assert_eq!(spec.container_port, 3000);
        assert_eq!(spec.env.get("MODE").map(String::as_str), Some("dev"));
        assert_eq!(spec.env.get("SECRET").map(String::as_str), Some(""));
        assert!(spec.command.is_empty());
    }

    #[test]
    fn docker_args_publish_loopback_port() {
        let spec = LaunchSpec::from_service(&service()).unwrap();
        let args = docker_run_args(&spec, 45000, "n");
        assert!(args.contains(&"127.0.0.1:45000:3000".to_string()));
        assert!(args.contains(&"PORT=3000".to_string()));
        assert!(args.contains(&"MODE=dev".to_string()));
        assert_eq!(args[args.len() - 2], "gcr.io/p/web:latest");
        assert_eq!(
            args[args.len() - 3],
            "--",
            "image is never parsed as an option"
        );
        assert_eq!(args.last().unwrap(), "--verbose");
    }

    #[test]
    fn docker_names_differ_across_projects_and_locations() {
        let mut a = service();
        a["latestCreatedRevision"] =
            json!("projects/p/locations/us-central1/services/web/revisions/web-v1");
        let mut b = a.clone();
        b["name"] = json!("projects/other/locations/us-central1/services/web");
        let mut c = a.clone();
        c["name"] = json!("projects/p/locations/europe-west1/services/web");
        let names: Vec<String> = [a, b, c]
            .iter()
            .map(|s| docker_container_name(&LaunchSpec::from_service(s).unwrap(), "run1"))
            .collect();
        assert!(names[0].starts_with("devcloud-run-web-web-v1-"));
        // Same resource names, two devcloud runs (two workspaces, one daemon).
        let spec = LaunchSpec::from_service(&service()).unwrap();
        let (a, b) = (Manager::new(None), Manager::new(None));
        assert_ne!(
            docker_container_name(&spec, &a.run_id),
            docker_container_name(&spec, &b.run_id)
        );
        assert_ne!(names[0], names[1]);
        assert_ne!(names[0], names[2]);
        assert_ne!(names[1], names[2]);
    }

    #[tokio::test]
    async fn image_only_container_is_not_runnable_without_docker() {
        let m = Manager::new(None);
        let spec = LaunchSpec::from_service(&service()).unwrap();
        match m.ensure(&spec.service, || Some(spec.clone())).await {
            Err(StartError::NotRunnable(msg)) => assert!(msg.contains("has no command")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn log_lines_are_bounded_even_without_newlines() {
        let reader = tokio::io::AsyncReadExt::chain(
            tokio::io::AsyncReadExt::take(tokio::io::repeat(b'x'), 32 * 1024 * 1024),
            &b"\nshort\n"[..],
        );
        let mut longest = 0;
        let mut last = Vec::new();
        read_log_lines(reader, |l| {
            longest = longest.max(l.len());
            last = l.to_vec();
        })
        .await;
        assert_eq!(longest, MAX_LOG_LINE_BYTES);
        assert_eq!(last, b"short");
    }

    #[tokio::test]
    async fn no_instance_starts_after_shutdown_began() {
        let m = Manager::new(None);
        let spec = LaunchSpec::from_service(&service()).unwrap();
        m.stop_all().await;
        assert_eq!(
            m.ensure(&spec.service, || Some(spec.clone())).await,
            Err(StartError::Gone)
        );
        assert!(m.slots.lock().unwrap().map.is_empty());
    }

    #[test]
    fn platform_env_wins_over_container_env_in_docker_args() {
        let mut svc = service();
        svc["template"]["containers"][0]["env"] =
            json!([{ "name": "PORT", "value": "9000" }, { "name": "K_SERVICE", "value": "spoof" }]);
        let args = docker_run_args(&LaunchSpec::from_service(&svc).unwrap(), 45000, "n");
        let last = |key: &str| {
            args.iter()
                .rev()
                .find(|a| a.starts_with(&format!("{key}=")))
                .cloned()
                .unwrap()
        };
        assert_eq!(last("PORT"), "PORT=3000", "published container port wins");
        assert_eq!(last("K_SERVICE"), "K_SERVICE=web");
    }

    #[tokio::test]
    async fn container_is_removed_when_the_docker_cli_exits_early() {
        let dir = std::env::temp_dir().join(format!(
            "devcloud-cloudrun-earlyexit-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("calls.log");
        let fake = dir.join("docker");
        // `run` dies right away (daemon disconnect); every call is logged.
        std::fs::write(
            &fake,
            format!(
                "#!/bin/sh\necho \"$@\" >> {}\n[ \"$1\" = run ] && exit 1\nexit 0\n",
                log.display()
            ),
        )
        .unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let m = Manager::new(Some(fake.to_string_lossy().into_owned()));
        let spec = LaunchSpec::from_service(&service()).unwrap();
        let err = m
            .ensure(&spec.service, || Some(spec.clone()))
            .await
            .unwrap_err();
        assert!(matches!(err, StartError::Unhealthy(_)), "{err:?}");
        let calls = std::fs::read_to_string(&log).unwrap();
        let name = docker_container_name(&spec, &m.run_id);
        assert!(
            calls.lines().any(|l| l == format!("rm -f {name}")),
            "{calls}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn cancelled_terminate_still_kills_a_term_ignoring_group() {
        let m = Manager::new(None);
        let mut cmd = Command::new("sh");
        // Ignores SIGTERM, so terminate sits in its grace wait when cancelled.
        cmd.args(["-c", "trap '' TERM; sleep 60 & wait"])
            .kill_on_drop(true)
            .process_group(0);
        let child = cmd.spawn().unwrap();
        let pid = child.id();
        let inst = Instance {
            spec: LaunchSpec::from_service(&service()).unwrap(),
            port: 0,
            pid,
            child,
            docker_name: None,
            started_at: String::new(),
            requests: 0,
            guard: TeardownGuard {
                pgid: pid,
                container: None,
                removals: Removals::default(),
            },
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Cancel mid-teardown, as a runtime shutdown would.
        let _ = tokio::time::timeout(Duration::from_millis(300), m.terminate(inst)).await;
        let pgid = pid.unwrap() as libc::pid_t;
        let mut gone = false;
        for _ in 0..40 {
            // SAFETY: signal 0 only probes whether the group still exists.
            if unsafe { libc::killpg(pgid, 0) } != 0 {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(gone, "process group survived a cancelled terminate");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn readiness_requires_the_child_to_own_its_listener() {
        // Someone else listens on the port...
        let other = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = other.local_addr().unwrap().port();
        let mut cmd = Command::new("sleep");
        cmd.arg("30").kill_on_drop(true).process_group(0);
        let child = cmd.spawn().unwrap();
        let pgid = child.id().unwrap();
        // ...so the child's group does not own it.
        assert_eq!(listener_in_group(port, pgid), Some(false));
        // Our own process group does.
        let own = unsafe { libc::getpgrp() } as u32;
        assert_eq!(listener_in_group(port, own), Some(true));
        drop(other);
        drop(child);
    }

    #[test]
    fn reserved_ports_are_never_handed_out_twice() {
        let m = Manager::new(None);
        let ports: HashSet<u16> = (0..20).map(|_| m.reserve_port().unwrap()).collect();
        assert_eq!(ports.len(), 20);
        let p = *ports.iter().next().unwrap();
        m.release_port(p);
        assert!(!m.ports.lock().unwrap().contains(&p));
    }

    #[tokio::test]
    async fn a_reaped_instance_forgets_its_process_group() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "exit 0"]).process_group(0);
        let child = cmd.spawn().unwrap();
        let pid = child.id();
        let mut inst = Instance {
            spec: LaunchSpec::from_service(&service()).unwrap(),
            port: 0,
            pid,
            child,
            docker_name: None,
            started_at: String::new(),
            requests: 0,
            guard: TeardownGuard {
                pgid: pid,
                container: None,
                removals: Removals::default(),
            },
        };
        // `alive` itself is the reap site, as in `ensure` and `list`.
        let mut tries = 0;
        while inst.alive() {
            tries += 1;
            assert!(tries < 200, "child never exited");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Nothing left to signal later, by `terminate` or the drop guard.
        assert_eq!(inst.pid, None);
        assert_eq!(inst.guard.pgid, None);
    }

    #[tokio::test]
    async fn hung_docker_cli_does_not_block_terminate() {
        let dir = std::env::temp_dir().join(format!(
            "devcloud-cloudrun-fakedocker-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let fake = dir.join("docker");
        std::fs::write(&fake, "#!/bin/sh\nexec sleep 60\n").unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let m = Manager::new(Some(fake.to_string_lossy().into_owned()));
        let mut cmd = Command::new("sleep");
        cmd.arg("60").kill_on_drop(true).process_group(0);
        let child = cmd.spawn().unwrap();
        let inst = Instance {
            spec: LaunchSpec::from_service(&service()).unwrap(),
            port: 0,
            pid: child.id(),
            child,
            docker_name: Some("devcloud-run-test".into()),
            started_at: String::new(),
            requests: 0,
            guard: TeardownGuard {
                pgid: None,
                container: None,
                removals: Removals::default(),
            },
        };
        let started = std::time::Instant::now();
        m.terminate(inst).await;
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "terminate hung for {:?}",
            started.elapsed()
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn deleted_service_is_never_started() {
        let m = Manager::new(None);
        let spec = LaunchSpec::from_service(&service()).unwrap();
        assert_eq!(
            m.ensure(&spec.service, || None).await,
            Err(StartError::Gone)
        );
        assert!(
            m.slots.lock().unwrap().map.is_empty(),
            "no slot is left behind"
        );
    }

    /// delete_service: remove from state, then `stop`. Requests already
    /// queued on the slot — ahead of or behind `stop` — must not start code.
    #[tokio::test]
    async fn requests_racing_a_delete_never_restart_the_service() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let m = Manager::new(None);
        let spec = LaunchSpec::from_service(&service()).unwrap();
        let deleted = AtomicBool::new(false);
        let current = || (!deleted.load(Ordering::SeqCst)).then(|| spec.clone());
        // Hold the slot like an in-flight cold start.
        let slot = m.slot(&spec.service).unwrap();
        let held = slot.lock().await;
        let ahead = m.ensure(&spec.service, current);
        let behind = async {
            tokio::task::yield_now().await;
            m.ensure(&spec.service, current).await
        };
        let delete = async {
            tokio::task::yield_now().await;
            deleted.store(true, Ordering::SeqCst);
            m.stop(&spec.service).await;
        };
        let release = async {
            for _ in 0..3 {
                tokio::task::yield_now().await;
            }
            drop(held);
        };
        let (a, b, (), ()) = tokio::join!(ahead, behind, delete, release);
        assert_eq!(a, Err(StartError::Gone));
        assert_eq!(b, Err(StartError::Gone));
        assert!(
            m.slots.lock().unwrap().map.is_empty(),
            "no slot outlives the delete"
        );
    }
}
