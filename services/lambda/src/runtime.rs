//! Local execution of one Lambda invocation.
//!
//! Each invocation spawns a fresh interpreter (`python3` / `node`) with a small
//! embedded bootstrap that imports the configured handler, feeds it the event
//! on stdin, and writes the handler outcome as JSON to a per-invocation result
//! file. stdout/stderr become the invocation log. The child runs with a cleared
//! environment (only `PATH`, Lambda's reserved variables, and the function's own
//! `Environment.Variables`), so host credentials never leak into handler code;
//! the only credentials a handler sees are the ones configured for functions
//! ([`FunctionCredentials`]).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Per-stream log bytes kept while an invocation runs (the API tail is 4 KiB).
const LOG_CAPTURE_BYTES: usize = 64 * 1024;

/// How long log readers may keep draining after the process group is killed.
const LOG_DRAIN_GRACE: Duration = Duration::from_millis(500);

const PYTHON_BOOTSTRAP: &str = r#"
import importlib, json, os, sys, time, traceback

def _devcloud_main():
    result_path = os.environ.pop("_DEVCLOUD_RESULT_PATH")
    deadline_ms = int(os.environ.pop("_DEVCLOUD_DEADLINE_MS"))
    request_id = os.environ.pop("_DEVCLOUD_REQUEST_ID")
    function_arn = os.environ.pop("_DEVCLOUD_FUNCTION_ARN")
    expected_version = os.environ.pop("_DEVCLOUD_RUNTIME_VERSION", "")
    real_stdout = sys.stdout
    sys.stderr = sys.stdout
    actual_version = "%d.%d" % sys.version_info[:2]
    if expected_version and expected_version != actual_version:
        print("[WARNING] devcloud: runtime %s is running on Python %s (%s)" % (os.environ["AWS_EXECUTION_ENV"][len("AWS_Lambda_"):], sys.version.split()[0], sys.executable))

    def write(obj):
        with open(result_path, "w") as f:
            json.dump(obj, f)

    def failure(message, error_type, stack):
        write({"error": {"errorMessage": message, "errorType": error_type, "requestId": request_id, "stackTrace": stack}})

    class LambdaContext:
        function_name = os.environ["AWS_LAMBDA_FUNCTION_NAME"]
        function_version = os.environ["AWS_LAMBDA_FUNCTION_VERSION"]
        invoked_function_arn = function_arn
        memory_limit_in_mb = os.environ["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"]
        aws_request_id = request_id
        log_group_name = os.environ["AWS_LAMBDA_LOG_GROUP_NAME"]
        log_stream_name = os.environ["AWS_LAMBDA_LOG_STREAM_NAME"]
        identity = None
        client_context = None

        def get_remaining_time_in_millis(self):
            return max(0, deadline_ms - int(time.time() * 1000))

    root = os.environ["LAMBDA_TASK_ROOT"]
    sys.path.insert(0, root)
    handler_spec = os.environ["_HANDLER"]
    module_name, _, func_name = handler_spec.rpartition(".")
    if not module_name:
        failure("Bad handler '%s': not enough values to unpack (expected 2, got 1)" % handler_spec, "Runtime.MalformedHandlerName", [])
        return
    try:
        module = importlib.import_module(module_name.replace("/", "."))
    except Exception as e:
        failure("Unable to import module '%s': %s" % (module_name, e), "Runtime.ImportModuleError", [])
        return
    handler = getattr(module, func_name, None)
    if handler is None:
        failure("Handler '%s' missing on module '%s'" % (func_name, module_name), "Runtime.HandlerNotFound", [])
        return
    event = json.loads(sys.stdin.read() or "{}")
    try:
        result = handler(event, LambdaContext())
    except Exception as e:
        frames = traceback.extract_tb(e.__traceback__)[1:]
        print("[ERROR] %s: %s" % (type(e).__name__, e))
        failure(str(e), type(e).__name__, traceback.format_list(frames))
        return
    try:
        body = json.dumps(result)
    except Exception as e:
        failure("Unable to marshal response: %s" % e, "Runtime.MarshalError", [])
        return
    real_stdout.flush()
    write({"result": body})

try:
    _devcloud_main()
except BaseException:
    traceback.print_exc()
finally:
    # The result is on disk: end now. A normal interpreter exit would wait
    # for non-daemon threads the handler left behind and turn a finished
    # invocation into a timeout.
    sys.stdout.flush()
    sys.__stdout__.flush()
    sys.__stderr__.flush()
    os._exit(0)
"#;

const NODE_BOOTSTRAP: &str = r#"
const fs = require('fs');
const path = require('path');
const url = require('url');

// Node's ESM loader ignores NODE_PATH, which is how layer dependencies under
// /opt are found. As in the Lambda Node runtime, ES module handlers resolve
// them too: a resolve hook retries a bare specifier the default resolution
// could not find against each NODE_PATH directory.
const ESM_LAYER_HOOK = `
import { createRequire } from 'node:module';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
let dirs = [];
export function initialize(data) { dirs = data.dirs; }
export async function resolve(specifier, context, next) {
  try {
    return await next(specifier, context);
  } catch (e) {
    const bare = !/^([./]|[a-zA-Z][a-zA-Z0-9+.-]*:)/.test(specifier);
    if (!bare || !e || e.code !== 'ERR_MODULE_NOT_FOUND') throw e;
    for (const dir of dirs) {
      // ESM resolution looks in <ancestor>/node_modules of the parent.
      if (path.basename(dir) === 'node_modules') {
        try {
          return await next(specifier, { ...context, parentURL: pathToFileURL(path.join(path.dirname(dir), 'index.mjs')).href });
        } catch {}
      }
      try {
        // CommonJS resolution honours NODE_PATH as is (any directory name).
        const file = createRequire(path.join(dir, 'index.js')).resolve(specifier);
        return { url: pathToFileURL(file).href, shortCircuit: true };
      } catch {}
    }
    throw e;
  }
}
`;
// `.mjs`, or `.js` under a package.json with "type": "module" (the nearest
// package.json decides, as in Node).
const isEsm = (file) => {
  if (file.endsWith('.mjs')) return true;
  if (!file.endsWith('.js')) return false;
  for (let dir = path.dirname(file); ; dir = path.dirname(dir)) {
    const pkg = path.join(dir, 'package.json');
    if (fs.existsSync(pkg)) {
      try {
        return JSON.parse(fs.readFileSync(pkg, 'utf8')).type === 'module';
      } catch {
        return false;
      }
    }
    if (path.dirname(dir) === dir) return false;
  }
};

let esmHookRegistered = false;
const importEsm = (file) => {
  const dirs = (process.env.NODE_PATH || '').split(path.delimiter).filter(Boolean);
  const { register } = require('module');
  if (!esmHookRegistered && dirs.length && typeof register === 'function') {
    register(`data:text/javascript,${encodeURIComponent(ESM_LAYER_HOOK)}`, { data: { dirs } });
    esmHookRegistered = true;
  }
  return import(url.pathToFileURL(file).href);
};

(async () => {
  const resultPath = process.env._DEVCLOUD_RESULT_PATH;
  const deadlineMs = Number(process.env._DEVCLOUD_DEADLINE_MS);
  const requestId = process.env._DEVCLOUD_REQUEST_ID;
  const functionArn = process.env._DEVCLOUD_FUNCTION_ARN;
  const expectedVersion = process.env._DEVCLOUD_RUNTIME_VERSION || '';
  for (const k of ['_DEVCLOUD_RESULT_PATH', '_DEVCLOUD_DEADLINE_MS', '_DEVCLOUD_REQUEST_ID', '_DEVCLOUD_FUNCTION_ARN', '_DEVCLOUD_RUNTIME_VERSION']) delete process.env[k];
  process.stderr.write = process.stdout.write.bind(process.stdout);
  if (expectedVersion && expectedVersion !== process.versions.node.split('.')[0]) {
    console.log(`[WARNING] devcloud: runtime ${process.env.AWS_EXECUTION_ENV.slice('AWS_Lambda_'.length)} is running on Node.js ${process.versions.node} (${process.execPath})`);
  }
  const write = (obj) => fs.writeFileSync(resultPath, JSON.stringify(obj));
  const errorBody = (e, type) => ({
    errorType: type || (e && e.name) || 'Error',
    errorMessage: e && e.message !== undefined ? String(e.message) : String(e),
    trace: e && e.stack ? String(e.stack).split('\n') : [],
  });
  // Pipe writes are asynchronous: exit only once everything queued on stdout
  // (stderr is routed there too) has been handed to the OS, or the log tail
  // would be cut off.
  const done = (obj) => { write(obj); process.stdout.write('', () => process.exit(0)); };

  const root = process.env.LAMBDA_TASK_ROOT;
  // As in the AWS Node runtime: `dir/module.prop.path` — the module is the
  // basename up to its *first* dot, the rest is a property path into the
  // exports (`index.api.handler` → module `index`, `exports.api.handler`).
  const spec = process.env._HANDLER;
  const slash = spec.lastIndexOf('/');
  const dot = spec.indexOf('.', slash + 1);
  if (dot <= slash + 1 || dot === spec.length - 1) {
    return done({ error: { errorType: 'Runtime.MalformedHandlerName', errorMessage: `Bad handler ${spec}`, trace: [] } });
  }
  const modName = spec.slice(0, dot);
  const fnName = spec.slice(dot + 1);
  const fnPath = fnName.split('.');
  let mod;
  try {
    // Like the AWS Node runtime: `<name>.js|.mjs|.cjs` first, then Node's own
    // resolution (directory index.js, package.json "main"/"exports", ...).
    const base = path.resolve(root, modName);
    const file = ['.js', '.mjs', '.cjs'].map((ext) => base + ext).find((f) => fs.existsSync(f))
      || require.resolve(base);
    if (isEsm(file)) {
      // Through import(), never require(): Node 22's require() can load ESM
      // too, but without the layer resolve hook.
      mod = await importEsm(file);
    } else {
      try {
        mod = require(file);
      } catch (e) {
        // ESM that require() cannot load synchronously (Node 22 can require
        // some ESM, but not modules with top-level await).
        if (e && (e.code === 'ERR_REQUIRE_ESM' || e.code === 'ERR_REQUIRE_ASYNC_MODULE')) {
          mod = await importEsm(file);
        } else throw e;
      }
    }
  } catch (e) {
    return done({ error: errorBody(e, 'Runtime.ImportModuleError') });
  }
  const walk = (root) => fnPath.reduce((obj, key) => (obj == null ? undefined : obj[key]), root);
  const fn = walk(mod) !== undefined ? walk(mod) : walk(mod.default);
  if (typeof fn !== 'function') {
    return done({ error: { errorType: 'Runtime.HandlerNotFound', errorMessage: `${modName}.${fnName} is undefined or not exported`, trace: [] } });
  }
  const event = JSON.parse(fs.readFileSync(0, 'utf8') || '{}');
  const context = {
    functionName: process.env.AWS_LAMBDA_FUNCTION_NAME,
    functionVersion: process.env.AWS_LAMBDA_FUNCTION_VERSION,
    invokedFunctionArn: functionArn,
    memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE,
    awsRequestId: requestId,
    logGroupName: process.env.AWS_LAMBDA_LOG_GROUP_NAME,
    logStreamName: process.env.AWS_LAMBDA_LOG_STREAM_NAME,
    callbackWaitsForEmptyEventLoop: true,
    getRemainingTimeInMillis: () => Math.max(0, deadlineMs - Date.now()),
  };
  let result;
  let failure;
  // Separate flag: `Promise.reject()` / `throw undefined` fail with an
  // undefined reason, which must still be reported as a function error.
  let failed = false;
  let settledByCallback = false;
  try {
    // Same completion rules as the AWS Node runtime: a returned promise
    // settles the invocation; otherwise wait for the callback, however the
    // handler declares it (default value, rest args, `arguments`). If the
    // event loop drains without a callback, the result is null. `fn.length`
    // says nothing about whether a callback is coming.
    result = await new Promise((resolve, reject) => {
      const callback = (err, res) => {
        settledByCallback = true;
        return err ? reject(err) : resolve(res);
      };
      // No callback by the time the loop drains → null. Deferred one tick so
      // a handler that itself calls back from a `beforeExit` listener (in
      // the same emission) still wins.
      process.once('beforeExit', () => setImmediate(() => resolve(undefined)));
      const ret = fn(event, context, callback);
      if (ret && typeof ret.then === 'function') ret.then(resolve, reject);
    });
  } catch (e) {
    failed = true;
    failure = e;
  }
  if (failed) {
    console.error(`ERROR\tInvoke Error\t${failure && failure.stack ? failure.stack : failure}`);
    return done({ error: errorBody(failure) });
  }
  let body;
  try {
    body = JSON.stringify(result === undefined ? null : result);
  } catch (e) {
    return done({ error: errorBody(e, 'Runtime.MarshalError') });
  }
  // A successful callback response honours callbackWaitsForEmptyEventLoop
  // (default true): record the result now and let the process end on its own
  // once timers and I/O the handler left running have finished — devcloud
  // waits for the exit (bounded by the function timeout). No second
  // `beforeExit` is awaited: it may already have fired, e.g. when the handler
  // itself called back from a `beforeExit` listener. Errors, promise results,
  // and handlers that set the flag to false end immediately.
  if (settledByCallback && context.callbackWaitsForEmptyEventLoop) {
    write({ result: body });
    return;
  }
  done({ result: body });
})();
"#;

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
    let (rest, family) = if let Some(rest) = runtime.strip_prefix("python") {
        (rest, Family::Python)
    } else if let Some(rest) = runtime.strip_prefix("nodejs") {
        (rest, Family::Node)
    } else {
        return None;
    };
    let v = match family {
        Family::Python => rest,
        Family::Node => rest.strip_suffix(".x").unwrap_or(rest),
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
}

const DEFAULT_PYTHON: &str = "python3";

impl Default for Interpreters {
    fn default() -> Self {
        Interpreters {
            python: DEFAULT_PYTHON.to_string(),
            node: "node".to_string(),
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
    pub work_dir: PathBuf,
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
    pub duration: Duration,
}

/// A failure to run the function at all (as opposed to a function error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// The runtime identifier has no local interpreter mapping.
    Unsupported(String),
    /// The interpreter binary could not be started.
    Spawn(String),
}

#[derive(Deserialize)]
struct BootstrapResult {
    result: Option<String>,
    error: Option<serde_json::Value>,
}

pub async fn run(inv: &Invocation, interpreters: &Interpreters) -> Result<Completed, RuntimeError> {
    let family =
        family(&inv.runtime).ok_or_else(|| RuntimeError::Unsupported(inv.runtime.clone()))?;
    std::fs::create_dir_all(&inv.work_dir).map_err(|e| RuntimeError::Spawn(e.to_string()))?;
    let result_path = inv.work_dir.join("result.json");
    let started = Instant::now();
    let deadline_ms = now_millis() + inv.timeout_seconds.max(1) as u128 * 1000;

    let bin = match family {
        Family::Python => &interpreters.python,
        Family::Node => &interpreters.node,
    };
    // Resolve the interpreter against devcloud's own PATH: the function may set
    // its own PATH, which applies to the handler, not to finding python/node.
    let program = interpreters.resolve(family, &inv.runtime).ok_or_else(|| {
        RuntimeError::Spawn(format!(
            "start {bin} for runtime {}: interpreter not found on devcloud's PATH",
            inv.runtime
        ))
    })?;
    let mut cmd = match family {
        Family::Python => {
            let mut c = tokio::process::Command::new(&program);
            // Unbuffered: output written before a timeout's SIGKILL must
            // still reach the log (a pipe would otherwise be block-buffered).
            c.arg("-u").arg("-c").arg(PYTHON_BOOTSTRAP);
            c
        }
        Family::Node => {
            let mut c = tokio::process::Command::new(&program);
            c.arg("-e").arg(NODE_BOOTSTRAP);
            c
        }
    };
    // Precedence: devcloud defaults < the function's Environment.Variables <
    // Lambda-reserved variables (which CreateFunction refuses to accept).
    cmd.env_clear()
        .envs(default_env(family, inv))
        .envs(&inv.environment)
        .envs(reserved_env(inv, &result_path, deadline_ms))
        .current_dir(&inv.code_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Own process group, so a timeout (or exit) can take down anything the
    // handler spawned along with the interpreter.
    #[cfg(unix)]
    cmd.process_group(0);

    let mut child = cmd.spawn().map_err(|e| {
        RuntimeError::Spawn(format!("start {bin} for runtime {}: {e}", inv.runtime))
    })?;

    // From here on, every exit path — including this future being dropped
    // when devcloud shuts down — tears down the process group and the I/O
    // tasks (see `Cleanup::drop`).
    let mut cleanup = Cleanup {
        pgid: child.id(),
        tasks: Vec::new(),
    };
    let mut stdin = child.stdin.take().expect("piped stdin");
    let payload = inv.payload.clone();
    let stdin_task = tokio::spawn(async move {
        let _ = stdin.write_all(&payload).await;
        let _ = stdin.shutdown().await;
    });
    cleanup.tasks.push(stdin_task.abort_handle());
    let out_buf = Arc::new(Mutex::new(Vec::new()));
    let err_buf = Arc::new(Mutex::new(Vec::new()));
    let mut out_task = tokio::spawn(collect(
        child.stdout.take().expect("piped stdout"),
        Arc::clone(&out_buf),
    ));
    let mut err_task = tokio::spawn(collect(
        child.stderr.take().expect("piped stderr"),
        Arc::clone(&err_buf),
    ));
    cleanup.tasks.push(out_task.abort_handle());
    cleanup.tasks.push(err_task.abort_handle());

    let timeout = Duration::from_secs(inv.timeout_seconds.max(1) as u64);
    let waited = tokio::time::timeout(timeout, child.wait()).await;
    let timed_out = waited.is_err();
    // How the runtime ended matters: a handler can call back with success and
    // then crash (e.g. an uncaught exception in a timer while the event loop
    // drains). A non-zero exit or a signal outranks the stored result.
    let crashed = match &waited {
        Ok(Ok(status)) if !status.success() => Some(status.to_string()),
        Ok(Err(e)) => Some(e.to_string()),
        _ => None,
    };
    // The invocation is over either way: stop the interpreter and every
    // descendant still holding the log pipes open.
    cleanup.kill_group();
    let _ = child.kill().await;
    // A descendant that escaped the group can keep a pipe open forever; give
    // the readers a bounded grace period, keep whatever they collected, and
    // abort them (dropping a JoinHandle would only detach them).
    let deadline = tokio::time::Instant::now() + LOG_DRAIN_GRACE;
    let _ = tokio::time::timeout_at(deadline, &mut out_task).await;
    let _ = tokio::time::timeout_at(deadline, &mut err_task).await;
    drop(cleanup);
    let duration = started.elapsed();
    let mut log_body = std::mem::take(&mut *out_buf.lock().unwrap());
    log_body.extend(std::mem::take(&mut *err_buf.lock().unwrap()));

    let outcome = if timed_out {
        Outcome::FunctionError(error_doc(
            &format!(
                "{} {} Task timed out after {}.00 seconds",
                crate::time_fmt::now_rfc3339(),
                inv.request_id,
                inv.timeout_seconds.max(1)
            ),
            "Sandbox.Timedout",
        ))
    } else if let Some(status) = crashed {
        Outcome::FunctionError(error_doc(
            &format!(
                "RequestId: {} Error: Runtime exited with error: {status}",
                inv.request_id
            ),
            "Runtime.ExitError",
        ))
    } else {
        read_result(&result_path)
    };
    let _ = std::fs::remove_dir_all(&inv.work_dir);

    let log = frame_log(
        inv,
        &String::from_utf8_lossy(&log_body),
        duration,
        timed_out,
    );
    Ok(Completed {
        outcome,
        log,
        duration,
    })
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

/// Tears down an invocation's process group and I/O tasks when dropped.
struct Cleanup {
    pgid: Option<u32>,
    tasks: Vec<tokio::task::AbortHandle>,
}

impl Cleanup {
    /// Kills the group once; later calls (and the drop) are no-ops, so a
    /// recycled process-group id is never signalled.
    fn kill_group(&mut self) {
        if let Some(pgid) = self.pgid.take() {
            kill_process_group(pgid);
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.kill_group();
        for task in &self.tasks {
            task.abort();
        }
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

fn reserved_env(inv: &Invocation, result_path: &Path, deadline_ms: u128) -> Vec<(String, String)> {
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
            format!("devcloud/[$LATEST]{}", inv.request_id),
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
        (
            "_DEVCLOUD_RESULT_PATH".into(),
            result_path.to_string_lossy().into_owned(),
        ),
        ("_DEVCLOUD_DEADLINE_MS".into(), deadline_ms.to_string()),
        ("_DEVCLOUD_REQUEST_ID".into(), inv.request_id.clone()),
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

fn read_result(path: &Path) -> Outcome {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => {
            return Outcome::FunctionError(error_doc(
                "RequestId: runtime exited without providing a reason",
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

fn frame_log(inv: &Invocation, body: &str, duration: Duration, timed_out: bool) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    let billed = (ms.ceil() as u64).max(1);
    let mut log = format!("START RequestId: {} Version: $LATEST\n", inv.request_id);
    log.push_str(body);
    if !body.is_empty() && !body.ends_with('\n') {
        log.push('\n');
    }
    if timed_out {
        log.push_str(&format!(
            "{} {} Task timed out after {}.00 seconds\n",
            crate::time_fmt::now_rfc3339(),
            inv.request_id,
            inv.timeout_seconds.max(1)
        ));
    }
    log.push_str(&format!("END RequestId: {}\n", inv.request_id));
    log.push_str(&format!(
        "REPORT RequestId: {}\tDuration: {:.2} ms\tBilled Duration: {} ms\tMemory Size: {} MB\n",
        inv.request_id, ms, billed, inv.memory_size
    ));
    log
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
        let out = read_result(Path::new("/nonexistent/devcloud/result.json"));
        match out {
            Outcome::FunctionError(body) => {
                let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(v["errorType"], "Runtime.ExitError");
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
