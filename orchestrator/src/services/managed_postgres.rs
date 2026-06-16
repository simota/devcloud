//! Managed local PostgreSQL lifecycle for the Redshift service.
//!
//! Faithful port of legacy `internal/app/managed_postgres.rs` (plus the
//! `redshiftDataDir` helper from `internal/app/daemon.rs`). When the Redshift
//! backend runs in `managed` mode the orchestrator must boot a private
//! PostgreSQL instance on disk, drive its bootstrap (role + database), and
//! expose a DSN — exactly as the legacy daemon did.
//!
//! This is a synchronous, blocking module by design: it orchestrates the
//! external `initdb`, `postgres`, and `psql` binaries via `std::process`,
//! mirroring the legacy `os/exec` usage. The Redshift service task calls
//! [`start_managed_redshift_postgres`] from `tokio::task::spawn_blocking`.
//!
//! Secret hygiene: the configured password is NEVER emitted unredacted in any
//! error string. Every command-output surface runs through
//! [`redact_managed_postgres_secret`] before it reaches an error, matching the
//! legacy `redactManagedPostgresSecret` contract.
//!
//! Dead-code is allowed at module scope: the public entry points are consumed
//! by the Redshift service task, which is wired in a later orchestrator step.
//! Until that wiring lands nothing in the binary reaches this module, so the
//! transitive private helpers would otherwise trip `dead_code`.
#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::config::Config;

/// Bootstrap superuser created by `initdb` (legacy `managedPostgresBootstrapUser`).
const BOOTSTRAP_USER: &str = "devcloud";

/// Resolved configuration for a managed PostgreSQL instance.
/// Mirrors legacy `managedPostgresConfig`.
#[derive(Debug, Clone)]
struct ManagedPostgresConfig {
    data_dir: String,
    socket_dir: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

/// A running managed PostgreSQL process plus the data needed to address and
/// tear it down. Mirrors legacy `managedPostgres`.
///
/// Drop SIGINTs the process and removes the socket dir, mirroring legacy `Close`;
/// [`close`](Self::close) exposes the same teardown with an explicit result.
pub struct ManagedPostgres {
    child: Option<Child>,
    dsn: String,
    socket_dir: String,
}

impl ManagedPostgres {
    /// Returns the DSN clients use to reach this instance.
    /// legacy `(*managedPostgres).DSN`.
    pub fn dsn(&self) -> String {
        self.dsn.clone()
    }

    /// Stops the process (SIGINT, then SIGKILL after a 5s grace) and removes
    /// the socket directory. Idempotent. legacy `(*managedPostgres).Close`.
    pub fn close(&mut self) -> Result<(), String> {
        let mut close_err: Option<String> = None;
        if let Some(mut child) = self.child.take() {
            // legacy: Signal(os.Interrupt); on failure, Kill.
            if signal_interrupt(&child).is_err() {
                let _ = child.kill();
            }
            // legacy: wait up to 5s, then Kill and wait again.
            match wait_with_timeout(&mut child, Duration::from_secs(5)) {
                Some(Ok(status)) => {
                    // legacy records a wait error unless it mentions "interrupt".
                    // A clean SIGINT shutdown yields a non-zero status that we
                    // treat as expected, matching the legacy string-match heuristic.
                    if !status.success() && !status_is_interrupt(&status) {
                        close_err = Some(format!("managed postgres exited: {status}"));
                    }
                }
                Some(Err(err)) => close_err = Some(err),
                None => {
                    if let Err(err) = child.kill() {
                        close_err = Some(format!("kill managed postgres: {err}"));
                    } else {
                        match child.wait() {
                            Ok(_) => {}
                            Err(err) => close_err = Some(format!("wait managed postgres: {err}")),
                        }
                    }
                }
            }
        }
        if !self.socket_dir.is_empty() {
            if let Err(err) = fs::remove_dir_all(&self.socket_dir) {
                if err.kind() != std::io::ErrorKind::NotFound && close_err.is_none() {
                    close_err = Some(format!("remove managed postgres socket dir: {err}"));
                }
            }
        }
        match close_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl Drop for ManagedPostgres {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Boots a managed PostgreSQL for the Redshift service from the shared config.
/// Equivalent to legacy `startManagedRedshiftPostgresProcess`.
///
/// BLOCKING: call from `tokio::task::spawn_blocking`.
pub fn start_managed_redshift_postgres(cfg: &Config) -> Result<ManagedPostgres, String> {
    let port = managed_redshift_postgres_port(cfg.server.redshift_port)?;
    let pg_cfg = ManagedPostgresConfig {
        data_dir: path_join(&redshift_data_dir(cfg), "postgres"),
        socket_dir: String::new(),
        host: "127.0.0.1".to_string(),
        port,
        database: cfg.services.redshift.database.clone(),
        user: cfg.auth.redshift.user.clone(),
        password: cfg.auth.redshift.password.clone(),
    };
    start_managed_postgres(pg_cfg)
}

/// legacy `managedRedshiftPostgresPort`. Deterministic default is
/// `redshift_port + 10000`; if that exceeds the TCP ceiling, fall back to an
/// OS-assigned ephemeral port.
fn managed_redshift_postgres_port(redshift_port: i32) -> Result<u16, String> {
    let port = redshift_port + 10000;
    if (1..=65535).contains(&port) {
        return Ok(port as u16);
    }
    managed_redshift_postgres_ephemeral_port()
}

/// legacy `managedRedshiftPostgresEphemeralPort`: bind `127.0.0.1:0`, read back the
/// OS-assigned port, release the listener.
fn managed_redshift_postgres_ephemeral_port() -> Result<u16, String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|err| format!("allocate managed redshift postgres port: {err}"))?;
    let addr = listener
        .local_addr()
        .map_err(|err| format!("allocate managed redshift postgres port: {err}"))?;
    Ok(addr.port())
}

/// legacy `startManagedPostgres`: validate, normalize, discover binaries, init data
/// dir, start the server, wait for TCP readiness, ensure role + database.
fn start_managed_postgres(cfg: ManagedPostgresConfig) -> Result<ManagedPostgres, String> {
    validate_managed_postgres_config(&cfg)?;
    let cfg = normalize_managed_postgres_config(cfg);
    let initdb_path = discover_managed_postgres_command("initdb")?;
    let postgres_path = discover_managed_postgres_command("postgres")?;
    let psql_path = discover_managed_postgres_command("psql")?;

    ensure_managed_postgres_data_dir(&initdb_path, &cfg)?;

    fs::create_dir_all(&cfg.socket_dir)
        .map_err(|err| format!("create managed redshift postgres socket directory: {err}"))?;
    set_dir_mode_0700(&cfg.socket_dir)?;

    // legacy: exec postgres -D <data> -h <host> -p <port> -k <socketDir>,
    // stdout/stderr discarded.
    let child = Command::new(&postgres_path)
        .arg("-D")
        .arg(&cfg.data_dir)
        .arg("-h")
        .arg(&cfg.host)
        .arg("-p")
        .arg(cfg.port.to_string())
        .arg("-k")
        .arg(&cfg.socket_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| format!("start managed redshift postgres process: {err}"))?;

    let mut pg = ManagedPostgres {
        child: Some(child),
        dsn: managed_postgres_dsn(&cfg),
        socket_dir: cfg.socket_dir.clone(),
    };

    if let Err(err) = wait_for_managed_postgres_tcp(&cfg.host, cfg.port) {
        let _ = pg.close();
        return Err(format!("wait for managed redshift postgres startup: {err}"));
    }
    if let Err(err) = ensure_managed_postgres_user(&psql_path, &cfg) {
        let _ = pg.close();
        return Err(err);
    }
    if let Err(err) = ensure_managed_postgres_database(&psql_path, &cfg) {
        let _ = pg.close();
        return Err(err);
    }
    Ok(pg)
}

/// legacy `normalizeManagedPostgresConfig`: default the socket dir under TempDir.
fn normalize_managed_postgres_config(mut cfg: ManagedPostgresConfig) -> ManagedPostgresConfig {
    if cfg.socket_dir.trim().is_empty() {
        let name = format!("devcloud-redshift-postgres-{}", cfg.port);
        cfg.socket_dir = std::env::temp_dir()
            .join(name)
            .to_string_lossy()
            .into_owned();
    }
    cfg
}

/// legacy `validateManagedPostgresConfig`.
fn validate_managed_postgres_config(cfg: &ManagedPostgresConfig) -> Result<(), String> {
    if cfg.data_dir.trim().is_empty() {
        return Err("managed redshift postgres data directory is required".to_string());
    }
    if cfg.host.trim().is_empty() {
        return Err("managed redshift postgres host is required".to_string());
    }
    // legacy validates 1..=65535; our port is u16 so the upper bound holds, but a
    // zero port is still invalid.
    if cfg.port == 0 {
        return Err(format!(
            "managed redshift postgres port must be between 1 and 65535: {}",
            cfg.port
        ));
    }
    if cfg.database.trim().is_empty() {
        return Err("managed redshift postgres database name is required".to_string());
    }
    if cfg.user.trim().is_empty() {
        return Err("managed redshift postgres user is required".to_string());
    }
    Ok(())
}

/// legacy `discoverManagedPostgresCommand`: look the binary up on PATH; the error
/// points users at installing PostgreSQL or using `backend.mode=external`.
fn discover_managed_postgres_command(name: &str) -> Result<PathBuf, String> {
    match look_path(name) {
        Some(path) => Ok(path),
        None => Err(format!(
            "redshift managed postgres requires {name:?} on PATH; install PostgreSQL client/server binaries or use services.redshift.backend.mode=external with externalDsn: executable file not found in $PATH"
        )),
    }
}

/// Resolve `name` against the `PATH` environment variable. Mirrors the subset of
/// legacy `exec.LookPath` we rely on (search PATH entries for an executable file).
fn look_path(name: &str) -> Option<PathBuf> {
    // If name contains a separator, legacy treats it as a direct path.
    if name.contains(MAIN_SEPARATOR) {
        let p = PathBuf::from(name);
        return if is_executable_file(&p) {
            Some(p)
        } else {
            None
        };
    }
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(name);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
    }
    None
}

fn is_executable_file(path: &Path) -> bool {
    match fs::metadata(path) {
        Ok(meta) => meta.is_file() && (meta.permissions().mode() & 0o111 != 0),
        Err(_) => false,
    }
}

/// legacy `ensureManagedPostgresDataDir`: mkdir 0700; if PG_VERSION exists, the dir
/// is already initialized; otherwise run `initdb` with a temp password file.
fn ensure_managed_postgres_data_dir(
    initdb_path: &Path,
    cfg: &ManagedPostgresConfig,
) -> Result<(), String> {
    fs::create_dir_all(&cfg.data_dir)
        .map_err(|err| format!("create managed redshift postgres data directory: {err}"))?;
    set_dir_mode_0700(&cfg.data_dir)?;

    let version_marker = Path::new(&cfg.data_dir).join("PG_VERSION");
    match fs::metadata(&version_marker) {
        Ok(_) => return Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(format!(
                "inspect managed redshift postgres data directory: {err}"
            ))
        }
    }

    let pw_file = write_managed_postgres_password_file(cfg)?;
    let result = (|| {
        let output = Command::new(initdb_path)
            .arg("-D")
            .arg(&cfg.data_dir)
            .arg("-U")
            .arg(BOOTSTRAP_USER)
            .arg("--auth-host=scram-sha-256")
            .arg("--auth-local=trust")
            .arg("--pwfile")
            .arg(&pw_file)
            .output()
            .map_err(|err| {
                format!("initialize managed redshift postgres data directory with initdb: {err}")
            })?;
        if !output.status.success() {
            let combined = combined_output(&output);
            return Err(format!(
                "initialize managed redshift postgres data directory with initdb: exit status {}: {}",
                exit_code_display(&output.status),
                redact_managed_postgres_secret(&combined, &cfg.password)
            ));
        }
        Ok(())
    })();
    // legacy: defer os.Remove(pwFile).
    let _ = fs::remove_file(&pw_file);
    result
}

/// legacy `writeManagedPostgresPasswordFile`: a 0600 temp file holding
/// `<password>\n`, created in the parent of the data dir. Caller removes it.
///
/// The password is written ONLY to this 0600 file and never to any error.
fn write_managed_postgres_password_file(cfg: &ManagedPostgresConfig) -> Result<PathBuf, String> {
    let parent = Path::new(&cfg.data_dir)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    // Unique-ish temp name in the parent dir (mirrors os.CreateTemp pattern
    // ".devcloud-postgres-pw-*"). std has no CreateTemp; build a name from pid
    // + a monotonic counter and create exclusively to avoid collisions.
    let path = create_temp_file_0600(&parent, ".devcloud-postgres-pw-")?;
    let mut file = match fs::OpenOptions::new().write(true).open(&path) {
        Ok(f) => f,
        Err(err) => {
            let _ = fs::remove_file(&path);
            return Err(format!(
                "write managed redshift postgres password file: {err}"
            ));
        }
    };
    if let Err(err) = file.write_all(format!("{}\n", cfg.password).as_bytes()) {
        let _ = fs::remove_file(&path);
        return Err(format!(
            "write managed redshift postgres password file: {err}"
        ));
    }
    if let Err(err) = file.sync_all() {
        let _ = fs::remove_file(&path);
        return Err(format!(
            "close managed redshift postgres password file: {err}"
        ));
    }
    Ok(path)
}

/// Create a new 0600 file with a unique name `<prefix><n>` in `dir`, returning
/// its path. Fails if a unique name cannot be found, mirroring os.CreateTemp's
/// guarantee of an exclusively-created file.
fn create_temp_file_0600(dir: &Path, prefix: &str) -> Result<PathBuf, String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    for _ in 0..10_000 {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let candidate = dir.join(format!("{prefix}{pid}-{nanos}-{n}"));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(format!(
                    "create managed redshift postgres password file: {err}"
                ))
            }
        }
    }
    Err("create managed redshift postgres password file: exhausted unique names".to_string())
}

/// legacy `ensureManagedPostgresUser`: create-or-alter the login role via a plpgsql
/// DO block, with the user name and password as SQL string literals.
fn ensure_managed_postgres_user(
    psql_path: &Path,
    cfg: &ManagedPostgresConfig,
) -> Result<(), String> {
    let user_lit = postgres_string_literal(&cfg.user);
    let pass_lit = postgres_string_literal(&cfg.password);
    // Byte-identical to the legacy fmt.Sprintf template (same %%I/%%L escaping).
    let script = format!(
        "\ndo $devcloud$\nbegin\n\tif exists (select 1 from pg_roles where rolname = {user}) then\n\t\texecute format('alter role %I with login password %L', {user}, {pass});\n\telse\n\t\texecute format('create role %I with login password %L', {user}, {pass});\n\tend if;\nend\n$devcloud$;\n",
        user = user_lit,
        pass = pass_lit,
    );
    run_managed_postgres_psql_script(psql_path, cfg, "postgres", &script)
        .map_err(|err| format!("ensure managed redshift postgres user: {err}"))
}

/// legacy `ensureManagedPostgresDatabase`: exists-check, then `create database`.
fn ensure_managed_postgres_database(
    psql_path: &Path,
    cfg: &ManagedPostgresConfig,
) -> Result<(), String> {
    let exists_sql = format!(
        "select 1 from pg_database where datname = {}",
        postgres_string_literal(&cfg.database)
    );
    let output = Command::new(psql_path)
        .arg("-h")
        .arg(&cfg.socket_dir)
        .arg("-p")
        .arg(cfg.port.to_string())
        .arg("-U")
        .arg(BOOTSTRAP_USER)
        .arg("-d")
        .arg("postgres")
        .arg("-tAc")
        .arg(&exists_sql)
        .output()
        .map_err(|err| format!("inspect managed redshift postgres database: {err}"))?;
    if !output.status.success() {
        let combined = combined_output(&output);
        return Err(format!(
            "inspect managed redshift postgres database: exit status {}: {}",
            exit_code_display(&output.status),
            redact_managed_postgres_secret(&combined, &cfg.password)
        ));
    }
    if String::from_utf8_lossy(&output.stdout).trim() == "1" {
        return Ok(());
    }

    let create_sql = format!(
        "create database {} owner {};",
        postgres_identifier(&cfg.database),
        postgres_identifier(&cfg.user)
    );
    run_managed_postgres_psql_script(psql_path, cfg, "postgres", &create_sql)
        .map_err(|err| format!("create managed redshift postgres database: {err}"))
}

/// legacy `runManagedPostgresPSQLScript`: feed `script` to psql over stdin with
/// ON_ERROR_STOP=1, connecting over the unix socket dir.
fn run_managed_postgres_psql_script(
    psql_path: &Path,
    cfg: &ManagedPostgresConfig,
    database: &str,
    script: &str,
) -> Result<(), String> {
    let mut child = Command::new(psql_path)
        .arg("-h")
        .arg(&cfg.socket_dir)
        .arg("-p")
        .arg(cfg.port.to_string())
        .arg("-U")
        .arg(BOOTSTRAP_USER)
        .arg("-d")
        .arg(database)
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-f")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("run psql script: {err}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        if let Err(err) = stdin.write_all(script.as_bytes()) {
            return Err(format!("run psql script: {err}"));
        }
        // Drop stdin to signal EOF before waiting.
        drop(stdin);
    }
    let output = child
        .wait_with_output()
        .map_err(|err| format!("run psql script: {err}"))?;
    if !output.status.success() {
        let combined = combined_output(&output);
        return Err(format!(
            "run psql script: exit status {}: {}",
            exit_code_display(&output.status),
            redact_managed_postgres_secret(&combined, &cfg.password)
        ));
    }
    Ok(())
}

/// legacy `postgresStringLiteral`: single-quote and double internal quotes.
fn postgres_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// legacy `postgresIdentifier`: double-quote and double internal double-quotes.
fn postgres_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// legacy `managedPostgresDSN`: byte-identical to
/// `postgres://<user>:<pass>@<host>:<port>/<database>?sslmode=disable`, with
/// `net/url` userinfo encoding applied to user and password and `net/url` path
/// encoding applied to the database segment.
fn managed_postgres_dsn(cfg: &ManagedPostgresConfig) -> String {
    let user = encode_userinfo(&cfg.user);
    let pass = encode_userinfo(&cfg.password);
    let host = join_host_port(&cfg.host, cfg.port);
    let database = encode_path_segment(&cfg.database);
    format!(
        "postgres://{user}:{pass}@{host}/{database}?sslmode=disable",
        user = user,
        pass = pass,
        host = host,
        database = database,
    )
}

/// legacy `net.JoinHostPort`: bracket the host if it contains a colon (IPv6).
fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Replicate legacy `net/url` userinfo escaping (`shouldEscape(c, encodeUserPassword)`).
///
/// Unescaped: ASCII letters, digits, the unreserved marks `-_.~`, and the
/// sub-delims permitted in userinfo by legacy: `$ & + , ; =`. Everything else —
/// including space, `! * ' ( ) @ : / ? # [ ] %` and all non-ASCII bytes — is
/// percent-encoded byte-wise with uppercase hex. Verified against legacy
/// `url.UserPassword(...).String()` output.
fn encode_userinfo(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        if userinfo_unescaped(b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_upper(b >> 4));
            out.push(hex_upper(b & 0x0f));
        }
    }
    out
}

fn userinfo_unescaped(b: u8) -> bool {
    matches!(b,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9'
        | b'-' | b'_' | b'.' | b'~'
        | b'$' | b'&' | b'+' | b',' | b';' | b'='
    )
}

/// Replicate legacy `net/url` path escaping (`shouldEscape(c, encodePath)`) as
/// applied to a `url.URL.Path` rendered by `String()`. Used for the database
/// segment so the DSN stays byte-identical to legacy `url.URL{Path:"/"+db}`.
///
/// Path keeps everything userinfo keeps, plus the additional sub-delims/gen-delims
/// legacy permits in a path: `: @ /`. `? # %` and non-ASCII bytes are still escaped.
/// Verified against legacy `url.URL.String()` output.
fn encode_path_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        if path_unescaped(b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_upper(b >> 4));
            out.push(hex_upper(b & 0x0f));
        }
    }
    out
}

fn path_unescaped(b: u8) -> bool {
    userinfo_unescaped(b) || matches!(b, b':' | b'@' | b'/')
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// legacy `redactManagedPostgresSecret`: replace every occurrence of the password
/// with "redacted"; empty secret is a no-op.
fn redact_managed_postgres_secret(value: &str, secret: &str) -> String {
    if secret.is_empty() {
        return value.to_string();
    }
    value.replace(secret, "redacted")
}

/// legacy `waitForManagedPostgresTCP`: 15s deadline, 300ms dial timeout, 100ms poll.
fn wait_for_managed_postgres_tcp(host: &str, port: u16) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let addr = join_host_port(host, port);
    let resolved: std::net::SocketAddr = addr
        .parse()
        .map_err(|err| format!("resolve managed postgres addr {addr}: {err}"))?;
    let mut last_err = String::from("timed out waiting for managed postgres");
    while Instant::now() < deadline {
        match TcpStream::connect_timeout(&resolved, Duration::from_millis(300)) {
            Ok(_conn) => return Ok(()),
            Err(err) => last_err = err.to_string(),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(last_err)
}

// ---------------------------------------------------------------------------
// Process control helpers (unix)
// ---------------------------------------------------------------------------

/// Send SIGINT to a child process (legacy `cmd.Process.Signal(os.Interrupt)`).
fn signal_interrupt(child: &Child) -> Result<(), String> {
    let pid = child.id() as i32;
    // std has no SIGINT API; call the libc `kill` syscall directly (linked via
    // the C ABI, no crate dependency). 2 == SIGINT on every unix platform
    // devcloud targets (Linux/macOS). SAFETY: a plain syscall with a valid pid
    // and a fixed signal number; no memory is shared.
    let rc = unsafe { kill(pid, 2) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

extern "C" {
    /// libc `kill(2)`.
    fn kill(pid: i32, sig: i32) -> i32;
}

/// Wait for the child up to `timeout`, polling exit status. Returns `None` on
/// timeout, mirroring legacy select-with-After teardown branch.
fn wait_with_timeout(
    child: &mut Child,
    timeout: Duration,
) -> Option<Result<std::process::ExitStatus, String>> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(Ok(status)),
            Ok(None) => {
                if Instant::now() >= deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Some(Err(format!("wait managed postgres: {err}"))),
        }
    }
}

/// Heuristic mirror of legacy `strings.Contains(err.Error(), "interrupt")`: a
/// process terminated by SIGINT is the expected shutdown path.
fn status_is_interrupt(status: &std::process::ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt;
    status.signal() == Some(2)
}

// ---------------------------------------------------------------------------
// Command-output + path helpers
// ---------------------------------------------------------------------------

/// Mirror legacy `exec.Cmd.CombinedOutput()` ordering: stdout then stderr.
fn combined_output(output: &std::process::Output) -> String {
    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    combined
}

/// Render an exit code like legacy `*exec.ExitError` "exit status N" suffix.
fn exit_code_display(status: &std::process::ExitStatus) -> String {
    match status.code() {
        Some(code) => code.to_string(),
        None => {
            use std::os::unix::process::ExitStatusExt;
            match status.signal() {
                Some(sig) => format!("signal {sig}"),
                None => "unknown".to_string(),
            }
        }
    }
}

/// chmod a directory to 0700 (legacy relies on MkdirAll's perm; we set it post-hoc
/// because create_dir_all does not apply a custom mode portably).
fn set_dir_mode_0700(path: &str) -> Result<(), String> {
    let mut perms = fs::metadata(path)
        .map_err(|err| format!("inspect managed redshift postgres directory: {err}"))?
        .permissions();
    perms.set_mode(0o700);
    fs::set_permissions(path, perms)
        .map_err(|err| format!("secure managed redshift postgres directory: {err}"))
}

/// legacy `redshiftDataDir` (internal/app/daemon.rs): resolve the on-disk root for
/// the Redshift backend. `.devcloud`-prefixed dirs are honored verbatim;
/// otherwise the dir is joined under the storage path.
fn redshift_data_dir(cfg: &Config) -> String {
    let data_dir = &cfg.services.redshift.data_dir;
    if data_dir.is_empty() {
        return path_join(&cfg.storage.path, "redshift");
    }
    let clean = clean_path(data_dir);
    let prefix = format!(".devcloud{MAIN_SEPARATOR}");
    if clean == ".devcloud" || clean.starts_with(&prefix) {
        return clean;
    }
    path_join(&cfg.storage.path, &clean)
}

/// legacy `filepath.Join` for the simple (no-`..`) inputs devcloud uses.
fn path_join(a: &str, b: &str) -> String {
    let mut p = PathBuf::from(a);
    p.push(b);
    p.to_string_lossy().into_owned()
}

/// Approximation of legacy `filepath.Clean` sufficient for devcloud config inputs.
fn clean_path(p: &str) -> String {
    PathBuf::from(p).to_string_lossy().into_owned()
}
