//! Lambda control plane + invoke.
//!
//! Function configurations live in memory behind a `Mutex` and persist to
//! `state.json`; deployment packages are stored per version,
//! content-addressed under `functions/<name>/` (see `code_store`).
//! Every mutation is applied to a cloned state that is persisted first and only
//! then swapped in, so a failed write never leaves memory and disk diverged.
//! Handlers return a [`Reply`] so the HTTP layer stays thin and dispatch is
//! unit-testable without sockets.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::code_store::{CodeStore, Lease, StageError};
use crate::concurrency::{
    ConcurrencyPermit, ConcurrencyTracker, ProvisionedConcurrencyConfig,
    ACCOUNT_CONCURRENT_EXECUTIONS,
};
use crate::container::ImageSpec;
use crate::event_source::EventSourceMappingRecord;
use crate::function_url::{self, UrlConfig};
use crate::http::Request;
use crate::layers::{self, AttachedLayer, LayerRecord};
use crate::runtime::{self, FunctionCredentials, Interpreters, Invocation, Outcome, RuntimeError};
use crate::time_fmt::{now_lambda, now_rfc3339};

const MAX_SYNC_PAYLOAD_BYTES: usize = 6 * 1024 * 1024;
const MAX_ASYNC_PAYLOAD_BYTES: usize = 256 * 1024;
const MAX_ZIP_BYTES: usize = 50 * 1024 * 1024;
const LOG_TAIL_BYTES: usize = 4096;
const MAX_INVOCATION_RECORDS: usize = 100;
const DEFAULT_LIST_MAX_ITEMS: usize = 50;

/// Runtime identifiers devcloud accepts. Python/Node and `provided*` (the
/// package's own `bootstrap`) execute locally; the
/// remaining families are accepted for control-plane compatibility (IaC tools
/// deploy them) but cannot be invoked.
const KNOWN_RUNTIME_PREFIXES: &[&str] = &[
    "python3", "nodejs", "java", "dotnet", "ruby", "provided", "go1.x",
];

/// Variables Lambda itself owns; functions may not override them.
const RESERVED_ENV_KEYS: &[&str] = &[
    "_HANDLER",
    "_X_AMZN_TRACE_ID",
    "AWS_DEFAULT_REGION",
    "AWS_REGION",
    "AWS_EXECUTION_ENV",
    "AWS_LAMBDA_FUNCTION_NAME",
    "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
    "AWS_LAMBDA_FUNCTION_VERSION",
    "AWS_LAMBDA_INITIALIZATION_TYPE",
    "AWS_LAMBDA_LOG_GROUP_NAME",
    "AWS_LAMBDA_LOG_STREAM_NAME",
    "AWS_ACCESS_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_LAMBDA_RUNTIME_API",
    "LAMBDA_TASK_ROOT",
    "LAMBDA_RUNTIME_DIR",
];

/// Server configuration.
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub region: String,
    pub account_id: String,
    pub auth_mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub storage_path: String,
    /// Public base URL used for `GetFunction` `Code.Location` links.
    pub endpoint: String,
    /// Root of the shared S3 file store (`<storage>/s3/buckets`), enabling
    /// `Code.S3Bucket`/`Code.S3Key` deployment packages. `None` disables it.
    pub object_store_root: Option<PathBuf>,
    pub interpreters: Interpreters,
    /// Auth mode for `AuthType: AWS_IAM` function URLs (`relaxed`,
    /// `signed-relaxed`, `strict`); empty follows `auth_mode`. Lets URL
    /// signatures be verified while the API itself stays relaxed.
    pub url_auth_mode: String,
    /// Also print every invocation's framed log (INIT_START/START/END/
    /// REPORT and the handler's output) to stdout, like a CloudWatch stream.
    pub log_invocations: bool,
    /// Credentials every handler receives in place of an execution role's.
    /// `None` leaves `AWS_ACCESS_KEY_ID` & co. unset.
    pub function_credentials: Option<FunctionCredentials>,
    /// Directory standing in for Lambda's `/opt` (layer contents); `None`
    /// means `/opt` itself.
    pub opt_dir: Option<PathBuf>,
    /// How long an idle execution environment is kept warm; `None` means
    /// [`runtime::DEFAULT_IDLE_TIMEOUT`], zero disables reuse (every
    /// invocation is a cold start).
    pub idle_timeout: Option<std::time::Duration>,
}

/// One HTTP reply: status, extra headers, body. `X-Amzn-ErrorType` is carried
/// in `headers` for errors.
#[derive(Debug)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub content_type: &'static str,
}

impl Reply {
    pub fn json(status: u16, value: &Value) -> Self {
        Reply {
            status,
            headers: Vec::new(),
            body: serde_json::to_vec(value).unwrap_or_default(),
            content_type: "application/json",
        }
    }

    pub fn empty(status: u16) -> Self {
        Reply {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            content_type: "application/json",
        }
    }

    pub fn error(status: u16, error_type: &str, message: &str) -> Self {
        let mut r = Reply::json(status, &json!({ "Type": "User", "message": message }));
        r.headers
            .push(("X-Amzn-ErrorType".to_string(), error_type.to_string()));
        r
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

pub(crate) fn revision_mismatch() -> Reply {
    Reply::error(
        412,
        "PreconditionFailedException",
        "The Revision Id provided does not match the latest Revision Id. Call the GetFunction/GetAlias API to retrieve the latest Revision Id",
    )
}

pub(crate) fn validation(message: &str) -> Reply {
    Reply::error(400, "ValidationException", message)
}

pub(crate) fn invalid_param(message: &str) -> Reply {
    Reply::error(400, "InvalidParameterValueException", message)
}

fn default_next_version() -> u64 {
    1
}

/// An immutable published version snapshot of a function.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct VersionRecord {
    pub version: String,
    pub runtime: String,
    pub role: String,
    pub handler: String,
    #[serde(default)]
    pub description: String,
    pub timeout: i64,
    pub memory_size: i64,
    pub last_modified: String,
    pub code_sha256: String,
    pub code_size: i64,
    pub revision_id: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default = "default_architectures")]
    pub architectures: Vec<String>,
    #[serde(default = "default_ephemeral")]
    pub ephemeral_storage_size: i64,
    #[serde(default = "default_package_type")]
    pub package_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_uri: String,
    #[serde(default, skip_serializing_if = "ImageConfig::is_empty")]
    pub image_config: ImageConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub layers: Vec<AttachedLayer>,
}

/// A named function alias pointing to `$LATEST` or a published version, with
/// optional deterministic weighted routing to one additional published version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct AliasRecord {
    pub name: String,
    pub function_version: String,
    #[serde(default)]
    pub description: String,
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub additional_version_weights: BTreeMap<String, f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_url: Option<UrlConfig>,
}

/// A stored function (`$LATEST` plus its published versions, aliases, and
/// concurrency configuration). Field names are the persisted `state.json` keys.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct FunctionRecord {
    pub function_name: String,
    pub runtime: String,
    pub role: String,
    pub handler: String,
    #[serde(default)]
    pub description: String,
    pub timeout: i64,
    pub memory_size: i64,
    pub last_modified: String,
    pub code_sha256: String,
    pub code_size: i64,
    pub revision_id: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default = "default_architectures")]
    pub architectures: Vec<String>,
    #[serde(default = "default_ephemeral")]
    pub ephemeral_storage_size: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_url: Option<UrlConfig>,
    #[serde(default = "default_package_type")]
    pub package_type: String,
    /// Image functions only.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_uri: String,
    #[serde(default, skip_serializing_if = "ImageConfig::is_empty")]
    pub image_config: ImageConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub layers: Vec<AttachedLayer>,
    #[serde(default = "default_next_version")]
    pub next_version: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub versions: BTreeMap<u64, VersionRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub aliases: BTreeMap<String, AliasRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved_concurrent_executions: Option<i64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub provisioned_concurrency: BTreeMap<String, ProvisionedConcurrencyConfig>,
}

impl FunctionRecord {
    pub(crate) fn is_image(&self) -> bool {
        self.package_type == "Image"
    }

    pub(crate) fn has_version(&self, version: &str) -> bool {
        version == "$LATEST"
            || version
                .parse::<u64>()
                .ok()
                .is_some_and(|n| self.versions.contains_key(&n))
    }

    pub(crate) fn references_code_sha(&self, sha: &str) -> bool {
        self.code_sha256 == sha || self.versions.values().any(|v| v.code_sha256 == sha)
    }

    pub(crate) fn all_revision_ids(&self) -> Vec<String> {
        let mut ids = Vec::with_capacity(1 + self.versions.len());
        ids.push(self.revision_id.clone());
        ids.extend(self.versions.values().map(|v| v.revision_id.clone()));
        ids
    }

    pub(crate) fn all_code_shas(&self) -> Vec<String> {
        let mut shas = vec![self.code_sha256.clone()];
        for v in self.versions.values() {
            if !shas.iter().any(|s| s == &v.code_sha256) {
                shas.push(v.code_sha256.clone());
            }
        }
        shas
    }

    fn matches_version(&self, v: &VersionRecord, effective_description: &str) -> bool {
        v.runtime == self.runtime
            && v.role == self.role
            && v.handler == self.handler
            && v.description == effective_description
            && v.timeout == self.timeout
            && v.memory_size == self.memory_size
            && v.code_sha256 == self.code_sha256
            && v.code_size == self.code_size
            && v.environment == self.environment
            && v.architectures == self.architectures
            && v.ephemeral_storage_size == self.ephemeral_storage_size
            && v.package_type == self.package_type
            && v.image_uri == self.image_uri
            && v.image_config == self.image_config
            && v.layers == self.layers
    }

    fn snapshot_version(
        &self,
        version: u64,
        description: String,
        revision_id: String,
    ) -> VersionRecord {
        VersionRecord {
            version: version.to_string(),
            runtime: self.runtime.clone(),
            role: self.role.clone(),
            handler: self.handler.clone(),
            description,
            timeout: self.timeout,
            memory_size: self.memory_size,
            last_modified: now_lambda(),
            code_sha256: self.code_sha256.clone(),
            code_size: self.code_size,
            revision_id,
            environment: self.environment.clone(),
            architectures: self.architectures.clone(),
            ephemeral_storage_size: self.ephemeral_storage_size,
            package_type: self.package_type.clone(),
            image_uri: self.image_uri.clone(),
            image_config: self.image_config.clone(),
            layers: self.layers.clone(),
        }
    }
}

/// A resolved view of either `$LATEST` or a published version (possibly
/// reached through an alias).
#[derive(Clone, Debug)]
pub(crate) struct ResolvedFunction {
    pub function_name: String,
    pub version: String,
    pub arn_qualifier: Option<String>,
    pub runtime: String,
    pub role: String,
    pub handler: String,
    pub description: String,
    pub timeout: i64,
    pub memory_size: i64,
    pub last_modified: String,
    pub code_sha256: String,
    pub code_size: i64,
    pub revision_id: String,
    pub environment: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub architectures: Vec<String>,
    pub ephemeral_storage_size: i64,
    pub function_url: Option<UrlConfig>,
    pub package_type: String,
    pub image_uri: String,
    pub image_config: ImageConfig,
    pub layers: Vec<AttachedLayer>,
    pub reserved_concurrent_executions: Option<i64>,
}

impl ResolvedFunction {
    pub(crate) fn is_image(&self) -> bool {
        self.package_type == "Image"
    }

    fn from_latest(
        rec: &FunctionRecord,
        arn_qualifier: Option<String>,
        url: Option<UrlConfig>,
    ) -> Self {
        ResolvedFunction {
            function_name: rec.function_name.clone(),
            version: "$LATEST".to_string(),
            arn_qualifier,
            runtime: rec.runtime.clone(),
            role: rec.role.clone(),
            handler: rec.handler.clone(),
            description: rec.description.clone(),
            timeout: rec.timeout,
            memory_size: rec.memory_size,
            last_modified: rec.last_modified.clone(),
            code_sha256: rec.code_sha256.clone(),
            code_size: rec.code_size,
            revision_id: rec.revision_id.clone(),
            environment: rec.environment.clone(),
            tags: rec.tags.clone(),
            architectures: rec.architectures.clone(),
            ephemeral_storage_size: rec.ephemeral_storage_size,
            function_url: url,
            package_type: rec.package_type.clone(),
            image_uri: rec.image_uri.clone(),
            image_config: rec.image_config.clone(),
            layers: rec.layers.clone(),
            reserved_concurrent_executions: rec.reserved_concurrent_executions,
        }
    }

    fn from_version(
        rec: &FunctionRecord,
        ver: &VersionRecord,
        arn_qualifier: Option<String>,
        url: Option<UrlConfig>,
    ) -> Self {
        ResolvedFunction {
            function_name: rec.function_name.clone(),
            version: ver.version.clone(),
            arn_qualifier,
            runtime: ver.runtime.clone(),
            role: ver.role.clone(),
            handler: ver.handler.clone(),
            description: ver.description.clone(),
            timeout: ver.timeout,
            memory_size: ver.memory_size,
            last_modified: ver.last_modified.clone(),
            code_sha256: ver.code_sha256.clone(),
            code_size: ver.code_size,
            revision_id: ver.revision_id.clone(),
            environment: ver.environment.clone(),
            tags: rec.tags.clone(),
            architectures: ver.architectures.clone(),
            ephemeral_storage_size: ver.ephemeral_storage_size,
            function_url: url,
            package_type: ver.package_type.clone(),
            image_uri: ver.image_uri.clone(),
            image_config: ver.image_config.clone(),
            layers: ver.layers.clone(),
            reserved_concurrent_executions: rec.reserved_concurrent_executions,
        }
    }
}

/// `ImageConfig`: overrides of the image's entrypoint, command, and working
/// directory.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub(crate) struct ImageConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entry_point: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub working_directory: String,
}

impl ImageConfig {
    fn is_empty(&self) -> bool {
        *self == ImageConfig::default()
    }
}

fn default_package_type() -> String {
    "Zip".to_string()
}

fn default_architectures() -> Vec<String> {
    vec!["x86_64".to_string()]
}

fn default_ephemeral() -> i64 {
    512
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct PersistedState {
    #[serde(default)]
    pub functions: BTreeMap<String, FunctionRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub layers: BTreeMap<String, LayerRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub event_source_mappings: BTreeMap<String, EventSourceMappingRecord>,
}

impl PersistedState {
    pub(crate) fn layer_sha_referenced(&self, layer_name: &str, sha: &str) -> bool {
        if let Some(layer) = self.layers.get(layer_name) {
            if layer.versions.values().any(|v| v.code_sha256 == sha) {
                return true;
            }
        }
        self.functions.values().any(|f| {
            f.layers
                .iter()
                .any(|l| l.layer_name == layer_name && l.code_sha256 == sha)
                || f.versions.values().any(|v| {
                    v.layers
                        .iter()
                        .any(|l| l.layer_name == layer_name && l.code_sha256 == sha)
                })
        })
    }

    pub(crate) fn total_reserved_concurrency(&self) -> i64 {
        self.functions
            .values()
            .filter_map(|f| f.reserved_concurrent_executions)
            .sum()
    }
}

/// Keeps both the function package tree and every attached layer tree alive
/// for the life of an invocation or warm execution environment.
struct LeaseBundle {
    code: Lease,
    layers: Vec<Lease>,
}

/// One entry of the recent-invocation ring buffer (introspection only).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationRecord {
    pub request_id: String,
    pub function_name: String,
    pub invocation_type: String,
    pub status: String,
    pub duration_ms: f64,
    pub started_at: String,
    pub log_tail: String,
}

pub struct Server {
    pub(crate) config: Config,
    pub(crate) state: Mutex<PersistedState>,
    invocations: Mutex<VecDeque<InvocationRecord>>,
    load_err: Option<String>,
    seq: AtomicU64,
    pub(crate) code: CodeStore,
    pub(crate) layer_code: CodeStore,
    /// Per-function locks serializing create / code update / delete, so two
    /// requests never race on the same function's packages.
    function_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Per-layer locks serializing publish / delete on the same layer.
    layer_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Per-alias invocation counters for deterministic weighted routing.
    alias_counters: Mutex<HashMap<String, u64>>,
    /// Active in-flight invocation tracker for reserved concurrency enforcement.
    concurrency: ConcurrencyTracker,
    /// Async (`Event`) invocations still running; aborted on shutdown.
    background: Mutex<Background>,
    /// Held for reading by every package job running on the blocking pool;
    /// `shutdown` takes it for writing (waiting for them) and sets it, after
    /// which no new job starts.
    blocking_gate: Arc<tokio::sync::RwLock<bool>>,
    /// Warm execution environments.
    pool: Arc<runtime::Pool>,
}

#[derive(Default)]
struct Background {
    tasks: tokio::task::JoinSet<()>,
    closed: bool,
}

impl Server {
    pub fn new(mut config: Config) -> Self {
        // Handlers run with their code directory as cwd, so every path handed
        // to the child (task root, result file, /opt) must be absolute.
        if !config.storage_path.is_empty() {
            if let Ok(abs) = std::path::absolute(&config.storage_path) {
                config.storage_path = abs.to_string_lossy().into_owned();
            }
        }
        if let Some(opt) = config.opt_dir.as_mut() {
            if let Ok(abs) = std::path::absolute(&*opt) {
                *opt = abs;
            }
        }
        let mut server = Server {
            state: Mutex::new(PersistedState::default()),
            invocations: Mutex::new(VecDeque::new()),
            load_err: None,
            seq: AtomicU64::new(0),
            code: CodeStore::new(PathBuf::from(&config.storage_path).join("functions")),
            layer_code: CodeStore::new(PathBuf::from(&config.storage_path).join("layers")),
            function_locks: Mutex::new(HashMap::new()),
            layer_locks: Mutex::new(HashMap::new()),
            alias_counters: Mutex::new(HashMap::new()),
            concurrency: ConcurrencyTracker::default(),
            background: Mutex::new(Background::default()),
            blocking_gate: Arc::new(tokio::sync::RwLock::new(false)),
            pool: runtime::Pool::new(
                PathBuf::from(&config.storage_path).join("environments"),
                config.idle_timeout.unwrap_or(runtime::DEFAULT_IDLE_TIMEOUT),
                instance_id(&config.storage_path),
            ),
            config,
        };
        if !server.config.storage_path.is_empty() {
            // Scratch space of environments a previous process left behind.
            let _ = std::fs::remove_dir_all(server.storage().join("environments"));
            if let Err(e) = server.load() {
                server.load_err = Some(e);
            }
        }
        server
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn load_err(&self) -> Option<&str> {
        self.load_err.as_deref()
    }

    pub(crate) fn region(&self) -> &str {
        if self.config.region.is_empty() {
            "us-east-1"
        } else {
            &self.config.region
        }
    }

    pub(crate) fn account_id(&self) -> &str {
        if self.config.account_id.is_empty() {
            "000000000000"
        } else {
            &self.config.account_id
        }
    }

    pub(crate) fn base_endpoint(&self) -> String {
        if self.config.endpoint.is_empty() {
            format!("http://{}", self.config.addr)
        } else {
            self.config.endpoint.trim_end_matches('/').to_string()
        }
    }

    pub(crate) fn function_arn(&self, name: &str) -> String {
        format!(
            "arn:aws:lambda:{}:{}:function:{name}",
            self.region(),
            self.account_id()
        )
    }

    pub(crate) fn qualified_function_arn(&self, name: &str, qualifier: Option<&str>) -> String {
        match qualifier.filter(|q| !q.is_empty()) {
            Some(q) => format!("{}:{q}", self.function_arn(name)),
            None => self.function_arn(name),
        }
    }

    fn storage(&self) -> PathBuf {
        PathBuf::from(&self.config.storage_path)
    }

    fn load(&mut self) -> Result<(), String> {
        let path = self.storage().join("state.json");
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let persisted: PersistedState = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        // Re-extract any package whose extracted tree went missing (image
        // functions have no package). One unrecoverable package fails only
        // that function's invocations: the rest of the API stays usable, and
        // the function can still be updated or deleted.
        for record in persisted.functions.values().filter(|r| !r.is_image()) {
            for sha in record.all_code_shas() {
                if let Err(e) = self.code.ensure_tree(&record.function_name, &sha) {
                    eprintln!(
                        "devcloud-lambda: function {} has no usable package: {e}",
                        record.function_name
                    );
                }
            }
            for layer in record
                .layers
                .iter()
                .chain(record.versions.values().flat_map(|v| v.layers.iter()))
            {
                if let Err(e) = self
                    .layer_code
                    .ensure_tree(&layer.layer_name, &layer.code_sha256)
                {
                    eprintln!(
                        "devcloud-lambda: attached layer {}:{} has no usable package: {e}",
                        layer.layer_name, layer.version
                    );
                }
            }
        }
        for layer in persisted.layers.values() {
            for ver in layer.versions.values() {
                if let Err(e) = self
                    .layer_code
                    .ensure_tree(&layer.layer_name, &ver.code_sha256)
                {
                    eprintln!(
                        "devcloud-lambda: layer {}:{} has no usable package: {e}",
                        layer.layer_name, ver.version
                    );
                }
            }
        }
        *self.state.get_mut().unwrap() = persisted;
        Ok(())
    }

    fn persist(&self, st: &PersistedState) -> Result<(), String> {
        if self.config.storage_path.is_empty() {
            return Ok(());
        }
        let root = self.storage();
        std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
        let data = serde_json::to_vec_pretty(st).map_err(|e| e.to_string())?;
        let path = root.join("state.json");
        let tmp = root.join("state.json.tmp");
        crate::code_store::write_synced(&tmp, &data).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
    }

    /// Applies `mutate` to a copy of the state, persists it, then commits.
    pub(crate) fn commit<T>(
        &self,
        mutate: impl FnOnce(&mut PersistedState) -> Result<T, Reply>,
    ) -> Result<T, Reply> {
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        let out = mutate(&mut next)?;
        self.persist(&next)
            .map_err(|_| Reply::error(500, "ServiceException", "failed to persist lambda state"))?;
        *guard = next;
        Ok(out)
    }

    pub(crate) fn new_id(&self) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let digest = Sha256::digest(format!("{nanos}:{n}:{}", std::process::id()).as_bytes());
        let h = hex::encode(&digest[..16]);
        format!(
            "{}-{}-4{}-a{}-{}",
            &h[0..8],
            &h[8..12],
            &h[13..16],
            &h[17..20],
            &h[20..32]
        )
    }

    /// The plain name a CreateFunction `FunctionName` designates: a bare
    /// name as is; a full or partial ARN only for this region/account and
    /// without a qualifier (a new function has only `$LATEST`).
    fn creatable_function_name(&self, raw: &str) -> Result<String, Reply> {
        if !raw.contains(':') {
            return Ok(raw.to_string());
        }
        let parts: Vec<&str> = raw.split(':').collect();
        let (name, region, account) = if raw.starts_with("arn:") {
            if !is_function_arn(raw) || parts.len() != 7 {
                return Err(validation(&format!(
                    "1 validation error detected: Value '{raw}' at 'functionName' failed to satisfy constraint: Member must be an unqualified Lambda function ARN"
                )));
            }
            (parts[6], Some(parts[3]), parts[4])
        } else if parts.len() == 3 && parts[1] == "function" {
            (parts[2], None, parts[0])
        } else {
            return Err(validation(&format!(
                "1 validation error detected: Value '{raw}' at 'functionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9-_]{{1,64}}"
            )));
        };
        if account != self.account_id() || region.is_some_and(|r| r != self.region()) {
            return Err(invalid_param(&format!(
                "FunctionName {raw} does not belong to this account and region ({}:{})",
                self.region(),
                self.account_id()
            )));
        }
        Ok(name.to_string())
    }

    fn function_lock(&self, name: &str) -> Arc<Mutex<()>> {
        Arc::clone(
            self.function_locks
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_default(),
        )
    }

    pub(crate) fn layer_lock(&self, name: &str) -> Arc<Mutex<()>> {
        Arc::clone(
            self.layer_locks
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_default(),
        )
    }

    fn stage_package(&self, name: &str, zip: &[u8], sha: &str) -> Result<(), Reply> {
        self.code.stage(name, zip, sha).map_err(|e| match e {
            StageError::Package(msg) => invalid_param(&msg),
            StageError::Io(msg) => {
                Reply::error(500, "ServiceException", &format!("store package: {msg}"))
            }
        })
    }

    pub(crate) fn stage_layer_package(
        &self,
        name: &str,
        zip: &[u8],
        sha: &str,
    ) -> Result<(), Reply> {
        self.layer_code.stage(name, zip, sha).map_err(|e| match e {
            StageError::Package(msg) => invalid_param(&msg),
            StageError::Io(msg) => {
                Reply::error(500, "ServiceException", &format!("store layer: {msg}"))
            }
        })
    }

    /// Resolves a list of layer version ARNs against `st.layers` into
    /// `AttachedLayer` snapshots. Rejects unknown, foreign, or duplicate layers.
    fn resolve_attached_layers(
        &self,
        st: &PersistedState,
        arns: &[String],
    ) -> Result<Vec<AttachedLayer>, Reply> {
        let mut out = Vec::with_capacity(arns.len());
        let mut seen_names = Vec::with_capacity(arns.len());
        for arn in arns {
            let (layer_name, version) =
                layers::parse_layer_version_arn(arn, self.region(), self.account_id())?;
            if seen_names.iter().any(|n| n == &layer_name) {
                return Err(invalid_param(&format!(
                    "Cannot reference the same layer twice: {layer_name}"
                )));
            }
            seen_names.push(layer_name.clone());
            let ver = st
                .layers
                .get(&layer_name)
                .and_then(|l| l.versions.get(&version))
                .ok_or_else(|| invalid_param(&format!("Layer version {arn} does not exist")))?;
            out.push(AttachedLayer {
                arn: layers::layer_version_arn(
                    self.region(),
                    self.account_id(),
                    &layer_name,
                    version,
                ),
                layer_name,
                version,
                code_sha256: ver.code_sha256.clone(),
                code_size: ver.code_size,
            });
        }
        Ok(out)
    }

    /// Retires any layer package SHAs that are no longer referenced by either
    /// the layer catalog or any function/version.
    pub(crate) fn retire_orphaned_layers(&self, candidates: &[(String, String)]) {
        if candidates.is_empty() {
            return;
        }
        let st = self.state.lock().unwrap();
        let to_retire: Vec<(String, String)> = candidates
            .iter()
            .filter(|(name, sha)| !st.layer_sha_referenced(name, sha))
            .cloned()
            .collect();
        drop(st);
        for (name, sha) in to_retire {
            self.layer_code.retire(&name, &sha);
        }
    }

    // ---- control plane ----------------------------------------------------

    pub fn create_function(&self, body: &[u8]) -> Reply {
        let req: Value = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        // AWS accepts a bare name, `123456789012:function:name`, or the full
        // ARN; store the plain name once the scope is confirmed to be ours.
        let name = match self.creatable_function_name(&str_field(&req, "FunctionName")) {
            Ok(n) => n,
            Err(r) => return r,
        };
        if let Err(r) = validate_function_name(&name) {
            return r;
        }
        let package_type = match str_field(&req, "PackageType").as_str() {
            "" | "Zip" => "Zip",
            "Image" => "Image",
            other => {
                return validation(&format!(
                    "1 validation error detected: Value '{other}' at 'packageType' failed to satisfy constraint: Member must satisfy enum value set: [Image, Zip]"
                ))
            }
        };
        let is_image = package_type == "Image";
        let role = str_field(&req, "Role");
        if role.is_empty() {
            return validation("1 validation error detected: Value null at 'role' failed to satisfy constraint: Member must not be null");
        }
        let runtime_id = str_field(&req, "Runtime");
        let handler = str_field(&req, "Handler");
        if is_image {
            if !runtime_id.is_empty() || !handler.is_empty() {
                return invalid_param(
                    "Runtime and Handler are not supported for the Image package type",
                );
            }
        } else {
            if let Err(r) = validate_runtime(&runtime_id) {
                return r;
            }
            if handler.is_empty() {
                return invalid_param("Handler is required for Zip package type");
            }
        }
        let image_config = match image_config(&req, is_image) {
            Ok(c) => c.unwrap_or_default(),
            Err(r) => return r,
        };
        let timeout = match int_field(&req, "Timeout", 3, 1, 900, "timeout") {
            Ok(v) => v,
            Err(r) => return r,
        };
        let memory = match int_field(&req, "MemorySize", 128, 128, 10240, "memorySize") {
            Ok(v) => v,
            Err(r) => return r,
        };
        let environment = match env_vars(&req) {
            Ok(v) => v.unwrap_or_default(),
            Err(r) => return r,
        };
        let ephemeral_storage_size = match ephemeral_storage(&req) {
            Ok(v) => v.unwrap_or_else(default_ephemeral),
            Err(r) => return r,
        };
        let (zip, image_uri) = if is_image {
            match image_uri(req.get("Code")) {
                Ok(u) => (Vec::new(), u),
                Err(r) => return r,
            }
        } else {
            match self.package_bytes(req.get("Code")) {
                Ok(z) => (z, String::new()),
                Err(r) => return r,
            }
        };
        let tags = match string_map(req.get("Tags"), "tags") {
            Ok(t) => t,
            Err(r) => return r,
        };
        let architectures = match architectures(&req) {
            Ok(a) => a.unwrap_or_else(default_architectures),
            Err(r) => return r,
        };
        let layer_arns = match layers::parse_layer_arns(&req) {
            Ok(l) => l.unwrap_or_default(),
            Err(r) => return r,
        };
        if is_image && !layer_arns.is_empty() {
            return invalid_param("Layers are not supported for the Image package type");
        }
        let publish = match req.get("Publish") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => return validation("Publish must be a boolean"),
        };

        let lock = self.function_lock(&name);
        let _serialized = lock.lock().unwrap();
        if self.state.lock().unwrap().functions.contains_key(&name) {
            return Reply::error(
                409,
                "ResourceConflictException",
                &format!("Function already exist: {name}"),
            );
        }

        let sha = if is_image {
            code_sha256(image_uri.as_bytes())
        } else {
            code_sha256(&zip)
        };
        if !is_image {
            if let Err(r) = self.stage_package(&name, &zip, &sha) {
                self.code.retire(&name, &sha);
                return r;
            }
        }
        let mut record = FunctionRecord {
            function_name: name.clone(),
            runtime: runtime_id,
            role,
            handler,
            description: str_field(&req, "Description"),
            timeout,
            memory_size: memory,
            last_modified: now_lambda(),
            code_sha256: sha.clone(),
            code_size: zip.len() as i64,
            revision_id: self.new_id(),
            environment,
            tags,
            architectures,
            ephemeral_storage_size,
            function_url: None,
            package_type: package_type.to_string(),
            image_uri,
            image_config,
            layers: Vec::new(),
            next_version: 1,
            versions: BTreeMap::new(),
            aliases: BTreeMap::new(),
            reserved_concurrent_executions: None,
            provisioned_concurrency: BTreeMap::new(),
        };
        let published_revision = publish.then(|| self.new_id());
        let result = self.commit(|st| {
            if st.functions.contains_key(&name) {
                return Err(Reply::error(
                    409,
                    "ResourceConflictException",
                    &format!("Function already exist: {name}"),
                ));
            }
            record.layers = self.resolve_attached_layers(st, &layer_arns)?;
            let returned = if let Some(pub_rev) = &published_revision {
                let ver_num = record.next_version;
                record.next_version += 1;
                let ver =
                    record.snapshot_version(ver_num, record.description.clone(), pub_rev.clone());
                let resolved =
                    ResolvedFunction::from_version(&record, &ver, Some(ver.version.clone()), None);
                record.versions.insert(ver_num, ver);
                resolved
            } else {
                ResolvedFunction::from_latest(&record, None, None)
            };
            st.functions.insert(name.clone(), record.clone());
            Ok(returned)
        });
        let returned = match result {
            Ok(r) => r,
            Err(r) => {
                // Serialized and the function did not exist: these files are ours.
                self.code.retire(&name, &sha);
                return r;
            }
        };
        emit("lambda.function.created", json!({ "functionName": name }));
        Reply::json(201, &self.configuration_json(&returned))
    }

    pub fn list_functions(&self, query: &BTreeMap<String, String>) -> Reply {
        let (start, max_items) = match parse_pagination(query) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let all_versions = match query.get("FunctionVersion").map(String::as_str) {
            None | Some("") => false,
            Some("ALL") => true,
            Some(other) => {
                return validation(&format!(
                    "1 validation error detected: Value '{other}' at 'functionVersion' failed to satisfy constraint: Member must satisfy enum value set: [ALL]"
                ));
            }
        };
        let st = self.state.lock().unwrap();
        let mut all: Vec<ResolvedFunction> = Vec::new();
        for rec in st.functions.values() {
            if all_versions {
                all.push(ResolvedFunction::from_latest(
                    rec,
                    Some("$LATEST".to_string()),
                    rec.function_url.clone(),
                ));
                for ver in rec.versions.values() {
                    all.push(ResolvedFunction::from_version(
                        rec,
                        ver,
                        Some(ver.version.clone()),
                        None,
                    ));
                }
            } else {
                all.push(ResolvedFunction::from_latest(
                    rec,
                    None,
                    rec.function_url.clone(),
                ));
            }
        }
        let page: Vec<Value> = all
            .iter()
            .skip(start)
            .take(max_items)
            .map(|r| self.configuration_json(r))
            .collect();
        let mut out = json!({ "Functions": page });
        let next = start.saturating_add(max_items);
        if next < all.len() {
            out["NextMarker"] = json!(next.to_string());
        }
        Reply::json(200, &out)
    }

    pub fn get_function(&self, id: &str, query: &BTreeMap<String, String>) -> Reply {
        let record = match self.lookup(id, query.get("Qualifier").map(String::as_str)) {
            Ok(r) => r,
            Err(r) => return r,
        };
        let base = self.base_endpoint();
        let code = if record.is_image() {
            json!({
                "RepositoryType": "ECR",
                "ImageUri": record.image_uri,
                "ResolvedImageUri": record.image_uri,
            })
        } else {
            json!({
                "RepositoryType": "S3",
                // Pinned to this code version: after an update the link
                // expires instead of silently serving different code.
                "Location": format!(
                    "{base}/_devcloud/functions/{}/code.zip?CodeSha256={}",
                    record.function_name,
                    query_encode(&record.code_sha256)
                ),
            })
        };
        let mut out = json!({
            "Configuration": self.configuration_json(&record),
            "Code": code,
            "Tags": record.tags,
        });
        if let Some(reserved) = record.reserved_concurrent_executions {
            out["Concurrency"] = json!({ "ReservedConcurrentExecutions": reserved });
        }
        Reply::json(200, &out)
    }

    pub fn get_function_configuration(&self, id: &str, query: &BTreeMap<String, String>) -> Reply {
        match self.lookup(id, query.get("Qualifier").map(String::as_str)) {
            Ok(r) => Reply::json(200, &self.configuration_json(&r)),
            Err(r) => r,
        }
    }

    pub fn update_function_configuration(&self, id: &str, body: &[u8]) -> Reply {
        let req: Value = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let runtime_id = str_field(&req, "Runtime");
        if !runtime_id.is_empty() {
            if let Err(r) = validate_runtime(&runtime_id) {
                return r;
            }
        }
        let timeout = match opt_int_field(&req, "Timeout", 1, 900, "timeout") {
            Ok(v) => v,
            Err(r) => return r,
        };
        let memory = match opt_int_field(&req, "MemorySize", 128, 10240, "memorySize") {
            Ok(v) => v,
            Err(r) => return r,
        };
        let environment = match env_vars(&req) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let ephemeral_storage_size = match ephemeral_storage(&req) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let new_image_config = match image_config(&req, true) {
            Ok(c) => c,
            Err(r) => return r,
        };
        let layer_arns = match layers::parse_layer_arns(&req) {
            Ok(l) => l,
            Err(r) => return r,
        };
        let revision = str_field(&req, "RevisionId");
        let new_revision = self.new_id();
        let result = self.commit(|st| {
            let resolved_layers = match &layer_arns {
                Some(arns) => Some(self.resolve_attached_layers(st, arns)?),
                None => None,
            };
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            if !revision.is_empty() && revision != rec.revision_id {
                return Err(revision_mismatch());
            }
            if rec.is_image() {
                if !runtime_id.is_empty() || !str_field(&req, "Handler").is_empty() {
                    return Err(invalid_param(
                        "Runtime and Handler are not supported for the Image package type",
                    ));
                }
                if resolved_layers.as_ref().is_some_and(|l| !l.is_empty()) {
                    return Err(invalid_param(
                        "Layers are not supported for the Image package type",
                    ));
                }
                if let Some(c) = new_image_config.clone() {
                    rec.image_config = c;
                }
            } else if new_image_config.is_some() {
                return Err(invalid_param(
                    "ImageConfig is supported only for the Image package type",
                ));
            }
            if !runtime_id.is_empty() {
                rec.runtime = runtime_id.clone();
            }
            for (field, target) in [("Role", &mut rec.role), ("Handler", &mut rec.handler)] {
                let v = str_field(&req, field);
                if !v.is_empty() {
                    *target = v;
                }
            }
            if req.get("Description").is_some() {
                rec.description = str_field(&req, "Description");
            }
            if let Some(t) = timeout {
                rec.timeout = t;
            }
            if let Some(m) = memory {
                rec.memory_size = m;
            }
            if let Some(env) = environment.clone() {
                rec.environment = env;
            }
            if let Some(size) = ephemeral_storage_size {
                rec.ephemeral_storage_size = size;
            }
            let old_layers: Vec<(String, String)> = if let Some(new_layers) = resolved_layers {
                std::mem::replace(&mut rec.layers, new_layers)
                    .into_iter()
                    .map(|l| (l.layer_name, l.code_sha256))
                    .collect()
            } else {
                Vec::new()
            };
            rec.last_modified = now_lambda();
            let retired = std::mem::replace(&mut rec.revision_id, new_revision.clone());
            Ok((
                ResolvedFunction::from_latest(rec, None, rec.function_url.clone()),
                retired,
                old_layers,
            ))
        });
        match result {
            Ok((rec, retired, old_layers)) => {
                // Environments of the previous revision can never serve again.
                self.pool.retire(&name, &retired);
                self.retire_orphaned_layers(&old_layers);
                emit("lambda.function.updated", json!({ "functionName": name }));
                Reply::json(200, &self.configuration_json(&rec))
            }
            Err(r) => r,
        }
    }

    pub fn update_function_code(&self, id: &str, body: &[u8]) -> Reply {
        let req: Value = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let is_image = match self.lookup(&name, None) {
            Ok(r) => r.is_image(),
            Err(r) => return r,
        };
        // The package type is fixed at creation, as on AWS.
        let (zip, image_uri) = if is_image {
            match image_uri(Some(&req)) {
                Ok(u) => (Vec::new(), u),
                Err(r) => return r,
            }
        } else {
            match self.package_bytes(Some(&req)) {
                Ok(z) => (z, String::new()),
                Err(r) => return r,
            }
        };
        let sha = if is_image {
            code_sha256(image_uri.as_bytes())
        } else {
            code_sha256(&zip)
        };
        let revision = str_field(&req, "RevisionId");
        let architectures = match architectures(&req) {
            Ok(a) => a,
            Err(r) => return r,
        };
        let publish = match req.get("Publish") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => return validation("Publish must be a boolean"),
        };

        let lock = self.function_lock(&name);
        let _serialized = lock.lock().unwrap();
        // Re-read under the function lock: a concurrent delete/update may have
        // landed between the first lookup and here.
        let current = match self.lookup(&name, None) {
            Ok(r) => r,
            Err(r) => return r,
        };
        if !revision.is_empty() && revision != current.revision_id {
            return revision_mismatch();
        }
        if req.get("DryRun").and_then(Value::as_bool) == Some(true) {
            let mut preview = current;
            preview.code_sha256 = sha;
            preview.code_size = zip.len() as i64;
            if is_image {
                preview.image_uri = image_uri;
            }
            if let Some(a) = architectures {
                preview.architectures = a;
            }
            return Reply::json(200, &self.configuration_json(&preview));
        }
        // The new version gets its own files; the committed one stays intact
        // until the configuration switch below is persisted.
        if !is_image {
            if let Err(r) = self.stage_package(&name, &zip, &sha) {
                let still_referenced = self
                    .state
                    .lock()
                    .unwrap()
                    .functions
                    .get(&name)
                    .is_some_and(|f| f.references_code_sha(&sha));
                if !still_referenced {
                    self.code.retire(&name, &sha);
                }
                return r;
            }
        }
        let new_revision = self.new_id();
        let pub_revision = publish.then(|| self.new_id());
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            if !revision.is_empty() && revision != rec.revision_id {
                return Err(revision_mismatch());
            }
            let previous = std::mem::replace(&mut rec.code_sha256, sha.clone());
            rec.code_size = zip.len() as i64;
            if is_image {
                rec.image_uri = image_uri.clone();
            }
            // Architectures travel with the code (UpdateFunctionCode), and are
            // committed together with it.
            if let Some(a) = architectures.clone() {
                rec.architectures = a;
            }
            rec.last_modified = now_lambda();
            let retired = std::mem::replace(&mut rec.revision_id, new_revision.clone());
            let returned = if let Some(pub_rev) = &pub_revision {
                let ver = match rec
                    .versions
                    .values()
                    .next_back()
                    .filter(|v| rec.matches_version(v, &rec.description))
                    .cloned()
                {
                    Some(existing) => existing,
                    None => {
                        let ver_num = rec.next_version;
                        rec.next_version += 1;
                        let v =
                            rec.snapshot_version(ver_num, rec.description.clone(), pub_rev.clone());
                        rec.versions.insert(ver_num, v.clone());
                        v
                    }
                };
                ResolvedFunction::from_version(rec, &ver, Some(ver.version.clone()), None)
            } else {
                ResolvedFunction::from_latest(rec, None, rec.function_url.clone())
            };
            let retire_previous = !rec.references_code_sha(&previous);
            Ok((returned, previous, retire_previous, retired))
        });
        match result {
            Ok((returned, previous, retire_previous, retired)) => {
                if retire_previous {
                    self.code.retire(&name, &previous);
                }
                // Environments of the previous revision can never serve again.
                self.pool.retire(&name, &retired);
                emit("lambda.function.updated", json!({ "functionName": name }));
                Reply::json(200, &self.configuration_json(&returned))
            }
            Err(r) => {
                let still_referenced = self
                    .state
                    .lock()
                    .unwrap()
                    .functions
                    .get(&name)
                    .is_some_and(|f| f.references_code_sha(&sha));
                if !still_referenced {
                    self.code.retire(&name, &sha);
                }
                r
            }
        }
    }

    pub fn delete_function(&self, id: &str, query: &BTreeMap<String, String>) -> Reply {
        let qualifier = match effective_qualifier(id, query.get("Qualifier").map(String::as_str)) {
            Ok(q) => q,
            Err(r) => return r,
        };
        let resolved = match self.lookup(id, query.get("Qualifier").map(String::as_str)) {
            Ok(r) => r,
            Err(r) => return r,
        };
        let name = resolved.function_name;
        if let Some(q) = qualifier {
            if q == "$LATEST" {
                return invalid_param(
                    "$LATEST version cannot be deleted without deleting the function.",
                );
            }
            let Ok(ver_num) = q.parse::<u64>() else {
                return invalid_param(&format!(
                    "Cannot delete function version using alias {q}. Use DeleteAlias instead."
                ));
            };
            let lock = self.function_lock(&name);
            let _serialized = lock.lock().unwrap();
            let result = self.commit(|st| {
                let rec = st
                    .functions
                    .get_mut(&name)
                    .ok_or_else(|| self.not_found(&name))?;
                let ver_str = ver_num.to_string();
                if let Some(alias) = rec.aliases.values().find(|a| {
                    a.function_version == ver_str
                        || a.additional_version_weights.contains_key(&ver_str)
                }) {
                    return Err(Reply::error(
                        409,
                        "ResourceConflictException",
                        &format!(
                            "Function version {}:{ver_str} is referenced by alias {}",
                            self.function_arn(&name),
                            alias.name
                        ),
                    ));
                }
                let removed = rec.versions.remove(&ver_num).ok_or_else(|| {
                    Reply::error(
                        404,
                        "ResourceNotFoundException",
                        &format!("Function not found: {}:{ver_str}", self.function_arn(&name)),
                    )
                })?;
                rec.provisioned_concurrency.remove(&ver_str);
                let retire_code = !rec.references_code_sha(&removed.code_sha256);
                let old_layers: Vec<(String, String)> = removed
                    .layers
                    .into_iter()
                    .map(|l| (l.layer_name, l.code_sha256))
                    .collect();
                Ok((
                    removed.revision_id,
                    removed.code_sha256,
                    retire_code,
                    old_layers,
                ))
            });
            return match result {
                Ok((rev, sha, retire_code, old_layers)) => {
                    self.pool.retire(&name, &rev);
                    if retire_code {
                        self.code.retire(&name, &sha);
                    }
                    self.retire_orphaned_layers(&old_layers);
                    Reply::empty(204)
                }
                Err(r) => r,
            };
        }

        let lock = self.function_lock(&name);
        let _serialized = lock.lock().unwrap();
        let result = self.commit(|st| {
            st.functions
                .remove(&name)
                .ok_or_else(|| self.not_found(&name))
        });
        let removed = match result {
            Ok(rec) => rec,
            Err(r) => return r,
        };
        // Running invocations keep their tree until they finish.
        self.pool
            .retire_function(&name, &removed.all_revision_ids());
        for sha in removed.all_code_shas() {
            self.code.retire(&name, &sha);
        }
        let old_layers: Vec<(String, String)> = removed
            .layers
            .into_iter()
            .chain(removed.versions.into_values().flat_map(|v| v.layers))
            .map(|l| (l.layer_name, l.code_sha256))
            .collect();
        self.retire_orphaned_layers(&old_layers);
        emit("lambda.function.deleted", json!({ "functionName": name }));
        Reply::empty(204)
    }

    // ---- versions -----------------------------------------------------------

    /// `POST /2015-03-31/functions/<id>/versions` (`PublishVersion`).
    pub fn publish_version(&self, id: &str, body: &[u8]) -> Reply {
        let req: Value = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let expected_sha = str_field(&req, "CodeSha256");
        let expected_rev = str_field(&req, "RevisionId");
        let custom_desc = req
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_string);
        let new_revision = self.new_id();
        let lock = self.function_lock(&name);
        let _serialized = lock.lock().unwrap();
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            if !expected_rev.is_empty() && expected_rev != rec.revision_id {
                return Err(revision_mismatch());
            }
            if !expected_sha.is_empty() && expected_sha != rec.code_sha256 {
                return Err(invalid_param(&format!(
                    "CodeSHA256 ({}) is different from current CodeSHA256 in input ({expected_sha}). Please try again with the current CodeSHA256.",
                    rec.code_sha256
                )));
            }
            let effective_desc = custom_desc
                .clone()
                .unwrap_or_else(|| rec.description.clone());
            // Idempotent when nothing has changed since the latest published version.
            if let Some(existing) = rec
                .versions
                .values()
                .next_back()
                .filter(|v| rec.matches_version(v, &effective_desc))
                .cloned()
            {
                return Ok(ResolvedFunction::from_version(
                    rec,
                    &existing,
                    Some(existing.version.clone()),
                    None,
                ));
            }
            let ver_num = rec.next_version;
            rec.next_version += 1;
            let ver = rec.snapshot_version(ver_num, effective_desc, new_revision.clone());
            let resolved =
                ResolvedFunction::from_version(rec, &ver, Some(ver.version.clone()), None);
            rec.versions.insert(ver_num, ver);
            Ok(resolved)
        });
        match result {
            Ok(resolved) => Reply::json(201, &self.configuration_json(&resolved)),
            Err(r) => r,
        }
    }

    /// `GET /2015-03-31/functions/<id>/versions` (`ListVersionsByFunction`).
    pub fn list_versions_by_function(&self, id: &str, query: &BTreeMap<String, String>) -> Reply {
        let (start, max_items) = match parse_pagination(query) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        let mut all = Vec::with_capacity(1 + rec.versions.len());
        all.push(ResolvedFunction::from_latest(
            rec,
            Some("$LATEST".to_string()),
            rec.function_url.clone(),
        ));
        for ver in rec.versions.values() {
            all.push(ResolvedFunction::from_version(
                rec,
                ver,
                Some(ver.version.clone()),
                None,
            ));
        }
        let page: Vec<Value> = all
            .iter()
            .skip(start)
            .take(max_items)
            .map(|r| self.configuration_json(r))
            .collect();
        let mut out = json!({ "Versions": page });
        let next = start.saturating_add(max_items);
        if next < all.len() {
            out["NextMarker"] = json!(next.to_string());
        }
        Reply::json(200, &out)
    }

    // ---- aliases ------------------------------------------------------------

    fn alias_json(&self, function_name: &str, alias: &AliasRecord) -> Value {
        let mut v = json!({
            "AliasArn": format!("{}:{}", self.function_arn(function_name), alias.name),
            "Name": alias.name,
            "FunctionVersion": alias.function_version,
            "Description": alias.description,
            "RevisionId": alias.revision_id,
        });
        if !alias.additional_version_weights.is_empty() {
            v["RoutingConfig"] = json!({
                "AdditionalVersionWeights": alias.additional_version_weights,
            });
        }
        v
    }

    fn alias_not_found(&self, function_name: &str, alias: &str) -> Reply {
        Reply::error(
            404,
            "ResourceNotFoundException",
            &format!(
                "Cannot find alias arn: {}:{alias}",
                self.function_arn(function_name)
            ),
        )
    }

    /// `POST /2015-03-31/functions/<id>/aliases` (`CreateAlias`).
    pub fn create_alias(&self, id: &str, body: &[u8]) -> Reply {
        let req: Value = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let alias_name = str_field(&req, "Name");
        if let Err(r) = validate_alias_name(&alias_name) {
            return r;
        }
        let function_version = str_field(&req, "FunctionVersion");
        if function_version.is_empty() {
            return validation("1 validation error detected: Value null at 'functionVersion' failed to satisfy constraint: Member must not be null");
        }
        let weights = match parse_routing_config(&req) {
            Ok(w) => w.unwrap_or_default(),
            Err(r) => return r,
        };
        let description = str_field(&req, "Description");
        let revision_id = self.new_id();
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            if rec.aliases.contains_key(&alias_name) {
                return Err(Reply::error(
                    409,
                    "ResourceConflictException",
                    &format!(
                        "Alias already exists: {}:{alias_name}",
                        self.function_arn(&name)
                    ),
                ));
            }
            self.validate_alias_targets(rec, &function_version, &weights)?;
            let alias = AliasRecord {
                name: alias_name.clone(),
                function_version: function_version.clone(),
                description: description.clone(),
                revision_id: revision_id.clone(),
                additional_version_weights: weights.clone(),
                function_url: None,
            };
            rec.aliases.insert(alias_name.clone(), alias.clone());
            Ok(alias)
        });
        match result {
            Ok(alias) => Reply::json(201, &self.alias_json(&name, &alias)),
            Err(r) => r,
        }
    }

    /// `GET /2015-03-31/functions/<id>/aliases/<alias>` (`GetAlias`).
    pub fn get_alias(&self, id: &str, alias_name: &str) -> Reply {
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        match rec.aliases.get(alias_name) {
            Some(alias) => Reply::json(200, &self.alias_json(&name, alias)),
            None => self.alias_not_found(&name, alias_name),
        }
    }

    /// `PUT /2015-03-31/functions/<id>/aliases/<alias>` (`UpdateAlias`).
    pub fn update_alias(&self, id: &str, alias_name: &str, body: &[u8]) -> Reply {
        let req: Value = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let new_version = req
            .get("FunctionVersion")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let new_description = req
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_string);
        let new_weights = match parse_routing_config(&req) {
            Ok(w) => w,
            Err(r) => return r,
        };
        let expected_rev = str_field(&req, "RevisionId");
        let next_rev = self.new_id();
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            let current = rec
                .aliases
                .get(alias_name)
                .ok_or_else(|| self.alias_not_found(&name, alias_name))?
                .clone();
            if !expected_rev.is_empty() && expected_rev != current.revision_id {
                return Err(revision_mismatch());
            }
            let target_version = new_version
                .clone()
                .unwrap_or_else(|| current.function_version.clone());
            let target_weights = new_weights
                .clone()
                .unwrap_or_else(|| current.additional_version_weights.clone());
            self.validate_alias_targets(rec, &target_version, &target_weights)?;
            let alias = rec.aliases.get_mut(alias_name).unwrap();
            alias.function_version = target_version;
            if let Some(d) = new_description.clone() {
                alias.description = d;
            }
            alias.additional_version_weights = target_weights;
            alias.revision_id = next_rev.clone();
            Ok(alias.clone())
        });
        match result {
            Ok(alias) => {
                // Reset deterministic counter when routing changes.
                self.alias_counters
                    .lock()
                    .unwrap()
                    .remove(&format!("{name}:{alias_name}"));
                Reply::json(200, &self.alias_json(&name, &alias))
            }
            Err(r) => r,
        }
    }

    /// `DELETE /2015-03-31/functions/<id>/aliases/<alias>` (`DeleteAlias`).
    pub fn delete_alias(&self, id: &str, alias_name: &str) -> Reply {
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            rec.aliases
                .remove(alias_name)
                .ok_or_else(|| self.alias_not_found(&name, alias_name))?;
            rec.provisioned_concurrency.remove(alias_name);
            Ok(())
        });
        match result {
            Ok(()) => {
                self.alias_counters
                    .lock()
                    .unwrap()
                    .remove(&format!("{name}:{alias_name}"));
                Reply::empty(204)
            }
            Err(r) => r,
        }
    }

    /// `GET /2015-03-31/functions/<id>/aliases` (`ListAliases`).
    pub fn list_aliases(&self, id: &str, query: &BTreeMap<String, String>) -> Reply {
        let (start, max_items) = match parse_pagination(query) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let filter_version = query.get("FunctionVersion").map(String::as_str);
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        let matching: Vec<&AliasRecord> = rec
            .aliases
            .values()
            .filter(|a| filter_version.is_none_or(|fv| fv.is_empty() || a.function_version == fv))
            .collect();
        let page: Vec<Value> = matching
            .iter()
            .skip(start)
            .take(max_items)
            .map(|a| self.alias_json(&name, a))
            .collect();
        let mut out = json!({ "Aliases": page });
        let next = start.saturating_add(max_items);
        if next < matching.len() {
            out["NextMarker"] = json!(next.to_string());
        }
        Reply::json(200, &out)
    }

    fn validate_alias_targets(
        &self,
        rec: &FunctionRecord,
        primary_version: &str,
        weights: &BTreeMap<String, f64>,
    ) -> Result<(), Reply> {
        if !rec.has_version(primary_version) {
            return Err(Reply::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "Function not found: {}:{primary_version}",
                    self.function_arn(&rec.function_name)
                ),
            ));
        }
        if weights.is_empty() {
            return Ok(());
        }
        if primary_version == "$LATEST" {
            return Err(invalid_param(
                "RoutingConfig is not supported when FunctionVersion is $LATEST",
            ));
        }
        if weights.len() > 1 {
            return Err(invalid_param(
                "RoutingConfig.AdditionalVersionWeights may specify at most one additional version",
            ));
        }
        for (ver, &weight) in weights {
            if ver == "$LATEST" {
                return Err(invalid_param(
                    "RoutingConfig cannot route traffic to $LATEST",
                ));
            }
            if ver == primary_version {
                return Err(invalid_param(
                    "RoutingConfig additional version cannot be the same as FunctionVersion",
                ));
            }
            if !rec.has_version(ver) {
                return Err(Reply::error(
                    404,
                    "ResourceNotFoundException",
                    &format!(
                        "Function not found: {}:{ver}",
                        self.function_arn(&rec.function_name)
                    ),
                ));
            }
            if !weight.is_finite() || !(0.0..=1.0).contains(&weight) {
                return Err(validation(
                    "RoutingConfig weight must be a number between 0.0 and 1.0",
                ));
            }
        }
        Ok(())
    }

    /// Deterministically selects either the primary or weighted additional
    /// version for an alias invocation using a Bresenham fractional accumulator.
    fn select_alias_version(&self, function_name: &str, alias: &AliasRecord) -> String {
        let Some((additional, &weight)) = alias.additional_version_weights.iter().next() else {
            return alias.function_version.clone();
        };
        if weight <= 0.0 {
            return alias.function_version.clone();
        }
        if weight >= 1.0 {
            return additional.clone();
        }
        let key = format!("{function_name}:{}", alias.name);
        let mut counters = self.alias_counters.lock().unwrap();
        let k = {
            let c = counters.entry(key).or_insert(0);
            *c = c.wrapping_add(1);
            *c
        };
        let prev = (((k - 1) as f64) * weight).floor();
        let curr = ((k as f64) * weight).floor();
        if curr > prev {
            additional.clone()
        } else {
            alias.function_version.clone()
        }
    }

    // ---- function URLs ------------------------------------------------------

    fn function_url_json(
        &self,
        name: &str,
        qualifier: Option<&str>,
        url: &UrlConfig,
        with_last_modified: bool,
    ) -> Value {
        let mut v = json!({
            "FunctionUrl": function_url::function_url(&self.config.endpoint, &url.url_id, self.region()),
            "FunctionArn": self.qualified_function_arn(name, qualifier),
            "AuthType": url.auth_type,
            "CreationTime": url.creation_time,
            "InvokeMode": url.invoke_mode,
        });
        if let Some(cors) = &url.cors {
            v["Cors"] = serde_json::to_value(cors).unwrap_or_default();
        }
        if with_last_modified {
            v["LastModifiedTime"] = json!(url.last_modified_time);
        }
        v
    }

    fn url_not_found(&self, name: &str, qualifier: Option<&str>) -> Reply {
        Reply::error(
            404,
            "ResourceNotFoundException",
            &format!(
                "The resource you requested does not exist. (Function URL config for {})",
                self.qualified_function_arn(name, qualifier)
            ),
        )
    }

    /// Resolves `(function_name, optional_alias_name)` for function URL config
    /// operations (`$LATEST` or an existing alias; numeric versions are rejected).
    fn resolve_url_target(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
    ) -> Result<(String, Option<String>), Reply> {
        let qualifier = effective_qualifier(id, query.get("Qualifier").map(String::as_str))?;
        let resolved = self.lookup(id, query.get("Qualifier").map(String::as_str))?;
        match qualifier.as_deref() {
            None | Some("$LATEST") => Ok((resolved.function_name, None)),
            Some(q) if q.parse::<u64>().is_ok() => Err(invalid_param(
                "Function URLs are only supported for $LATEST and aliases, not published versions",
            )),
            Some(alias) => Ok((resolved.function_name, Some(alias.to_string()))),
        }
    }

    /// `POST /2021-10-31/functions/<id>/url`.
    pub fn create_function_url_config(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Reply {
        let settings = match parse_json(body).and_then(|req| function_url::parse_settings(&req)) {
            Ok(s) => s,
            Err(r) => return r,
        };
        let Some(auth_type) = settings.auth_type else {
            return validation("1 validation error detected: Value null at 'authType' failed to satisfy constraint: Member must not be null");
        };
        let (name, alias) = match self.resolve_url_target(id, query) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let url_seed = match &alias {
            Some(a) => format!("{name}:{a}"),
            None => name.clone(),
        };
        let now = now_rfc3339();
        let url = UrlConfig {
            url_id: function_url::url_id_for(self.account_id(), self.region(), &url_seed),
            auth_type,
            cors: settings.cors,
            invoke_mode: settings
                .invoke_mode
                .unwrap_or_else(|| "BUFFERED".to_string()),
            creation_time: now.clone(),
            last_modified_time: now,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            let slot = match &alias {
                Some(a) => {
                    &mut rec
                        .aliases
                        .get_mut(a)
                        .ok_or_else(|| self.alias_not_found(&name, a))?
                        .function_url
                }
                None => &mut rec.function_url,
            };
            if slot.is_some() {
                return Err(Reply::error(
                    409,
                    "ResourceConflictException",
                    &format!(
                        "Failed to create function url config for [functionArn = {}]. Error message:  FunctionUrlConfig exists for this Lambda function",
                        self.qualified_function_arn(&name, alias.as_deref())
                    ),
                ));
            }
            *slot = Some(url.clone());
            Ok(())
        });
        match result {
            Ok(()) => Reply::json(
                201,
                &self.function_url_json(&name, alias.as_deref(), &url, false),
            ),
            Err(r) => r,
        }
    }

    /// `GET /2021-10-31/functions/<id>/url`.
    pub fn get_function_url_config(&self, id: &str, query: &BTreeMap<String, String>) -> Reply {
        let (name, alias) = match self.resolve_url_target(id, query) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        let url = match &alias {
            Some(a) => match rec.aliases.get(a) {
                Some(al) => &al.function_url,
                None => return self.alias_not_found(&name, a),
            },
            None => &rec.function_url,
        };
        match url {
            Some(url) => Reply::json(
                200,
                &self.function_url_json(&name, alias.as_deref(), url, true),
            ),
            None => self.url_not_found(&name, alias.as_deref()),
        }
    }

    /// `PUT /2021-10-31/functions/<id>/url`: only the fields present change.
    pub fn update_function_url_config(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Reply {
        let settings = match parse_json(body).and_then(|req| function_url::parse_settings(&req)) {
            Ok(s) => s,
            Err(r) => return r,
        };
        let (name, alias) = match self.resolve_url_target(id, query) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            let slot = match &alias {
                Some(a) => {
                    &mut rec
                        .aliases
                        .get_mut(a)
                        .ok_or_else(|| self.alias_not_found(&name, a))?
                        .function_url
                }
                None => &mut rec.function_url,
            };
            let url = slot
                .as_mut()
                .ok_or_else(|| self.url_not_found(&name, alias.as_deref()))?;
            if let Some(a) = settings.auth_type {
                url.auth_type = a;
            }
            if let Some(c) = settings.cors {
                url.cors = Some(c);
            }
            if let Some(m) = settings.invoke_mode {
                url.invoke_mode = m;
            }
            url.last_modified_time = now_rfc3339();
            Ok(url.clone())
        });
        match result {
            Ok(url) => Reply::json(
                200,
                &self.function_url_json(&name, alias.as_deref(), &url, true),
            ),
            Err(r) => r,
        }
    }

    /// `DELETE /2021-10-31/functions/<id>/url`.
    pub fn delete_function_url_config(&self, id: &str, query: &BTreeMap<String, String>) -> Reply {
        let (name, alias) = match self.resolve_url_target(id, query) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            let slot = match &alias {
                Some(a) => {
                    &mut rec
                        .aliases
                        .get_mut(a)
                        .ok_or_else(|| self.alias_not_found(&name, a))?
                        .function_url
                }
                None => &mut rec.function_url,
            };
            slot.take()
                .map(|_| ())
                .ok_or_else(|| self.url_not_found(&name, alias.as_deref()))
        });
        match result {
            Ok(()) => Reply::empty(204),
            Err(r) => r,
        }
    }

    /// `GET /2021-10-31/functions/<id>/urls` (`$LATEST` and any alias URLs).
    pub fn list_function_url_configs(&self, id: &str) -> Reply {
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        let mut configs: Vec<Value> = rec
            .function_url
            .iter()
            .map(|u| self.function_url_json(&name, None, u, true))
            .collect();
        for alias in rec.aliases.values() {
            if let Some(u) = &alias.function_url {
                configs.push(self.function_url_json(&name, Some(&alias.name), u, true));
            }
        }
        Reply::json(200, &json!({ "FunctionUrlConfigs": configs }))
    }

    /// Serves one request addressed to a function URL: CORS preflight, the
    /// URL's AuthType, then a synchronous invoke with a payload 2.0 event.
    pub(crate) async fn serve_function_url(
        self: &Arc<Self>,
        req: &Request,
        target: &function_url::UrlTarget,
        raw_path: &str,
    ) -> Reply {
        let target = {
            let st = self.state.lock().unwrap();
            st.functions.values().find_map(|r| {
                if let Some(u) = r.function_url.as_ref().filter(|u| match target {
                    function_url::UrlTarget::Id(id) => &u.url_id == id,
                    function_url::UrlTarget::Function(name) => &r.function_name == name,
                }) {
                    return Some((r.function_name.clone(), None, u.clone()));
                }
                r.aliases.values().find_map(|a| {
                    a.function_url
                        .as_ref()
                        .filter(|u| match target {
                            function_url::UrlTarget::Id(id) => &u.url_id == id,
                            function_url::UrlTarget::Function(name) => {
                                format!("{}:{}", r.function_name, a.name) == *name
                            }
                        })
                        .map(|u| (r.function_name.clone(), Some(a.name.clone()), u.clone()))
                })
            })
        };
        let Some((name, alias, url)) = target else {
            return function_url::forbidden();
        };
        let origin = req.headers.get("origin").map(String::as_str).unwrap_or("");
        if let Some(cors) = &url.cors {
            if req.method == "OPTIONS" && req.headers.contains_key("access-control-request-method")
            {
                return function_url::preflight(cors, &req.headers);
            }
        }
        let iam_access_key = if url.auth_type == "AWS_IAM" {
            match function_url::verify_iam(req, &self.config) {
                Ok(key) => Some(key),
                Err(r) => return r,
            }
        } else {
            None
        };
        let url_id = url.url_id.as_str();
        let request_id = self.new_id();
        let event = function_url::build_event(
            req,
            &function_url::EventContext {
                url_id,
                raw_path,
                account_id: self.account_id(),
                request_id: &request_id,
                iam_access_key: iam_access_key.as_deref(),
                domain_name: format!("{url_id}.lambda-url.{}.localhost", self.region()),
            },
        );
        let payload = serde_json::to_vec(&event).unwrap_or_default();
        let mut query = BTreeMap::new();
        if let Some(a) = alias {
            query.insert("Qualifier".to_string(), a);
        }
        let invoked = self
            .invoke_pinned(&name, &query, "RequestResponse", "", &payload, Some(&url))
            .await;
        let reply = function_url::map_response(invoked);
        match &url.cors {
            Some(cors) => function_url::apply_cors(reply, cors, origin),
            None => reply,
        }
    }

    pub fn list_tags(&self, arn: &str) -> Reply {
        match self.lookup_arn(arn) {
            Ok(rec) => Reply::json(200, &json!({ "Tags": rec.tags })),
            Err(r) => r,
        }
    }

    pub fn tag_resource(&self, arn: &str, body: &[u8]) -> Reply {
        let req: Value = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match self.lookup_arn(arn) {
            Ok(r) => r.function_name,
            Err(r) => return r,
        };
        let tags = match string_map(req.get("Tags"), "tags") {
            Ok(t) => t,
            Err(r) => return r,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            rec.tags.extend(tags);
            Ok(())
        });
        match result {
            Ok(()) => Reply::empty(204),
            Err(r) => r,
        }
    }

    pub fn untag_resource(&self, arn: &str, keys: &[String]) -> Reply {
        let name = match self.lookup_arn(arn) {
            Ok(r) => r.function_name,
            Err(r) => return r,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            for k in keys {
                rec.tags.remove(k);
            }
            Ok(())
        });
        match result {
            Ok(()) => Reply::empty(204),
            Err(r) => r,
        }
    }

    pub fn account_settings(&self) -> Reply {
        let st = self.state.lock().unwrap();
        let fn_total: i64 = st
            .functions
            .values()
            .map(|f| f.code_size + f.versions.values().map(|v| v.code_size).sum::<i64>())
            .sum();
        let layer_total: i64 = st
            .layers
            .values()
            .flat_map(|l| l.versions.values())
            .map(|v| v.code_size)
            .sum();
        let reserved = st.total_reserved_concurrency();
        let unreserved = (ACCOUNT_CONCURRENT_EXECUTIONS - reserved).max(0);
        Reply::json(
            200,
            &json!({
                "AccountLimit": {
                    "TotalCodeSize": 80530636800i64,
                    "CodeSizeUnzipped": 262144000,
                    "CodeSizeZipped": 52428800,
                    "ConcurrentExecutions": ACCOUNT_CONCURRENT_EXECUTIONS,
                    "UnreservedConcurrentExecutions": unreserved,
                },
                "AccountUsage": {
                    "TotalCodeSize": fn_total + layer_total,
                    "FunctionCount": st.functions.len(),
                },
            }),
        )
    }

    /// `GET /_devcloud/functions/<name>/code.zip` — the `Code.Location` target.
    /// `expected_sha` (the `CodeSha256` query of a `Code.Location` link)
    /// pins the version; a link for replaced code gets 410 instead of the
    /// current package.
    pub fn code_package(&self, name: &str, expected_sha: Option<&str>) -> Reply {
        if !self.state.lock().unwrap().functions.contains_key(name) {
            return self.not_found(name);
        }
        // Hold the function lock while reading: code updates and deletes
        // retire the old zip under it, so the sha we read stays on disk.
        let lock = self.function_lock(name);
        let _serialized = lock.lock().unwrap();
        let sha = {
            let st = self.state.lock().unwrap();
            let Some(rec) = st.functions.get(name) else {
                return self.not_found(name);
            };
            match expected_sha {
                Some(want) if rec.references_code_sha(want) => want.to_string(),
                Some(_) => {
                    return Reply::error(
                        410,
                        "ResourceNotFoundException",
                        "This code location has expired: the function's code was updated. Call GetFunction for a new link.",
                    );
                }
                None => rec.code_sha256.clone(),
            }
        };
        match std::fs::read(self.code.zip_path(name, &sha)) {
            Ok(data) => Reply {
                status: 200,
                headers: Vec::new(),
                body: data,
                content_type: "application/zip",
            },
            Err(_) => Reply::error(500, "ServiceException", "deployment package is missing"),
        }
    }

    /// `GET /_introspect/invocations` — recent invocations, newest first.
    pub fn introspect_invocations(&self) -> Reply {
        let list: Vec<InvocationRecord> = self
            .invocations
            .lock()
            .unwrap()
            .iter()
            .rev()
            .cloned()
            .collect();
        Reply::json(200, &json!({ "invocations": list }))
    }

    // ---- invoke -------------------------------------------------------------

    /// `POST /2015-03-31/functions/<id>/invocations`.
    pub async fn invoke(
        self: &Arc<Self>,
        id: &str,
        query: &BTreeMap<String, String>,
        invocation_type: &str,
        log_type: &str,
        payload: &[u8],
    ) -> Reply {
        self.invoke_pinned(id, query, invocation_type, log_type, payload, None)
            .await
    }

    /// [`Self::invoke`], refused with 403 unless the function being run still
    /// carries `url` — the function URL configuration the request was
    /// authorized against. The check and the code lease happen under one
    /// state lock, so a URL request can never run a function that was
    /// deleted and recreated (or reconfigured) after its authorization.
    async fn invoke_pinned(
        self: &Arc<Self>,
        id: &str,
        query: &BTreeMap<String, String>,
        invocation_type: &str,
        log_type: &str,
        payload: &[u8],
        url: Option<&UrlConfig>,
    ) -> Reply {
        let invocation_type = if invocation_type.is_empty() {
            "RequestResponse"
        } else {
            invocation_type
        };
        if !matches!(invocation_type, "RequestResponse" | "Event" | "DryRun") {
            return validation(&format!(
                "1 validation error detected: Value '{invocation_type}' at 'invocationType' failed to satisfy constraint: Member must satisfy enum value set: [Event, RequestResponse, DryRun]"
            ));
        }
        let (record, leases) =
            match self.lookup_leased_for_invoke(id, query.get("Qualifier").map(String::as_str)) {
                Ok(r) => r,
                Err(r) => return r,
            };
        if let Some(url) = url {
            if record.function_url.as_ref() != Some(url) {
                return function_url::forbidden();
            }
        }
        let limit = if invocation_type == "Event" {
            MAX_ASYNC_PAYLOAD_BYTES
        } else {
            MAX_SYNC_PAYLOAD_BYTES
        };
        if payload.len() > limit {
            return Reply::error(
                413,
                "RequestTooLargeException",
                &format!(
                    "Request must be smaller than {limit} bytes for the InvokeFunction operation"
                ),
            );
        }
        // Only a truly empty payload means "no event" (the runtimes default it
        // to `{}`); anything else, whitespace included, must be JSON.
        if !payload.is_empty() {
            if let Err(e) = serde_json::from_slice::<Value>(payload) {
                return Reply::error(
                    400,
                    "InvalidRequestContentException",
                    &format!("Could not parse request body into json: {e}"),
                );
            }
        }
        // DryRun validates the request without running the handler, so it
        // must not depend on a local interpreter being available.
        if invocation_type == "DryRun" {
            return Reply::empty(204);
        }
        let permit = match self
            .concurrency
            .try_acquire(&record.function_name, record.reserved_concurrent_executions)
        {
            Ok(p) => p,
            Err(r) => return r,
        };
        let image = record.is_image().then(|| ImageSpec {
            uri: record.image_uri.clone(),
            entry_point: record.image_config.entry_point.clone(),
            command: record.image_config.command.clone(),
            working_directory: record.image_config.working_directory.clone(),
        });
        if image.is_some() && self.config.interpreters.docker.is_none() {
            return Reply::error(
                502,
                "ServiceException",
                "devcloud runs container image functions with Docker: enable services.lambda.docker (DEVCLOUD_LAMBDA_DOCKER=true for devcloud-lambda)",
            );
        }
        if image.is_none() && runtime::family(&record.runtime).is_none() {
            return Reply::error(
                502,
                "InvalidRuntimeException",
                &format!(
                    "devcloud cannot execute runtime {} locally (supported: python3.x, nodejs*.x)",
                    record.runtime
                ),
            );
        }
        if image.is_none()
            && !self
                .code
                .tree_path(&record.function_name, &record.code_sha256)
                .is_dir()
        {
            return Reply::error(
                502,
                "ServiceException",
                "the function's deployment package is missing from local storage: update the function code to restore it",
            );
        }

        let layer_dirs: Vec<PathBuf> = leases
            .layers
            .iter()
            .map(|l| l.dir().to_path_buf())
            .collect();
        let request_id = self.new_id();
        let executed_version = record.version.clone();
        let inv = Invocation {
            runtime: record.runtime.clone(),
            handler: record.handler.clone(),
            code_dir: leases.code.dir().to_path_buf(),
            function_name: record.function_name.clone(),
            function_arn: self
                .qualified_function_arn(&record.function_name, record.arn_qualifier.as_deref()),
            function_version: executed_version.clone(),
            memory_size: record.memory_size,
            timeout_seconds: record.timeout,
            region: self.region().to_string(),
            request_id: request_id.clone(),
            environment: record.environment.clone(),
            payload: payload.to_vec(),
            opt_dir: self
                .config
                .opt_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("/opt")),
            layer_dirs,
            credentials: self.config.function_credentials.clone(),
            env_key: record.revision_id.clone(),
            image,
        };

        if invocation_type == "Event" {
            let server = Arc::clone(self);
            let mut bg = self.background.lock().unwrap();
            if bg.closed {
                return Reply::error(503, "ServiceException", "devcloud lambda is shutting down");
            }
            while bg.tasks.try_join_next().is_some() {}
            bg.tasks.spawn(async move {
                let _ = server.execute(inv, "Event", leases, permit).await;
            });
            return Reply::empty(202).header("X-Amzn-RequestId", &request_id);
        }

        match self.execute(inv, "RequestResponse", leases, permit).await {
            Ok((outcome, log)) => {
                let (body, error) = match outcome {
                    Outcome::Success(b) => (b, false),
                    Outcome::FunctionError(b) => (b, true),
                };
                let mut reply = Reply {
                    status: 200,
                    headers: Vec::new(),
                    body,
                    content_type: "application/json",
                }
                .header("X-Amz-Executed-Version", &executed_version)
                .header("X-Amzn-RequestId", &request_id);
                if error {
                    reply = reply.header("X-Amz-Function-Error", "Unhandled");
                }
                if log_type == "Tail" {
                    reply = reply.header("X-Amz-Log-Result", &log_tail_b64(&log));
                }
                reply
            }
            Err(RuntimeError::Unsupported(rt)) => Reply::error(
                502,
                "InvalidRuntimeException",
                &format!("devcloud cannot execute runtime {rt} locally"),
            ),
            Err(RuntimeError::Spawn(msg)) => Reply::error(502, "ServiceException", &msg),
        }
    }

    /// Runs one invocation. `leases` pins the code and layer trees it started
    /// with until the handler is done (or, for a new environment, until the
    /// environment stops), even if the code or layers are updated meanwhile.
    async fn execute(
        &self,
        inv: Invocation,
        invocation_type: &str,
        leases: LeaseBundle,
        _permit: ConcurrencyPermit,
    ) -> Result<(Outcome, String), RuntimeError> {
        let started_at = now_rfc3339();
        let result = runtime::run(
            &inv,
            &self.config.interpreters,
            &self.pool,
            Box::new(leases),
        )
        .await;
        let (status, duration_ms, log) = match &result {
            Ok(done) => (
                match done.outcome {
                    Outcome::Success(_) => "Success",
                    Outcome::FunctionError(_) => "Unhandled",
                },
                done.duration.as_secs_f64() * 1000.0,
                done.log.clone(),
            ),
            Err(RuntimeError::Unsupported(_)) => ("InvalidRuntime", 0.0, String::new()),
            Err(RuntimeError::Spawn(msg)) => ("SpawnFailed", 0.0, format!("{msg}\n")),
        };
        if self.config.log_invocations && !log.is_empty() {
            // One write per invocation, so concurrent invocations do not
            // interleave mid-line.
            use std::io::Write;
            let out = tagged_log(&inv.function_name, &log);
            let _ = std::io::stdout().lock().write_all(out.as_bytes());
        }
        self.record_invocation(InvocationRecord {
            request_id: inv.request_id.clone(),
            function_name: inv.function_name.clone(),
            invocation_type: invocation_type.to_string(),
            status: status.to_string(),
            duration_ms,
            started_at,
            log_tail: tail(&log, LOG_TAIL_BYTES).to_string(),
        });
        emit(
            "lambda.function.invoked",
            json!({
                "functionName": inv.function_name,
                "requestId": inv.request_id,
                "invocationType": invocation_type,
                "status": status,
            }),
        );
        result.map(|done| (done.outcome, done.log))
    }

    fn record_invocation(&self, rec: InvocationRecord) {
        let mut list = self.invocations.lock().unwrap();
        if list.len() >= MAX_INVOCATION_RECORDS {
            list.pop_front();
        }
        list.push_back(rec);
    }

    // ---- helpers ------------------------------------------------------------

    /// Stops accepting async invocations, then aborts and awaits the running
    /// ones so each tears down its handler process group.
    pub async fn shutdown(&self) {
        // Package jobs are not cancellable midway: let running ones finish
        // so nothing touches storage after `serve` returns.
        *self.blocking_gate.write().await = true;
        let mut tasks = {
            let mut bg = self.background.lock().unwrap();
            bg.closed = true;
            std::mem::take(&mut bg.tasks)
        };
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        self.pool.close().await;
    }

    /// Runs `job` on the blocking pool, tracked so that `shutdown` waits for
    /// it. Dropping the returned future does not stop the job.
    pub(crate) async fn run_blocking(
        self: &Arc<Self>,
        job: impl FnOnce(&Server) -> Reply + Send + 'static,
    ) -> Reply {
        let gate = Arc::clone(&self.blocking_gate).read_owned().await;
        if *gate {
            return Reply::error(503, "ServiceException", "devcloud lambda is shutting down");
        }
        let server = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _gate = gate;
            job(&server)
        })
        .await
        .unwrap_or_else(|_| Reply::error(500, "ServiceException", "internal error"))
    }

    /// Removes function containers that a previous, killed run of this
    /// instance (same storage) left behind. Call once at startup, before
    /// serving; a no-op without Docker.
    pub async fn remove_orphaned_containers(&self) {
        let Some(docker) = self.config.interpreters.docker.as_deref() else {
            return;
        };
        match runtime::remove_orphaned_containers(docker, self.pool.owner()).await {
            Ok(0) => {}
            Ok(n) => eprintln!(
                "devcloud-lambda: removed {n} function container(s) left by a previous run"
            ),
            Err(e) => eprintln!(
                "devcloud-lambda: warning: could not look for containers left by a previous run: {e}"
            ),
        }
    }

    /// Warm execution environments currently idle for `function`.
    pub fn idle_environments(&self, function: &str) -> usize {
        self.pool.idle_count(function)
    }

    pub(crate) fn not_found(&self, name: &str) -> Reply {
        Reply::error(
            404,
            "ResourceNotFoundException",
            &format!("Function not found: {}", self.function_arn(name)),
        )
    }

    /// Resolves a function identifier that must address the unqualified
    /// function (or `$LATEST`), rejecting published version/alias qualifiers.
    pub(crate) fn lookup_unqualified_or_latest(&self, id: &str) -> Result<String, Reply> {
        let qualifier = effective_qualifier(id, None)?;
        if let Some(q) = qualifier.as_deref() {
            if q != "$LATEST" {
                return Err(invalid_param(&format!(
                    "This operation does not support qualifier '{q}'; specify the unqualified function name or ARN"
                )));
            }
        }
        self.lookup(id, None).map(|r| r.function_name)
    }

    /// Resolves a function identifier (name, `name:qualifier`, partial or full
    /// ARN) to its resolved view (`$LATEST`, published version, or alias).
    pub(crate) fn lookup(
        &self,
        id: &str,
        qualifier: Option<&str>,
    ) -> Result<ResolvedFunction, Reply> {
        let st = self.state.lock().unwrap();
        self.resolve(&st, id, qualifier, false)
    }

    /// Like [`Self::lookup`], but selects weighted alias targets for `Invoke`
    /// and leases the function's code tree and attached layer trees under one
    /// state lock, so concurrent updates cannot retire them first.
    fn lookup_leased_for_invoke(
        &self,
        id: &str,
        qualifier: Option<&str>,
    ) -> Result<(ResolvedFunction, LeaseBundle), Reply> {
        let st = self.state.lock().unwrap();
        let record = self.resolve(&st, id, qualifier, true)?;
        let code = self.code.lease(&record.function_name, &record.code_sha256);
        let layers = record
            .layers
            .iter()
            .map(|l| self.layer_code.lease(&l.layer_name, &l.code_sha256))
            .collect();
        Ok((record, LeaseBundle { code, layers }))
    }

    fn resolve(
        &self,
        st: &PersistedState,
        id: &str,
        qualifier: Option<&str>,
        apply_routing: bool,
    ) -> Result<ResolvedFunction, Reply> {
        // Only a Lambda *function* ARN may name a function: `arn:aws:cloudwatch:
        // ...:alarm:demo` must not resolve to the local function `demo`.
        if id.starts_with("arn:") && !is_function_arn(id) {
            return Err(validation(&format!(
                "1 validation error detected: Value '{id}' at 'functionName' failed to satisfy constraint: Member must satisfy regular expression pattern: (arn:(aws[a-zA-Z-]*)?:lambda:)?([a-z]{{2}}(-gov)?-[a-z]+-\\d{{1}}:)?(\\d{{12}}:)?(function:)?([a-zA-Z0-9-_\\.]+)(:(\\$LATEST|[a-zA-Z0-9-_]+))?"
            )));
        }
        let (name, _) = parse_function_id(id);
        // An ARN names one account and region; one for another scope must not
        // resolve to the same-named local function.
        let (region, account) = function_id_scope(id);
        if region.is_some_and(|r| r != self.region())
            || account.is_some_and(|a| a != self.account_id())
        {
            return Err(Reply::error(
                404,
                "ResourceNotFoundException",
                &format!("Function not found: {id}"),
            ));
        }
        let qualifier = effective_qualifier(id, qualifier)?;
        let rec = st
            .functions
            .get(&name)
            .ok_or_else(|| self.not_found(&name))?;
        let Some(q) = qualifier else {
            return Ok(ResolvedFunction::from_latest(
                rec,
                None,
                rec.function_url.clone(),
            ));
        };
        if q == "$LATEST" {
            return Ok(ResolvedFunction::from_latest(
                rec,
                Some("$LATEST".to_string()),
                rec.function_url.clone(),
            ));
        }
        if let Ok(ver_num) = q.parse::<u64>() {
            let ver = rec.versions.get(&ver_num).ok_or_else(|| {
                Reply::error(
                    404,
                    "ResourceNotFoundException",
                    &format!("Function not found: {}:{q}", self.function_arn(&name)),
                )
            })?;
            return Ok(ResolvedFunction::from_version(rec, ver, Some(q), None));
        }
        let alias = rec.aliases.get(&q).ok_or_else(|| {
            Reply::error(
                404,
                "ResourceNotFoundException",
                &format!("Function not found: {}:{q}", self.function_arn(&name)),
            )
        })?;
        let target_ver = if apply_routing {
            self.select_alias_version(&name, alias)
        } else {
            alias.function_version.clone()
        };
        if target_ver == "$LATEST" {
            return Ok(ResolvedFunction::from_latest(
                rec,
                Some(alias.name.clone()),
                alias.function_url.clone(),
            ));
        }
        let ver_num = target_ver.parse::<u64>().ok();
        let ver = ver_num.and_then(|n| rec.versions.get(&n)).ok_or_else(|| {
            Reply::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "Function not found: {}:{target_ver}",
                    self.function_arn(&name)
                ),
            )
        })?;
        Ok(ResolvedFunction::from_version(
            rec,
            ver,
            Some(alias.name.clone()),
            alias.function_url.clone(),
        ))
    }

    fn lookup_arn(&self, arn: &str) -> Result<ResolvedFunction, Reply> {
        if !arn.starts_with("arn:aws:lambda:") {
            return Err(validation(&format!(
                "1 validation error detected: Value '{arn}' at 'resource' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:(aws[a-zA-Z-]*):lambda:.*"
            )));
        }
        // Tags belong to the function, not to a version or alias.
        if parse_function_id(arn).1.is_some() {
            return Err(invalid_param(
                "Tags on function aliases and versions are not supported. Please specify a function ARN.",
            ));
        }
        self.lookup(arn, None)
    }

    /// Decodes the deployment package from `Code` or `Content` (`ZipFile`
    /// base64, or an `S3Bucket`/`S3Key` object in the shared local S3 store).
    pub(crate) fn package_bytes(&self, code: Option<&Value>) -> Result<Vec<u8>, Reply> {
        let code = code.ok_or_else(|| {
            validation("1 validation error detected: Value null at 'code' failed to satisfy constraint: Member must not be null")
        })?;
        let zip = if let Some(b64) = code.get("ZipFile").and_then(Value::as_str) {
            base64::engine::general_purpose::STANDARD
                .decode(b64.trim())
                .map_err(|_| invalid_param("Could not decode ZipFile: invalid base64"))?
        } else if let Some(bucket) = code.get("S3Bucket").and_then(Value::as_str) {
            let key = code.get("S3Key").and_then(Value::as_str).unwrap_or("");
            let root = self.config.object_store_root.as_ref().ok_or_else(|| {
                invalid_param("Code.S3Bucket requires the devcloud S3 service to be enabled")
            })?;
            // Honor S3ObjectVersion: deploying the latest object instead of
            // the requested version would silently run different code.
            let version = code
                .get("S3ObjectVersion")
                .and_then(Value::as_str)
                .unwrap_or("");
            let store = devcloud_s3::store::FileBucketStore::new(root.clone());
            match store.get_object_version(bucket, key, version) {
                // A delete marker has no package: report it as missing.
                Ok(Some((object, data))) if !object.delete_marker => data,
                _ if !version.is_empty() => {
                    return Err(invalid_param(&format!(
                        "Error occurred while GetObject. S3 Error Code: NoSuchVersion. S3 Error Message: The specified version does not exist. (bucket={bucket}, key={key}, version={version})"
                    )))
                }
                _ => {
                    return Err(invalid_param(&format!(
                        "Error occurred while GetObject. S3 Error Code: NoSuchKey. S3 Error Message: The specified key does not exist. (bucket={bucket}, key={key})"
                    )))
                }
            }
        } else if code.get("ImageUri").is_some() {
            return Err(invalid_param(
                "ImageUri is supported only for the Image package type",
            ));
        } else {
            return Err(invalid_param("Please provide a source for function code."));
        };
        // The 50 MB cap is AWS's limit for direct uploads; packages from S3
        // are bounded by their unzipped size (checked by `read_entries`).
        let direct_upload = code.get("ZipFile").is_some();
        if direct_upload && zip.len() > MAX_ZIP_BYTES {
            return Err(Reply::error(
                413,
                "RequestEntityTooLargeException",
                "Request must be smaller than 52428800 bytes for the CreateFunction operation",
            ));
        }
        crate::zip::read_entries(&zip).map_err(|e| invalid_param(&e))?;
        Ok(zip)
    }

    fn configuration_json(&self, r: &ResolvedFunction) -> Value {
        let mut v = json!({
            "FunctionName": r.function_name,
            "FunctionArn": self.qualified_function_arn(&r.function_name, r.arn_qualifier.as_deref()),
            "Runtime": r.runtime,
            "Role": r.role,
            "Handler": r.handler,
            "CodeSize": r.code_size,
            "Description": r.description,
            "Timeout": r.timeout,
            "MemorySize": r.memory_size,
            "LastModified": r.last_modified,
            "CodeSha256": r.code_sha256,
            "Version": r.version,
            "TracingConfig": { "Mode": "PassThrough" },
            "RevisionId": r.revision_id,
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "PackageType": r.package_type,
            "Architectures": r.architectures,
            "EphemeralStorage": { "Size": r.ephemeral_storage_size },
            "LoggingConfig": {
                "LogFormat": "Text",
                "LogGroup": format!("/aws/lambda/{}", r.function_name),
            },
        });
        if !r.environment.is_empty() {
            v["Environment"] = json!({ "Variables": r.environment });
        }
        if !r.layers.is_empty() {
            v["Layers"] = Value::Array(r.layers.iter().map(AttachedLayer::summary_json).collect());
        }
        if r.is_image() {
            if let Some(obj) = v.as_object_mut() {
                obj.remove("Runtime");
                obj.remove("Handler");
            }
            if !r.image_config.is_empty() {
                v["ImageConfigResponse"] = json!({ "ImageConfig": r.image_config });
            }
        }
        v
    }
}

/// Resolves the qualifier from `id` and `query_qualifier`, rejecting mismatches
/// when both are specified.
pub(crate) fn effective_qualifier(
    id: &str,
    query_qualifier: Option<&str>,
) -> Result<Option<String>, Reply> {
    let (_, id_q) = parse_function_id(id);
    let q_q = query_qualifier.filter(|q| !q.is_empty());
    match (id_q.as_deref(), q_q) {
        (Some(a), Some(b)) if a != b => Err(invalid_param(&format!(
            "The qualifier in the function name ({a}) and the qualifier parameter ({b}) must match"
        ))),
        (Some(a), _) => Ok(Some(a.to_string())),
        (None, Some(b)) => Ok(Some(b.to_string())),
        (None, None) => Ok(None),
    }
}

pub(crate) fn parse_pagination(query: &BTreeMap<String, String>) -> Result<(usize, usize), Reply> {
    let max_items = match query.get("MaxItems") {
        Some(v) => match v.parse::<usize>() {
            Ok(n) if (1..=10000).contains(&n) => n.min(DEFAULT_LIST_MAX_ITEMS),
            _ => return Err(validation("MaxItems must be between 1 and 10000")),
        },
        None => DEFAULT_LIST_MAX_ITEMS,
    };
    let start = match query.get("Marker") {
        Some(m) if !m.is_empty() => match m.parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Err(invalid_param("Invalid marker")),
        },
        _ => 0,
    };
    Ok((start, max_items))
}

fn validate_alias_name(name: &str) -> Result<(), Reply> {
    if name.is_empty() {
        return Err(validation("1 validation error detected: Value null at 'name' failed to satisfy constraint: Member must not be null"));
    }
    let valid_chars = name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    let all_digits = name.bytes().all(|b| b.is_ascii_digit());
    if !valid_chars || all_digits {
        return Err(validation(&format!(
            "1 validation error detected: Value '{name}' at 'name' failed to satisfy constraint: Member must satisfy regular expression pattern: (?!^[0-9]+$)([a-zA-Z0-9-_]+)"
        )));
    }
    Ok(())
}

fn parse_routing_config(req: &Value) -> Result<Option<BTreeMap<String, f64>>, Reply> {
    let Some(rc) = req.get("RoutingConfig") else {
        return Ok(None);
    };
    if rc.is_null() {
        return Ok(Some(BTreeMap::new()));
    }
    let Some(obj) = rc.as_object() else {
        return Err(validation("RoutingConfig must be an object"));
    };
    let Some(weights) = obj.get("AdditionalVersionWeights") else {
        return Ok(Some(BTreeMap::new()));
    };
    if weights.is_null() {
        return Ok(Some(BTreeMap::new()));
    }
    let Some(map) = weights.as_object() else {
        return Err(validation(
            "RoutingConfig.AdditionalVersionWeights must be a map of version to weight",
        ));
    };
    let mut out = BTreeMap::new();
    for (k, v) in map {
        let Some(w) = v.as_f64() else {
            return Err(validation(&format!(
                "RoutingConfig.AdditionalVersionWeights.{k} must be a number"
            )));
        };
        out.insert(k.clone(), w);
    }
    Ok(Some(out))
}

/// `name`, `name:qualifier`, `123456789012:function:name[:q]`, or a full ARN.
/// `arn:aws*:lambda:<region>:<account>:function:<name>[:<qualifier>]`.
fn is_function_arn(id: &str) -> bool {
    let parts: Vec<&str> = id.split(':').collect();
    matches!(parts.len(), 7 | 8)
        && parts[0] == "arn"
        && parts[1].starts_with("aws")
        && parts[2] == "lambda"
        && parts[5] == "function"
        && !parts[6].is_empty()
        && parts.get(7).is_none_or(|q| !q.is_empty())
}

/// The `(region, account)` an identifier is scoped to: both for a full ARN,
/// the account for `123456789012:function:name`, neither for a plain name.
fn function_id_scope(id: &str) -> (Option<&str>, Option<&str>) {
    let parts: Vec<&str> = id.split(':').collect();
    if id.starts_with("arn:") && parts.len() >= 7 {
        (Some(parts[3]), Some(parts[4]))
    } else if parts.len() >= 3 && parts[1] == "function" {
        (None, Some(parts[0]))
    } else {
        (None, None)
    }
}

fn parse_function_id(id: &str) -> (String, Option<String>) {
    let parts: Vec<&str> = id.split(':').collect();
    let (name, qualifier) = if id.starts_with("arn:") && parts.len() >= 7 {
        (parts[6], parts.get(7).copied())
    } else if parts.len() >= 3 && parts[1] == "function" {
        (parts[2], parts.get(3).copied())
    } else if parts.len() == 2 {
        (parts[0], Some(parts[1]))
    } else {
        (id, None)
    };
    (name.to_string(), qualifier.map(str::to_string))
}

pub(crate) fn code_sha256(zip: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(Sha256::digest(zip))
}

/// Percent-encodes a query value (base64 `+`, `/`, `=` included).
pub(crate) fn query_encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn log_tail_b64(log: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(tail(log, LOG_TAIL_BYTES))
}

fn tail(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut start = s.len() - max;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

pub(crate) fn parse_json(body: &[u8]) -> Result<Value, Reply> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(v) if v.is_object() => Ok(v),
        _ => Err(Reply::error(
            400,
            "InvalidRequestContentException",
            "Could not parse request body into json",
        )),
    }
}

pub(crate) fn str_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn int_field(
    v: &Value,
    key: &str,
    default: i64,
    min: i64,
    max: i64,
    wire: &str,
) -> Result<i64, Reply> {
    Ok(opt_int_field(v, key, min, max, wire)?.unwrap_or(default))
}

fn opt_int_field(
    v: &Value,
    key: &str,
    min: i64,
    max: i64,
    wire: &str,
) -> Result<Option<i64>, Reply> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(val) => match val.as_i64() {
            Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
            _ => Err(validation(&format!(
                "1 validation error detected: Value '{val}' at '{wire}' failed to satisfy constraint: Member must have value between {min} and {max}"
            ))),
        },
    }
}

/// A `{string: string}` map. Absent / null is empty; any other shape, or a
/// non-string value, is a 400 — never silently dropped (an update would
/// otherwise wipe the stored map).
fn string_map(v: Option<&Value>, field: &str) -> Result<BTreeMap<String, String>, Reply> {
    let Some(v) = v.filter(|v| !v.is_null()) else {
        return Ok(BTreeMap::new());
    };
    let Some(obj) = v.as_object() else {
        return Err(invalid_param(&format!(
            "{field} must be a map of string keys to string values"
        )));
    };
    obj.iter()
        .map(|(k, v)| match v.as_str() {
            Some(s) => Ok((k.clone(), s.to_string())),
            None => Err(invalid_param(&format!(
                "{field}.{k} must be a string, got {v}"
            ))),
        })
        .collect()
}

/// `Environment.Variables`, rejecting Lambda-reserved keys. `Ok(None)` when the
/// request carries no `Environment` at all (leave unchanged on update).
fn env_vars(req: &Value) -> Result<Option<BTreeMap<String, String>>, Reply> {
    let Some(env) = req.get("Environment") else {
        return Ok(None);
    };
    if !env.is_null() && !env.is_object() {
        return Err(invalid_param("Environment must be an object"));
    }
    let vars = string_map(env.get("Variables"), "Environment.Variables")?;
    // Lambda's key pattern (single letters allowed, as devcloud always did).
    // It also keeps names from smuggling extra lines (or `=`) into a
    // container's env-file.
    if let Some(bad) = vars.keys().find(|k| !valid_env_key(k)) {
        return Err(validation(&format!(
            "1 validation error detected: Value '{bad}' at 'environment.variables' failed to satisfy constraint: Map keys must satisfy constraint: [Member must satisfy regular expression pattern: [a-zA-Z]([a-zA-Z0-9_])+]"
        )));
    }
    let reserved: Vec<&str> = vars
        .keys()
        .map(String::as_str)
        .filter(|k| RESERVED_ENV_KEYS.contains(k))
        .collect();
    if !reserved.is_empty() {
        return Err(invalid_param(&format!(
            "Lambda was unable to configure your environment variables because the environment variables you have provided contains reserved keys that are currently not supported for modification. Reserved keys used in this request: {}",
            reserved.join(", ")
        )));
    }
    Ok(Some(vars))
}

/// `[a-zA-Z][a-zA-Z0-9_]*`: Lambda's environment variable key pattern, minus
/// its two-character minimum.
fn valid_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `EphemeralStorage.Size` (MiB, 512–10240). `Ok(None)` when absent.
fn ephemeral_storage(req: &Value) -> Result<Option<i64>, Reply> {
    let Some(storage) = req.get("EphemeralStorage").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    match storage.get("Size").and_then(Value::as_i64) {
        Some(n) if (512..=10240).contains(&n) => Ok(Some(n)),
        _ => Err(validation(&format!(
            "1 validation error detected: Value '{}' at 'ephemeralStorage.size' failed to satisfy constraint: Member must have value between 512 and 10240",
            storage.get("Size").unwrap_or(&Value::Null)
        ))),
    }
}

/// `Architectures`: exactly one of `x86_64` / `arm64`. `Ok(None)` when absent.
fn architectures(req: &Value) -> Result<Option<Vec<String>>, Reply> {
    let Some(value) = req.get("Architectures").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    match value.as_array().map(Vec::as_slice) {
        Some([arch]) if matches!(arch.as_str(), Some("x86_64" | "arm64")) => {
            Ok(Some(vec![arch.as_str().unwrap_or_default().to_string()]))
        }
        _ => Err(validation(&format!(
            "1 validation error detected: Value '{value}' at 'architectures' failed to satisfy constraint: Member must have length less than or equal to 1, Member must satisfy enum value set: [x86_64, arm64]"
        ))),
    }
}

fn validate_function_name(name: &str) -> Result<(), Reply> {
    if name.is_empty() {
        return Err(validation("1 validation error detected: Value null at 'functionName' failed to satisfy constraint: Member must not be null"));
    }
    let valid = name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !valid {
        return Err(validation(&format!(
            "1 validation error detected: Value '{name}' at 'functionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9-_]{{1,64}}"
        )));
    }
    Ok(())
}

/// `Code.ImageUri` of an Image function; zip sources are refused.
fn image_uri(code: Option<&Value>) -> Result<String, Reply> {
    let code = code.ok_or_else(|| {
        validation("1 validation error detected: Value null at 'code' failed to satisfy constraint: Member must not be null")
    })?;
    if code.get("ZipFile").is_some() || code.get("S3Bucket").is_some() {
        return Err(invalid_param(
            "ZipFile and S3Bucket are not supported for the Image package type",
        ));
    }
    match code.get("ImageUri").and_then(Value::as_str).map(str::trim) {
        Some(uri) if !uri.is_empty() => Ok(uri.to_string()),
        _ => Err(invalid_param(
            "ImageUri is required for the Image package type",
        )),
    }
}

/// `ImageConfig`, if the request has one; only Image functions take it.
fn image_config(req: &Value, is_image: bool) -> Result<Option<ImageConfig>, Reply> {
    let Some(value) = req.get("ImageConfig").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    if !is_image {
        return Err(invalid_param(
            "ImageConfig is supported only for the Image package type",
        ));
    }
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|e| invalid_param(&format!("Invalid ImageConfig: {e}")))
}

pub(crate) fn validate_runtime(runtime_id: &str) -> Result<(), Reply> {
    if runtime_id.is_empty() {
        return Err(invalid_param("Runtime is required for Zip package type"));
    }
    if KNOWN_RUNTIME_PREFIXES
        .iter()
        .any(|p| runtime_id.starts_with(p))
    {
        return Ok(());
    }
    Err(invalid_param(&format!(
        "Value {runtime_id} at 'runtime' failed to satisfy constraint: Member must satisfy enum value set"
    )))
}

fn emit(event_type: &str, payload: Value) {
    let event = json!({
        "type": event_type,
        "service": "lambda",
        "payload": payload,
    });
    if let Some(tx) = crate::event_sink() {
        let _ = tx.send(event.to_string());
    }
}

/// This instance's id: kept in `<storage>/instance-id` so it survives
/// restarts (and identifies containers a killed run left behind), distinct
/// per storage so instances sharing a Docker daemon never touch each
/// other's containers. Without storage the id is per process.
fn instance_id(storage: &str) -> String {
    let fresh = || {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        hex::encode(&Sha256::digest(format!("{nanos}:{}", std::process::id()).as_bytes())[..8])
    };
    if storage.is_empty() {
        return fresh();
    }
    let path = PathBuf::from(storage).join("instance-id");
    if let Ok(id) = std::fs::read_to_string(&path) {
        let id = id.trim();
        if !id.is_empty() && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return id.to_string();
        }
    }
    let id = fresh();
    let _ = std::fs::create_dir_all(storage);
    let _ = std::fs::write(&path, &id);
    id
}

/// An invocation log for devcloud's stdout: every line tagged with the
/// function, like a CloudWatch log stream name.
fn tagged_log(function: &str, log: &str) -> String {
    log.lines()
        .map(|line| format!("[{function}] {line}\n"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A package job aborted with its connection still finishes before
    /// `shutdown` returns, and none starts afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_for_package_jobs() {
        let server = Arc::new(Server::new(Config::default()));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (started_tx, started) = std::sync::mpsc::channel();
        let job = {
            let (server, done) = (Arc::clone(&server), Arc::clone(&done));
            tokio::spawn(async move {
                server
                    .run_blocking(move |_| {
                        started_tx.send(()).unwrap();
                        std::thread::sleep(std::time::Duration::from_millis(300));
                        done.store(true, Ordering::SeqCst);
                        Reply::empty(200)
                    })
                    .await
            })
        };
        tokio::task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();
        // The connection goes away mid-job, as on shutdown.
        job.abort();
        server.shutdown().await;
        assert!(
            done.load(Ordering::SeqCst),
            "shutdown returned before the job ended"
        );
        let late = server.run_blocking(|_| Reply::empty(200)).await;
        assert_eq!(late.status, 503);
    }

    #[test]
    fn stdout_logs_tag_every_line_with_the_function() {
        let log = "START RequestId: r Version: $LATEST\nhello\nEND RequestId: r\nREPORT RequestId: r\tDuration: 1.00 ms\n";
        assert_eq!(
            tagged_log("fn", log),
            "[fn] START RequestId: r Version: $LATEST\n[fn] hello\n[fn] END RequestId: r\n[fn] REPORT RequestId: r\tDuration: 1.00 ms\n"
        );
    }

    /// A URL request authorized against one function must not run another:
    /// the function deleted and recreated (with or without a new URL) while
    /// the request was in flight, or its URL reconfigured.
    #[tokio::test]
    async fn url_invocations_run_only_the_function_they_were_authorized_for() {
        let dir = std::env::temp_dir().join(format!(
            "devcloud-lambda-pinned-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let server = Arc::new(Server::new(Config {
            storage_path: dir.to_string_lossy().into_owned(),
            ..Config::default()
        }));
        let zip = crate::zip::build_stored(&[("app.py", b"def handler(e, c):\n    return 1\n")]);
        let create = || {
            json!({
                "FunctionName": "pinned", "Runtime": "python3.12", "Role": "r",
                "Handler": "app.handler",
                "Code": { "ZipFile": base64::engine::general_purpose::STANDARD.encode(&zip) },
            })
            .to_string()
        };
        let none = json!({ "AuthType": "NONE" }).to_string();
        let no_query = BTreeMap::new();
        let url_of = |server: &Server| {
            server.state.lock().unwrap().functions["pinned"]
                .function_url
                .clone()
        };
        let dry_run = |url: UrlConfig| {
            let server = Arc::clone(&server);
            async move {
                server
                    .invoke_pinned("pinned", &BTreeMap::new(), "DryRun", "", b"{}", Some(&url))
                    .await
                    .status
            }
        };

        assert_eq!(server.create_function(create().as_bytes()).status, 201);
        assert_eq!(
            server
                .create_function_url_config("pinned", &no_query, none.as_bytes())
                .status,
            201
        );
        let authorized = url_of(&server).unwrap();
        assert_eq!(
            dry_run(authorized.clone()).await,
            204,
            "same function, same URL"
        );

        // Deleted and recreated without a URL.
        assert_eq!(server.delete_function("pinned", &no_query).status, 204);
        assert_eq!(server.create_function(create().as_bytes()).status, 201);
        assert_eq!(dry_run(authorized.clone()).await, 403);
        // ... or recreated with a URL of its own.
        assert_eq!(
            server
                .create_function_url_config("pinned", &no_query, none.as_bytes())
                .status,
            201
        );
        assert_eq!(dry_run(authorized).await, 403);

        // The URL reconfigured (NONE -> AWS_IAM) mid-request.
        let current = url_of(&server).unwrap();
        let iam = json!({ "AuthType": "AWS_IAM" }).to_string();
        assert_eq!(
            server
                .update_function_url_config("pinned", &no_query, iam.as_bytes())
                .status,
            200
        );
        assert_eq!(dry_run(current).await, 403);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn parses_function_identifiers() {
        assert_eq!(parse_function_id("fn"), ("fn".into(), None));
        assert_eq!(
            parse_function_id("fn:$LATEST"),
            ("fn".into(), Some("$LATEST".into()))
        );
        assert_eq!(
            parse_function_id("arn:aws:lambda:us-east-1:000000000000:function:fn"),
            ("fn".into(), None)
        );
        assert_eq!(
            parse_function_id("arn:aws:lambda:us-east-1:000000000000:function:fn:prod"),
            ("fn".into(), Some("prod".into()))
        );
        assert_eq!(
            parse_function_id("000000000000:function:fn"),
            ("fn".into(), None)
        );
    }

    #[test]
    fn only_lambda_function_arns_qualify() {
        assert!(is_function_arn(
            "arn:aws:lambda:us-east-1:000000000000:function:f"
        ));
        assert!(is_function_arn(
            "arn:aws:lambda:us-east-1:000000000000:function:f:$LATEST"
        ));
        assert!(!is_function_arn(
            "arn:aws:cloudwatch:us-east-1:000000000000:alarm:f"
        ));
        assert!(!is_function_arn(
            "arn:aws:lambda:us-east-1:000000000000:layer:f"
        ));
        assert!(!is_function_arn(
            "arn:aws:lambda:us-east-1:000000000000:function:"
        ));
        assert!(!is_function_arn(
            "arn:aws:lambda:us-east-1:000000000000:function:f:1:extra"
        ));
    }

    #[test]
    fn tail_respects_char_boundaries() {
        let s = "ああああ";
        let t = tail(s, 4);
        assert_eq!(t, "あ");
    }

    #[test]
    fn rejects_reserved_env_keys() {
        let req = json!({ "Environment": { "Variables": { "AWS_REGION": "x", "OK": "y" } } });
        assert!(env_vars(&json!({ "Environment": { "Variables": { "STAGE": 42 } } })).is_err());
        assert!(env_vars(&json!({ "Environment": { "Variables": ["STAGE"] } })).is_err());
        assert!(env_vars(&json!({ "Environment": "x" })).is_err());
        assert_eq!(
            env_vars(&json!({ "Environment": {} })).unwrap(),
            Some(BTreeMap::new())
        );
        let err = env_vars(&req).unwrap_err();
        assert_eq!(err.status, 400);
        assert!(String::from_utf8_lossy(&err.body).contains("AWS_REGION"));
    }
}
