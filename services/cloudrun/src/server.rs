//! Cloud Run Admin API v2 control plane.
//!
//! Services, revisions and IAM policies live in memory behind a `Mutex` and
//! persist to `state.json`; long-running operations are kept in memory only
//! (every mutation completes synchronously, so each returned Operation is
//! already `done`). Mutations are applied to a cloned state that is persisted
//! before it is swapped in. Resource JSON is stored as the wire shape so reads
//! echo user-supplied fields verbatim.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::instances::{LaunchSpec, Manager};
use crate::time_fmt::now_rfc3339;

const SERVICE_TYPE: &str = "type.googleapis.com/google.cloud.run.v2.Service";
const REVISION_TYPE: &str = "type.googleapis.com/google.cloud.run.v2.Revision";
const MAX_OPERATIONS: usize = 1000;
const DEFAULT_PAGE_SIZE: usize = 100;

/// Fields a client may set on a Service; everything else is output-only.
const WRITABLE_SERVICE_FIELDS: &[&str] = &[
    "description",
    "labels",
    "annotations",
    "client",
    "clientVersion",
    "ingress",
    "launchStage",
    "binaryAuthorization",
    "template",
    "traffic",
    "scaling",
    "invokerIamDisabled",
    "defaultUriDisabled",
    "customAudiences",
    "buildConfig",
];

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub project: String,
    pub region: String,
    pub auth_mode: String,
    pub bearer_token: String,
    pub storage_path: String,
    /// Docker CLI used for image-only containers; `None` disables Docker
    /// execution (containers then need a `command`).
    pub docker_bin: Option<String>,
}

#[derive(Debug)]
pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn json(status: u16, value: &Value) -> Self {
        Reply {
            status,
            body: serde_json::to_vec(value).unwrap_or_default(),
        }
    }

    /// Google API error envelope (`google.rpc.Status` JSON mapping).
    pub fn error(status: u16, code: &str, message: &str) -> Self {
        Reply::json(
            status,
            &json!({ "error": { "code": status, "message": message, "status": code } }),
        )
    }
}

fn not_found(name: &str) -> Reply {
    Reply::error(
        404,
        "NOT_FOUND",
        &format!("Resource '{name}' was not found"),
    )
}

fn invalid(message: &str) -> Reply {
    Reply::error(400, "INVALID_ARGUMENT", message)
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct State {
    #[serde(default)]
    services: BTreeMap<String, Value>,
    #[serde(default)]
    revisions: BTreeMap<String, Value>,
    #[serde(default)]
    policies: BTreeMap<String, Value>,
}

#[derive(Default)]
struct Operations {
    order: VecDeque<String>,
    by_name: BTreeMap<String, Value>,
}

pub struct Server {
    config: Config,
    state: Mutex<State>,
    operations: Mutex<Operations>,
    load_err: Option<String>,
    seq: AtomicU64,
    pub instances: Manager,
}

impl Server {
    pub fn new(config: Config) -> Self {
        let instances = Manager::new(config.docker_bin.clone());
        let mut server = Server {
            config,
            state: Mutex::new(State::default()),
            operations: Mutex::new(Operations::default()),
            load_err: None,
            seq: AtomicU64::new(0),
            instances,
        };
        if !server.config.storage_path.is_empty() {
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

    fn state_path(&self) -> PathBuf {
        PathBuf::from(&self.config.storage_path).join("state.json")
    }

    fn load(&mut self) -> Result<(), String> {
        let data = match std::fs::read(self.state_path()) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let mut state: State = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        // Published URLs embed the listen port, which may have changed since
        // they were stored: derive them from the current config again.
        for (name, service) in state.services.iter_mut() {
            if let [_, project, _, location, _, service_id] =
                name.split('/').collect::<Vec<_>>()[..]
            {
                let (uri, urls) = self.public_urls(project, location, service_id);
                service["urls"] = json!(urls);
                service["uri"] = json!(uri);
            }
        }
        for (name, revision) in state.revisions.iter_mut() {
            if let [_, project, _, location, _, service_id, ..] =
                name.split('/').collect::<Vec<_>>()[..]
            {
                revision["logUri"] = json!(self.log_uri(project, location, service_id));
            }
        }
        *self.state.get_mut().unwrap() = state;
        Ok(())
    }

    fn persist(&self, st: &State) -> Result<(), String> {
        if self.config.storage_path.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.config.storage_path).map_err(|e| e.to_string())?;
        let data = serde_json::to_vec_pretty(st).map_err(|e| e.to_string())?;
        let path = self.state_path();
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, data).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
    }

    fn commit<T>(&self, mutate: impl FnOnce(&mut State) -> Result<T, Reply>) -> Result<T, Reply> {
        self.apply(false, mutate)
    }

    /// Runs `mutate` against a copy of the state. With `validate_only` the copy
    /// is discarded (full validation, no side effects); otherwise it is
    /// persisted and swapped in.
    fn apply<T>(
        &self,
        validate_only: bool,
        mutate: impl FnOnce(&mut State) -> Result<T, Reply>,
    ) -> Result<T, Reply> {
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        let out = mutate(&mut next)?;
        if validate_only {
            return Ok(out);
        }
        self.persist(&next)
            .map_err(|_| Reply::error(500, "INTERNAL", "failed to persist cloud run state"))?;
        *guard = next;
        Ok(out)
    }

    fn new_id(&self) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let h = hex::encode(Sha256::digest(format!(
            "{nanos}:{n}:{}",
            std::process::id()
        )));
        format!(
            "{}-{}-4{}-a{}-{}",
            &h[0..8],
            &h[8..12],
            &h[13..16],
            &h[17..20],
            &h[20..32]
        )
    }

    fn port(&self) -> &str {
        self.config.addr.rsplit(':').next().unwrap_or("18095")
    }

    /// Host-routed URL (`http://<svc>.<location>.<project>.run.localhost:<port>`).
    pub fn host_url(&self, project: &str, location: &str, service_id: &str) -> String {
        format!(
            "http://{service_id}.{location}.{project}.run.localhost:{}",
            self.port()
        )
    }

    /// `(uri, urls)`. The host form only works when service, location and
    /// project are each a single DNS label (`example.com:proj` is neither a
    /// label nor valid in an authority), so otherwise the path form — always
    /// routable — becomes the `uri` and the only URL.
    pub fn public_urls(
        &self,
        project: &str,
        location: &str,
        service_id: &str,
    ) -> (String, Vec<String>) {
        let path = self.path_url(project, location, service_id);
        if [service_id, location, project]
            .iter()
            .all(|l| is_dns_label(l))
        {
            let host = self.host_url(project, location, service_id);
            (host.clone(), vec![host, path])
        } else {
            (path.clone(), vec![path])
        }
    }

    /// Path-routed fallback URL for clients that cannot resolve `*.localhost`.
    pub fn path_url(&self, project: &str, location: &str, service_id: &str) -> String {
        format!(
            // Trailing slash: relative links/redirects from the app must
            // resolve inside the service's prefix.
            "http://127.0.0.1:{}/_run/{project}/{location}/{service_id}/",
            self.port()
        )
    }

    fn log_uri(&self, project: &str, location: &str, service_id: &str) -> String {
        format!(
            "http://127.0.0.1:{}/_introspect/logs/{project}/{location}/{service_id}",
            self.port()
        )
    }

    fn record_operation(&self, parent: &str, response: Value, metadata_type: &str) -> Value {
        let name = format!("{parent}/operations/{}", self.new_id());
        let mut typed = response.clone();
        if let Some(obj) = typed.as_object_mut() {
            obj.insert("@type".to_string(), json!(metadata_type));
        }
        let op = json!({
            "name": name,
            "metadata": typed,
            "done": true,
            "response": typed,
        });
        let mut ops = self.operations.lock().unwrap();
        ops.order.push_back(name.clone());
        ops.by_name.insert(name, op.clone());
        while ops.order.len() > MAX_OPERATIONS {
            if let Some(old) = ops.order.pop_front() {
                ops.by_name.remove(&old);
            }
        }
        op
    }

    // ---- locations ---------------------------------------------------------

    pub fn list_locations(&self, project: &str) -> Reply {
        let region = if self.config.region.is_empty() {
            "us-central1"
        } else {
            &self.config.region
        };
        Reply::json(
            200,
            &json!({
                "locations": [{
                    "name": format!("projects/{project}/locations/{region}"),
                    "locationId": region,
                    "displayName": region,
                }]
            }),
        )
    }

    // ---- services ----------------------------------------------------------

    pub fn create_service(
        &self,
        project: &str,
        location: &str,
        query: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Reply {
        let input = match parse_object(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let service_id = query.get("serviceId").cloned().unwrap_or_default();
        if let Err(r) = validate_id(&service_id, "serviceId", 49) {
            return r;
        }
        let parent = format!("projects/{project}/locations/{location}");
        let name = format!("{parent}/services/{service_id}");
        if let Err(r) = validate_service(&input) {
            return r;
        }
        let validate_only = is_validate_only(query);
        let now = now_rfc3339();
        let mut service = Map::new();
        copy_writable(&input, &mut service);
        service.insert("name".into(), json!(name));
        service.insert("uid".into(), json!(self.new_id()));
        service.insert("createTime".into(), json!(now));
        service.insert("creator".into(), json!("devcloud@local"));
        service.insert("generation".into(), json!("0"));
        let mut service = Value::Object(service);
        apply_service_defaults(&mut service);

        let result = self.apply(validate_only, |st| {
            if st.services.contains_key(&name) {
                return Err(Reply::error(
                    409,
                    "ALREADY_EXISTS",
                    &format!("Resource '{service_id}' already exists."),
                ));
            }
            self.roll_out(st, &mut service, None, project, location, &service_id, &now)?;
            st.services.insert(name.clone(), service.clone());
            Ok(service.clone())
        });
        match result {
            Ok(svc) if validate_only => {
                Reply::json(200, &validation_operation(&parent, svc, SERVICE_TYPE))
            }
            Ok(svc) => {
                emit("cloudrun.service.created", json!({ "service": name }));
                Reply::json(200, &self.record_operation(&parent, svc, SERVICE_TYPE))
            }
            Err(r) => r,
        }
    }

    pub fn list_services(
        &self,
        project: &str,
        location: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let prefix = if location == "-" {
            format!("projects/{project}/locations/")
        } else {
            format!("projects/{project}/locations/{location}/services/")
        };
        let st = self.state.lock().unwrap();
        let all: Vec<&Value> = st
            .services
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v)
            .collect();
        paginate(&all, query, "services")
    }

    pub fn get_service(&self, name: &str) -> Reply {
        match self.state.lock().unwrap().services.get(name) {
            Some(s) => Reply::json(200, s),
            None => not_found(name),
        }
    }

    pub fn update_service(
        &self,
        project: &str,
        location: &str,
        service_id: &str,
        query: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Reply {
        let name = format!("projects/{project}/locations/{location}/services/{service_id}");
        let exists = self.state.lock().unwrap().services.contains_key(&name);
        if !exists {
            if query.get("allowMissing").map(String::as_str) == Some("true") {
                let mut q = query.clone();
                q.insert("serviceId".into(), service_id.to_string());
                return self.create_service(project, location, &q, body);
            }
            return not_found(&name);
        }
        let input = match parse_object(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let mask: Option<Vec<Vec<String>>> = match query.get("updateMask").filter(|m| !m.is_empty())
        {
            Some(m) => match parse_update_mask(m) {
                Ok(paths) => Some(paths),
                Err(r) => return r,
            },
            None => None,
        };
        let validate_only = is_validate_only(query);
        let now = now_rfc3339();
        let result = self.apply(validate_only, |st| {
            let current = st
                .services
                .get(&name)
                .cloned()
                .ok_or_else(|| not_found(&name))?;
            if let Some(etag) = input.get("etag").and_then(Value::as_str) {
                if !etag.is_empty() && Some(etag) != current.get("etag").and_then(Value::as_str) {
                    return Err(Reply::error(
                        409,
                        "ABORTED",
                        "etag mismatch: the service was modified concurrently",
                    ));
                }
            }
            let mut next = current.clone();
            match &mask {
                // Only the masked paths change; everything else keeps its
                // current value (e.g. `template.containers` leaves the
                // template's timeout alone).
                Some(paths) => {
                    for path in paths {
                        apply_mask_path(&mut next, &input, path);
                    }
                }
                None => {
                    for field in WRITABLE_SERVICE_FIELDS {
                        if let Some(v) = input.get(*field) {
                            next[*field] = v.clone();
                        }
                    }
                }
            }
            validate_service(&next)?;
            apply_service_defaults(&mut next);
            self.roll_out(
                st,
                &mut next,
                Some(&current),
                project,
                location,
                service_id,
                &now,
            )?;
            st.services.insert(name.clone(), next.clone());
            Ok(next)
        });
        match result {
            Ok(svc) if validate_only => Reply::json(
                200,
                &validation_operation(
                    &format!("projects/{project}/locations/{location}"),
                    svc,
                    SERVICE_TYPE,
                ),
            ),
            Ok(svc) => {
                // No restart here: the next request compares the launch spec
                // and replaces the instance only if the revision changed.
                emit("cloudrun.service.updated", json!({ "service": name }));
                let parent = format!("projects/{project}/locations/{location}");
                Reply::json(200, &self.record_operation(&parent, svc, SERVICE_TYPE))
            }
            Err(r) => r,
        }
    }

    pub async fn delete_service(
        &self,
        project: &str,
        location: &str,
        service_id: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let name = format!("projects/{project}/locations/{location}/services/{service_id}");
        let etag = query.get("etag").cloned().unwrap_or_default();
        let validate_only = is_validate_only(query);
        let result = self.apply(validate_only, |st| {
            let mut svc = st.services.remove(&name).ok_or_else(|| not_found(&name))?;
            if !etag.is_empty() && Some(etag.as_str()) != svc.get("etag").and_then(Value::as_str) {
                return Err(Reply::error(409, "ABORTED", "etag mismatch"));
            }
            let prefix = format!("{name}/revisions/");
            st.revisions.retain(|k, _| !k.starts_with(&prefix));
            st.policies.remove(&name);
            svc["deleteTime"] = json!(now_rfc3339());
            Ok(svc)
        });
        match result {
            Ok(svc) if validate_only => Reply::json(
                200,
                &validation_operation(
                    &format!("projects/{project}/locations/{location}"),
                    svc,
                    SERVICE_TYPE,
                ),
            ),
            Ok(svc) => {
                self.instances.stop(&name).await;
                self.instances.forget_logs(&name);
                emit("cloudrun.service.deleted", json!({ "service": name }));
                let parent = format!("projects/{project}/locations/{location}");
                Reply::json(200, &self.record_operation(&parent, svc, SERVICE_TYPE))
            }
            Err(r) => r,
        }
    }

    /// Creates a revision when the template changed (always on create) and
    /// refreshes every output-only field of `service`.
    #[allow(clippy::too_many_arguments)]
    fn roll_out(
        &self,
        st: &mut State,
        service: &mut Value,
        previous: Option<&Value>,
        project: &str,
        location: &str,
        service_id: &str,
        now: &str,
    ) -> Result<(), Reply> {
        let name = format!("projects/{project}/locations/{location}/services/{service_id}");
        let generation = previous
            .and_then(|p| p.get("generation"))
            .and_then(Value::as_str)
            .and_then(|g| g.parse::<u64>().ok())
            .unwrap_or(0)
            + 1;
        let template_changed = previous
            .map(|p| p.get("template") != service.get("template"))
            .unwrap_or(true);

        let latest_revision = if template_changed {
            let requested = service
                .pointer("/template/revision")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let revision_id = if requested.is_empty() {
                let suffix: String =
                    hex::encode(Sha256::digest(format!("{name}:{generation}:{now}")))
                        .bytes()
                        .take(3)
                        .map(|b| (b'a' + b % 26) as char)
                        .collect();
                format!("{service_id}-{generation:05}-{suffix}")
            } else {
                if !requested.starts_with(&format!("{service_id}-")) {
                    return Err(invalid(&format!(
                        "template.revision must be prefixed by the service name: '{service_id}-'"
                    )));
                }
                // One RFC 1035 path segment: anything else (`web-a/b`) would
                // create a revision no URL can address.
                validate_id(&requested, "template.revision", 63)?;
                requested
            };
            let revision_name = format!("{name}/revisions/{revision_id}");
            if st.revisions.contains_key(&revision_name) {
                return Err(Reply::error(
                    409,
                    "ALREADY_EXISTS",
                    &format!("Revision named '{revision_id}' with different configuration already exists."),
                ));
            }
            let revision = self.revision_json(
                service,
                &revision_name,
                &name,
                generation,
                now,
                project,
                location,
                service_id,
            );
            st.revisions.insert(revision_name.clone(), revision);
            revision_name
        } else {
            previous
                .and_then(|p| p.get("latestCreatedRevision"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };

        let traffic = service.get("traffic").cloned().unwrap_or_else(
            || json!([{ "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", "percent": 100 }]),
        );
        let traffic_statuses: Vec<Value> = traffic
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|t| {
                        let mut s = t.clone();
                        if s.get("type").is_none() {
                            s["type"] = json!("TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION");
                        }
                        s
                    })
                    .collect()
            })
            .unwrap_or_default();
        let ready = json!({
            "type": "Ready",
            "state": "CONDITION_SUCCEEDED",
            "lastTransitionTime": now,
        });
        let (uri, urls) = self.public_urls(project, location, service_id);
        let obj = service.as_object_mut().expect("service is an object");
        obj.insert("generation".into(), json!(generation.to_string()));
        obj.insert("observedGeneration".into(), json!(generation.to_string()));
        obj.insert("updateTime".into(), json!(now));
        obj.insert("lastModifier".into(), json!("devcloud@local"));
        obj.insert("traffic".into(), traffic);
        obj.insert("trafficStatuses".into(), json!(traffic_statuses));
        obj.insert("latestCreatedRevision".into(), json!(latest_revision));
        obj.insert("latestReadyRevision".into(), json!(latest_revision));
        obj.insert("terminalCondition".into(), ready);
        obj.insert(
            "conditions".into(),
            json!([
                { "type": "RoutesReady", "state": "CONDITION_SUCCEEDED", "lastTransitionTime": now },
                { "type": "ConfigurationsReady", "state": "CONDITION_SUCCEEDED", "lastTransitionTime": now },
            ]),
        );
        obj.insert("uri".into(), json!(uri));
        obj.insert("urls".into(), json!(urls));
        obj.insert("reconciling".into(), json!(false));
        obj.remove("etag");
        let etag =
            hex::encode(&Sha256::digest(Value::Object(obj.clone()).to_string().as_bytes())[..12]);
        obj.insert("etag".into(), json!(etag));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn revision_json(
        &self,
        service: &Value,
        revision_name: &str,
        service_name: &str,
        generation: u64,
        now: &str,
        project: &str,
        location: &str,
        service_id: &str,
    ) -> Value {
        let template = service
            .get("template")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let mut rev = json!({
            "name": revision_name,
            "uid": self.new_id(),
            "generation": "1",
            "createTime": now,
            "updateTime": now,
            "launchStage": service.get("launchStage").cloned().unwrap_or(json!("GA")),
            "service": service_name,
            "containers": template.get("containers").cloned().unwrap_or(json!([])),
            "scaling": template.get("scaling").cloned().unwrap_or(json!({ "maxInstanceCount": 100 })),
            "timeout": template.get("timeout").cloned().unwrap_or(json!("300s")),
            "maxInstanceRequestConcurrency": template.get("maxInstanceRequestConcurrency").cloned().unwrap_or(json!(80)),
            "observedGeneration": "1",
            "conditions": [
                { "type": "Ready", "state": "CONDITION_SUCCEEDED", "lastTransitionTime": now },
                { "type": "ContainerHealthy", "state": "CONDITION_SUCCEEDED", "lastTransitionTime": now },
            ],
            "logUri": self.log_uri(project, location, service_id),
            "reconciling": false,
            "labels": template.get("labels").cloned().unwrap_or(json!({})),
            "annotations": template.get("annotations").cloned().unwrap_or(json!({})),
        });
        for key in [
            "serviceAccount",
            "executionEnvironment",
            "volumes",
            "vpcAccess",
            "encryptionKey",
            "sessionAffinity",
            "nodeSelector",
        ] {
            if let Some(v) = template.get(key) {
                rev[key] = v.clone();
            }
        }
        rev["etag"] = json!(format!(
            "{generation}-{}",
            &hex::encode(Sha256::digest(rev.to_string()))[..12]
        ));
        rev
    }

    // ---- revisions ---------------------------------------------------------

    pub fn list_revisions(&self, service_name: &str, query: &BTreeMap<String, String>) -> Reply {
        let st = self.state.lock().unwrap();
        if !st.services.contains_key(service_name) {
            return not_found(service_name);
        }
        let prefix = format!("{service_name}/revisions/");
        let mut all: Vec<&Value> = st
            .revisions
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v)
            .collect();
        // Newest first, like the real API.
        // Compare instants, not strings: fractional seconds are trimmed, so
        // `.1Z` sorts after `.11Z` lexicographically although it is earlier.
        let created = |r: &Value| {
            r.get("createTime")
                .and_then(Value::as_str)
                .map(crate::time_fmt::sortable)
                .unwrap_or_default()
        };
        all.sort_by(|a, b| {
            created(b)
                .cmp(&created(a))
                .then_with(|| b["name"].as_str().cmp(&a["name"].as_str()))
        });
        paginate(&all, query, "revisions")
    }

    pub fn get_revision(&self, name: &str) -> Reply {
        match self.state.lock().unwrap().revisions.get(name) {
            Some(r) => Reply::json(200, r),
            None => not_found(name),
        }
    }

    pub fn delete_revision(
        &self,
        service_name: &str,
        revision_name: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let validate_only = is_validate_only(query);
        let result = self.apply(validate_only, |st| {
            let service = st
                .services
                .get(service_name)
                .ok_or_else(|| not_found(service_name))?;
            if service.get("latestCreatedRevision").and_then(Value::as_str) == Some(revision_name) {
                return Err(Reply::error(
                    400,
                    "FAILED_PRECONDITION",
                    "Revision is the latest revision of its service and cannot be deleted",
                ));
            }
            st.revisions
                .remove(revision_name)
                .ok_or_else(|| not_found(revision_name))
        });
        let parent = service_name
            .split("/services/")
            .next()
            .unwrap_or("")
            .to_string();
        match result {
            Ok(rev) if validate_only => {
                Reply::json(200, &validation_operation(&parent, rev, REVISION_TYPE))
            }
            Ok(rev) => Reply::json(200, &self.record_operation(&parent, rev, REVISION_TYPE)),
            Err(r) => r,
        }
    }

    // ---- operations ----------------------------------------------------------

    pub fn get_operation(&self, name: &str) -> Reply {
        match self.operations.lock().unwrap().by_name.get(name) {
            Some(op) => Reply::json(200, op),
            None => not_found(name),
        }
    }

    pub fn list_operations(&self, parent: &str) -> Reply {
        let prefix = format!("{parent}/operations/");
        let ops = self.operations.lock().unwrap();
        let list: Vec<&Value> = ops
            .order
            .iter()
            .rev()
            .filter(|n| n.starts_with(&prefix))
            .filter_map(|n| ops.by_name.get(n))
            .collect();
        Reply::json(200, &json!({ "operations": list }))
    }

    pub fn delete_operation(&self, name: &str) -> Reply {
        let mut ops = self.operations.lock().unwrap();
        if ops.by_name.remove(name).is_none() {
            return not_found(name);
        }
        ops.order.retain(|n| n != name);
        Reply::json(200, &json!({}))
    }

    // ---- IAM -------------------------------------------------------------------

    pub fn get_iam_policy(&self, service_name: &str) -> Reply {
        let st = self.state.lock().unwrap();
        if !st.services.contains_key(service_name) {
            return not_found(service_name);
        }
        let policy = st
            .policies
            .get(service_name)
            .cloned()
            .unwrap_or_else(|| json!({ "version": 1, "etag": "ACAB" }));
        Reply::json(200, &policy)
    }

    pub fn set_iam_policy(&self, service_name: &str, body: &[u8]) -> Reply {
        let input = match parse_object(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let Some(policy) = input.get("policy").filter(|p| p.is_object()) else {
            return invalid("policy is required");
        };
        let mut policy = policy.clone();
        let result = self.commit(|st| {
            if !st.services.contains_key(service_name) {
                return Err(not_found(service_name));
            }
            let current_etag = st
                .policies
                .get(service_name)
                .and_then(|p| p.get("etag"))
                .and_then(Value::as_str)
                .unwrap_or("ACAB")
                .to_string();
            if let Some(etag) = policy.get("etag").and_then(Value::as_str) {
                if !etag.is_empty() && etag != current_etag {
                    return Err(Reply::error(
                        409,
                        "ABORTED",
                        "There were concurrent policy changes.",
                    ));
                }
            }
            if policy.get("version").is_none() {
                policy["version"] = json!(1);
            }
            policy["etag"] = json!(hex::encode(
                &Sha256::digest(format!("{}{}", policy, self.new_id()))[..8]
            ));
            st.policies.insert(service_name.to_string(), policy.clone());
            Ok(policy)
        });
        match result {
            Ok(p) => Reply::json(200, &p),
            Err(r) => r,
        }
    }

    pub fn test_iam_permissions(&self, service_name: &str, body: &[u8]) -> Reply {
        if !self
            .state
            .lock()
            .unwrap()
            .services
            .contains_key(service_name)
        {
            return not_found(service_name);
        }
        let input = match parse_object(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        Reply::json(
            200,
            &json!({ "permissions": input.get("permissions").cloned().unwrap_or(json!([])) }),
        )
    }

    // ---- data plane support --------------------------------------------------

    fn current_launch(&self, service_name: &str) -> Option<LaunchSpec> {
        self.state
            .lock()
            .unwrap()
            .services
            .get(service_name)
            .and_then(LaunchSpec::from_service)
    }

    /// The launch spec for a routed request, or `None` when the service does
    /// not exist.
    pub fn launch_spec(
        &self,
        project: &str,
        location: &str,
        service_id: &str,
    ) -> Option<LaunchSpec> {
        self.current_launch(&format!(
            "projects/{project}/locations/{location}/services/{service_id}"
        ))
    }

    /// Whether unauthenticated callers may invoke the service: always in relaxed
    /// mode; in strict mode only with `invokerIamDisabled` or an `allUsers`
    /// `roles/run.invoker` binding.
    pub fn allows_public_invoke(&self, service_name: &str) -> bool {
        if !is_strict(&self.config.auth_mode) {
            return true;
        }
        let st = self.state.lock().unwrap();
        if st
            .services
            .get(service_name)
            .and_then(|s| s.get("invokerIamDisabled"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            return true;
        }
        st.policies
            .get(service_name)
            .and_then(|p| p.get("bindings"))
            .and_then(Value::as_array)
            .map(|bindings| {
                bindings.iter().any(|b| {
                    b.get("role").and_then(Value::as_str) == Some("roles/run.invoker")
                        && b.get("members")
                            .and_then(Value::as_array)
                            .map(|m| m.iter().any(|x| x.as_str() == Some("allUsers")))
                            .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    }

    /// Bearer-token check for strict mode (relaxed accepts everything).
    pub fn authorized(&self, authorization: &str) -> bool {
        if !is_strict(&self.config.auth_mode) {
            return true;
        }
        let token = authorization.strip_prefix("Bearer ").unwrap_or("").trim();
        !token.is_empty() && token == self.config.bearer_token
    }

    /// Every service across projects/locations (introspection only).
    pub fn all_services(&self) -> Vec<Value> {
        self.state
            .lock()
            .unwrap()
            .services
            .values()
            .cloned()
            .collect()
    }
}

/// Lowercase letters, digits and inner hyphens, at most 63 bytes.
fn is_dns_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 63
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn is_strict(mode: &str) -> bool {
    mode.eq_ignore_ascii_case("strict")
}

fn parse_object(body: &[u8]) -> Result<Value, Reply> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(v) if v.is_object() => Ok(v),
        Ok(_) => Err(invalid(
            "Invalid JSON payload received. Expected an object.",
        )),
        Err(e) => Err(invalid(&format!("Invalid JSON payload received. {e}"))),
    }
}

fn copy_writable(input: &Value, out: &mut Map<String, Value>) {
    for field in WRITABLE_SERVICE_FIELDS {
        if let Some(v) = input.get(*field) {
            out.insert((*field).to_string(), v.clone());
        }
    }
}

/// RFC 1035 label: lowercase letter first, then lowercase letters, digits or
/// hyphens, not ending with a hyphen.
fn validate_id(id: &str, field: &str, max: usize) -> Result<(), Reply> {
    let ok = !id.is_empty()
        && id.len() <= max
        && id.starts_with(|c: char| c.is_ascii_lowercase())
        && !id.ends_with('-')
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(invalid(&format!(
            "Invalid {field} '{id}': must be 1-{max} characters of lowercase letters, digits or hyphens, start with a letter and not end with a hyphen"
        )))
    }
}

fn is_validate_only(query: &BTreeMap<String, String>) -> bool {
    query.get("validateOnly").map(String::as_str) == Some("true")
}

/// The Operation returned for `validateOnly` requests: done, not recorded.
fn validation_operation(parent: &str, mut resource: Value, type_url: &str) -> Value {
    if let Some(obj) = resource.as_object_mut() {
        obj.insert("@type".to_string(), json!(type_url));
    }
    json!({
        "name": format!("{parent}/operations/validate"),
        "done": true,
        "metadata": resource,
        "response": resource,
    })
}

/// Splits an `updateMask` into field paths (camelCase; snake_case accepted),
/// rejecting paths outside the writable Service fields.
fn parse_update_mask(mask: &str) -> Result<Vec<Vec<String>>, Reply> {
    // `*` is the FieldMask wildcard: replace every writable field.
    if mask.trim() == "*" {
        return Ok(WRITABLE_SERVICE_FIELDS
            .iter()
            .map(|f| vec![(*f).to_string()])
            .collect());
    }
    mask.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|p| {
            let path: Vec<String> = p.split('.').map(snake_to_camel).collect();
            if WRITABLE_SERVICE_FIELDS.contains(&path[0].as_str()) {
                Ok(path)
            } else {
                Err(invalid(&format!(
                    "Invalid update mask path '{p}': not a writable Service field"
                )))
            }
        })
        .collect()
}

fn snake_to_camel(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    let mut upper = false;
    for c in segment.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// Copies the value at `path` from `input` into `target`, creating parent
/// objects as needed; a path absent from `input` is cleared in `target`.
fn apply_mask_path(target: &mut Value, input: &Value, path: &[String]) {
    let source = path.iter().try_fold(input, |v, key| v.get(key));
    let (last, parents) = path.split_last().expect("mask paths are non-empty");
    let mut node = target;
    for key in parents {
        if !node.get(key).is_some_and(Value::is_object) {
            if source.is_none() {
                return; // Nothing to clear below a missing parent.
            }
            node[key] = json!({});
        }
        node = node.get_mut(key).expect("parent exists");
    }
    match source {
        Some(v) => node[last] = v.clone(),
        None => {
            if let Some(obj) = node.as_object_mut() {
                obj.remove(last);
            }
        }
    }
}

/// Validates the client-settable shape of a Service before anything is
/// derived from it (template, containers, traffic targets).
fn validate_service(service: &Value) -> Result<(), Reply> {
    validate_template(service.get("template"))?;
    let Some(traffic) = service.get("traffic") else {
        return Ok(());
    };
    let Some(targets) = traffic.as_array() else {
        return Err(invalid("traffic must be a list of traffic targets"));
    };
    let mut total = 0i64;
    for (i, t) in targets.iter().enumerate() {
        let Some(obj) = t.as_object() else {
            return Err(invalid(&format!("traffic[{i}] must be an object")));
        };
        if obj.get("type").is_some_and(|v| !v.is_string()) {
            return Err(invalid(&format!("traffic[{i}].type must be a string")));
        }
        match obj.get("percent") {
            None => {}
            Some(p) => match p.as_i64() {
                Some(n) if (0..=100).contains(&n) => total += n,
                _ => {
                    return Err(invalid(&format!(
                        "traffic[{i}].percent must be an integer between 0 and 100"
                    )))
                }
            },
        }
    }
    if !targets.is_empty() && total != 100 {
        return Err(invalid(&format!(
            "traffic percentages must sum to 100, got {total}"
        )));
    }
    Ok(())
}

fn validate_template(template: Option<&Value>) -> Result<(), Reply> {
    let Some(template) = template.filter(|t| t.is_object()) else {
        return Err(invalid(
            "Violation in CreateServiceRequest.service.template: should be set.",
        ));
    };
    let containers = template.get("containers").and_then(Value::as_array);
    let Some(containers) = containers.filter(|c| !c.is_empty()) else {
        return Err(invalid(
            "template.containers: at least one container is required",
        ));
    };
    for (i, c) in containers.iter().enumerate() {
        if c.get("image")
            .and_then(Value::as_str)
            .unwrap_or("")
            .is_empty()
        {
            return Err(invalid(&format!(
                "Violation in CreateServiceRequest.service.template.containers[{i}].image: should be non-empty."
            )));
        }
        // command / args are argv: every element must be a string. Dropping
        // a non-string would shift positions and run something else than
        // what was stored.
        for key in ["command", "args"] {
            if let Some(v) = c.get(key) {
                let ok = v.as_array().is_some_and(|a| a.iter().all(Value::is_string));
                if !ok {
                    return Err(invalid(&format!(
                        "template.containers[{i}].{key} must be a list of strings"
                    )));
                }
            }
        }
        if let Some(env) = c.get("env") {
            let ok = env.as_array().is_some_and(|a| {
                a.iter().all(|e| {
                    e.get("name").is_some_and(Value::is_string)
                        && e.get("value").is_none_or(Value::is_string)
                })
            });
            if !ok {
                return Err(invalid(&format!(
                    "template.containers[{i}].env must be a list of {{name, value}} with string fields"
                )));
            }
        }
        if c.get("workingDir").is_some_and(|w| !w.is_string()) {
            return Err(invalid(&format!(
                "template.containers[{i}].workingDir must be a string"
            )));
        }
        // An image reference never starts with `-` and holds no whitespace or
        // control bytes; anything else could pose as a `docker run` option.
        let image = c.get("image").and_then(Value::as_str).unwrap_or("");
        if image.starts_with('-') || image.bytes().any(|b| b <= b' ' || b == 0x7f) {
            return Err(invalid(&format!(
                "template.containers[{i}].image {image:?} is not a valid image reference"
            )));
        }
        // Ports are stored verbatim and later narrowed to u16: an
        // out-of-range value would silently become a different port.
        if let Some(ports) = c.get("ports") {
            let Some(ports) = ports.as_array() else {
                return Err(invalid(&format!(
                    "template.containers[{i}].ports must be a list"
                )));
            };
            for (j, p) in ports.iter().enumerate() {
                let port = p.get("containerPort");
                let valid = match port {
                    None => true,
                    Some(v) => v.as_u64().is_some_and(|n| (1..=65535).contains(&n)),
                };
                if !p.is_object() || !valid {
                    return Err(invalid(&format!(
                        "template.containers[{i}].ports[{j}].containerPort must be an integer between 1 and 65535"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Fills the server-side defaults the real API returns.
fn apply_service_defaults(service: &mut Value) {
    if service.get("ingress").is_none() {
        service["ingress"] = json!("INGRESS_TRAFFIC_ALL");
    }
    if service.get("launchStage").is_none() {
        service["launchStage"] = json!("GA");
    }
    let Some(template) = service.get_mut("template").and_then(Value::as_object_mut) else {
        return;
    };
    template.entry("timeout").or_insert(json!("300s"));
    template
        .entry("maxInstanceRequestConcurrency")
        .or_insert(json!(80));
    template
        .entry("scaling")
        .or_insert(json!({ "maxInstanceCount": 100 }));
    if let Some(containers) = template.get_mut("containers").and_then(Value::as_array_mut) {
        for (i, c) in containers.iter_mut().enumerate() {
            let Some(c) = c.as_object_mut() else { continue };
            if i == 0 {
                c.entry("ports")
                    .or_insert(json!([{ "name": "http1", "containerPort": 8080 }]));
            }
            c.entry("resources").or_insert(
                json!({ "limits": { "cpu": "1000m", "memory": "512Mi" }, "cpuIdle": true }),
            );
        }
    }
}

fn paginate(all: &[&Value], query: &BTreeMap<String, String>, key: &str) -> Reply {
    let size = query
        .get("pageSize")
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PAGE_SIZE);
    let start = match query.get("pageToken").filter(|t| !t.is_empty()) {
        Some(t) => match t.parse::<usize>() {
            Ok(n) => n,
            Err(_) => return invalid("Invalid page token"),
        },
        None => 0,
    };
    let page: Vec<&Value> = all.iter().skip(start).take(size).copied().collect();
    let mut out = json!({ key: page });
    // Page tokens come from clients: never let `start + size` overflow (a
    // panic here would poison the state lock).
    let next = start.saturating_add(size);
    if next < all.len() {
        out["nextPageToken"] = json!(next.to_string());
    }
    Reply::json(200, &out)
}

fn emit(event_type: &str, payload: Value) {
    let event = json!({ "type": event_type, "service": "cloudrun", "payload": payload });
    if let Some(tx) = crate::event_sink() {
        let _ = tx.send(event.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_validation() {
        assert!(validate_id("web-1", "serviceId", 49).is_ok());
        assert!(validate_id("Web", "serviceId", 49).is_err());
        assert!(validate_id("1web", "serviceId", 49).is_err());
        assert!(validate_id("web-", "serviceId", 49).is_err());
        assert!(validate_id("", "serviceId", 49).is_err());
    }

    #[test]
    fn defaults_fill_container_port_and_resources() {
        let mut s = json!({ "template": { "containers": [{ "image": "x" }] } });
        apply_service_defaults(&mut s);
        assert_eq!(
            s["template"]["containers"][0]["ports"][0]["containerPort"],
            8080
        );
        assert_eq!(s["template"]["timeout"], "300s");
        assert_eq!(s["ingress"], "INGRESS_TRAFFIC_ALL");
    }
}
