//! Provisioned-cluster / serverless control-plane models and helpers.
//!
//! Parity: `internal/services/redshift/cluster.rs` (the AWS Query "Redshift"
//! control plane) plus the serverless namespace/workgroup metadata structs.
//! `ClusterSnapshot` / `ClusterSnapshotMetadata` are the real models that
//! replace part 3's opaque state.json passthrough; `normalize_cluster_endpoints`
//! rewrites every persisted cluster endpoint to the current config's SQL addr on
//! restore.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::server::{default_str, SharedConfig};

/// Mirrors `ClusterSnapshot`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    #[serde(rename = "clusterIdentifier", default)]
    pub cluster_identifier: String,
    #[serde(rename = "clusterStatus", default)]
    pub cluster_status: String,
    #[serde(rename = "databaseName", default)]
    pub database_name: String,
    #[serde(default)]
    pub endpoint: ClusterEndpoint,
    #[serde(rename = "nodeType", default)]
    pub node_type: String,
    #[serde(rename = "numberOfNodes", default)]
    pub number_of_nodes: i64,
    #[serde(rename = "masterUsername", default)]
    pub master_username: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<Tag>,
}

/// Mirrors `ClusterSnapshotMetadata` (the cluster-snapshot control plane).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSnapshotMetadata {
    #[serde(rename = "snapshotIdentifier", default)]
    pub snapshot_identifier: String,
    #[serde(rename = "clusterIdentifier", default)]
    pub cluster_identifier: String,
    #[serde(rename = "snapshotCreateTime", default)]
    pub snapshot_create_time: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub port: i64,
    #[serde(rename = "availabilityZone", default)]
    pub availability_zone: String,
    #[serde(rename = "clusterCreateTime", default)]
    pub cluster_create_time: String,
    #[serde(rename = "masterUsername", default)]
    pub master_username: String,
    #[serde(rename = "clusterVersion", default)]
    pub cluster_version: String,
    #[serde(rename = "engineFullVersion", default)]
    pub engine_full_version: String,
    #[serde(rename = "nodeType", default)]
    pub node_type: String,
    #[serde(rename = "numberOfNodes", default)]
    pub number_of_nodes: i64,
    #[serde(rename = "dbName", default)]
    pub db_name: String,
    #[serde(default)]
    pub encrypted: bool,
}

/// Mirrors `ClusterEndpoint`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterEndpoint {
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub port: i64,
}

/// Mirrors `Tag`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tag {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub value: String,
}

/// Mirrors `serverlessNamespace`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ServerlessNamespace {
    #[serde(rename = "namespaceName")]
    pub namespace_name: String,
    #[serde(rename = "dbName")]
    pub db_name: String,
    pub status: String,
}

/// Mirrors `serverlessWorkgroup`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ServerlessWorkgroup {
    #[serde(rename = "workgroupName")]
    pub workgroup_name: String,
    #[serde(rename = "namespaceName")]
    pub namespace_name: String,
    pub status: String,
    pub endpoint: ClusterEndpoint,
}

/// Mirrors `defaultClusterIdentifier`.
pub fn default_cluster_identifier(cluster_identifier: &str) -> String {
    default_str(cluster_identifier, "devcloud")
}

/// Mirrors `clusterSnapshotFromConfig`.
pub(crate) fn cluster_snapshot_from_config(config: &SharedConfig) -> ClusterSnapshot {
    ClusterSnapshot {
        cluster_identifier: default_cluster_identifier(&config.cluster_identifier),
        cluster_status: "available".to_string(),
        database_name: default_str(&config.database, "dev"),
        endpoint: ClusterEndpoint {
            address: host_from_addr(&config.sql_addr),
            port: port_from_addr(&config.sql_addr, 5439),
        },
        node_type: default_str(&config.node_type, "dc2.large"),
        number_of_nodes: positive_or_default(config.number_of_nodes, 1),
        master_username: default_str(&config.user, "dev"),
        tags: Vec::new(),
    }
}

/// Mirrors `clusterSnapshotMetadataFromCluster`.
pub(crate) fn cluster_snapshot_metadata_from_cluster(
    identifier: &str,
    cluster: &ClusterSnapshot,
    now_rfc3339: &str,
) -> ClusterSnapshotMetadata {
    ClusterSnapshotMetadata {
        snapshot_identifier: identifier.to_string(),
        cluster_identifier: cluster.cluster_identifier.clone(),
        snapshot_create_time: now_rfc3339.to_string(),
        status: "available".to_string(),
        port: cluster.endpoint.port,
        availability_zone: "devcloud-local".to_string(),
        cluster_create_time: now_rfc3339.to_string(),
        master_username: cluster.master_username.clone(),
        cluster_version: "1.0".to_string(),
        engine_full_version: "devcloud-redshift-1.0".to_string(),
        node_type: cluster.node_type.clone(),
        number_of_nodes: cluster.number_of_nodes,
        db_name: cluster.database_name.clone(),
        encrypted: false,
    }
}

/// Mirrors `clusterSnapshotFromSnapshotMetadata`.
pub(crate) fn cluster_snapshot_from_snapshot_metadata(
    identifier: &str,
    snapshot: &ClusterSnapshotMetadata,
    config: &SharedConfig,
) -> ClusterSnapshot {
    ClusterSnapshot {
        cluster_identifier: identifier.to_string(),
        cluster_status: "available".to_string(),
        database_name: default_str(&snapshot.db_name, &default_str(&config.database, "dev")),
        endpoint: ClusterEndpoint {
            address: host_from_addr(&config.sql_addr),
            port: positive_or_default(snapshot.port, port_from_addr(&config.sql_addr, 5439)),
        },
        node_type: default_str(
            &snapshot.node_type,
            &default_str(&config.node_type, "dc2.large"),
        ),
        number_of_nodes: positive_or_default(
            snapshot.number_of_nodes,
            positive_or_default(config.number_of_nodes, 1),
        ),
        master_username: default_str(&snapshot.master_username, &default_str(&config.user, "dev")),
        tags: Vec::new(),
    }
}

/// Mirrors `normalizeClusterEndpoints`: rewrite every endpoint to the current
/// config's SQL addr (host + port, default 5439).
pub(crate) fn normalize_cluster_endpoints(
    clusters: &mut BTreeMap<String, ClusterSnapshot>,
    config: &SharedConfig,
) {
    for cluster in clusters.values_mut() {
        cluster.endpoint = ClusterEndpoint {
            address: host_from_addr(&config.sql_addr),
            port: port_from_addr(&config.sql_addr, 5439),
        };
    }
}

/// Mirrors `positiveOrDefault`.
pub fn positive_or_default(value: i64, fallback: i64) -> i64 {
    if value > 0 {
        value
    } else {
        fallback
    }
}

/// Mirrors `hostFromAddr` (net.SplitHostPort).
pub fn host_from_addr(addr: &str) -> String {
    match split_host_port(addr) {
        Some((host, _)) if !host.is_empty() => host,
        _ => "127.0.0.1".to_string(),
    }
}

/// Mirrors `portFromAddr`.
pub fn port_from_addr(addr: &str, fallback: i64) -> i64 {
    let Some((_, port)) = split_host_port(addr) else {
        return fallback;
    };
    match port.parse::<i64>() {
        Ok(parsed) if parsed > 0 => parsed,
        _ => fallback,
    }
}

/// Minimal `net.SplitHostPort`: splits on the final `:`; returns None when there
/// is no port separator (mirroring legacy error path so callers fall back).
fn split_host_port(addr: &str) -> Option<(String, String)> {
    let idx = addr.rfind(':')?;
    let host = &addr[..idx];
    let port = &addr[idx + 1..];
    // A bracketed IPv6 host keeps its brackets stripped (devcloud only uses
    // host:port forms, so a plain rfind is sufficient).
    Some((host.to_string(), port.to_string()))
}

/// Mirrors `mergeTags`: union by key (updates win), sorted by key.
pub fn merge_tags(existing: &[Tag], updates: &[Tag]) -> Vec<Tag> {
    let mut by_key: BTreeMap<String, String> = BTreeMap::new();
    for tag in existing {
        by_key.insert(tag.key.clone(), tag.value.clone());
    }
    for tag in updates {
        by_key.insert(tag.key.clone(), tag.value.clone());
    }
    by_key
        .into_iter()
        .map(|(key, value)| Tag { key, value })
        .collect()
}

/// Mirrors `deleteTags`: drop tags whose key is in `keys`, preserving order.
pub fn delete_tags(existing: &[Tag], keys: &[String]) -> Vec<Tag> {
    let remove: std::collections::HashSet<&str> = keys.iter().map(String::as_str).collect();
    existing
        .iter()
        .filter(|tag| !remove.contains(tag.key.as_str()))
        .cloned()
        .collect()
}
